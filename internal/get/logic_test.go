package get

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eyotang/s3flow/internal/pkg"
)

func TestParseDownloadSourceKeepsDisplayTerminalSafe(t *testing.T) {
	const source = "s3://checkpoint/folder%0A%1B%3F%23%25.txt"
	parsed, err := parseDownloadSource(source)
	if err != nil {
		t.Fatalf("parse S3 source with escaped control characters: %v", err)
	}
	if parsed.key != "folder\n\x1b?#%.txt" {
		t.Fatalf("decoded key = %q", parsed.key)
	}
	if parsed.display != source {
		t.Fatalf("safe display = %q, want canonical URI %q", parsed.display, source)
	}
	if strings.ContainsAny(parsed.display, "\n\r\x1b") {
		t.Fatalf("safe display contains terminal control characters: %q", parsed.display)
	}
}

func TestResolveConfigCLIOverridesEnvironmentAndKeepsAWSSelection(t *testing.T) {
	clearAWSEnvironment(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "env-ak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-sk")
	t.Setenv("AWS_SESSION_TOKEN", "env-token")
	AWSDirectory := writeAWSSharedDirectory(t, "[profile team]\nregion = profile-Region", "[team]\naws_access_key_id = profile-ak\naws_secret_access_key = profile-sk")

	resolved, err := resolveConfig(cliOptions{Options: pkg.Options{CredentialsExplicit: true, AWSDirectory: AWSDirectory, AWSProfile: "team", Endpoint: " https://cli.example.test/ ", AccessKey: " cli-ak ", SecretKey: " cli-sk ", SessionToken: " cli-token ", Region: " cli-Region "},
		source: "s3://assets/releases/archive.tar",
		output: "download.tar",

		partSize:    pkg.DefaultPartSize,
		concurrency: pkg.DefaultConcurrency,
	})
	if err != nil {
		t.Fatalf("resolve config: %v", err)
	}

	if resolved.AccessKey != "cli-ak" || resolved.SecretKey != "cli-sk" || resolved.SessionToken != "cli-token" {
		t.Fatalf("expected CLI credentials to win, got ak=%q sk=%q token=%q", resolved.AccessKey, resolved.SecretKey, resolved.SessionToken)
	}
	if resolved.Region != "cli-Region" {
		t.Fatalf("expected CLI Region to win, got %q", resolved.Region)
	}
	if resolved.Endpoint != "https://cli.example.test" {
		t.Fatalf("expected normalized CLI Endpoint to win, got %q", resolved.Endpoint)
	}
	if resolved.bucket != "assets" || resolved.key != "releases/archive.tar" {
		t.Fatalf("unexpected S3 target: bucket=%q key=%q", resolved.bucket, resolved.key)
	}
	if resolved.destination != "download.tar" {
		t.Fatalf("unexpected destination: %q", resolved.destination)
	}
	if resolved.AWSDirectory != AWSDirectory || resolved.AWSProfile != "team" {
		t.Fatalf("unexpected AWS selection: directory=%q profile=%q", resolved.AWSDirectory, resolved.AWSProfile)
	}
}

func TestResolveConfigEnvironmentCredentialsAreTrimmed(t *testing.T) {
	clearAWSEnvironment(t)
	t.Setenv("AWS_ACCESS_KEY_ID", " env-ak ")
	t.Setenv("AWS_SECRET_ACCESS_KEY", " env-sk ")
	t.Setenv("AWS_SESSION_TOKEN", " env-token ")

	resolved, err := resolveConfig(cliOptions{
		source:      "s3://assets/releases/object.bin",
		partSize:    pkg.DefaultPartSize,
		concurrency: pkg.DefaultConcurrency,
	})
	if err != nil {
		t.Fatalf("resolve config: %v", err)
	}

	if resolved.AccessKey != "env-ak" || resolved.SecretKey != "env-sk" || resolved.SessionToken != "env-token" {
		t.Fatalf("expected environment credentials to win, got ak=%q sk=%q token=%q", resolved.AccessKey, resolved.SecretKey, resolved.SessionToken)
	}
	if resolved.destination != "object.bin" {
		t.Fatalf("expected destination derived from object key, got %q", resolved.destination)
	}
}

func TestResolveConfigRejectsPartialCLICredentialsInsteadOfMixingSources(t *testing.T) {
	clearAWSEnvironment(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "env-ak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-sk")

	_, err := resolveConfig(cliOptions{Options: pkg.Options{CredentialsExplicit: true, AccessKey: "cli-ak"},
		source: "s3://assets/object.bin",

		partSize:    pkg.DefaultPartSize,
		concurrency: pkg.DefaultConcurrency,
	})
	if err == nil || !strings.Contains(err.Error(), "命令行 --ak 和 --sk 必须同时设置") {
		t.Fatalf("expected partial CLI credentials error, got %v", err)
	}
}

func TestResolveConfigRejectsPartialEnvironmentCredentialsInsteadOfMixingSources(t *testing.T) {
	clearAWSEnvironment(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "env-ak")

	_, err := resolveConfig(cliOptions{
		source:      "s3://assets/object.bin",
		partSize:    pkg.DefaultPartSize,
		concurrency: pkg.DefaultConcurrency,
	})
	if err == nil || !strings.Contains(err.Error(), "必须同时设置") {
		t.Fatalf("expected partial environment credentials error, got %v", err)
	}
}

func TestNewS3ClientCombinesCLIConfigWithEnvironmentCredentials(t *testing.T) {
	clearAWSEnvironment(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "env-ak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-sk")
	t.Setenv("AWS_SESSION_TOKEN", "env-token")
	opts := defaultCLIOptions()
	opts.source = "s3://assets/object.bin"
	opts.Endpoint = "https://s3.example.test/"
	opts.Region = "cli-Region"

	clientOptions := requireS3ClientOptions(t, opts)
	requireS3ClientValues(t, clientOptions, "cli-Region", "https://s3.example.test", "env-ak", "env-sk", "env-token")
	if !clientOptions.UsePathStyle {
		t.Fatal("expected path-style to be enabled by default for a CLI Endpoint")
	}
}

func TestNewS3ClientLoadsDefaultAWSFiles(t *testing.T) {
	home := setTestHome(t)
	clearAWSEnvironment(t)
	AWSDirectory := filepath.Join(home, ".aws")
	writeAWSSharedDirectoryAt(t, AWSDirectory,
		"[default]\nregion = default-Region\nendpoint_url = https://default.example.test",
		"[default]\naws_access_key_id = default-ak\naws_secret_access_key = default-sk\naws_session_token = default-token",
	)
	setSDKDefaultAWSFiles(t, AWSDirectory)

	opts := defaultCLIOptions()
	opts.source = "s3://assets/object.bin"
	clientOptions := requireS3ClientOptions(t, opts)
	requireS3ClientValues(t, clientOptions, "default-Region", "https://default.example.test", "default-ak", "default-sk", "default-token")
	if !clientOptions.UsePathStyle {
		t.Fatal("expected path-style to be enabled by default for a shared-config Endpoint")
	}
}

func TestNewS3ClientLoadsNamedProfileFromCustomAWSDirectory(t *testing.T) {
	clearAWSEnvironment(t)
	AWSDirectory := writeAWSSharedDirectory(t, `
[default]
Region = default-Region
endpoint_url = https://default.example.test
[profile team]
Region = team-Region
endpoint_url = https://team.example.test
`, `
[default]
aws_access_key_id = default-ak
aws_secret_access_key = default-sk
[team]
aws_access_key_id = team-ak
aws_secret_access_key = team-sk
aws_session_token = team-token
`)
	decoyDirectory := writeAWSSharedDirectory(t, "[profile team]\nregion = decoy-Region", "[team]\naws_access_key_id = decoy-ak\naws_secret_access_key = decoy-sk")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(decoyDirectory, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(decoyDirectory, "credentials"))

	opts := defaultCLIOptions()
	opts.source = "s3://assets/object.bin"
	opts.AWSDirectory = AWSDirectory
	opts.AWSProfile = "team"
	clientOptions := requireS3ClientOptions(t, opts)
	requireS3ClientValues(t, clientOptions, "team-Region", "https://team.example.test", "team-ak", "team-sk", "team-token")
}

func TestNewS3ClientHonorsAWSProfileWithoutProfileFlag(t *testing.T) {
	home := setTestHome(t)
	clearAWSEnvironment(t)
	writeAWSSharedDirectoryAt(t, filepath.Join(home, ".aws"), `
[default]
Region = default-Region
[profile environment]
Region = environment-Region
`, `
[default]
aws_access_key_id = default-ak
aws_secret_access_key = default-sk
[environment]
aws_access_key_id = environment-ak
aws_secret_access_key = environment-sk
`)
	setSDKDefaultAWSFiles(t, filepath.Join(home, ".aws"))
	t.Setenv("AWS_PROFILE", "environment")

	opts := defaultCLIOptions()
	opts.source = "s3://assets/object.bin"
	clientOptions := requireS3ClientOptions(t, opts)
	requireS3ClientValues(t, clientOptions, "environment-Region", "", "environment-ak", "environment-sk", "")
}

func TestNewS3ClientProfileFlagOverridesAWSProfile(t *testing.T) {
	clearAWSEnvironment(t)
	AWSDirectory := writeAWSSharedDirectory(t, `
[profile environment]
Region = environment-Region
[profile command]
Region = command-Region
`, `
[environment]
aws_access_key_id = environment-ak
aws_secret_access_key = environment-sk
[command]
aws_access_key_id = command-ak
aws_secret_access_key = command-sk
`)
	t.Setenv("AWS_PROFILE", "environment")

	opts := defaultCLIOptions()
	opts.source = "s3://assets/object.bin"
	opts.AWSDirectory = AWSDirectory
	opts.AWSProfile = "command"
	clientOptions := requireS3ClientOptions(t, opts)
	requireS3ClientValues(t, clientOptions, "command-Region", "", "command-ak", "command-sk", "")
}

func TestNewS3ClientCLIOverridesEnvironmentAndSharedFiles(t *testing.T) {
	clearAWSEnvironment(t)
	AWSDirectory := writeAWSSharedDirectory(t,
		"[default]\nregion = file-Region\nendpoint_url = https://file.example.test",
		"[default]\naws_access_key_id = file-ak\naws_secret_access_key = file-sk\naws_session_token = file-token",
	)
	t.Setenv("AWS_ACCESS_KEY_ID", "env-ak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-sk")
	t.Setenv("AWS_SESSION_TOKEN", "env-token")
	t.Setenv("AWS_REGION", "env-Region")
	t.Setenv("AWS_ENDPOINT_URL", "https://env.example.test")

	opts := defaultCLIOptions()
	opts.source = "s3://assets/object.bin"
	opts.AWSDirectory = AWSDirectory
	opts.Endpoint = "https://cli.example.test/"
	opts.Region = "cli-Region"
	opts.AccessKey = "cli-ak"
	opts.SecretKey = "cli-sk"
	opts.SessionToken = "cli-token"
	opts.CredentialsExplicit = true
	clientOptions := requireS3ClientOptions(t, opts)
	requireS3ClientValues(t, clientOptions, "cli-Region", "https://cli.example.test", "cli-ak", "cli-sk", "cli-token")
}

func TestNewS3ClientEnvironmentOverridesSharedFiles(t *testing.T) {
	clearAWSEnvironment(t)
	AWSDirectory := writeAWSSharedDirectory(t,
		"[default]\nregion = file-Region\nendpoint_url = https://file.example.test",
		"[default]\naws_access_key_id = file-ak\naws_secret_access_key = file-sk\naws_session_token = file-token",
	)
	t.Setenv("AWS_ACCESS_KEY_ID", "env-ak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-sk")
	t.Setenv("AWS_SESSION_TOKEN", "env-token")
	t.Setenv("AWS_REGION", "env-Region")
	t.Setenv("AWS_ENDPOINT_URL", "https://env.example.test")

	opts := defaultCLIOptions()
	opts.source = "s3://assets/object.bin"
	opts.AWSDirectory = AWSDirectory
	clientOptions := requireS3ClientOptions(t, opts)
	requireS3ClientValues(t, clientOptions, "env-Region", "https://env.example.test", "env-ak", "env-sk", "env-token")
	if !clientOptions.UsePathStyle {
		t.Fatal("expected path-style to be enabled by default for an environment Endpoint")
	}
}

func TestNewS3ClientExplicitProfileOverridesEnvironmentCredentials(t *testing.T) {
	clearAWSEnvironment(t)
	AWSDirectory := writeAWSSharedDirectory(t,
		"[profile team]\nregion = team-Region",
		"[team]\naws_access_key_id = team-ak\naws_secret_access_key = team-sk\naws_session_token = team-token",
	)
	t.Setenv("AWS_ACCESS_KEY_ID", "env-ak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-sk")
	t.Setenv("AWS_SESSION_TOKEN", "env-token")

	opts := defaultCLIOptions()
	opts.source = "s3://assets/object.bin"
	opts.AWSDirectory = AWSDirectory
	opts.AWSProfile = "team"
	clientOptions := requireS3ClientOptions(t, opts)
	requireS3ClientValues(t, clientOptions, "team-Region", "", "team-ak", "team-sk", "team-token")
}

func TestNewS3ClientDefaultsRegionToCNSh(t *testing.T) {
	clearAWSEnvironment(t)
	AWSDirectory := writeAWSSharedDirectory(t, "", "[default]\naws_access_key_id = file-ak\naws_secret_access_key = file-sk")

	opts := defaultCLIOptions()
	opts.source = "s3://assets/object.bin"
	opts.AWSDirectory = AWSDirectory
	clientOptions := requireS3ClientOptions(t, opts)
	requireS3ClientValues(t, clientOptions, pkg.DefaultRegion, "", "file-ak", "file-sk", "")
	if !clientOptions.UsePathStyle {
		t.Fatal("expected path-style to be enabled by default")
	}
}

func TestNewS3ClientPathStyleFalseUsesVirtualHostedStyle(t *testing.T) {
	clearAWSEnvironment(t)
	opts := defaultCLIOptions()
	opts.source = "s3://assets/object.bin"
	opts.Endpoint = "https://s3.example.test"
	opts.AccessKey = "cli-ak"
	opts.SecretKey = "cli-sk"
	opts.CredentialsExplicit = true
	opts.PathStyle = false

	clientOptions := requireS3ClientOptions(t, opts)
	if clientOptions.UsePathStyle {
		t.Fatal("expected --path-style=false to select virtual-hosted-style")
	}
}

func TestNewS3ClientCustomEndpointRequestsBucketInPath(t *testing.T) {
	clearAWSEnvironment(t)
	const escapedSource = "s3://checkpoint/%E7%BA%BF%E4%B8%8A%E6%9C%8D_rm42_p4.internal.example/2026-09-13_040000/master.ckp.147.gz"
	const decodedKey = "线上服_rm42_p4.internal.example/2026-09-13_040000/master.ckp.147.gz"
	const escapedRequestPath = "/checkpoint/%E7%BA%BF%E4%B8%8A%E6%9C%8D_rm42_p4.internal.example/2026-09-13_040000/master.ckp.147.gz"
	var requestHosts []string
	var requestPaths []string
	transport := presignedRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		requestHosts = append(requestHosts, request.URL.Host)
		requestPaths = append(requestPaths, request.URL.EscapedPath())
		body := ""
		if request.Method == http.MethodGet {
			body = "x"
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header: http.Header{
				"Content-Length": []string{"1"},
				"ETag":           []string{`"fixture-etag"`},
			},
			Body:    io.NopCloser(strings.NewReader(body)),
			Request: request,
		}, nil
	})

	opts := defaultCLIOptions()
	opts.source = escapedSource
	opts.Endpoint = "https://objects.example.test"
	opts.AccessKey = "cli-ak"
	opts.SecretKey = "cli-sk"
	opts.CredentialsExplicit = true
	resolved, err := resolveConfig(opts)
	if err != nil {
		t.Fatalf("resolve config: %v", err)
	}
	if resolved.key != decodedKey {
		t.Fatalf("resolved key = %q, want %q", resolved.key, decodedKey)
	}
	if resolved.sourceDisplay != escapedSource {
		t.Fatalf("source display = %q, want canonical URI %q", resolved.sourceDisplay, escapedSource)
	}
	if resolved.destination != "master.ckp.147.gz" {
		t.Fatalf("destination = %q, want decoded object filename", resolved.destination)
	}
	client, err := pkg.NewS3Client(context.Background(), resolved.Config)
	if err != nil {
		t.Fatalf("create S3 client: %v", err)
	}
	if _, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(resolved.bucket),
		Key:    aws.String(resolved.key),
	}, func(options *s3.Options) {
		options.HTTPClient = &http.Client{Transport: transport}
		options.RetryMaxAttempts = 1
	}); err != nil {
		t.Fatalf("HEAD object through custom Endpoint: %v", err)
	}
	object, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(resolved.bucket),
		Key:    aws.String(resolved.key),
		Range:  aws.String("bytes=0-0"),
	}, func(options *s3.Options) {
		options.HTTPClient = &http.Client{Transport: transport}
		options.RetryMaxAttempts = 1
	})
	if err != nil {
		t.Fatalf("GET object through custom Endpoint: %v", err)
	}
	if err := object.Body.Close(); err != nil {
		t.Fatalf("close GET object body: %v", err)
	}

	if len(requestHosts) != 2 || len(requestPaths) != 2 {
		t.Fatalf("captured requests = hosts %v paths %v, want HEAD and GET", requestHosts, requestPaths)
	}
	for index := range requestPaths {
		if requestHosts[index] != "objects.example.test" {
			t.Fatalf("request %d host = %q, want Endpoint host", index, requestHosts[index])
		}
		if requestPaths[index] != escapedRequestPath {
			t.Fatalf("request %d path = %q, want %q", index, requestPaths[index], escapedRequestPath)
		}
	}
}

func TestResolveAWSDirectoryValidationAndTildeExpansion(t *testing.T) {
	home := setTestHome(t)
	clearAWSEnvironment(t)
	validDirectory := filepath.Join(home, "custom-aws")
	writeAWSSharedDirectoryAt(t, validDirectory, "[default]\nregion = test-Region", "")

	resolved, err := pkg.ResolveAWSDirectory("~/custom-aws")
	if err != nil {
		t.Fatalf("resolve tilde AWS directory: %v", err)
	}
	if resolved != validDirectory {
		t.Fatalf("resolved directory = %q, want %q", resolved, validDirectory)
	}

	regularFile := filepath.Join(home, "aws-file")
	writeTestFile(t, regularFile, "not a directory")
	emptyDirectory := filepath.Join(home, "empty")
	if err := os.MkdirAll(emptyDirectory, 0o700); err != nil {
		t.Fatalf("create empty directory: %v", err)
	}
	badConfigDirectory := filepath.Join(home, "bad-config")
	if err := os.MkdirAll(filepath.Join(badConfigDirectory, "config"), 0o700); err != nil {
		t.Fatalf("create config directory fixture: %v", err)
	}

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "missing", path: filepath.Join(home, "missing"), want: "读取 AWS 配置目录"},
		{name: "regular file", path: regularFile, want: "不是目录"},
		{name: "empty directory", path: emptyDirectory, want: "至少需要 config 或 credentials"},
		{name: "config is directory", path: badConfigDirectory, want: "不是普通文件"},
		{name: "unsupported tilde user", path: "~another/aws", want: "仅支持 ~ 或 ~/path"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := pkg.ResolveAWSDirectory(test.path)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("pkg.ResolveAWSDirectory(%q) error = %v, want substring %q", test.path, err, test.want)
			}
		})
	}
}

func TestResolveConfigRejectsMissingExplicitAWSDirectory(t *testing.T) {
	clearAWSEnvironment(t)
	missing := filepath.Join(t.TempDir(), "missing")
	opts := defaultCLIOptions()
	opts.source = "s3://assets/object.bin"
	opts.AWSDirectory = missing

	_, err := resolveConfig(opts)
	if err == nil || !errors.Is(err, os.ErrNotExist) || !strings.Contains(err.Error(), missing) {
		t.Fatalf("expected missing AWS directory error for %q, got %v", missing, err)
	}
}

func TestDownloadObjectRequestsRangesConcurrentlyAndWritesAtOffsets(t *testing.T) {
	object := []byte("0123456789")
	client := newFakeRangeS3(object, map[string]string{
		"bytes=0-3": "0123",
		"bytes=4-7": "4567",
		"bytes=8-9": "89",
	})
	destination := filepath.Join(t.TempDir(), "object.bin")
	request := downloadRequest{
		bucket:      "assets",
		key:         "objects/object.bin",
		destination: destination,
		partSize:    4,
		concurrency: 3,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var progressMu sync.Mutex
	var lastProgress downloadProgress
	err := downloadObject(ctx, client, request, func(progress downloadProgress) {
		progressMu.Lock()
		lastProgress = progress
		progressMu.Unlock()
	})
	if err != nil {
		t.Fatalf("download object: %v", err)
	}

	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if !reflect.DeepEqual(got, object) {
		t.Fatalf("downloaded content mismatch: got %q want %q", got, object)
	}

	client.mu.Lock()
	ranges := append([]string(nil), client.ranges...)
	ifMatches := append([]string(nil), client.ifMatches...)
	targets := append([]string(nil), client.targets...)
	maxActive := client.maxActive
	headCalls := client.headCalls
	headTarget := client.headTarget
	client.mu.Unlock()
	sort.Strings(ranges)
	wantRanges := []string{"bytes=0-3", "bytes=4-7", "bytes=8-9"}
	if !reflect.DeepEqual(ranges, wantRanges) {
		t.Fatalf("unexpected Range requests: got %v want %v", ranges, wantRanges)
	}
	if maxActive < 2 {
		t.Fatalf("expected concurrent range requests, max active calls was %d", maxActive)
	}
	if headCalls != 1 || headTarget != "assets/objects/object.bin" {
		t.Fatalf("unexpected HeadObject calls: count=%d target=%q", headCalls, headTarget)
	}
	for index, target := range targets {
		if target != "assets/objects/object.bin" {
			t.Fatalf("request %d used unexpected target %q", index, target)
		}
	}
	for index, ifMatch := range ifMatches {
		if ifMatch != "fixture-etag" {
			t.Fatalf("request %d did not pin the HEAD ETag: %q", index, ifMatch)
		}
	}

	progressMu.Lock()
	finalProgress := lastProgress
	progressMu.Unlock()
	if finalProgress.downloaded != int64(len(object)) || finalProgress.total != int64(len(object)) {
		t.Fatalf("unexpected final byte progress: %#v", finalProgress)
	}
	if finalProgress.completedParts != 3 || finalProgress.totalParts != 3 {
		t.Fatalf("unexpected final part progress: %#v", finalProgress)
	}
	temporaryFiles, err := filepath.Glob(filepath.Join(filepath.Dir(destination), ".object.bin-*.part"))
	if err != nil {
		t.Fatalf("glob temporary files: %v", err)
	}
	if len(temporaryFiles) != 0 {
		t.Fatalf("expected temporary download to be committed and cleaned up, got %v", temporaryFiles)
	}
}

func TestDownloadObjectFallsBackToSingleRangeWithoutStrongETag(t *testing.T) {
	object := []byte("0123456789")
	client := newFakeRangeS3(object, map[string]string{"bytes=0-9": string(object)})
	client.headETag = nil
	client.getETag = nil
	destination := filepath.Join(t.TempDir(), "object.bin")
	var last downloadProgress

	err := downloadObject(context.Background(), client, downloadRequest{
		bucket:      "assets",
		key:         "object.bin",
		destination: destination,
		partSize:    4,
		concurrency: 3,
	}, func(progress downloadProgress) {
		last = progress
	})
	if err != nil {
		t.Fatalf("download without ETag: %v", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if !reflect.DeepEqual(got, object) {
		t.Fatalf("downloaded content mismatch: got %q want %q", got, object)
	}
	client.mu.Lock()
	ranges := append([]string(nil), client.ranges...)
	maxActive := client.maxActive
	client.mu.Unlock()
	if !reflect.DeepEqual(ranges, []string{"bytes=0-9"}) || maxActive != 1 {
		t.Fatalf("expected safe single-range fallback, ranges=%v maxActive=%d", ranges, maxActive)
	}
	if last.totalParts != 1 {
		t.Fatalf("expected one reported part after fallback, got %#v", last)
	}
}

func TestDownloadObjectRejectsChangedPartETag(t *testing.T) {
	object := []byte("0123")
	client := newFakeRangeS3(object, map[string]string{"bytes=0-3": string(object)})
	client.getETag = aws.String("changed-etag")
	destination := filepath.Join(t.TempDir(), "object.bin")

	err := downloadObject(context.Background(), client, downloadRequest{
		bucket:      "assets",
		key:         "object.bin",
		destination: destination,
		partSize:    4,
		concurrency: 1,
	}, func(downloadProgress) {})
	if err == nil || !strings.Contains(err.Error(), "ETag 不匹配") {
		t.Fatalf("expected changed ETag to fail, got %v", err)
	}
	if _, statError := os.Stat(destination); !os.IsNotExist(statError) {
		t.Fatalf("ETag mismatch committed destination, stat error: %v", statError)
	}
}

func TestDownloadObjectCancellationDoesNotCommit(t *testing.T) {
	object := []byte("0123")
	client := newFakeRangeS3(object, map[string]string{"bytes=0-3": "0123"})
	destination := filepath.Join(t.TempDir(), "cancelled.bin")
	request := downloadRequest{
		bucket:      "assets",
		key:         "cancelled.bin",
		destination: destination,
		partSize:    4,
		concurrency: 1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	err := downloadObject(ctx, client, request, func(progress downloadProgress) {
		if progress.completedParts == 1 {
			cancel()
		}
	})
	if err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if _, statError := os.Stat(destination); !os.IsNotExist(statError) {
		t.Fatalf("cancelled download committed destination, stat error: %v", statError)
	}
}

func TestValidateContentRangeStrictly(t *testing.T) {
	part := pkg.Part{Start: 4, End: 7}
	valid := "bytes 4-7/10"
	if err := validateContentRange(&valid, part, 10); err != nil {
		t.Fatalf("valid Content-Range rejected: %v", err)
	}
	for _, invalid := range []*string{
		nil,
		aws.String("bytes 4-7/11"),
		aws.String("bytes 0-3/10"),
		aws.String("bytes 4-7/10 trailing"),
	} {
		if err := validateContentRange(invalid, part, 10); err == nil {
			t.Fatalf("invalid Content-Range accepted: %v", invalid)
		}
	}
}

func writeAWSSharedDirectory(t *testing.T, config, credentials string) string {
	t.Helper()
	directory := t.TempDir()
	writeAWSSharedDirectoryAt(t, directory, config, credentials)
	return directory
}

func writeAWSSharedDirectoryAt(t *testing.T, directory, config, credentials string) {
	t.Helper()
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatalf("create AWS shared config directory %s: %v", directory, err)
	}
	if strings.TrimSpace(config) != "" {
		writeTestFile(t, filepath.Join(directory, "config"), config)
	}
	if strings.TrimSpace(credentials) != "" {
		writeTestFile(t, filepath.Join(directory, "credentials"), credentials)
	}
}

func writeTestFile(t *testing.T, filename, contents string) {
	t.Helper()
	if err := os.WriteFile(filename, []byte(strings.TrimSpace(contents)+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture %s: %v", filename, err)
	}
}

func clearAWSEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"AWS_ACCESS_KEY_ID",
		"AWS_ACCESS_KEY",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_SECRET_KEY",
		"AWS_SESSION_TOKEN",
		"AWS_PROFILE",
		"AWS_DEFAULT_PROFILE",
		"AWS_CONFIG_FILE",
		"AWS_SHARED_CREDENTIALS_FILE",
		"AWS_REGION",
		"AWS_DEFAULT_REGION",
		"AWS_ENDPOINT_URL",
		"AWS_ENDPOINT_URL_S3",
		"AWS_IGNORE_CONFIGURED_ENDPOINT_URLS",
		"AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_ROLE_ARN",
		"AWS_ROLE_SESSION_NAME",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

func setTestHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	return home
}

func setSDKDefaultAWSFiles(t *testing.T, directory string) {
	t.Helper()
	originalConfigFiles := append([]string(nil), awsconfig.DefaultSharedConfigFiles...)
	originalCredentialsFiles := append([]string(nil), awsconfig.DefaultSharedCredentialsFiles...)
	awsconfig.DefaultSharedConfigFiles = []string{filepath.Join(directory, "config")}
	awsconfig.DefaultSharedCredentialsFiles = []string{filepath.Join(directory, "credentials")}
	t.Cleanup(func() {
		awsconfig.DefaultSharedConfigFiles = originalConfigFiles
		awsconfig.DefaultSharedCredentialsFiles = originalCredentialsFiles
	})
}

func requireS3ClientOptions(t *testing.T, opts cliOptions) s3.Options {
	t.Helper()
	resolved, err := resolveConfig(opts)
	if err != nil {
		t.Fatalf("resolve config: %v", err)
	}
	client, err := pkg.NewS3Client(context.Background(), resolved.Config)
	if err != nil {
		t.Fatalf("create S3 client: %v", err)
	}
	return client.Options()
}

func requireS3ClientValues(
	t *testing.T,
	options s3.Options,
	wantRegion, wantEndpoint, wantAccessKey, wantSecretKey, wantSessionToken string,
) {
	t.Helper()
	if options.Region != wantRegion {
		t.Fatalf("client Region = %q, want %q", options.Region, wantRegion)
	}
	if Endpoint := aws.ToString(options.BaseEndpoint); Endpoint != wantEndpoint {
		t.Fatalf("client Endpoint = %q, want %q", Endpoint, wantEndpoint)
	}
	credentials, err := options.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("retrieve client credentials: %v", err)
	}
	if credentials.AccessKeyID != wantAccessKey || credentials.SecretAccessKey != wantSecretKey || credentials.SessionToken != wantSessionToken {
		t.Fatalf(
			"client credentials = (%q, %q, %q), want (%q, %q, %q)",
			credentials.AccessKeyID,
			credentials.SecretAccessKey,
			credentials.SessionToken,
			wantAccessKey,
			wantSecretKey,
			wantSessionToken,
		)
	}
}

type fakeRangeS3 struct {
	object   []byte
	payloads map[string]string
	release  chan struct{}
	once     sync.Once
	headETag *string
	getETag  *string

	mu         sync.Mutex
	headCalls  int
	headTarget string
	ranges     []string
	ifMatches  []string
	targets    []string
	active     int
	maxActive  int
}

func newFakeRangeS3(object []byte, payloads map[string]string) *fakeRangeS3 {
	return &fakeRangeS3{
		object:   append([]byte(nil), object...),
		payloads: payloads,
		release:  make(chan struct{}),
		headETag: aws.String("fixture-etag"),
		getETag:  aws.String("fixture-etag"),
	}
}

func (f *fakeRangeS3) HeadObject(
	_ context.Context,
	input *s3.HeadObjectInput,
	_ ...func(*s3.Options),
) (*s3.HeadObjectOutput, error) {
	f.mu.Lock()
	f.headCalls++
	f.headTarget = aws.ToString(input.Bucket) + "/" + aws.ToString(input.Key)
	f.mu.Unlock()
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(f.object))),
		ETag:          f.headETag,
	}, nil
}

func (f *fakeRangeS3) GetObject(
	ctx context.Context,
	input *s3.GetObjectInput,
	_ ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	rangeHeader := aws.ToString(input.Range)
	payload, ok := f.payloads[rangeHeader]
	if !ok {
		return nil, fmt.Errorf("unexpected Range request %q", rangeHeader)
	}

	f.mu.Lock()
	f.ranges = append(f.ranges, rangeHeader)
	f.ifMatches = append(f.ifMatches, aws.ToString(input.IfMatch))
	f.targets = append(f.targets, aws.ToString(input.Bucket)+"/"+aws.ToString(input.Key))
	f.active++
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	if len(f.ranges) == len(f.payloads) {
		f.once.Do(func() { close(f.release) })
	}
	f.mu.Unlock()

	select {
	case <-f.release:
	case <-ctx.Done():
		f.finishCall()
		return nil, ctx.Err()
	}

	// Delay lower ranges so the parts complete in reverse order. The resulting
	// file must still be correct because each part is written at its own offset.
	delay := map[string]time.Duration{
		"bytes=0-3": 20 * time.Millisecond,
		"bytes=4-7": 10 * time.Millisecond,
	}[rangeHeader]
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			f.finishCall()
			return nil, ctx.Err()
		}
	}
	f.finishCall()

	contentRange := fmt.Sprintf("bytes %s/%d", strings.TrimPrefix(rangeHeader, "bytes="), len(f.object))
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(strings.NewReader(payload)),
		ContentLength: aws.Int64(int64(len(payload))),
		ContentRange:  aws.String(contentRange),
		ETag:          f.getETag,
	}, nil
}

func (f *fakeRangeS3) finishCall() {
	f.mu.Lock()
	f.active--
	f.mu.Unlock()
}
