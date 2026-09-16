package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	defaultRegion      = "cn-sh"
	defaultPartSize    = int64(16 << 20)
	defaultConcurrency = 16
	maxConcurrency     = 128
	maxDownloadParts   = int64(10_000)
	maxPartAttempts    = 3
)

type cliOptions struct {
	source               string
	output               string
	credentialsExplicit  bool
	awsDirectory         string
	awsDirectoryExplicit bool
	awsProfile           string
	awsProfileExplicit   bool
	endpoint             string
	accessKey            string
	secretKey            string
	sessionToken         string
	region               string
	partSize             int64
	concurrency          int
	pathStyle            bool
	force                bool
}

type resolvedConfig struct {
	bucket        string
	key           string
	presignedURL  string
	sourceDisplay string
	destination   string
	endpoint      string
	accessKey     string
	secretKey     string
	sessionToken  string
	region        string
	awsDirectory  string
	awsProfile    string
	partSize      int64
	concurrency   int
	pathStyle     bool
	force         bool
}

type staticCredentials struct {
	AccessKey    string
	SecretKey    string
	SessionToken string
}

type downloadSource struct {
	bucket       string
	key          string
	presignedURL string
	display      string
	filename     string
}

type s3API interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type downloadRequest struct {
	bucket        string
	key           string
	sourceDisplay string
	destination   string
	partSize      int64
	concurrency   int
	force         bool
}

type byteRange struct {
	index int
	start int64
	end   int64
}

func (p byteRange) size() int64 { return p.end - p.start + 1 }

type downloadProgress struct {
	stage          string
	downloaded     int64
	total          int64
	completedParts int
	totalParts     int
	done           bool
	err            error
}

type progressFunc func(downloadProgress)

type presignedURLClient struct {
	rawURL  string
	display string
	client  *http.Client
}

// parseCLI keeps argument handling independent from main so its precedence and
// validation rules can be tested without starting the terminal UI.
func parseCLI(args []string) (cliOptions, error) {
	opts := defaultCLIOptions()
	fs, partSize := newCLIFlagSet(&opts, io.Discard)
	if err := fs.Parse(args); err != nil {
		return cliOptions{}, err
	}
	fs.Visit(func(parsed *flag.Flag) {
		switch parsed.Name {
		case "ak", "sk", "session-token":
			opts.credentialsExplicit = true
		case "aws":
			opts.awsDirectoryExplicit = true
		case "p", "profile":
			opts.awsProfileExplicit = true
		}
	})
	opts.partSize = partSize.value
	positionals := fs.Args()
	if len(positionals) < 1 || len(positionals) > 2 {
		return cliOptions{}, errors.New("用法: s3get [参数] <s3://bucket/key|预签名URL> [输出文件]")
	}
	opts.source = positionals[0]
	if len(positionals) == 2 {
		if opts.output != "" {
			return cliOptions{}, errors.New("输出文件不能同时使用位置参数和 --output")
		}
		opts.output = positionals[1]
	}
	if opts.concurrency < 1 || opts.concurrency > maxConcurrency {
		return cliOptions{}, fmt.Errorf("--concurrency 必须在 1 到 %d 之间", maxConcurrency)
	}
	if opts.partSize <= 0 {
		return cliOptions{}, errors.New("--part-size 必须大于 0")
	}
	return opts, nil
}

func defaultCLIOptions() cliOptions {
	return cliOptions{
		partSize:    defaultPartSize,
		concurrency: defaultConcurrency,
		pathStyle:   true,
	}
}

// newCLIFlagSet is the single source of truth for flags and their help text.
// ContinueOnError keeps parsing testable while still supporting the standard
// flag package's -h/--help behavior.
func newCLIFlagSet(opts *cliOptions, output io.Writer) (*flag.FlagSet, *byteSizeFlag) {
	fs := flag.NewFlagSet("s3get", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&opts.endpoint, "endpoint", "", "S3 endpoint `URL`，例如 https://s3.example.com")
	fs.StringVar(&opts.accessKey, "ak", "", "access key `VALUE`，也可使用 AWS_ACCESS_KEY_ID")
	fs.StringVar(&opts.secretKey, "sk", "", "secret key `VALUE`，也可使用 AWS_SECRET_ACCESS_KEY")
	fs.StringVar(&opts.sessionToken, "session-token", "", "临时凭据 session token `VALUE`，也可使用 AWS_SESSION_TOKEN")
	fs.StringVar(&opts.region, "region", "", "S3 region `VALUE`（未配置时使用 cn-sh）")
	fs.StringVar(&opts.awsDirectory, "aws", "", "AWS 配置目录 `DIR`，目录内包含 config 与 credentials（默认 ~/.aws）")
	fs.StringVar(&opts.awsProfile, "p", "", "AWS 命名 profile `NAME`（等同 --profile）")
	fs.StringVar(&opts.awsProfile, "profile", "", "AWS 命名 profile `NAME`（语义与 aws --profile 相同，默认 AWS_PROFILE/default）")
	fs.StringVar(&opts.output, "o", "", "输出文件 `FILE`")
	fs.StringVar(&opts.output, "output", "", "输出文件 `FILE`（等同 -o）")
	fs.IntVar(&opts.concurrency, "concurrency", defaultConcurrency, "并发分片数 `N`，范围 1-128")
	fs.BoolVar(&opts.pathStyle, "path-style", true, "使用 path-style S3 请求；设为 false 时使用 bucket.endpoint")
	fs.BoolVar(&opts.force, "force", false, "覆盖已有输出文件")
	partSize := &byteSizeFlag{value: defaultPartSize}
	fs.Var(partSize, "part-size", "分片大小 `SIZE`，例如 16MiB、1GiB")
	fs.Usage = func() {
		fmt.Fprintln(output, "用法:")
		fmt.Fprintln(output, "  s3get [参数] s3://bucket/key [输出文件]")
		fmt.Fprintln(output, "  s3get [参数] 'https://endpoint/bucket/key?预签名参数' [输出文件]")
		fmt.Fprintln(output)
		fmt.Fprintln(output, "参数（-flag 与 --flag 均可）:")
		fs.PrintDefaults()
	}
	return fs, partSize
}

func printCLIUsage(output io.Writer) {
	opts := defaultCLIOptions()
	fs, _ := newCLIFlagSet(&opts, output)
	fs.Usage()
}

type byteSizeFlag struct{ value int64 }

func (v *byteSizeFlag) String() string { return formatBytes(v.value) }

func (v *byteSizeFlag) Set(raw string) error {
	n, err := parseByteSize(raw)
	if err != nil {
		return err
	}
	v.value = n
	return nil
}

func parseByteSize(raw string) (int64, error) {
	value := strings.ToUpper(strings.TrimSpace(raw))
	units := []struct {
		suffix     string
		multiplier float64
	}{
		{"GIB", 1 << 30}, {"GB", 1e9},
		{"MIB", 1 << 20}, {"MB", 1e6},
		{"KIB", 1 << 10}, {"KB", 1e3},
		{"B", 1},
	}
	multiplier := float64(1)
	for _, unit := range units {
		if strings.HasSuffix(value, unit.suffix) {
			value = strings.TrimSpace(strings.TrimSuffix(value, unit.suffix))
			multiplier = unit.multiplier
			break
		}
	}
	n, err := strconv.ParseFloat(value, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("无效的字节大小 %q", raw)
	}
	result := int64(n * multiplier)
	if result <= 0 {
		return 0, fmt.Errorf("无效的字节大小 %q", raw)
	}
	return result, nil
}

// resolveConfig keeps direct CLI credentials highest. An explicit -p/--profile
// follows AWS CLI semantics and selects that profile's credentials; otherwise
// environment credentials precede the AWS shared files. Region and endpoint
// precedence is delegated to the AWS SDK after applying explicit CLI values.
func resolveConfig(opts cliOptions) (resolvedConfig, error) {
	resolved := resolvedConfig{
		endpoint:     strings.TrimSpace(opts.endpoint),
		accessKey:    strings.TrimSpace(opts.accessKey),
		secretKey:    strings.TrimSpace(opts.secretKey),
		sessionToken: strings.TrimSpace(opts.sessionToken),
		region:       strings.TrimSpace(opts.region),
		awsDirectory: strings.TrimSpace(opts.awsDirectory),
		awsProfile:   strings.TrimSpace(opts.awsProfile),
		partSize:     opts.partSize,
		concurrency:  opts.concurrency,
		pathStyle:    opts.pathStyle,
		force:        opts.force,
	}
	source, err := parseDownloadSource(opts.source)
	if err != nil {
		return resolvedConfig{}, err
	}
	resolved.bucket = source.bucket
	resolved.key = source.key
	resolved.presignedURL = source.presignedURL
	resolved.sourceDisplay = source.display
	resolved.destination = strings.TrimSpace(opts.output)
	if resolved.destination == "" {
		resolved.destination = source.filename
	}
	if resolved.destination == "" || resolved.destination == "." || resolved.destination == string(filepath.Separator) {
		return resolvedConfig{}, errors.New("无法推导输出文件名，请使用 --output 指定")
	}
	if source.presignedURL != "" {
		return resolved, nil
	}

	if opts.awsDirectoryExplicit && resolved.awsDirectory == "" {
		return resolvedConfig{}, errors.New("--aws 不能为空")
	}
	if opts.awsProfileExplicit && resolved.awsProfile == "" {
		return resolvedConfig{}, errors.New("-p/--profile 不能为空")
	}
	if resolved.awsDirectory != "" {
		resolved.awsDirectory, err = resolveAWSDirectory(resolved.awsDirectory)
		if err != nil {
			return resolvedConfig{}, err
		}
	}
	cliCredentialsSet := opts.credentialsExplicit || resolved.accessKey != "" || resolved.secretKey != "" || resolved.sessionToken != ""
	if cliCredentialsSet {
		if resolved.accessKey == "" || resolved.secretKey == "" {
			return resolvedConfig{}, errors.New("命令行 --ak 和 --sk 必须同时设置")
		}
	} else if resolved.awsProfile == "" {
		environmentCredentials, environmentSet, err := credentialsFromEnvironment()
		if err != nil {
			return resolvedConfig{}, err
		}
		if environmentSet {
			resolved.accessKey = environmentCredentials.AccessKey
			resolved.secretKey = environmentCredentials.SecretKey
			resolved.sessionToken = environmentCredentials.SessionToken
		}
	}
	if resolved.endpoint != "" {
		normalized, err := normalizeEndpoint(resolved.endpoint)
		if err != nil {
			return resolvedConfig{}, err
		}
		resolved.endpoint = normalized
	}
	return resolved, nil
}

func parseDownloadSource(raw string) (downloadSource, error) {
	value := strings.TrimSpace(raw)
	if strings.HasPrefix(value, "s3://") {
		bucket, key, err := parseS3URI(value)
		if err != nil {
			return downloadSource{}, err
		}
		return downloadSource{
			bucket:   bucket,
			key:      key,
			display:  formatS3URIForDisplay(bucket, key),
			filename: path.Base(key),
		}, nil
	}

	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return downloadSource{}, errors.New("对象地址必须使用 s3://bucket/key 或 http(s) 预签名 URL 格式")
	}
	if parsed.User != nil {
		return downloadSource{}, errors.New("预签名 URL 不能包含 userinfo")
	}
	if parsed.Fragment != "" {
		return downloadSource{}, errors.New("预签名 URL 不能包含 fragment")
	}
	if err := validatePresignedSignedHeaders(parsed); err != nil {
		return downloadSource{}, err
	}
	filename := ""
	if escapedPath := parsed.EscapedPath(); escapedPath != "" && !strings.HasSuffix(escapedPath, "/") {
		filename = path.Base(parsed.Path)
	}
	return downloadSource{
		presignedURL: value,
		display:      safeURLDisplay(parsed),
		filename:     filename,
	}, nil
}

// formatS3URIForDisplay returns a canonical, terminal-safe S3 URI while the
// decoded key remains available separately for SDK requests.
func formatS3URIForDisplay(bucket, key string) string {
	displayURL := &url.URL{
		Scheme: "s3",
		Host:   bucket,
		Path:   "/" + key,
	}
	return displayURL.String()
}

func safeURLDisplay(parsed *url.URL) string {
	copy := *parsed
	copy.RawQuery = ""
	copy.ForceQuery = false
	copy.Fragment = ""
	copy.User = nil
	return copy.String()
}

func validatePresignedSignedHeaders(parsed *url.URL) error {
	for name, values := range parsed.Query() {
		if !strings.EqualFold(name, "X-Amz-SignedHeaders") && !strings.EqualFold(name, "X-Goog-SignedHeaders") {
			continue
		}
		for _, value := range values {
			for _, header := range strings.Split(value, ";") {
				header = strings.ToLower(strings.TrimSpace(header))
				switch header {
				case "", "host":
					continue
				case "range":
					return errors.New("预签名 URL 已将 Range 请求头加入签名，无法为不同分片改写 Range；请生成仅签名 host 的 GET URL")
				default:
					return fmt.Errorf("预签名 URL 依赖未随 URL 提供的已签名请求头 %q", header)
				}
			}
		}
	}
	return nil
}

func credentialsFromEnvironment() (staticCredentials, bool, error) {
	credentials := staticCredentials{
		AccessKey:    firstEnvironmentValue("AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY"),
		SecretKey:    firstEnvironmentValue("AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY"),
		SessionToken: strings.TrimSpace(os.Getenv("AWS_SESSION_TOKEN")),
	}
	configured := credentials.AccessKey != "" || credentials.SecretKey != "" || credentials.SessionToken != ""
	if !configured {
		return staticCredentials{}, false, nil
	}
	if credentials.AccessKey == "" || credentials.SecretKey == "" {
		return staticCredentials{}, false, errors.New("AWS access key 和 secret key 环境变量必须同时设置")
	}
	return credentials, true, nil
}

func firstEnvironmentValue(names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	return ""
}

func resolveAWSDirectory(raw string) (string, error) {
	directory := strings.TrimSpace(raw)
	if directory == "~" || strings.HasPrefix(directory, "~/") || strings.HasPrefix(directory, `~\`) {
		home, err := os.UserHomeDir()
		if err != nil || home == "" {
			return "", errors.New("无法展开 AWS 配置目录中的 ~")
		}
		if directory == "~" {
			directory = home
		} else {
			directory = filepath.Join(home, directory[2:])
		}
	} else if strings.HasPrefix(directory, "~") {
		return "", errors.New("AWS 配置目录仅支持 ~ 或 ~/path 形式")
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return "", fmt.Errorf("解析 AWS 配置目录: %w", err)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return "", fmt.Errorf("读取 AWS 配置目录 %s: %w", absolute, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("AWS 配置路径不是目录: %s", absolute)
	}
	configPath := filepath.Join(absolute, "config")
	credentialsPath := filepath.Join(absolute, "credentials")
	configExists, err := regularFileExists(configPath)
	if err != nil {
		return "", err
	}
	credentialsExist, err := regularFileExists(credentialsPath)
	if err != nil {
		return "", err
	}
	if !configExists && !credentialsExist {
		return "", fmt.Errorf("AWS 配置目录 %s 中至少需要 config 或 credentials 文件", absolute)
	}
	return absolute, nil
}

func regularFileExists(filename string) (bool, error) {
	info, err := os.Stat(filename)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("读取 AWS 配置文件 %s: %w", filename, err)
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("AWS 配置文件不是普通文件: %s", filename)
	}
	return true, nil
}

func normalizeEndpoint(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", fmt.Errorf("无效的 endpoint %q，必须是 http(s) URL", raw)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("endpoint %q 不能包含 query 或 fragment", raw)
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

// parseS3URI separates the bucket and key, then decodes URI path escapes in
// the key exactly once so the AWS SDK can apply the required HTTP escaping.
func parseS3URI(raw string) (string, string, error) {
	if !strings.HasPrefix(raw, "s3://") {
		return "", "", fmt.Errorf("对象地址必须使用 s3://bucket/key 格式: %q", raw)
	}
	remainder := strings.TrimPrefix(raw, "s3://")
	bucket, escapedKey, found := strings.Cut(remainder, "/")
	if !found || strings.TrimSpace(bucket) == "" || escapedKey == "" {
		return "", "", fmt.Errorf("对象地址必须包含 bucket 和 key: %q", raw)
	}
	if strings.ContainsAny(bucket, "?#") || strings.ContainsAny(escapedKey, "?#") {
		return "", "", fmt.Errorf("对象地址不能包含 query 或 fragment: %q", raw)
	}
	key, err := url.PathUnescape(escapedKey)
	if err != nil {
		return "", "", fmt.Errorf("对象 key 包含无效的 percent 编码: %w", err)
	}
	return bucket, key, nil
}

func newS3Client(ctx context.Context, cfg resolvedConfig) (s3API, error) {
	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithDefaultRegion(defaultRegion),
	}
	if cfg.awsDirectory != "" {
		loadOptions = append(loadOptions,
			awsconfig.WithSharedConfigFiles([]string{filepath.Join(cfg.awsDirectory, "config")}),
			awsconfig.WithSharedCredentialsFiles([]string{filepath.Join(cfg.awsDirectory, "credentials")}),
		)
	}
	if cfg.awsProfile != "" {
		loadOptions = append(loadOptions, awsconfig.WithSharedConfigProfile(cfg.awsProfile))
	}
	if cfg.region != "" {
		loadOptions = append(loadOptions, awsconfig.WithRegion(cfg.region))
	}
	if cfg.accessKey != "" || cfg.secretKey != "" || cfg.sessionToken != "" {
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.accessKey, cfg.secretKey, cfg.sessionToken),
		))
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("初始化 AWS 配置: %w", err)
	}
	return s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		if cfg.endpoint != "" {
			options.BaseEndpoint = aws.String(cfg.endpoint)
		}
		options.UsePathStyle = cfg.pathStyle
	}), nil
}

func newDownloadClient(ctx context.Context, cfg resolvedConfig) (s3API, error) {
	if cfg.presignedURL != "" {
		return newPresignedURLClient(cfg.presignedURL)
	}
	return newS3Client(ctx, cfg)
}

func newPresignedURLClient(rawURL string) (*presignedURLClient, error) {
	source, err := parseDownloadSource(rawURL)
	if err != nil {
		return nil, err
	}
	if source.presignedURL == "" {
		return nil, errors.New("预签名下载地址必须是 http(s) URL")
	}
	if _, err := http.NewRequest(http.MethodGet, source.presignedURL, nil); err != nil {
		return nil, errors.New("创建预签名 URL 请求失败：URL 格式无效")
	}

	transport := http.DefaultTransport
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		clone := base.Clone()
		clone.MaxIdleConns = max(clone.MaxIdleConns, maxConcurrency)
		clone.MaxIdleConnsPerHost = max(clone.MaxIdleConnsPerHost, maxConcurrency)
		transport = clone
	}
	return &presignedURLClient{
		rawURL:  source.presignedURL,
		display: source.display,
		client: &http.Client{
			Transport: transport,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (c *presignedURLClient) HeadObject(
	ctx context.Context,
	_ *s3.HeadObjectInput,
	_ ...func(*s3.Options),
) (*s3.HeadObjectOutput, error) {
	response, err := c.doRangeRequest(ctx, "bytes=0-0", "")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	switch response.StatusCode {
	case http.StatusPartialContent:
		start, end, total, err := parseContentRange(response.Header.Get("Content-Range"))
		if err != nil || start != 0 || end != 0 || total < 1 {
			return nil, fmt.Errorf("预签名 URL 探测返回无效的 Content-Range %q", response.Header.Get("Content-Range"))
		}
		read, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 2))
		if readErr != nil {
			return nil, fmt.Errorf("读取预签名 URL 探测响应: %w", readErr)
		}
		if read != 1 {
			return nil, fmt.Errorf("预签名 URL 探测响应大小不匹配: 收到 %d，期望 1", read)
		}
		output := &s3.HeadObjectOutput{ContentLength: aws.Int64(total)}
		if etag := response.Header.Get("ETag"); etag != "" {
			output.ETag = aws.String(etag)
		}
		return output, nil

	case http.StatusRequestedRangeNotSatisfiable:
		total, ok := parseUnsatisfiedContentRange(response.Header.Get("Content-Range"))
		if ok && total == 0 {
			return &s3.HeadObjectOutput{ContentLength: aws.Int64(0)}, nil
		}
		return nil, c.statusError(response)

	case http.StatusOK:
		return nil, errors.New("服务端忽略了 Range 请求，无法安全地并行切片下载此 URL")

	default:
		return nil, c.statusError(response)
	}
}

func (c *presignedURLClient) GetObject(
	ctx context.Context,
	input *s3.GetObjectInput,
	_ ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	if input == nil || input.Range == nil || strings.TrimSpace(*input.Range) == "" {
		return nil, errors.New("预签名 URL 分片请求缺少 Range")
	}
	ifMatch := ""
	if input.IfMatch != nil {
		ifMatch = *input.IfMatch
	}
	response, err := c.doRangeRequest(ctx, *input.Range, ifMatch)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusPartialContent {
		defer response.Body.Close()
		if response.StatusCode == http.StatusOK {
			return nil, errors.New("服务端忽略了 Range 请求，无法安全地并行切片下载此 URL")
		}
		return nil, c.statusError(response)
	}

	output := &s3.GetObjectOutput{
		Body:         response.Body,
		ContentRange: aws.String(response.Header.Get("Content-Range")),
	}
	if response.ContentLength >= 0 {
		output.ContentLength = aws.Int64(response.ContentLength)
	}
	if etag := response.Header.Get("ETag"); etag != "" {
		output.ETag = aws.String(etag)
	}
	return output, nil
}

func (c *presignedURLClient) doRangeRequest(ctx context.Context, rangeHeader, ifMatch string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.rawURL, nil)
	if err != nil {
		return nil, errors.New("创建预签名 URL 请求失败：URL 格式无效")
	}
	request.Header.Set("Range", rangeHeader)
	request.Header.Set("Accept-Encoding", "identity")
	if ifMatch != "" {
		request.Header.Set("If-Match", ifMatch)
	}
	response, err := c.client.Do(request)
	if err == nil {
		return response, nil
	}
	if contextError := ctx.Err(); contextError != nil {
		return nil, contextError
	}
	var urlError *url.Error
	if errors.As(err, &urlError) && urlError.Err != nil {
		err = urlError.Err
	}
	return nil, fmt.Errorf("请求预签名 URL %s: %w", c.display, err)
}

func (c *presignedURLClient) statusError(response *http.Response) error {
	requestID := response.Header.Get("x-amz-request-id")
	if requestID == "" {
		return fmt.Errorf("预签名 URL 请求失败: HTTP %s", response.Status)
	}
	return fmt.Errorf("预签名 URL 请求失败: HTTP %s（request id: %s）", response.Status, requestID)
}

func parseContentRange(raw string) (start, end, total int64, err error) {
	value := strings.TrimSpace(raw)
	rangeAndTotal, ok := strings.CutPrefix(value, "bytes ")
	if !ok {
		return 0, 0, 0, fmt.Errorf("无效的 Content-Range %q", value)
	}
	rangeValue, totalValue, ok := strings.Cut(rangeAndTotal, "/")
	if !ok || strings.Contains(totalValue, "/") {
		return 0, 0, 0, fmt.Errorf("无效的 Content-Range %q", value)
	}
	startValue, endValue, ok := strings.Cut(rangeValue, "-")
	if !ok || strings.Contains(endValue, "-") {
		return 0, 0, 0, fmt.Errorf("无效的 Content-Range %q", value)
	}
	start, startError := strconv.ParseInt(startValue, 10, 64)
	end, endError := strconv.ParseInt(endValue, 10, 64)
	total, totalError := strconv.ParseInt(totalValue, 10, 64)
	if startError != nil || endError != nil || totalError != nil || start < 0 || end < start || total <= end ||
		value != fmt.Sprintf("bytes %d-%d/%d", start, end, total) {
		return 0, 0, 0, fmt.Errorf("无效的 Content-Range %q", value)
	}
	return start, end, total, nil
}

func parseUnsatisfiedContentRange(raw string) (int64, bool) {
	value := strings.TrimSpace(raw)
	totalValue, ok := strings.CutPrefix(value, "bytes */")
	if !ok {
		return 0, false
	}
	total, err := strconv.ParseInt(totalValue, 10, 64)
	return total, err == nil && total >= 0 && value == fmt.Sprintf("bytes */%d", total)
}

func planParts(size, requestedPartSize int64) ([]byteRange, error) {
	if size < 0 {
		return nil, errors.New("对象大小不能为负数")
	}
	if requestedPartSize <= 0 {
		return nil, errors.New("分片大小必须大于 0")
	}
	if size == 0 {
		return nil, nil
	}
	partSize := requestedPartSize
	minimum := size / maxDownloadParts
	if size%maxDownloadParts != 0 {
		minimum++
	}
	if minimum > partSize {
		partSize = minimum
	}
	count := size / partSize
	if size%partSize != 0 {
		count++
	}
	parts := make([]byteRange, 0, count)
	for start, index := int64(0), 0; start < size; index++ {
		length := min(partSize, size-start)
		end := start + length - 1
		parts = append(parts, byteRange{index: index, start: start, end: end})
		start += length
	}
	return parts, nil
}

func runDownload(ctx context.Context, client s3API, request downloadRequest, events chan downloadProgress) {
	defer close(events)
	var latest downloadProgress
	var latestMu sync.Mutex
	report := func(progress downloadProgress) {
		latestMu.Lock()
		latest = progress
		latestMu.Unlock()
		select {
		case events <- progress:
		default:
		}
	}
	report(downloadProgress{stage: "读取对象信息"})
	err := downloadObject(ctx, client, request, report)
	latestMu.Lock()
	final := latest
	latestMu.Unlock()
	final.stage = "下载完成"
	final.done = true
	if err != nil {
		final.stage = "下载失败"
		final.err = err
		if errors.Is(err, context.Canceled) {
			final.stage = "已取消"
		}
	}
	// Replace one stale progress snapshot if necessary so a stopped UI cannot
	// leave the worker blocked while trying to publish its terminal state.
	select {
	case events <- final:
		return
	default:
	}
	select {
	case <-events:
	default:
	}
	select {
	case events <- final:
	default:
	}
}

func downloadObject(ctx context.Context, client s3API, request downloadRequest, report progressFunc) error {
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(request.bucket),
		Key:    aws.String(request.key),
	})
	if err != nil {
		source := request.sourceDisplay
		if source == "" {
			source = fmt.Sprintf("s3://%s/%s", request.bucket, request.key)
		}
		return fmt.Errorf("读取 %s 元信息: %w", source, err)
	}
	if head.ContentLength == nil || *head.ContentLength < 0 {
		return errors.New("S3 返回了无效的对象大小")
	}
	parts, err := planParts(*head.ContentLength, request.partSize)
	if err != nil {
		return err
	}
	total := *head.ContentLength
	etag := head.ETag
	stage := "下载中"
	if !isStrongETag(etag) {
		etag = nil
		if len(parts) > 1 {
			parts = []byteRange{{index: 0, start: 0, end: total - 1}}
			stage = "下载中（服务未提供强 ETag，已使用单分片）"
		}
	}
	report(downloadProgress{stage: stage, total: total, totalParts: len(parts)})
	if err := ctx.Err(); err != nil {
		return err
	}

	destination, err := filepath.Abs(request.destination)
	if err != nil {
		return fmt.Errorf("解析输出路径: %w", err)
	}
	if err := prepareDestination(destination, request.force); err != nil {
		return err
	}
	parent := filepath.Dir(destination)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("创建输出目录 %s: %w", parent, err)
	}
	temporary, err := os.CreateTemp(parent, "."+filepath.Base(destination)+"-*.part")
	if err != nil {
		return fmt.Errorf("创建临时文件: %w", err)
	}
	temporaryName := temporary.Name()
	committed := false
	defer func() {
		temporary.Close()
		if !committed {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Truncate(total); err != nil {
		return fmt.Errorf("预分配临时文件: %w", err)
	}

	if len(parts) > 0 {
		if err := downloadParts(ctx, client, temporary, request, etag, parts, total, report); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("同步临时文件: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("关闭临时文件: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := commitDownload(ctx, temporaryName, destination, request.force); err != nil {
		return err
	}
	committed = true
	report(downloadProgress{
		stage:          "校验并保存",
		downloaded:     total,
		total:          total,
		completedParts: len(parts),
		totalParts:     len(parts),
	})
	return nil
}

func isStrongETag(etag *string) bool {
	if etag == nil {
		return false
	}
	value := strings.TrimSpace(*etag)
	return value != "" && !strings.HasPrefix(strings.ToUpper(value), "W/")
}

func prepareDestination(destination string, force bool) error {
	info, err := os.Lstat(destination)
	if err == nil {
		if info.IsDir() {
			return fmt.Errorf("输出路径是目录: %s", destination)
		}
		if !force {
			return fmt.Errorf("输出文件已存在: %s（使用 --force 覆盖）", destination)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("检查输出路径 %s: %w", destination, err)
	}
	return nil
}

func commitDownload(ctx context.Context, temporaryName, destination string, force bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if force {
		if err := os.Rename(temporaryName, destination); err != nil {
			return fmt.Errorf("保存下载文件: %w", err)
		}
		return nil
	}
	// A hard link gives us atomic no-clobber semantics because both files live
	// in the same directory. Rename would overwrite a file created mid-download.
	if err := os.Link(temporaryName, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("输出文件已存在: %s（使用 --force 覆盖）", destination)
		}
		return fmt.Errorf("文件系统不支持安全提交: %w（确认目标路径可覆盖后可使用 --force）", err)
	}
	_ = os.Remove(temporaryName)
	return nil
}

func downloadParts(
	ctx context.Context,
	client s3API,
	file *os.File,
	request downloadRequest,
	etag *string,
	parts []byteRange,
	total int64,
	report progressFunc,
) error {
	workerCount := min(request.concurrency, len(parts))
	workerContext, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan byteRange)
	results := make(chan error, workerCount)
	var workers sync.WaitGroup
	var downloaded atomic.Int64
	var completed atomic.Int64
	var reportMu sync.Mutex
	emit := func(stage string) {
		reportMu.Lock()
		report(downloadProgress{
			stage:          stage,
			downloaded:     downloaded.Load(),
			total:          total,
			completedParts: int(completed.Load()),
			totalParts:     len(parts),
		})
		reportMu.Unlock()
	}

	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for part := range jobs {
				err := downloadPartWithRetry(workerContext, client, file, request, etag, part, total, func(delta int64) {
					downloaded.Add(delta)
					emit("下载中")
				})
				if err == nil {
					completed.Add(1)
					emit("下载中")
				} else {
					cancel()
				}
				results <- err
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, part := range parts {
			select {
			case jobs <- part:
			case <-workerContext.Done():
				return
			}
		}
	}()
	go func() {
		workers.Wait()
		close(results)
	}()

	var firstError error
	for result := range results {
		if result == nil {
			continue
		}
		if firstError == nil || (errors.Is(firstError, context.Canceled) && !errors.Is(result, context.Canceled)) {
			firstError = result
		}
	}
	if firstError != nil {
		return firstError
	}
	if downloaded.Load() != total {
		return fmt.Errorf("下载字节数不匹配: 收到 %d，期望 %d", downloaded.Load(), total)
	}
	return nil
}

func downloadPartWithRetry(
	ctx context.Context,
	client s3API,
	file *os.File,
	request downloadRequest,
	etag *string,
	part byteRange,
	total int64,
	onBytes func(int64),
) error {
	var lastError error
	for attempt := 1; attempt <= maxPartAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		written, err := downloadPart(ctx, client, file, request, etag, part, total, onBytes)
		if err == nil {
			return nil
		}
		if written > 0 {
			onBytes(-written)
		}
		lastError = err
		if attempt == maxPartAttempts {
			break
		}
		delay := time.Duration(1<<(attempt-1)) * 200 * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
	return fmt.Errorf("分片 %d（bytes=%d-%d）下载失败: %w", part.index+1, part.start, part.end, lastError)
}

func downloadPart(
	ctx context.Context,
	client s3API,
	file *os.File,
	request downloadRequest,
	etag *string,
	part byteRange,
	total int64,
	onBytes func(int64),
) (int64, error) {
	byteRangeHeader := fmt.Sprintf("bytes=%d-%d", part.start, part.end)
	input := &s3.GetObjectInput{
		Bucket: aws.String(request.bucket),
		Key:    aws.String(request.key),
		Range:  aws.String(byteRangeHeader),
	}
	if etag != nil && *etag != "" {
		input.IfMatch = etag
	}
	output, err := client.GetObject(ctx, input)
	if err != nil {
		return 0, err
	}
	if output.Body == nil {
		return 0, errors.New("S3 返回了空响应体")
	}
	if etag != nil && strings.TrimSpace(*etag) != "" {
		if output.ETag == nil || strings.TrimSpace(*output.ETag) != strings.TrimSpace(*etag) {
			_ = output.Body.Close()
			return 0, fmt.Errorf("分片响应 ETag 不匹配: 收到 %q，期望 %q", aws.ToString(output.ETag), strings.TrimSpace(*etag))
		}
	}
	expected := part.size()
	if output.ContentLength != nil && *output.ContentLength != expected {
		_ = output.Body.Close()
		return 0, fmt.Errorf("分片响应大小不匹配: 收到 %d，期望 %d", *output.ContentLength, expected)
	}
	if err := validateContentRange(output.ContentRange, part, total); err != nil {
		_ = output.Body.Close()
		return 0, err
	}

	offsetWriter := io.NewOffsetWriter(file, part.start)
	countingWriter := &countingWriter{writer: offsetWriter, onBytes: onBytes}
	written, copyError := io.CopyBuffer(countingWriter, io.LimitReader(output.Body, expected+1), make([]byte, 256<<10))
	closeError := output.Body.Close()
	if copyError != nil {
		return written, copyError
	}
	if closeError != nil {
		return written, closeError
	}
	if written != expected {
		return written, fmt.Errorf("分片响应大小不匹配: 收到 %d，期望 %d", written, expected)
	}
	return written, nil
}

func validateContentRange(raw *string, part byteRange, total int64) error {
	if raw == nil {
		return errors.New("分片响应缺少 Content-Range")
	}
	start, end, objectSize, err := parseContentRange(*raw)
	if err != nil || start != part.start || end != part.end || objectSize != total {
		return fmt.Errorf("Content-Range 不匹配: 收到 %q，期望 %q", strings.TrimSpace(*raw),
			fmt.Sprintf("bytes %d-%d/%d", part.start, part.end, total))
	}
	return nil
}

type countingWriter struct {
	writer  io.Writer
	onBytes func(int64)
}

func (w *countingWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	if n > 0 {
		w.onBytes(int64(n))
	}
	return n, err
}

func formatBytes(value int64) string {
	if value < 1024 {
		return fmt.Sprintf("%d B", value)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	size := float64(value)
	unit := "B"
	for _, candidate := range units {
		size /= 1024
		unit = candidate
		if size < 1024 {
			break
		}
	}
	return fmt.Sprintf("%.1f %s", size, unit)
}
