package pkg

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Options struct {
	CredentialsExplicit  bool
	AWSDirectory         string
	AWSDirectoryExplicit bool
	AWSProfile           string
	AWSProfileExplicit   bool
	Endpoint             string
	AccessKey            string
	SecretKey            string
	SessionToken         string
	Region               string
	PathStyle            bool
}

type Config struct {
	Endpoint     string
	AccessKey    string
	SecretKey    string
	SessionToken string
	Region       string
	AWSDirectory string
	AWSProfile   string
	PathStyle    bool
}

type staticCredentials struct {
	AccessKey    string
	SecretKey    string
	SessionToken string
}

func ConnectionConfig(opts Options) Config {
	resolved := Config{
		Endpoint:     strings.TrimSpace(opts.Endpoint),
		AccessKey:    strings.TrimSpace(opts.AccessKey),
		SecretKey:    strings.TrimSpace(opts.SecretKey),
		SessionToken: strings.TrimSpace(opts.SessionToken),
		Region:       strings.TrimSpace(opts.Region),
		AWSDirectory: strings.TrimSpace(opts.AWSDirectory),
		AWSProfile:   strings.TrimSpace(opts.AWSProfile),
		PathStyle:    opts.PathStyle,
	}
	return resolved
}

func ResolveConnectionConfig(opts Options, resolved Config) (Config, error) {
	var err error

	if opts.AWSDirectoryExplicit && resolved.AWSDirectory == "" {
		return Config{}, errors.New("--aws 不能为空")
	}
	if opts.AWSProfileExplicit && resolved.AWSProfile == "" {
		return Config{}, errors.New("-p/--profile 不能为空")
	}
	if resolved.AWSDirectory != "" {
		resolved.AWSDirectory, err = ResolveAWSDirectory(resolved.AWSDirectory)
		if err != nil {
			return Config{}, err
		}
	}
	cliCredentialsSet := opts.CredentialsExplicit || resolved.AccessKey != "" || resolved.SecretKey != "" || resolved.SessionToken != ""
	if cliCredentialsSet {
		if resolved.AccessKey == "" || resolved.SecretKey == "" {
			return Config{}, errors.New("命令行 --ak 和 --sk 必须同时设置")
		}
	} else if resolved.AWSProfile == "" {
		environmentCredentials, environmentSet, err := credentialsFromEnvironment()
		if err != nil {
			return Config{}, err
		}
		if environmentSet {
			resolved.AccessKey = environmentCredentials.AccessKey
			resolved.SecretKey = environmentCredentials.SecretKey
			resolved.SessionToken = environmentCredentials.SessionToken
		}
	}
	if resolved.Endpoint != "" {
		normalized, err := normalizeEndpoint(resolved.Endpoint)
		if err != nil {
			return Config{}, err
		}
		resolved.Endpoint = normalized
	}
	return resolved, nil
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

func ResolveAWSDirectory(raw string) (string, error) {
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
		return "", fmt.Errorf("无效的 Endpoint %q，必须是 http(s) URL", raw)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("Endpoint %q 不能包含 query 或 fragment", raw)
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

// ParseS3URI separates the bucket and key, then decodes URI path escapes in
// the key exactly once so the AWS SDK can apply the required HTTP escaping.
func ParseS3URI(raw string) (string, string, error) {
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

func NewS3Client(ctx context.Context, cfg Config) (*s3.Client, error) {
	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithDefaultRegion(DefaultRegion),
	}
	if cfg.AWSDirectory != "" {
		loadOptions = append(loadOptions,
			awsconfig.WithSharedConfigFiles([]string{filepath.Join(cfg.AWSDirectory, "config")}),
			awsconfig.WithSharedCredentialsFiles([]string{filepath.Join(cfg.AWSDirectory, "credentials")}),
		)
	}
	if cfg.AWSProfile != "" {
		loadOptions = append(loadOptions, awsconfig.WithSharedConfigProfile(cfg.AWSProfile))
	}
	if cfg.Region != "" {
		loadOptions = append(loadOptions, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.AccessKey != "" || cfg.SecretKey != "" || cfg.SessionToken != "" {
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken),
		))
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("初始化 AWS 配置: %w", err)
	}
	return s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		if cfg.Endpoint != "" {
			options.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		options.UsePathStyle = cfg.PathStyle
	}), nil
}
