package pkg

import "flag"

func AddConnectionFlags(fs *flag.FlagSet, opts *Options) {
	fs.StringVar(&opts.Endpoint, "Endpoint", "", "S3 Endpoint `URL`，例如 https://s3.example.com")
	fs.StringVar(&opts.AccessKey, "ak", "", "access key `VALUE`，也可使用 AWS_ACCESS_KEY_ID")
	fs.StringVar(&opts.SecretKey, "sk", "", "secret key `VALUE`，也可使用 AWS_SECRET_ACCESS_KEY")
	fs.StringVar(&opts.SessionToken, "session-token", "", "临时凭据 session token `VALUE`，也可使用 AWS_SESSION_TOKEN")
	fs.StringVar(&opts.Region, "Region", "", "S3 Region `VALUE`（未配置时使用 cn-sh）")
	fs.StringVar(&opts.AWSDirectory, "aws", "", "AWS 配置目录 `DIR`，目录内包含 config 与 credentials（默认 ~/.aws）")
	fs.StringVar(&opts.AWSProfile, "p", "", "AWS 命名 profile `NAME`（等同 --profile）")
	fs.StringVar(&opts.AWSProfile, "profile", "", "AWS 命名 profile `NAME`（语义与 aws --profile 相同，默认 AWS_PROFILE/default）")
	fs.BoolVar(&opts.PathStyle, "path-style", true, "使用 path-style S3 请求；设为 false 时使用 bucket.Endpoint")
}
