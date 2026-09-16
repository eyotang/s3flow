# s3get

`s3get` 是一个基于 Bubble Tea 的 S3 并发分片下载工具。S3 URI 模式通过 `HeadObject` 获取对象大小，预签名 URL 模式通过 `GET bytes=0-0` 探测大小，随后自动规划 Range 分片并发下载；界面使用 Bubbles Animated Progress 展示实时进度、速度和剩余时间。

下载内容先写入目标目录中的临时文件。所有分片完成并同步到磁盘后才提交为最终文件；失败或取消时会清理临时文件。每个分片最多尝试 3 次，请求通过 ETag `If-Match` 固定对象版本，并校验每片响应的 ETag。若服务端未提供强 ETag，工具会自动降为单 Range 请求，避免并发拼接到不同版本的内容。

## 构建

```bash
go build -o s3get ./cmd
```

也可以使用 Makefile，产物位于 `bin/s3get`：

```bash
make build
```

## AWS 标准配置

未指定 `--aws` 时，工具直接使用 AWS SDK 的标准配置目录 `~/.aws`，读取原生 INI 格式的 `config` 与 `credentials`，不再使用 YAML。

`~/.aws/credentials`：

```ini
[default]
aws_access_key_id = your-access-key
aws_secret_access_key = your-secret-key
# 临时凭据可选
aws_session_token = your-session-token

[production]
aws_access_key_id = production-access-key
aws_secret_access_key = production-secret-key
```

`~/.aws/config`：

```ini
[default]
region = cn-sh
endpoint_url = https://s3.example.com

[profile production]
region = cn-sh
endpoint_url = https://production-s3.example.com
```

`-p NAME` 是 `--profile NAME` 的缩写，含义与 AWS CLI 的 `aws --profile NAME` 一致。它在 `credentials` 中选择 `[NAME]`，在 `config` 中选择 `[profile NAME]`；未指定时使用 `AWS_PROFILE`/`AWS_DEFAULT_PROFILE`，再缺省为 `default`。

`--aws DIR` 用来指定另一套 AWS 配置目录，工具读取 `DIR/config` 和 `DIR/credentials`。例如：

```bash
./s3get --aws ./aws-production --profile production \
  s3://my-bucket/releases/app.tar.zst
```

未指定 `--aws` 时，也兼容 AWS 标准的 `AWS_CONFIG_FILE` 与 `AWS_SHARED_CREDENTIALS_FILE`。

凭据也可以通过环境变量传入：

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_SESSION_TOKEN=your-session-token # 可选
export AWS_REGION=cn-sh
export AWS_ENDPOINT_URL_S3=https://s3.example.com
```

配置与凭据作为两个独立分组解析：

- 配置组 `endpoint/region`：命令行参数优先于 AWS 环境变量，AWS 环境变量优先于选中的 shared config profile；region 最终缺省为 `cn-sh`。
- 凭据组 `ak/sk/session_token`：直接传入的 `--ak/--sk/--session-token` 优先级最高；显式使用 `-p/--profile` 时，按 AWS CLI 语义使用该 profile 的凭据；未显式选择 profile 时，环境凭据优先于 `AWS_PROFILE`/`default` 对应的 shared credentials。AK/SK 按完整的一组读取，不会跨来源拼接。

因此可以在命令行提供 endpoint 和 region，同时只从环境变量读取凭据。

`--path-style` 默认为 `true`，工具会将 bucket 放入 URL 路径：

```text
https://s3.example.com/my-bucket/path/to/object.bin
```

这可直接兼容 XSky、MinIO 等不提供 `bucket.endpoint` DNS 解析的 S3 兼容服务。如果目标服务要求虚拟主机寻址，可传入 `--path-style=false`，请求会改为 `https://my-bucket.s3.example.com/path/to/object.bin`。

S3 URI 的对象 key 既可以直接使用 Unicode，也可以使用 URL percent 编码。工具会将路径编码解码一次，再交给 AWS SDK 生成请求，例如下面两种写法指向同一个对象：

```text
s3://checkpoint/线上服/object.gz
s3://checkpoint/%E7%BA%BF%E4%B8%8A%E6%9C%8D/object.gz
```

`+` 按路径字符处理，不会转换为空格；对象名中的字面 `%` 应写成 `%25`。

## 使用

参数定义和帮助文案由 Go `flag` 包统一生成：

```bash
./s3get --help
```

使用 profile：

```bash
./s3get -p production s3://my-bucket/releases/app.tar.zst
```

使用环境变量凭据和默认配置：

```bash
AWS_ACCESS_KEY_ID=your-access-key \
AWS_SECRET_ACCESS_KEY=your-secret-key \
./s3get s3://my-bucket/releases/app.tar.zst
```

命令行配置配合环境变量凭据：

```bash
AWS_ACCESS_KEY_ID=your-access-key \
AWS_SECRET_ACCESS_KEY=your-secret-key \
./s3get --endpoint https://s3.example.com --region cn-sh \
  s3://my-bucket/releases/app.tar.zst
```

直接传入连接参数：

```bash
./s3get \
  --endpoint https://s3.example.com \
  --ak your-access-key \
  --sk your-secret-key \
  --region cn-sh \
  s3://my-bucket/path/to/object.bin \
  ./object.bin
```

调整并发和分片大小：

```bash
./s3get --profile production --concurrency 16 --part-size 32MiB \
  -o ./downloads/object.bin s3://my-bucket/path/to/object.bin
```

## 预签名 URL

可以把带 endpoint 和签名查询参数的 GET URL 直接作为下载源。查询参数应使用引号保护，避免 shell 把 `&` 解释为控制符：

```bash
./s3get --concurrency 16 --part-size 32MiB \
  'https://s3.example.com/my-bucket/path/object.bin?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=...'
```

预签名模式不读取本地 AK/SK，也不使用 `--endpoint` 重写 URL。工具先发送 `GET` 与 `Range: bytes=0-0`，从 `Content-Range` 获得对象总大小，再复用原始 URL 并行请求各字节范围。原始 query 会逐请求保留，但不会显示在终端中。

该模式要求服务端支持 Range，并且 `X-Amz-SignedHeaders` 不能包含 `range`。若 Range 已参与签名，同一个 URL 只能使用签名时的固定 Range，无法改写成多个分片；请重新生成只签名 `host` 的 GET URL，或为每个分片分别签名。GET 预签名 URL 也不能改成 HEAD，因此工具使用 `bytes=0-0` GET 探测大小。

常用参数：

```text
--endpoint URL       S3 endpoint
--ak VALUE           access key
--sk VALUE           secret key
--session-token VALUE 临时凭据 token
--region VALUE       region，默认 cn-sh
--aws DIR            AWS 配置目录，默认 ~/.aws
-p, --profile NAME   AWS 命名 profile
--path-style         是否使用 path-style，默认 true；false 使用 bucket.endpoint
-o, --output FILE    输出文件
--part-size SIZE     分片大小，默认 16MiB
--concurrency N      并发数，默认 16，最大 128
--force              覆盖已有输出文件
```

下载过程中按 `q`、`esc` 或 `ctrl+c` 会取消请求并清理临时文件。

默认模式会用同目录 hard link 提交文件，以保证不覆盖下载期间新建的同名文件。若目标文件系统不支持 hard link，工具会安全退出；确认目标路径允许覆盖后，可用 `--force` 通过 rename 提交。

## 代码结构

- `cmd/main.go`：CLI 入口、依赖装配和退出码。
- `cmd/logic.go`：参数与 AWS shared config、S3/预签名 URL 客户端、分片规划、并发 Range 下载和文件提交。
- `cmd/ui.go`：Bubble Tea model、消息循环和 Animated Progress 渲染。
