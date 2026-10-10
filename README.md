# s3flow

`s3flow` 包含两个独立命令：`s3get` 用于下载，`s3push` 用于上传文件或目录，共用 AWS 配置。

`s3get` 是一个基于 Bubble Tea 的 S3 并发分片下载工具。S3 URI 模式通过 `HeadObject` 获取对象大小，预签名 URL 模式通过 `GET bytes=0-0` 探测大小，随后自动规划 Range 分片并发下载；界面使用 Bubbles Animated Progress 展示实时进度、速度、剩余时间和最终耗时。

下载内容先写入目标目录中的临时文件。所有分片完成并同步到磁盘后才提交为最终文件；失败或取消时会清理临时文件。每个分片最多尝试 3 次，请求通过 ETag `If-Match` 固定对象版本，并校验每片响应的 ETag。若服务端未提供强 ETag，工具会自动降为单 Range 请求，避免并发拼接到不同版本的内容。

## 安装与运行（优先使用 npx）

### npm 一键安装

安装体验参考 [lark-cli](https://github.com/larksuite/cli) 的 `npx @larksuite/cli@latest install`：执行一次安装命令，之后直接使用两个工具。

> npm 短命令将在首次 npm 发布完成后可用；当前可用版本仍为下方 GitHub Release 方式。发布包名以实际 npm 发布结果为准。

```bash
npx s3flow@latest install
s3get --help
s3push --help
```

安装器使用 npm 的全局安装目录，同时安装两个命令。可指定可写目录：`npx s3flow@latest install --prefix <目录>`，并将输出提示中的命令目录加入 `PATH`。全局安装只保留当前平台的二进制，不依赖 npx 缓存；同一条安装命令可用于升级。已有 AWS 配置继续使用，不需要在安装时填写凭据。

临时使用而不进行全局安装：`npx s3flow@latest s3get --help` 或 `npx s3flow@latest s3push --help`。公开 npm 包内置二进制，不要求使用者访问私有 GitHub 仓库。

### GitHub Release 安装（当前可用）

需要 Node.js 18 或更高版本（含 npm/npx）。本仓库为私有仓库，先使用有仓库访问权限的 GitHub 账号登录 GitHub CLI（首次使用执行 `gh auth login`），下载安装包后用 npx 运行：

```bash
gh release download v0.1.1 --repo eyotang/s3flow --pattern s3flow-0.1.1.tgz
npx --yes --package=./s3flow-0.1.1.tgz s3get --help
npx --yes --package=./s3flow-0.1.1.tgz s3push --help
```

也可在已登录 GitHub 的浏览器中从 [v0.1.1 发布页](https://github.com/eyotang/s3flow/releases/tag/v0.1.1) 下载 `s3flow-0.1.1.tgz`，然后在下载目录执行上述 npx 命令。私有 Release 的匿名 URL 会返回 404，因此不能直接把下载 URL 传给 npx。

把 `--help` 替换成下文对应命令的参数即可下载或上传。npx 会自动将包安装到 npm 缓存，并选择当前系统的二进制；Windows、macOS、Linux 的 x86_64 和 ARM64 使用相同命令。包中包含两个工具的全部平台二进制，约 50 MB，无需 Go、编译器或安装脚本。再次运行可复用缓存；运行目录改变时请将 `--package` 指向安装包的实际路径。

也可以一次安装，之后直接使用 `s3get` 和 `s3push`：

```bash
npm install --global ./s3flow-0.1.1.tgz
s3get --help
s3push --help
```

GitHub Release 方式无需 npm 账号，不依赖 npm registry 中的同名包。npx 的包执行及缓存行为见 [npm 官方文档](https://docs.npmjs.com/cli/v11/commands/npx/)。

### 可选：直接下载二进制

从 [GitHub Releases](https://github.com/eyotang/s3flow/releases/latest) 下载对应系统和 CPU 架构的压缩包。每个包都包含 `s3get`、`s3push`、README 和许可证，无需安装 Go。

| 系统 | x86_64 / Intel / AMD | ARM64 |
| --- | --- | --- |
| Windows | `s3flow_<版本>_windows_amd64.zip` | `s3flow_<版本>_windows_arm64.zip` |
| macOS | `s3flow_<版本>_darwin_amd64.tar.gz` | `s3flow_<版本>_darwin_arm64.tar.gz`（Apple Silicon） |
| Linux | `s3flow_<版本>_linux_amd64.tar.gz` | `s3flow_<版本>_linux_arm64.tar.gz` |

解压后可直接运行，或将两个程序放入 `PATH` 中的目录。Windows 的程序名为 `s3get.exe` 和 `s3push.exe`。发布页提供 `checksums.txt`，macOS/Linux 可用 `shasum -a 256 <压缩包>`，Windows PowerShell 可用 `Get-FileHash <压缩包> -Algorithm SHA256`，与文件中的对应值比较。

## 构建

```bash
go build -o s3get ./cmd/s3get
go build -o s3push ./cmd/s3push
```

也可以使用 Makefile，产物位于 `bin/s3get` 和 `bin/s3push`：

```bash
make build
```

## 发布

本地构建全部六种系统/架构的发布包及 npx 安装包（需要 Go、Node.js/npm、Bash、tar、zip 和 shasum）：

```bash
make release VERSION=v0.1.2
```

产物位于 `dist/v0.1.2/`，包含六个系统压缩包、通用 npm 安装包和 `checksums.txt`。构建禁用 CGO，每个包包含两个命令。相同版本的输出目录存在时会拒绝覆盖。版本标签必须与 `package.json` 的版本一致；npm 安装包文件名由包名与版本号生成。

将发布配置提交并推送后，推送版本标签即可触发 GitHub Actions：

```bash
git tag v0.1.2
git push origin v0.1.2
```

工作流先在 Windows、macOS、Linux 上运行 Go 测试、Node.js 启动器测试和真实 npm 包安装/运行测试，通过后构建全部发布包并创建 GitHub Release。npm 测试在独立缓存中禁用网络和安装脚本，验证两个命令、退出码以及全局安装后清理缓存仍可运行。手动触发工作流只运行测试。带连字符的版本（例如 `v0.2.0-rc.1`）会标记为预发布。

需要发布到 npm registry 时，先确认包名的发布权限并完成 `npm login`，再使用 `npm publish <构建好的安装包.tgz> --access public` 发布；不能直接发布源码目录，因为其中没有内置二进制。当前 GitHub 工作流只向 GitHub Release 上传安装包。

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

## 下载目录

```bash
./s3get -p production s3://my-bucket/backups/ ./backups
./s3get --recursive s3://my-bucket/backups ./backups
./s3get s3://my-bucket/ ./bucket-copy
```

以 `/` 结尾的 S3 地址或仅指定 bucket 时按目录下载；也可用 `--recursive` / `-r` 明确指定。不带 `/` 时优先读取同名文件；仅当 `HeadObject` 返回 404，才用 `ListObjectsV2(Prefix=key+"/", MaxKeys=1)` 探测目录，找到对象后自动切换为目录下载。文件与同名目录同时存在时优先下载文件。403、连接失败等错误不会触发目录探测；探测失败会明确报告，文件和目录都不存在则报错。目录内容保留相对路径，例如 `backups/a/b.txt` 保存为 `./backups/a/b.txt`。未指定输出目录时使用前缀末级名称，bucket 根目录使用 bucket 名称。目录列举支持分页，需要 `ListBucket` 权限；预签名 GET URL 仅支持单对象下载。

目录内文件依次下载，每个文件的分片并行。界面显示整体和当前文件进度条、文件数量及分片汇总，不逐条展示分片。零字节目录标记跳过；不安全的相对路径或文件/目录冲突会报错。下载文件通过受限目录句柄写入，不能通过对象路径或符号链接逃逸输出目录。已有文件仍需 `--force` 才覆盖；失败时保留之前完成的文件并清理当前临时文件。

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
--part-size SIZE     分片大小，默认 128MiB
--concurrency N      并发数，默认 16，最大 128
--force              覆盖已有输出文件
```

下载过程中按 `q`、`esc` 或 `ctrl+c` 会取消请求并清理临时文件。

默认模式会用同目录 hard link 提交文件，以保证不覆盖下载期间新建的同名文件。若目标文件系统不支持 hard link，工具会安全退出；确认目标路径允许覆盖后，可用 `--force` 通过 rename 提交。

## 上传文件或目录

```bash
./s3push --help
./s3push -p production ./archive.tar.zst s3://my-bucket/backups/archive.tar.zst
./s3push --concurrency 16 --part-size 128MiB ./data s3://my-bucket/backups/
```

文件目标以 `/` 结尾（或仅指定 bucket）时，自动追加本地文件名；否则使用指定的完整 key。目录上传其内容并保留相对路径，例如 `data/a/b.txt` 上传到 `backups/a/b.txt`，不包含顶层 `data`。本地目录为空或仅含空子目录时，在连接 S3 前报错退出；`--force` 不跳过此检查。零字节文件仍可上传。符号链接和特殊文件会在上传前报错。

小文件和空文件使用 `PutObject`，超过分片大小的文件使用并行 multipart upload。目录中的文件依次上传，每个大文件内部并行；分片默认 128MiB，允许 5MiB–5GiB，并发默认 16、最大 128。请求重试由 AWS SDK 处理。

默认在写入任何文件之前检查目标目录前缀，存在任何文件（包括不同名或子目录中的文件）都会拒绝上传，需显式指定 `--force` 强推；仅有零字节目录标记视为空目录。单文件上传检查目标 key 所在的父前缀，上传至 bucket 根目录则检查整个 bucket。检查需要 `ListBucket` 权限，检查失败会停止上传。写入时仍通过 `If-None-Match: *` 保护同名对象；`--force` 跳过空目录检查并允许覆盖同名对象，不删除其他对象。前缀检查不是目录锁，检查后其他客户端仍可能写入。S3 兼容服务需要支持条件写入，不支持时请求可能失败。失败或 Ctrl+C 取消时会尝试清理未完成的 multipart upload，清理失败会报告 upload ID；已上传成功的文件保留，目录上传不是原子操作。上传期间请保持源文件不变。

上传使用与下载一致的紫色 Bubble Tea 动态进度界面，显示当前文件、目标、已完成/总文件数、当前文件分片数、整体字节进度、平均速度和预计剩余时间，结束时显示耗时。界面以整体和当前文件两条进度条为主，未完成部分显示可见轨道，分片仅显示汇总，不逐条铺满屏幕。按 `q`、`esc` 或 `Ctrl+C` 取消并等待未完成分片清理。字节进度按请求体读取量估算，SDK 重试回卷时会回退，完成状态以服务端成功响应为准。上传支持上述 AWS 连接和认证参数，不支持下载专用的 `--output` 或预签名 GET URL。

## 代码结构

- `cmd/s3get/`：下载命令的入口、参数处理、下载逻辑、Bubble Tea 界面和测试。
- `cmd/s3push/`：上传命令的入口、参数处理、目录遍历、分片上传、Bubble Tea 界面和测试。
- `internal/pkg/`：两个命令共用的 AWS 配置、连接参数、S3 客户端、URI 解析、字节大小和分片规划。

两个命令使用一致的文件职责：`main.go` 负责入口、执行流程和退出码，`cli.go` 负责参数、帮助和配置解析，`logic.go` 负责传输逻辑；对应测试放在同名 `_test.go` 中。两个命令均使用各自的 `ui.go` 展示动态进度。
