# VALIDATION_KAFSCOUT

执行环境：2026-10-22，`kafscout`，分支 `feat/v1`；Go 1.24.4 linux/arm64，页大小 16384。所有临时文件使用 `TMPDIR=/var/tmp/codex`。网络仅用于首次模块准备；未拉取 Docker 镜像，未访问知识库、VM 或内网端点。

**验收状态：功能、离线构建、格式、vet 和普通并发测试通过；race 构建通过，但 race 测试运行被宿主 VMA 限制阻断，必过项尚未满足，不能声明全部验收通过。Docker Kafka 冒烟按约定跳过。**

## 1. 依赖准备及离线构建

已执行：

```sh
export TMPDIR=/var/tmp/codex
mkdir -p "$TMPDIR"
go mod init kafscout
go get github.com/IBM/sarama@v1.43.3
go mod tidy
go mod vendor
rm -f vendor/github.com/IBM/sarama/Vagrantfile
gofmt -w vendor
```

输出：`go: creating new go.mod: module kafscout`；`go: added github.com/IBM/sarama v1.43.3`，并下载 Sarama 传递依赖。`go.mod` 的唯一直接第三方依赖是 IBM/sarama；Go 版本要求为 1.22.0。vendor 及许可证入库；去除上游非构建 Vagrantfile，避免其私网 IP 部署示例进入交付；对 24 个旧版传递依赖源文件执行 gofmt，保持逻辑不变。

之后全部构建/测试指定 `GOPROXY=off GOSUMDB=off -mod=vendor`。另实测空模块缓存构建：

```sh
mkdir -p /var/tmp/codex/empty-modcache
TMPDIR=/var/tmp/codex GOPROXY=off GOSUMDB=off \
  GOMODCACHE=/var/tmp/codex/empty-modcache CGO_ENABLED=0 \
  go build -mod=vendor -o /var/tmp/codex/kafscout-native .
```

输出：无，exit 0。未依赖目标机网络或模块缓存。

## 2. 单元测试与 100000 条 mock 核心循环

```sh
TMPDIR=/var/tmp/codex GOPROXY=off GOSUMDB=off \
  go test -mod=vendor -v -timeout 30s ./...
```

```text
=== RUN   TestTimeAndTimezone
--- PASS: TestTimeAndTimezone (0.00s)
=== RUN   TestQueries
--- PASS: TestQueries (0.00s)
=== RUN   TestContainsAny
--- PASS: TestContainsAny (0.00s)
=== RUN   TestDisplay
--- PASS: TestDisplay (0.00s)
=== RUN   TestOffsetBoundaries
--- PASS: TestOffsetBoundaries (0.00s)
=== RUN   TestMock100000
    main_test.go:144: 100000 messages, bytes=1400021, matches=3, summary verified
--- PASS: TestMock100000 (0.08s)
=== RUN   TestParallelLimit
--- PASS: TestParallelLimit (0.00s)
=== RUN   TestTimeRangeAndSnapshot
--- PASS: TestTimeRangeAndSnapshot (0.00s)
=== RUN   TestConsumerError
--- PASS: TestConsumerError (0.00s)
=== RUN   TestArguments
--- PASS: TestArguments (0.00s)
=== RUN   TestReadOnlyConfig
--- PASS: TestReadOnlyConfig (0.00s)
=== RUN   TestKafkaReadOnlyProtocol
=== RUN   TestKafkaReadOnlyProtocol/no_timestamp_offset=false
    main_test.go:287: read-only Kafka protocol verified, fetches=2
=== RUN   TestKafkaReadOnlyProtocol/no_timestamp_offset=true
    main_test.go:287: read-only Kafka protocol verified, fetches=0
--- PASS: TestKafkaReadOnlyProtocol (0.10s)
    --- PASS: TestKafkaReadOnlyProtocol/no_timestamp_offset=false (0.10s)
    --- PASS: TestKafkaReadOnlyProtocol/no_timestamp_offset=true (0.00s)
PASS
ok  	kafscout	0.360s
```

`TestMock100000` 使用 Sarama 自带 `mocks.NewConsumer`（该版本没有 `sarama.NewMockConsumer`），预喂 100000 条消息，其中 3 条命中，断言：输出恰好 3 条、扫描 100000 条、字节 1400021、最后 offset 99999、汇总 hits=3。其他用例覆盖时间/tz、多 query、原始二进制匹配、转义/hex/截断、offsetsForTimes=-1、空分区、start/end 半开边界、8 分区 limit 取消、消费者错误及参数错误。

`TestKafkaReadOnlyProtocol` 在回环随机端口运行 Sarama MockBroker，通过真实 client/consumer 验证完整调用链；检查 Metadata 自动建 topic 标志关闭，且仅出现 MetadataRequest、OffsetRequest、FetchRequest；时间 offset=-1 时没有 FetchRequest。

普通核心测试额外执行 20 轮（仅作为并发稳定性检查，不替代 race）：

```sh
TMPDIR=/var/tmp/codex GOPROXY=off GOSUMDB=off \
  go test -mod=vendor -count=20 -timeout 60s \
  -run 'TestMock100000|TestParallelLimit|TestKafkaReadOnlyProtocol' ./...
```

```text
ok  	kafscout	3.582s
```

## 3. race：构建通过，运行受环境阻断

```sh
TMPDIR=/var/tmp/codex GOPROXY=off GOSUMDB=off CGO_ENABLED=1 \
  go build -mod=vendor -race -o /var/tmp/codex/kafscout-race .
```

输出：无，exit 0。

```sh
TMPDIR=/var/tmp/codex GOPROXY=off GOSUMDB=off CGO_ENABLED=1 \
  go test -mod=vendor -race -v -timeout 30s ./...
```

```text
FATAL: ThreadSanitizer: unsupported VMA range
FATAL: Found 47 - Supported 48
FAIL	kafscout	0.018s
FAIL
```

exit 1；ThreadSanitizer 初始化失败，尚未进入任何测试。已进一步尝试独立 race 测试二进制、`setarch aarch64 -R`、`qemu-aarch64 -L /`，均得到同一 VMA 错误；QEMU 预留 48 位虚拟空间也失败：

```text
qemu-aarch64: ...: Unable to find a guest_base to satisfy all guest address mapping requirements
  0000000000000000-0000ffffffffffff
```

这项仍需在兼容的 Linux amd64 或 48 位 VMA 的 ARM64 宿主执行上述 race 命令验证；没有修改/禁用 race runtime 或将普通测试冒充 race 通过。

## 4. vet、格式和只读审计

```sh
TMPDIR=/var/tmp/codex GOPROXY=off GOSUMDB=off go vet -mod=vendor ./...
gofmt -l .
git diff --check
```

三条命令输出均为空、exit 0。`gofmt -l .` 包括 vendor。自有代码无业务反序列化；匹配入口为 `bytes.Contains`。生产代码写 API 检索：

```sh
rg -n 'New(Sync|Async)?Producer|NewConsumerGroup|NewOffsetManager|\.Commit\(|\.MarkOffset\(' main.go
```

无输出（rg exit 1 表示无匹配）。配置明确 `Metadata.AllowAutoTopicCreation=false` 和 `Consumer.Offsets.AutoCommit.Enable=false`，不创建消费组、不提交 offset；协议级测试也验证只读请求集合。vendor 内的通用 producer/mock producer 定义为未调用的上游库代码。

## 5. CLI 实测（Linux amd64 交付文件）

宿主为 ARM64，使用已安装的 `qemu-x86_64` 执行 amd64 静态二进制：

```sh
qemu-x86_64 /var/tmp/codex/kafscout-bin/kafscout-linux-amd64 --help
qemu-x86_64 /var/tmp/codex/kafscout-bin/kafscout-linux-amd64 \
  --brokers 127.0.0.1:1 --topic test \
  --start '2026-10-22 00:00:00' --end '2026-10-22 01:00:00' \
  --query id123 --quiet
qemu-x86_64 /var/tmp/codex/kafscout-bin/kafscout-linux-amd64
```

各退出码：

```text
help exit=0
unreachable exit=1
invalid exit=2
```

连接失败输出：

```text
error: Kafka connection failed; check broker reachability, advertised listeners and SASL settings
```

参数缺失输出：

```text
argument error: --brokers, --topic, --start, --end and --query are required
```

帮助完整输出：

```text
Usage of kafscout:
  -brokers string
    	comma-separated Kafka brokers (required)
  -end string
    	exclusive local time or RFC3339 (required)
  -hex
    	display key/value as hexadecimal
  -limit int
    	maximum printed matches; 0 unlimited
  -max-bytes int
    	maximum raw bytes displayed per key/value (default 2097152)
  -partition int
    	partition; default all (default -1)
  -query value
    	raw UTF-8 substring(s), comma-separated; repeatable (required)
  -quiet
    	disable 10-second progress
  -sasl-pass string
    	SASL/PLAIN password (prefer --sasl-pass-env)
  -sasl-pass-env string
    	environment variable containing SASL password
  -sasl-user string
    	SASL/PLAIN username
  -start string
    	inclusive local time or RFC3339 (required)
  -topic string
    	topic (required)
  -tz string
    	IANA timezone (default "Asia/Shanghai")
```

## 6. Docker Compose 冒烟

```sh
./scripts/smoke.sh
```

```text
SKIP: apache/kafka:3.9.1 is not cached; image pulls are prohibited
```

exit 0（明确 SKIP，不算真实 Kafka 集成通过）。本机 Docker daemon/Compose 可用，但只缓存非 Kafka 镜像；遵守禁止联网拉镜像要求，没有执行 pull。脚本与 compose 文件已入库，`pull_policy: never`、`--pull never`，仅监听回环地址，默认不建 topic、不生产消息、不提交 offset。存在已有 fixture topic 时可传 `KAFSCOUT_SMOKE_TOPIC` 运行读扫描。

## 7. 静态交叉构建、文件格式和 SHA256

```sh
./scripts/build.sh
```

内部命令：

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -buildvcs=false -trimpath -ldflags='-s -w' -o /var/tmp/codex/kafscout-bin/kafscout-linux-amd64 .
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -mod=vendor -buildvcs=false -trimpath -ldflags='-s -w' -o /var/tmp/codex/kafscout-bin/kafscout-windows-amd64.exe .
```

输出（SHA256）：

```text
2bc45c878860015909461c12d2941569b91603ebb93e9ff3faea647971749aa3  kafscout-linux-amd64
ee548314f0c0a22621b2b363059c66aa3bb785df33914bad828f7acf205d83fb  kafscout-windows-amd64.exe
7c6d844ef354a180df1315ff68cca2d6e22b910ceeca3483e30e37f50525b1ea  README.md
```

```sh
file /var/tmp/codex/kafscout-bin/kafscout-linux-amd64 /var/tmp/codex/kafscout-bin/kafscout-windows-amd64.exe
readelf -l /var/tmp/codex/kafscout-bin/kafscout-linux-amd64
```

```text
/var/tmp/codex/kafscout-bin/kafscout-linux-amd64:       ELF 64-bit LSB executable, x86-64, version 1 (SYSV), statically linked, BuildID[sha1]=427a00b83e176ffd4003f8c2333a545d29f7dc89, stripped
/var/tmp/codex/kafscout-bin/kafscout-windows-amd64.exe: PE32+ executable for MS Windows 6.01 (console), x86-64, 8 sections
```

ELF program headers 无 INTERP 段；`file` 确认 statically linked。Windows 文件为 PE32+ x86-64；本机没有 Windows 运行环境，未声称实机运行验证。

## 8. 文档与交付封装

README 包含全部参数、内网三步落地、输出/退出码、性能参数及理由、只读机制、消息时间戳/乱序局限、compaction/大报文 max-bytes、key hash 定位留 v2、离线构建和实际验证限制。

提交后生成的交付路径：

- `/var/tmp/kafscout-v1.bundle`，完整 `feat/v1` 历史；SHA256 位于 `/var/tmp/kafscout-v1.bundle.sha256`。
- `/var/tmp/kafscout-bin.zip`，包含 Linux amd64、Windows amd64.exe、README 和 SHA256SUMS；SHA256 位于 `/var/tmp/kafscout-bin.zip.sha256`。
- `/var/tmp/kafscout-delivery.log`：封装、bundle verify、bundle 独立 clone 后离线构建/测试、zip 完整性/内部 SHA256 校验和最终工作区状态的实际输出。

交付后校验命令：

```sh
git bundle verify /var/tmp/kafscout-v1.bundle
sha256sum -c /var/tmp/kafscout-v1.bundle.sha256
sha256sum -c /var/tmp/kafscout-bin.zip.sha256
unzip -t /var/tmp/kafscout-bin.zip
git status --short --branch
```

最终封装日志放在仓库外，避免将包含自身提交的 bundle 哈希写回提交导致循环。所有 git 变更仅位于本独立仓库；构建及交付产物位于指定 `/var/tmp` 路径。
