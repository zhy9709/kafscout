# kafscout

Kafka 时间段子串快扫器。Go 1.22+，唯一直接第三方依赖为 BSD-2-Clause 的 IBM/sarama（v1.43.3），其传递依赖及许可证随 vendor 入库。交付 Linux amd64 静态 ELF 和 Windows amd64 单文件 EXE；已内嵌 IANA 时区库，目标机无需 Go、Java 或时区文件。

## 公司内网落地三步

1. 拷贝对应平台的二进制到目标机器，核对 SHA256；Linux 执行 `chmod +x kafscout-linux-amd64`。
2. 运行以下命令，将 `$BROKERS`、`$TOPIC` 换成运行时参数（示例不包含任何内网端点或凭据）。
3. 在 stdout 查看命中和汇总，或重定向到结果文件；进度和错误在 stderr。Windows PowerShell 使用 `./kafscout-windows-amd64.exe` 和 `$env:BROKERS` 等环境变量。

```sh
./kafscout-linux-amd64 --brokers "$BROKERS" --topic "$TOPIC" \
  --start '2026-10-22 00:00:00' --end '2026-10-22 01:00:00' \
  --query 'id123' --tz Asia/Shanghai > result.txt
```

## 参数

| 参数 | 行为 |
| --- | --- |
| `--conf <path>` | 可选 key=value 配置文件；显式命令行 > 文件 > 内置默认 |
| `--brokers`、`--topic` | 必填；broker 逗号分隔，地址全部运行时提供 |
| `--start`、`--end` | 必填；`YYYY-MM-DD HH:MM:SS` 或 RFC3339（可带小数秒），范围 `[start,end)` |
| `--query` | 必填；UTF-8 原始字节、大小写敏感；`--query a,b,c` 或多次传入；任一命中即算一次；拒绝空项；空格作为查询内容保留，暂不支持查询中含逗号 |
| `--partition` | 默认全部分区；指定非负分区号则只扫该分区 |
| `--limit` | 最多打印的命中数；默认 0 不限；到达后取消所有消费者，汇总 `limited=true` |
| `--tz` | 默认 `Asia/Shanghai`；无偏移的输入时间按此解释，带偏移 RFC3339 按其偏移解释 |
| `--sasl-user`、`--sasl-pass` | 可选 SASL/PLAIN；必须成对；密码优先改用下项，避免出现在命令行历史/进程列表 |
| `--sasl-pass-env VAR` | 从指定环境变量读取密码，与 `--sasl-pass` 互斥；变量必须存在且非空 |
| `--max-bytes` | 默认 2097152（2 MiB）；每个 key/value 最多展示这么多原始字节，必须正数；不限制读取或匹配长度 |
| `--hex` | key/value 以十六进制展示 |
| `--quiet` | 关闭每 10 秒 stderr 进度 |
| `--help` | 完整帮助，exit 0 |

SASL 示例：由外部安全机制填充变量后，传 `--sasl-user "$KAFKA_USER" --sasl-pass-env KAFKA_PASSWORD`。目前支持无认证或 SASL/PLAIN 的明文 Kafka listener；不提供 TLS、SCRAM 或 Kerberos 配置入口。

## 输出与退出码

逐条命中立即写入 stdout，无应用层输出缓冲，跨分区不保证顺序：

```text
[2026-10-22T00:00:01+08:00 UTC=2026-10-21T16:00:01Z] partition=0 offset=42 ts=1792598401000 [KEY:order] payload-id123
SUMMARY partition=0 scanned=100000 bytes=1400021 last_offset=99999
SUMMARY duration=1.23s hits=3 limited=false
```

每行包含本地和 UTC 时间、毫秒时间戳、分区、offset、key、value。默认可打印 UTF-8 原样输出；含控制字符或无效 UTF-8 时使用 Go 字符串引号及转义，避免一条消息破坏多行输出；`--hex` 可逐字节查看。超长显示 `[truncated: 展示字节/原始字节 bytes]`。截断可能切开 UTF-8 字符，此时保留字节并转义，原始消息仍完整参与匹配。需要完整大报文时调大 `--max-bytes`。

成功 exit 0；无命中同时打印 `NOT FOUND`。参数错误 exit 2；连接、元数据、消费或输出失败 exit 1。扫描途中出错/中断会输出 `PARTIAL ... incomplete=true`，不将其报告为 `NOT FOUND`。初始连接失败会提示检查 broker 可达性、advertised listeners 和 SASL，不输出凭据。

汇总按分区统计实际检查的消息数和 `len(key)+len(value)` 字节数（解压后的负载，不含协议开销），包括触发结束的边界消息；`hits` 为已成功输出的条数。`limit` 取消传播期间其他分区可能已检查更多消息。进度中的 watermark 是该分区最近检查消息的时间，不是全局有序水位；尚未检查消息时为 `unknown`。

## 为什么快，以及扫描边界

- 每个分区独立 goroutine/PartitionConsumer，共享 Sarama client 的连接和批量 fetch；分区间计数使用独立锁，无逐条全局锁。无需串行扫完一个分区再扫下一个。
- broker 的 offsetsForTimes（Sarama `GetOffset(timestamp)`）定位起点，`-1` 表示没有满足时间的记录，直接跳过该分区；保留已删除起点后的最早可用 offset。读到 `timestamp >= end` 停止。
- 直接对 `ConsumerMessage.Value` 做 `bytes.Contains`，不反序列化 JSON、不创建业务对象。Kafka 压缩批次仍需由客户端解压，无法跳过这部分工作。
- `Consumer.Fetch.Min=64 KiB`：减少小批请求；`Fetch.Default=4 MiB`：提高单分区批量；`MaxWaitTime=100ms`：低流量分区不久等；`Fetch.Max=0`：允许响应随大报文增长，不因显示上限而漏匹配。Sarama 全局响应保护上限仍为默认约 100 MiB。内存随分区数量、压缩批大小和输出背压增长，不承诺固定内存或吞吐量。
- 查询开始前记录各分区最新 offset（exclusive），只检查这份分区快照内的数据；普通分区到快照末尾即结束，不等未来新消息。各分区快照不是一个跨分区原子时刻。broker 预取可能携带越界记录；应用不匹配/输出这些记录，并在遇到 end 边界时立即关闭该消费者。
- 使用手动 offset 的 `NewConsumerFromClient` / `ConsumePartition`；禁用 AutoCommit、禁用自动建 topic。不调用 producer、consumer group、OffsetManager、commit 或管理写 API。仅需 topic Describe/Read 权限。

## 局限

- 时间筛选使用 Kafka 消息时间戳，毫秒精度；是否代表 broker 写入时间取决于 topic 的 `message.timestamp.type`。`LogAppendTime` 才是 broker 追加时间；`CreateTime` 是生产者提供的时间，可能偏移、乱序。没有时间戳的旧消息不适用。
- 按要求在首条 `timestamp >= end` 时停止，依赖每分区时间戳基本单调；CreateTime 严重乱序可能漏掉后续时间回退的记录。这一模式不能保证任意乱序数据的穷尽性。
- 只查询当前保留日志，已被 retention/compaction 删除的历史不可恢复。log 压缩（compaction）topic 的大 value 请调大 `--max-bytes`，匹配不受显示截断影响。Kafka 批压缩则由 Sarama 自动解压。
- compaction、事务控制记录或保留策略变更可能导致末尾 offset 没有用户消息；若到达快照末尾前连续 30 秒无进展，报告扫描不完整并 exit 1，避免静默误报无命中或永久等待。broker 暂停也会触发这一保护；恢复后可重跑。
- 不支持按 key hash 定位分区，留给 v2；当前只匹配 value，不匹配 key。
- 未在真实大规模集群测量吞吐，不提供性能倍数承诺。Windows/amd64 在此 ARM64 Linux 环境只做交叉构建与文件格式检查。

## 离线构建与验证

首次依赖准备（仅此阶段需要 Go 模块代理）：

```sh
export TMPDIR=/var/tmp/codex
mkdir -p "$TMPDIR"
go mod tidy
go mod vendor
rm -f vendor/github.com/IBM/sarama/Vagrantfile  # 非构建文件，移除上游私网部署示例
gofmt -w vendor  # 统一旧版传递依赖的格式；不改变逻辑
```

vendor 已入库，以下无需联网：

```sh
export TMPDIR=/var/tmp/codex GOPROXY=off GOSUMDB=off
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -buildvcs=false -trimpath -ldflags='-s -w' -o kafscout-linux-amd64 .
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -mod=vendor -buildvcs=false -trimpath -ldflags='-s -w' -o kafscout-windows-amd64.exe .
go test -mod=vendor -v ./...
go vet -mod=vendor ./...
gofmt -l .
CGO_ENABLED=1 go build -mod=vendor -race -o "$TMPDIR/kafscout-race" .
CGO_ENABLED=1 go test -mod=vendor -race -v ./...
./scripts/smoke.sh
```

race 工具需要本机 C 工具链；交付二进制仍用 `CGO_ENABLED=0`。`TestMock100000` 使用 Sarama 提供的 `mocks.NewConsumer`（该版本没有 `sarama.NewMockConsumer`），喂入 100000 条消息/3 条命中，校验输出、扫描数、字节数和汇总；另测 8 分区并发 limit、错误退出和时间边界。

`scripts/compose.yaml` 使用 Apache 官方 Kafka 单节点 KRaft 镜像，仅绑定回环端口；`smoke.sh` 强制 `--pull never`，无本地镜像则明确跳过。默认检查 broker 可用和不存在 topic 的只读元数据失败；可用 `KAFSCOUT_SMOKE_TOPIC` 指向已有测试数据 topic，执行时间段消费。脚本不创建 topic、不生产消息、不提交 offset。实测记录见 `VALIDATION_KAFSCOUT.md`。当前宿主为 16 KiB 页/47 位 VMA 的 ARM64 Linux，Go 1.24.4 的 ThreadSanitizer 在执行测试前报 `unsupported VMA range`；race 构建通过，race 运行验收尚未通过，需在兼容的宿主上补跑，不能将普通测试视为替代。

## Java 工具配置迁移

```sh
./kafscout-linux-amd64 --conf conf.example.conf --query 'your-query' --quiet
```

必填 brokers/topic/start/end/query 可分散在文件和命令行中提供。文件为 UTF-8 的 `key=value`，每行一键；空行和以 `#` 开头的行忽略。键和值两端空白会去掉，不解析引号、转义、环境变量替换或行尾注释；值内的 `#` 和 `=` 原样保留。未知键、重复键、非法行或无效的最终参数 exit 2。每行最多 1 MiB。命令行重复 `--query` 继续累加，但只要显式指定 query，就整体覆盖文件 query；布尔覆盖请用 `--hex=false`、`--quiet=false`。

| Java 工具字段/用途 | kafscout 配置键 | 命令行 |
| --- | --- | --- |
| bootstrap.servers | brokers | --brokers |
| topic | topic | --topic |
| 起止时间 | start / end | --start / --end |
| 查询词（逗号多值） | query | --query |
| 分区 | partition | --partition |
| 输出条数上限 | limit | --limit |
| 时区 | tz | --tz |
| 单个 key/value 展示字节上限 | max_bytes | --max-bytes |
| SASL/PLAIN 用户名 | sasl_user | --sasl-user |
| SASL/PLAIN 密码 | sasl_pass | --sasl-pass |
| 密码环境变量名称 | sasl_pass_env | --sasl-pass-env |
| 十六进制展示 | hex | --hex |
| 关闭进度 | quiet | --quiet |
| group.id | **不需要——kafscout 不建消费组不提交 offset** | 无；勿写入配置文件 |

Java 的 `sasl.jaas.config` 不能直接复制；将用户名迁移到 `sasl_user`，优先通过 `sasl_pass_env` 指定已有密码环境变量名称。文件中出现 `sasl_pass` 会在 stderr 提示建议使用 `sasl_pass_env`，不打印密码。两种密码来源仍互斥（包括跨文件/命令行混用）；切换来源时可显式传空值清除另一项，例如 `--sasl-pass= --sasl-pass-env YOUR_KAFKA_PASSWORD`。配置值含两端空白时请使用命令行提供。示例文件见 `conf.example.conf`，所有端点和凭据均为占位符。
