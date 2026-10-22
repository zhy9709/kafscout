# kafscout --conf 验收记录

日期：2026-10-22；分支：feat/v1。先读取 README.md/main.go 后实施。所有临时文件位于 /var/tmp/codex；模块与校验服务关闭，使用仓库 vendor，未下载依赖或访问真实 Kafka。原有 mock 协议测试使用本地回环端口。

## 功能范围

支持全部 15 个配置键，显式 CLI > conf > 默认值；文件/CLI 混合提供必填项；CLI query 整体替换文件 query。支持整行 # 注释、空行、CRLF，拒绝未知键、重复键、非法行。配置 sasl_pass 提醒使用 sasl_pass_env，提醒不含密码；保留密码来源互斥规则。未修改 Simple Consumer、bytes.Contains、分区并行、无 group / 无 commit 行为。

新增 conf.example.conf 仅包含占位端点与凭据；README 增加 Java 配置迁移字段表，并明确 group.id 不需要。

## 验证命令及输出

所有 Go 命令环境：

```sh
export TMPDIR=/var/tmp/codex GOPROXY=off GOSUMDB=off
```

```console
$ go vet -mod=vendor ./...
# 无输出，exit 0
$ gofmt -l .
# 零输出（0 bytes），exit 0
$ go test -mod=vendor -count=1 -v ./...
=== RUN   TestConfParser
--- PASS: TestConfParser (0.00s)
=== RUN   TestConfOptions
--- PASS: TestConfOptions (0.00s)
=== RUN   TestConfMixedRequiredAndDefaults
--- PASS: TestConfMixedRequiredAndDefaults (0.00s)
=== RUN   TestConfErrorsExitTwo
=== RUN   TestConfErrorsExitTwo/bad-line
=== RUN   TestConfErrorsExitTwo/unknown_key=value
=== RUN   TestConfErrorsExitTwo/hex=perhaps
=== RUN   TestConfErrorsExitTwo/quiet=perhaps
=== RUN   TestConfErrorsExitTwo/limit=abc
=== RUN   TestConfErrorsExitTwo/limit=-1
=== RUN   TestConfErrorsExitTwo/partition=-2
=== RUN   TestConfErrorsExitTwo/max_bytes=0
=== RUN   TestConfErrorsExitTwo/query=
=== RUN   TestConfErrorsExitTwo/query=a,,b
=== RUN   TestConfErrorsExitTwo/tz=bad-zone
=== RUN   TestConfErrorsExitTwo/start=bad-time
=== RUN   TestConfErrorsExitTwo/sasl_pass_env=KAFSCOUT_TEST_UNSET
--- PASS: TestConfErrorsExitTwo (0.00s)
    --- PASS: TestConfErrorsExitTwo/bad-line (0.00s)
    --- PASS: TestConfErrorsExitTwo/unknown_key=value (0.00s)
    --- PASS: TestConfErrorsExitTwo/hex=perhaps (0.00s)
    --- PASS: TestConfErrorsExitTwo/quiet=perhaps (0.00s)
    --- PASS: TestConfErrorsExitTwo/limit=abc (0.00s)
    --- PASS: TestConfErrorsExitTwo/limit=-1 (0.00s)
    --- PASS: TestConfErrorsExitTwo/partition=-2 (0.00s)
    --- PASS: TestConfErrorsExitTwo/max_bytes=0 (0.00s)
    --- PASS: TestConfErrorsExitTwo/query= (0.00s)
    --- PASS: TestConfErrorsExitTwo/query=a,,b (0.00s)
    --- PASS: TestConfErrorsExitTwo/tz=bad-zone (0.00s)
    --- PASS: TestConfErrorsExitTwo/start=bad-time (0.00s)
    --- PASS: TestConfErrorsExitTwo/sasl_pass_env=KAFSCOUT_TEST_UNSET (0.00s)
=== RUN   TestConfPasswordWarning
--- PASS: TestConfPasswordWarning (0.00s)
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
ok  	kafscout	0.191s
```

另按交付要求运行原样命令：

```console
$ go vet ./...
# 无输出，exit 0
$ gofmt -l .
# 零输出，exit 0
$ go test ./...
ok  kafscout  (cached)
$ git diff --check
# 无输出，exit 0
```

## 交叉编译及打包

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -buildvcs=false -trimpath -ldflags='-s -w' -o /var/tmp/codex/kafscout-conf-package/kafscout-linux-amd64 .
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -mod=vendor -buildvcs=false -trimpath -ldflags='-s -w' -o /var/tmp/codex/kafscout-conf-package/kafscout-windows-amd64.exe .
```

两项构建无输出，exit 0。file 检查输出（省略 BuildID）：

```text
kafscout-linux-amd64: ELF 64-bit LSB executable, x86-64, version 1 (SYSV), statically linked, stripped
kafscout-windows-amd64.exe: PE32+ executable for MS Windows 6.01 (console), x86-64, 8 sections
```

用 Python zipfile ZIP_DEFLATED（compresslevel=9）重新生成 /var/tmp/kafscout-bin.zip；包含两平台程序、README.md、conf.example.conf、SHA256SUMS，并保留上一交付包完整 licenses 目录。程序格式检查通过；本 ARM64 宿主未原生运行 amd64/Windows 程序。

```console
$ (cd /var/tmp/codex/kafscout-conf-package && sha256sum -c SHA256SUMS)
kafscout-linux-amd64: OK
kafscout-windows-amd64.exe: OK
README.md: OK
conf.example.conf: OK
$ unzip -t /var/tmp/kafscout-bin.zip
# 各文件均 OK
No errors detected in compressed data of /var/tmp/kafscout-bin.zip.
```

交付校验清单位于 /var/tmp/SHA256SUMS；包内 SHA256SUMS 校验程序及文档，外部清单另含 zip 自身校验值：

```text
a6945d0f4359f83f04fa70962f9d17d290c52b987a270519b1ac663af28e3af7  kafscout-linux-amd64
85ea118a2fe953157281618aca90614425057fe2c2d6f3c869e652116306a816  kafscout-windows-amd64.exe
8830213b9fc6510491b5bd4b463b56e0b96560ddc9026afb22ba7f884626efd1  README.md
01652c58b2f1d317900455b94b54f6e2f4f199090467a51f46e892e3b3ae84a1  conf.example.conf
c84b1bd576fb2d87d98d5e8ab2d5614687295bf694dd4e08d27513293c4c0258  kafscout-bin.zip
```
