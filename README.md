# kafscout

从 Kafka 海量消息里，按时间段快速找出包含某个订单号、设备号或关键字的消息。

不用安装 Java，不创建消费组，不提交 offset；下载一个文件就能查。

[English](README.en.md) · [完整中文技术手册](README.zh.md)

## 什么时候用它

比如线上有人问：

> “昨天 14:00 到 15:00，订单 `ORD-8842117` 到底有没有进 Kafka？”

Kafka 本身不能直接按消息内容搜索。传统脚本可能一个分区一个分区慢慢扫，kafscout 会并行扫描所有分区，只读数据，不影响原有消费者进度。

如果你平时不维护 Kafka，这个工具确实用不上；它就是给排查 Kafka 消息的开发和运维人员准备的。

## 三步使用

### 1. 下载

在 [Releases](https://github.com/zhy9709/kafscout/releases/latest) 下载压缩包，选择 Linux 或 Windows 版本。

### 2. 填入 Kafka 地址和查询条件

```bash
./kafscout-linux-amd64 \
  --brokers kafka1:9092,kafka2:9092 \
  --topic order-events \
  --start "2026-10-22 14:00:00" \
  --end "2026-10-22 15:00:00" \
  --query "ORD-8842117"
```

### 3. 看结果

找到时会打印消息所在的分区、offset、时间和内容；没找到会明确显示 `NOT FOUND`。如果扫描中途失败，它会显示 `PARTIAL ... incomplete=true`，不会把“没扫完”冒充成“没找到”。

## 它会不会影响线上 Kafka？

设计上只使用读取接口：

- 不生产消息
- 不删除或修改数据
- 不创建消费组
- 不提交 offset
- 不改变现有消费者进度

它仍会读取指定时间段的数据，因此大范围扫描会消耗 Kafka 网络和磁盘资源。生产环境建议先查较短时间段，并使用只读账号。

## 当前支持

- Linux amd64、Windows amd64 单文件运行
- 全分区并行扫描
- 指定时间范围和多个关键词
- 无认证或 SASL/PLAIN
- 输出条数限制、进度显示和超长消息截断

暂不支持 TLS、SCRAM、Kerberos。完整参数、边界条件、构建方法和验证记录请看[完整中文技术手册](README.zh.md)。

## License

Apache-2.0
