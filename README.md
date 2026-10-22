# kafscout

**Fast, read-only Kafka substring scanner for time-windowed troubleshooting.**

One static binary. No JVM, no install, no consumer group, no offset commits.
Give it brokers + topic + time window + a substring (e.g. an order ID) and it
fans out across **all partitions in parallel**, matches on **raw bytes** (zero
deserialization), and prints every hit with partition, offset, and write
timestamp.

> ⚠️ AI-developed project. See [DISCLAIMER.md](DISCLAIMER.md). Chinese docs: [README.zh.md](README.zh.md)

## Why it exists

Ops teams at scale hit the same wall: a Kafka topic holds 100M–1B messages a
day, and you must find every occurrence of one value inside a time range.
Kafka has no server-side value filter, so *someone* must pull and scan the
bytes. JVM-based in-house tools usually scan serially per partition, allocate
an object per message, and then wonder why an hour of data takes half the day.

kafscout keeps the physics (you must read the window) and deletes everything
else:

| | traditional JVM scanner | kafscout |
|---|---|---|
| Parallelism | often serial per partition | goroutine per partition, shared client batching |
| Matching | deserialize each record → `String.contains` | `bytes.Contains` on the raw payload, zero allocation |
| Artifacts | jar + `lib/` (30 MB+) + JRE required | single static binary **7.6 MB**, nothing to install |
| Kafka footprint | consumer group, commits offsets | manual offsets, **no group, no commit, read-only APIs only** |
| Output | ad-hoc logs | `[time±tz] partition offset ts key value` + per-partition scan summary |

## Quick start

```sh
# one-off
./kafscout-linux-amd64 \
  --brokers kafka1:9092,kafka2:9092 \
  --topic order-events \
  --start "2026-10-22 00:00:00" --end "2026-10-22 01:00:00" \
  --query "ORD-8842117" > hits.txt

# or config-file style (mirrors a Java tool's properties file)
./kafscout-linux-amd64 --conf kafscout.conf --query "ORD-8842117"
```

```ini
# kafscout.conf  (key=value; see conf.example.conf)
brokers = kafka1:9092,kafka2:9092
topic   = order-events
tz      = Asia/Shanghai
```

Exit codes: `0` done (prints `NOT FOUND` if zero hits), `1` connect/scan error
(`PARTIAL ... incomplete=true` — never a fake "not found"), `2` bad arguments.

## Features

- **Time-windowed scan**: `offsetsForTimes` seeks each partition to the start
  boundary; stops at `timestamp >= end`; never rewinds your real consumer groups.
- **Parallel by design**: every partition gets its own consumer goroutine.
- **Raw-byte matching**: compressed batches are inflated once by the client,
  then matched without any JSON/Avro/Protobuf parsing.
- **Multi-query**: `--query a,b,c`, first match wins per record.
- **Safety rails**: read-only API surface (Describe/Read ACLs suffice),
  per-record display truncation (`--max-bytes`), `--limit` early exit,
  stall detection that fails loudly instead of hanging forever.
- **Portable**: static Linux `amd64` binary and Windows `.exe`, IANA tzdata
  embedded; runs in air-gapped networks with zero dependencies.
- **SASL/PLAIN** supported; password via env var to keep secrets out of shell
  history and process lists.

## What it does NOT do (by design)

- No TLS/SCRAM/Kerberos yet (plain or SASL/PLAIN listeners only).
- No key-hash partition pruning (coming; today it scans values).
- No guarantee on non-monotonic `CreateTime` topics (see limitations).
- No server-side index — below ~100M msgs/day any scanner reads the window;
  if you need instant search daily, that's ClickHouse/Elastic territory.

## Build & test (offline)

```sh
go build -mod=vendor -trimpath -ldflags='-s -w' .
go test  -mod=vendor ./...   # includes a 100k-message mock scan with 3 planted hits
go vet   -mod=vendor ./... && gofmt -l .
./scripts/build.sh dist/     # cross-build linux+windows + SHA256SUMS
```

All dependencies vendored under `vendor/` (Apache-2.0 project; dep licenses in
`vendor/` and `licenses/` in release archives).

## Performance reality check

An hour of 100 GB traffic-bound scan is limited by your NIC and brokers'
disk, not the CPU — on a 10 GbE network expect single-digit minutes, on 1 GbE
tens of minutes. We publish **no benchmark multiples**: the mock suite proves
correctness; production numbers are yours to measure. Compare against `kcat`
per-partition runs if you want an independent baseline.

## Security & compliance notes

Read [DISCLAIMER.md](DISCLAIMER.md). Short version: authorized use only;
output contains raw message payloads which may include personal data —
protect your `hits.txt`.

## Origin story

This tool was **developed end-to-end by AI agents**: specifications and
acceptance gates written by an AI project manager (running on a Raspberry Pi),
code generated and iterated by OpenAI Codex under strict red-line taskbooks
(no credentials, no endpoints, no production writes, independent re-verification
before merge). Human oversight: the whole pipeline is published; the human
decides what ships. Yes, this README was written by the same machine.

## License

Apache-2.0. Third-party attributions in `vendor/` and `licenses/`.
