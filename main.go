// kafscout is a read-only Kafka timestamp-range byte substring scanner.
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"
	"unicode"
	"unicode/utf8"

	"github.com/IBM/sarama"
)

type options struct {
	brokers, topic, startText, endText, tz, user, pass, passEnv string
	queries                                                     queryFlags
	partition                                                   int
	limit                                                       int64
	maxBytes                                                    int
	quiet, hex                                                  bool
	start, end                                                  time.Time
	loc                                                         *time.Location
}
type queryFlags []string

func (q *queryFlags) String() string { return strings.Join(*q, ",") }
func (q *queryFlags) Set(s string) error {
	for _, v := range strings.Split(s, ",") {
		if v == "" {
			return errors.New("query must not contain empty values")
		}
		*q = append(*q, v)
	}
	return nil
}
func parseTime(s string, loc *time.Location) (time.Time, error) {
	if t, e := time.Parse(time.RFC3339Nano, s); e == nil {
		return t, nil
	}
	return time.ParseInLocation("2006-01-02 15:04:05", s, loc)
}
func parseOptions(args []string, errOut io.Writer) (options, error) {
	o := options{}
	var confPath string
	f := flag.NewFlagSet("kafscout", flag.ContinueOnError)
	f.SetOutput(errOut)
	f.StringVar(&confPath, "conf", "", "key=value configuration file")
	f.StringVar(&o.brokers, "brokers", "", "comma-separated Kafka brokers (required)")
	f.StringVar(&o.topic, "topic", "", "topic (required)")
	f.StringVar(&o.startText, "start", "", "inclusive local time or RFC3339 (required)")
	f.StringVar(&o.endText, "end", "", "exclusive local time or RFC3339 (required)")
	f.Var(&o.queries, "query", "raw UTF-8 substring(s), comma-separated; repeatable (required)")
	f.StringVar(&o.tz, "tz", "Asia/Shanghai", "IANA timezone")
	f.IntVar(&o.partition, "partition", -1, "partition; default all")
	f.Int64Var(&o.limit, "limit", 0, "maximum printed matches; 0 unlimited")
	f.IntVar(&o.maxBytes, "max-bytes", 2*1024*1024, "maximum raw bytes displayed per key/value")
	f.StringVar(&o.user, "sasl-user", "", "SASL/PLAIN username")
	f.StringVar(&o.pass, "sasl-pass", "", "SASL/PLAIN password (prefer --sasl-pass-env)")
	f.StringVar(&o.passEnv, "sasl-pass-env", "", "environment variable containing SASL password")
	f.BoolVar(&o.quiet, "quiet", false, "disable 10-second progress")
	f.BoolVar(&o.hex, "hex", false, "display key/value as hexadecimal")
	if e := f.Parse(args); e != nil {
		return o, e
	}
	if f.NArg() != 0 {
		return o, errors.New("unexpected positional arguments")
	}
	if confPath != "" {
		explicit := make(map[string]bool)
		f.Visit(func(v *flag.Flag) { explicit[v.Name] = true })
		file, e := os.Open(confPath)
		if e != nil {
			return o, fmt.Errorf("conf: %w", e)
		}
		defer file.Close()
		entries, e := parseConf(file)
		if e != nil {
			return o, e
		}
		for _, entry := range entries {
			if entry.key == "sasl-pass" {
				fmt.Fprintln(errOut, "warning: conf contains sasl_pass; prefer sasl_pass_env")
			}
			if !explicit[entry.key] {
				if e := f.Set(entry.key, entry.value); e != nil {
					return o, fmt.Errorf("conf line %d: invalid %s", entry.line, entry.key)
				}
			}
		}
	}
	if o.brokers == "" || o.topic == "" || o.startText == "" || o.endText == "" || len(o.queries) == 0 {
		return o, errors.New("--brokers, --topic, --start, --end and --query are required")
	}
	for _, b := range strings.Split(o.brokers, ",") {
		if strings.TrimSpace(b) == "" {
			return o, errors.New("empty broker")
		}
	}
	if o.partition < -1 || int64(o.partition) > 2147483647 || o.limit < 0 || o.maxBytes <= 0 {
		return o, errors.New("invalid partition, limit or max-bytes")
	}
	var e error
	o.loc, e = time.LoadLocation(o.tz)
	if e != nil {
		return o, fmt.Errorf("timezone: %w", e)
	}
	o.start, e = parseTime(o.startText, o.loc)
	if e != nil {
		return o, fmt.Errorf("start time: %w", e)
	}
	o.end, e = parseTime(o.endText, o.loc)
	if e != nil {
		return o, fmt.Errorf("end time: %w", e)
	}
	if !o.start.Before(o.end) {
		return o, errors.New("start must precede end")
	}
	if o.passEnv != "" {
		if o.pass != "" {
			return o, errors.New("use only one of --sasl-pass and --sasl-pass-env")
		}
		var ok bool
		o.pass, ok = os.LookupEnv(o.passEnv)
		if !ok || o.pass == "" {
			return o, errors.New("SASL password environment variable is unset or empty")
		}
	}
	if (o.user == "") != (o.pass == "") {
		return o, errors.New("SASL requires both username and password")
	}
	return o, nil
}
func containsAny(value []byte, queries [][]byte) bool {
	for _, q := range queries {
		if bytes.Contains(value, q) {
			return true
		}
	}
	return false
}
func display(b []byte, max int, asHex bool) string {
	n := len(b)
	if len(b) > max {
		b = b[:max]
	}
	var s string
	if asHex {
		s = hex.EncodeToString(b)
	} else {
		printable := utf8.Valid(b)
		if printable {
			for _, r := range string(b) {
				if !unicode.IsPrint(r) {
					printable = false
					break
				}
			}
		}
		if printable {
			s = string(b)
		} else {
			s = strconv.Quote(string(b))
		}
	}
	if n > max {
		s += fmt.Sprintf(" [truncated: %d/%d bytes]", max, n)
	}
	return s
}

type plan struct {
	partition   int32
	start, stop int64
}

// -1 from offsetsForTimes means no offset at/after the requested timestamp.
func planOffset(part int32, timed, oldest, newest int64) (plan, bool, error) {
	if timed == -1 {
		return plan{}, false, nil
	}
	if timed < 0 || oldest < 0 || newest < oldest {
		return plan{}, false, errors.New("invalid broker offset bounds")
	}
	if timed < oldest {
		timed = oldest
	}
	if timed >= newest {
		return plan{}, false, nil
	}
	return plan{part, timed, newest}, true, nil
}

type stream interface {
	Messages() <-chan *sarama.ConsumerMessage
	Errors() <-chan *sarama.ConsumerError
	Close() error
}
type openStream func(int32, int64) (stream, error)
type partitionStats struct {
	Partition       int32
	Messages, Bytes int64
	Offset          int64
	Watermark       time.Time
}

// Each partition owns its counter lock; unrelated consumers never contend.
type partitionProgress struct {
	mu    sync.Mutex
	stats partitionStats
}
type tracker struct{ rows map[int32]*partitionProgress }

func (t *tracker) update(p int32, m *sarama.ConsumerMessage) {
	row := t.rows[p]
	row.mu.Lock()
	defer row.mu.Unlock()
	row.stats.Messages++
	row.stats.Bytes += int64(len(m.Key) + len(m.Value))
	row.stats.Offset = m.Offset
	row.stats.Watermark = m.Timestamp
}
func (t *tracker) snapshot(plans []plan) []partitionStats {
	rows := make([]partitionStats, 0, len(plans))
	for _, p := range plans {
		row := t.rows[p.partition]
		row.mu.Lock()
		rows = append(rows, row.stats)
		row.mu.Unlock()
	}
	return rows
}

type result struct {
	Rows     []partitionStats
	Hits     int64
	Duration time.Duration
	Limited  bool
}

func scan(ctx context.Context, o options, plans []plan, open openStream, out, errOut io.Writer) (result, error) {
	began := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	t := tracker{rows: make(map[int32]*partitionProgress)}
	for _, p := range plans {
		t.rows[p.partition] = &partitionProgress{stats: partitionStats{Partition: p.partition, Offset: p.start - 1}}
	}
	queries := make([][]byte, len(o.queries))
	for i, q := range o.queries {
		queries[i] = []byte(q)
	}
	matches := make(chan *sarama.ConsumerMessage)
	failures := make(chan error, len(plans))
	var wg sync.WaitGroup
	for _, p := range plans {
		wg.Add(1)
		go func(p plan) {
			defer wg.Done()
			if p.start >= p.stop {
				return
			}
			pc, e := open(p.partition, p.start)
			if e != nil {
				failures <- fmt.Errorf("partition %d: %w", p.partition, e)
				cancel()
				return
			}
			defer pc.Close()
			messages, errs := pc.Messages(), pc.Errors()
			idle := time.NewTimer(30 * time.Second)
			defer idle.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-idle.C:
					failures <- fmt.Errorf("partition %d made no progress for 30s before snapshot end (check broker, retention/compaction or transaction control offsets); scan incomplete", p.partition)
					cancel()
					return
				case e, ok := <-errs:
					if !ok {
						errs = nil
						continue
					}
					if e != nil {
						failures <- fmt.Errorf("partition %d consume: %w", p.partition, e)
						cancel()
						return
					}
				case m, ok := <-messages:
					if !ok {
						if ctx.Err() == nil {
							failures <- fmt.Errorf("partition %d stream closed before snapshot end", p.partition)
							cancel()
						}
						return
					}
					if m == nil {
						continue
					}
					if !idle.Stop() {
						select {
						case <-idle.C:
						default:
						}
					}
					idle.Reset(30 * time.Second)
					if m.Offset >= p.stop {
						return
					}
					t.update(p.partition, m)
					if !m.Timestamp.Before(o.end) {
						return
					}
					if !m.Timestamp.Before(o.start) && containsAny(m.Value, queries) {
						select {
						case matches <- m:
						case <-ctx.Done():
							return
						}
					}
					// Snapshot upper bound avoids waiting forever on idle partitions.
					if m.Offset >= p.stop-1 {
						return
					}
				}
			}
		}(p)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(matches); close(done) }()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	r := result{}
	var outputErr error
	for matches != nil {
		select {
		case m, ok := <-matches:
			if !ok {
				matches = nil
				continue
			}
			if r.Limited || outputErr != nil {
				continue
			}
			_, e := fmt.Fprintf(out, "[%s UTC=%s] partition=%d offset=%d ts=%d [KEY:%s] %s\n", m.Timestamp.In(o.loc).Format(time.RFC3339Nano), m.Timestamp.UTC().Format(time.RFC3339Nano), m.Partition, m.Offset, m.Timestamp.UnixMilli(), display(m.Key, o.maxBytes, o.hex), display(m.Value, o.maxBytes, o.hex))
			if e != nil {
				outputErr = e
				cancel()
				continue
			}
			r.Hits++
			if o.limit > 0 && r.Hits >= o.limit {
				r.Limited = true
				cancel()
			}
		case <-ticker.C:
			if !o.quiet {
				for _, s := range t.snapshot(plans) {
					fmt.Fprintf(errOut, "progress partition=%d offset=%d watermark=%s scanned=%d bytes=%d\n", s.Partition, s.Offset, watermark(s.Watermark, o.loc), s.Messages, s.Bytes)
				}
			}
		}
	}
	<-done
	r.Rows = t.snapshot(plans)
	r.Duration = time.Since(began)
	if outputErr != nil {
		return r, outputErr
	}
	select {
	case e := <-failures:
		return r, e
	default:
	}
	if ctx.Err() != nil && !r.Limited {
		return r, ctx.Err()
	}
	return r, nil
}
func watermark(t time.Time, loc *time.Location) string {
	if t.IsZero() {
		return "unknown"
	}
	return t.In(loc).Format(time.RFC3339Nano)
}
func printSummary(out io.Writer, r result) error {
	for _, s := range r.Rows {
		if _, e := fmt.Fprintf(out, "SUMMARY partition=%d scanned=%d bytes=%d last_offset=%d\n", s.Partition, s.Messages, s.Bytes, s.Offset); e != nil {
			return e
		}
	}
	_, e := fmt.Fprintf(out, "SUMMARY duration=%s hits=%d limited=%t\n", r.Duration.Round(time.Millisecond), r.Hits, r.Limited)
	if e != nil {
		return e
	}
	if r.Hits == 0 {
		_, e = fmt.Fprintln(out, "NOT FOUND")
	}
	return e
}
func kafkaConfig(o options) *sarama.Config {
	c := sarama.NewConfig()
	c.Version = sarama.V2_1_0_0
	c.ClientID = "kafscout"
	c.Metadata.AllowAutoTopicCreation = false
	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.AutoCommit.Enable = false
	c.Consumer.Fetch.Min = 64 * 1024
	c.Consumer.Fetch.Default = 4 * 1024 * 1024
	c.Consumer.Fetch.Max = 0
	c.Consumer.MaxWaitTime = 100 * time.Millisecond
	c.Net.DialTimeout = 5 * time.Second
	c.Net.ReadTimeout = 10 * time.Second
	c.Net.WriteTimeout = 10 * time.Second
	c.Metadata.Retry.Max = 1
	c.Metadata.Retry.Backoff = 200 * time.Millisecond
	if o.user != "" {
		c.Net.SASL.Enable = true
		c.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		c.Net.SASL.User = o.user
		c.Net.SASL.Password = o.pass
	}
	return c
}
func execute(ctx context.Context, o options, out, errOut io.Writer) error {
	brokers := strings.Split(o.brokers, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	client, e := sarama.NewClient(brokers, kafkaConfig(o))
	if e != nil {
		return errors.New("Kafka connection failed; check broker reachability, advertised listeners and SASL settings")
	}
	defer client.Close()
	parts, e := client.Partitions(o.topic)
	if e != nil {
		return fmt.Errorf("topic metadata: %w", e)
	}
	if o.partition >= 0 {
		found := false
		for _, p := range parts {
			if p == int32(o.partition) {
				found = true
			}
		}
		if !found {
			return errors.New("requested partition does not exist")
		}
		parts = []int32{int32(o.partition)}
	}
	plans := make([]plan, 0, len(parts))
	for _, p := range parts {
		oldest, e := client.GetOffset(o.topic, p, sarama.OffsetOldest)
		if e != nil {
			return fmt.Errorf("partition %d oldest offset: %w", p, e)
		}
		newest, e := client.GetOffset(o.topic, p, sarama.OffsetNewest)
		if e != nil {
			return fmt.Errorf("partition %d newest offset: %w", p, e)
		}
		offset, e := client.GetOffset(o.topic, p, o.start.UnixMilli())
		if e != nil {
			return fmt.Errorf("partition %d offsetsForTimes: %w", p, e)
		}
		bound, ok, e := planOffset(p, offset, oldest, newest)
		if e != nil {
			return e
		}
		if !ok {
			bound = plan{p, newest, newest}
		}
		plans = append(plans, bound)
	}
	consumer, e := sarama.NewConsumerFromClient(client)
	if e != nil {
		return fmt.Errorf("create consumer: %w", e)
	}
	defer consumer.Close()
	r, e := scan(ctx, o, plans, func(p int32, offset int64) (stream, error) { return consumer.ConsumePartition(o.topic, p, offset) }, out, errOut)
	if e != nil {
		for _, row := range r.Rows {
			fmt.Fprintf(out, "PARTIAL partition=%d scanned=%d bytes=%d last_offset=%d\n", row.Partition, row.Messages, row.Bytes, row.Offset)
		}
		fmt.Fprintf(out, "PARTIAL duration=%s hits=%d incomplete=true\n", r.Duration.Round(time.Millisecond), r.Hits)
		return e
	}
	se := printSummary(out, r)
	return se
}
func run(ctx context.Context, args []string, out, errOut io.Writer) int {
	o, e := parseOptions(args, errOut)
	if errors.Is(e, flag.ErrHelp) {
		return 0
	}
	if e != nil {
		fmt.Fprintln(errOut, "argument error:", e)
		return 2
	}
	if e = execute(ctx, o, out, errOut); e != nil {
		fmt.Fprintln(errOut, "error:", e)
		return 1
	}
	return 0
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
