package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

func testOptions() options {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	start := time.Date(2026, 10, 22, 0, 0, 0, 0, loc)
	return options{topic: "test", queries: queryFlags{"needle"}, start: start, end: start.Add(time.Hour), loc: loc, maxBytes: 2 * 1024 * 1024, quiet: true}
}
func TestTimeAndTimezone(t *testing.T) {
	loc, e := time.LoadLocation("Asia/Shanghai")
	if e != nil {
		t.Fatal(e)
	}
	a, e := parseTime("2026-10-22 00:00:00", loc)
	if e != nil {
		t.Fatal(e)
	}
	b, e := parseTime("2026-10-21T16:00:00Z", loc)
	if e != nil || !a.Equal(b) {
		t.Fatalf("timezone mismatch %v %v", a, b)
	}
	if _, e = parseTime("2026-02-30 00:00:00", loc); e == nil {
		t.Fatal("invalid date accepted")
	}
}
func TestQueries(t *testing.T) {
	var q queryFlags
	if e := q.Set("a,b,c"); e != nil {
		t.Fatal(e)
	}
	if e := q.Set("中文"); e != nil {
		t.Fatal(e)
	}
	if q.String() != "a,b,c,中文" {
		t.Fatal(q)
	}
	for _, s := range []string{"", ",a", "a,", "a,,b"} {
		if e := q.Set(s); e == nil {
			t.Fatalf("accepted %q", s)
		}
	}
}
func TestContainsAny(t *testing.T) {
	for _, tc := range []struct {
		value   []byte
		queries [][]byte
		want    bool
	}{
		{[]byte{0, 1, 255, 2}, [][]byte{{255, 2}}, true},
		{[]byte("业务id123"), [][]byte{[]byte("absent"), []byte("id123")}, true},
		{[]byte("id123"), [][]byte{[]byte("ID123")}, false},
		{nil, [][]byte{[]byte("x")}, false},
		{[]byte("anything"), nil, false},
	} {
		if got := containsAny(tc.value, tc.queries); got != tc.want {
			t.Fatalf("got %v want %v", got, tc.want)
		}
	}
}
func TestDisplay(t *testing.T) {
	for _, tc := range []struct {
		in   string
		max  int
		hex  bool
		want string
	}{
		{"中文 hello", 100, false, "中文 hello"},
		{"a\n\x00\x1b", 100, false, `"a\n\x00\x1b"`},
		{string([]byte{255, 'a'}), 100, false, `"\xffa"`},
		{"abcdef", 3, false, "abc [truncated: 3/6 bytes]"},
		{"a\x00", 10, true, "6100"},
		{"abcdef", 2, true, "6162 [truncated: 2/6 bytes]"},
		{"中", 1, false, `"\xe4" [truncated: 1/3 bytes]`},
		{"", 10, false, ""},
	} {
		if got := display([]byte(tc.in), tc.max, tc.hex); got != tc.want {
			t.Errorf("%q got %q want %q", tc.in, got, tc.want)
		}
	}
}
func TestOffsetBoundaries(t *testing.T) {
	for _, tc := range []struct {
		timed, oldest, newest int64
		ok, bad               bool
		start                 int64
	}{
		{-1, 4, 10, false, false, 0}, {10, 4, 10, false, false, 0}, {4, 4, 4, false, false, 0},
		{0, 4, 10, true, false, 4}, {7, 4, 10, true, false, 7}, {-2, 4, 10, false, true, 0}, {7, 10, 4, false, true, 0},
	} {
		p, ok, e := planOffset(2, tc.timed, tc.oldest, tc.newest)
		if ok != tc.ok || (e != nil) != tc.bad || (ok && (p.start != tc.start || p.stop != tc.newest)) {
			t.Fatalf("%+v => %+v %v %v", tc, p, ok, e)
		}
	}
}
func TestMock100000(t *testing.T) {
	o := testOptions()
	cfg := kafkaConfig(o)
	cfg.ChannelBufferSize = 100000
	c := mocks.NewConsumer(t, cfg)
	defer c.Close()
	pc := c.ExpectConsumePartition(o.topic, 0, 0).ExpectMessagesDrainedOnClose()
	var totalBytes int64
	for i := 0; i < 100000; i++ {
		v := []byte("plain-payload")
		if i == 7 || i == 50000 || i == 99999 {
			v = []byte("prefix-needle-suffix")
		}
		totalBytes += int64(len(v) + 1)
		pc.YieldMessage(&sarama.ConsumerMessage{Value: v, Key: []byte("k"), Timestamp: o.start.Add(time.Second)})
	}
	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, e := scan(ctx, o, []plan{{0, 0, 100000}}, func(p int32, offset int64) (stream, error) { return c.ConsumePartition(o.topic, p, offset) }, &out, io.Discard)
	if e != nil {
		t.Fatal(e)
	}
	if r.Hits != 3 || len(r.Rows) != 1 || r.Rows[0].Messages != 100000 || r.Rows[0].Bytes != totalBytes || r.Rows[0].Offset != 99999 {
		t.Fatalf("bad summary %+v", r)
	}
	if strings.Count(out.String(), "[KEY:k]") != 3 {
		t.Fatal(out.String())
	}
	if e = printSummary(&out, r); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), fmt.Sprintf("scanned=100000 bytes=%d", totalBytes)) || !strings.Contains(out.String(), "hits=3") {
		t.Fatal(out.String())
	}
	t.Logf("100000 messages, bytes=%d, matches=3, summary verified", totalBytes)
}
func TestParallelLimit(t *testing.T) {
	o := testOptions()
	o.limit = 3
	cfg := kafkaConfig(o)
	cfg.ChannelBufferSize = 100
	c := mocks.NewConsumer(t, cfg)
	defer c.Close()
	var plans []plan
	for p := int32(0); p < 8; p++ {
		plans = append(plans, plan{p, 0, 100})
		pc := c.ExpectConsumePartition(o.topic, p, 0)
		for i := 0; i < 100; i++ {
			pc.YieldMessage(&sarama.ConsumerMessage{Timestamp: o.start, Value: []byte("needle")})
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var out bytes.Buffer
	r, e := scan(ctx, o, plans, func(p int32, offset int64) (stream, error) { return c.ConsumePartition(o.topic, p, offset) }, &out, io.Discard)
	if e != nil || r.Hits != 3 || !r.Limited || strings.Count(out.String(), "[KEY:") != 3 {
		t.Fatalf("%+v %v %s", r, e, out.String())
	}
}
func TestTimeRangeAndSnapshot(t *testing.T) {
	o := testOptions()
	cfg := kafkaConfig(o)
	cfg.ChannelBufferSize = 5
	c := mocks.NewConsumer(t, cfg)
	defer c.Close()
	pc := c.ExpectConsumePartition(o.topic, 0, 0)
	for _, stamp := range []time.Time{o.start.Add(-time.Millisecond), o.start, o.end.Add(-time.Millisecond), o.end, o.end.Add(time.Millisecond)} {
		pc.YieldMessage(&sarama.ConsumerMessage{Timestamp: stamp, Value: []byte("needle")})
	}
	var out bytes.Buffer
	r, e := scan(context.Background(), o, []plan{{0, 0, 5}}, func(p int32, off int64) (stream, error) { return c.ConsumePartition(o.topic, p, off) }, &out, io.Discard)
	if e != nil || r.Hits != 2 || r.Rows[0].Messages != 4 {
		t.Fatalf("%+v %v", r, e)
	}
	r, e = scan(context.Background(), o, []plan{{0, 9, 9}}, func(int32, int64) (stream, error) { t.Fatal("empty partition opened"); return nil, nil }, io.Discard, io.Discard)
	if e != nil || r.Rows[0].Messages != 0 {
		t.Fatalf("%+v %v", r, e)
	}
	var summary bytes.Buffer
	printSummary(&summary, r)
	if !strings.Contains(summary.String(), "NOT FOUND") {
		t.Fatal(summary.String())
	}
}
func TestConsumerError(t *testing.T) {
	o := testOptions()
	c := mocks.NewConsumer(t, kafkaConfig(o))
	defer c.Close()
	c.ExpectConsumePartition(o.topic, 0, 0).YieldError(errors.New("mock read failed"))
	_, e := scan(context.Background(), o, []plan{{0, 0, 2}}, func(p int32, off int64) (stream, error) { return c.ConsumePartition(o.topic, p, off) }, io.Discard, io.Discard)
	if e == nil || !strings.Contains(e.Error(), "mock read failed") {
		t.Fatal(e)
	}
}
func TestArguments(t *testing.T) {
	base := []string{"--brokers", "127.0.0.1:1", "--topic", "test", "--start", "2026-10-22 00:00:00", "--end", "2026-10-22 01:00:00", "--query", "a,b"}
	for _, extra := range [][]string{{"--tz", "not/a/zone"}, {"--limit", "-1"}, {"--partition", "-2"}, {"--max-bytes", "0"}, {"--sasl-user", "test"}, {"--start", "bad"}, {"--end", "2026-10-21 00:00:00"}, {"--partition", "2147483648"}} {
		args := append(append([]string{}, base...), extra...)
		if _, e := parseOptions(args, io.Discard); e == nil {
			t.Fatalf("accepted %v", extra)
		}
	}
	if o, e := parseOptions(base, io.Discard); e != nil || len(o.queries) != 2 {
		t.Fatalf("%+v %v", o, e)
	}
	if got := run(context.Background(), []string{"--help"}, io.Discard, io.Discard); got != 0 {
		t.Fatal(got)
	}
	if got := run(context.Background(), nil, io.Discard, io.Discard); got != 2 {
		t.Fatal(got)
	}
}
func TestReadOnlyConfig(t *testing.T) {
	c := kafkaConfig(testOptions())
	if c.Metadata.AllowAutoTopicCreation || c.Consumer.Offsets.AutoCommit.Enable || !c.Consumer.Return.Errors || c.Consumer.Fetch.Max != 0 {
		t.Fatal("unsafe config")
	}
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
}

// Exercise the real client/consumer over a loopback Kafka protocol mock.
func TestKafkaReadOnlyProtocol(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("no_timestamp_offset=%t", missing), func(t *testing.T) {
			o := testOptions()
			broker := sarama.NewMockBroker(t, 0)
			defer broker.Close()
			o.brokers = broker.Addr()
			timed := int64(0)
			if missing {
				timed = -1
			}
			metadata := sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), 0).SetLeader(o.topic, 0, 0)
			offsets := sarama.NewMockOffsetResponse(t).SetOffset(o.topic, 0, sarama.OffsetOldest, 0).SetOffset(o.topic, 0, sarama.OffsetNewest, 4).SetOffset(o.topic, 0, o.start.UnixMilli(), timed)
			fetch := &sarama.FetchResponse{Version: 10}
			for i, stamp := range []time.Time{o.start, o.start.Add(time.Second), o.end, o.end.Add(time.Second)} {
				fetch.AddRecordWithTimestamp(o.topic, 0, sarama.StringEncoder("k"), sarama.StringEncoder("needle"), int64(i), stamp)
			}
			fetch.GetBlock(o.topic, 0).HighWaterMarkOffset = 4
			broker.SetHandlerByMap(map[string]sarama.MockResponse{"MetadataRequest": metadata, "OffsetRequest": offsets, "FetchRequest": sarama.NewMockWrapper(fetch)})
			var out bytes.Buffer
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if e := execute(ctx, o, &out, io.Discard); e != nil {
				t.Fatal(e)
			}
			if missing {
				if !strings.Contains(out.String(), "NOT FOUND") || !strings.Contains(out.String(), "scanned=0") {
					t.Fatal(out.String())
				}
			} else {
				if strings.Count(out.String(), "[KEY:k]") != 2 || !strings.Contains(out.String(), "hits=2") {
					t.Fatal(out.String())
				}
			}
			var fetches int
			for _, pair := range broker.History() {
				switch req := pair.Request.(type) {
				case *sarama.MetadataRequest:
					if req.AllowAutoTopicCreation {
						t.Fatal("auto topic creation requested")
					}
				case *sarama.OffsetRequest:
				case *sarama.FetchRequest:
					fetches++
				default:
					t.Fatalf("unexpected non-read request %T", req)
				}
			}
			if missing && fetches != 0 {
				t.Fatal("-1 must skip fetch")
			}
			if !missing && fetches == 0 {
				t.Fatal("no fetch performed")
			}
			t.Logf("read-only Kafka protocol verified, fetches=%d", fetches)
		})
	}
}
