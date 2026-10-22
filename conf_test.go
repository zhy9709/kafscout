package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const baseConf = "brokers=your-broker:9092\ntopic=your-topic\nstart=2026-10-22 00:00:00\nend=2026-10-22 01:00:00\nquery=file-a,file-b\n"

func writeConf(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "test.conf")
	if err := os.WriteFile(p, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestConfParser(t *testing.T) {
	entries, err := parseConf(strings.NewReader(" # comment\r\n\r\n topic = your-topic \r\nquery=a#b=c,d\n"))
	if err != nil || len(entries) != 2 || entries[0].value != "your-topic" || entries[1].value != "a#b=c,d" {
		t.Fatalf("%+v %v", entries, err)
	}
	for _, bad := range []string{"topic", "=secret", "group.id=unused", "conf=other", "max-bytes=1", "topic=a\ntopic=b", strings.Repeat("x", 1024*1024+1)} {
		if _, err := parseConf(strings.NewReader(bad)); err == nil {
			t.Errorf("accepted invalid input (%d bytes)", len(bad))
		}
	}
}

func TestConfOptions(t *testing.T) {
	p := writeConf(t, baseConf+"partition=2\nlimit=12\ntz=UTC\nmax_bytes=42\nhex=true\nquiet=true\nsasl_user=your-user\nsasl_pass_env=KAFSCOUT_TEST_PASS\n")
	t.Setenv("KAFSCOUT_TEST_PASS", "your-password")
	o, err := parseOptions([]string{"--conf", p}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if o.brokers != "your-broker:9092" || o.topic != "your-topic" || o.partition != 2 || o.limit != 12 || o.tz != "UTC" || o.maxBytes != 42 || !o.hex || !o.quiet || o.user != "your-user" || o.pass != "your-password" || !reflect.DeepEqual(o.queries, queryFlags{"file-a", "file-b"}) {
		t.Fatalf("unexpected configuration")
	}
	o, err = parseOptions([]string{"--topic=cli-topic", "--query=cli-a,cli-b", "--conf", p, "--query=cli-c", "--partition=-1", "--limit=0", "--hex=false", "--quiet=false", "--max-bytes=7", "--tz=Asia/Shanghai"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if o.topic != "cli-topic" || o.partition != -1 || o.limit != 0 || o.hex || o.quiet || o.maxBytes != 7 || o.tz != "Asia/Shanghai" || !reflect.DeepEqual(o.queries, queryFlags{"cli-a", "cli-b", "cli-c"}) {
		t.Fatalf("override failed")
	}
}

func TestConfMixedRequiredAndDefaults(t *testing.T) {
	p := writeConf(t, "brokers=your-broker:9092\nstart=2026-10-22 00:00:00\nend=2026-10-22 01:00:00\n")
	o, err := parseOptions([]string{"--conf=" + p, "--topic=your-topic", "--query=cli"}, io.Discard)
	if err != nil || o.partition != -1 || o.limit != 0 || o.maxBytes != 2097152 || o.hex || o.quiet || o.tz != "Asia/Shanghai" {
		t.Fatalf("defaults/mixed: %v", err)
	}
	if _, err = parseOptions([]string{"--conf", p, "--query=cli"}, io.Discard); err == nil {
		t.Fatal("missing topic accepted")
	}
	p = writeConf(t, baseConf)
	if _, err = parseOptions([]string{"--conf", p, "--topic="}, io.Discard); err == nil {
		t.Fatal("explicit empty topic must override file")
	}
}

func TestConfErrorsExitTwo(t *testing.T) {
	for _, extra := range []string{"bad-line", "unknown_key=value", "hex=perhaps", "quiet=perhaps", "limit=abc", "limit=-1", "partition=-2", "max_bytes=0", "query=", "query=a,,b", "tz=bad-zone", "start=bad-time", "sasl_pass_env=KAFSCOUT_TEST_UNSET"} {
		t.Run(extra, func(t *testing.T) {
			// Replace existing entries where necessary, retaining one key per line.
			key, _, _ := strings.Cut(extra, "=")
			var content strings.Builder
			for _, line := range strings.Split(baseConf, "\n") {
				if !strings.HasPrefix(line, key+"=") {
					content.WriteString(line + "\n")
				}
			}
			p := writeConf(t, content.String()+extra+"\n")
			if code := run(context.Background(), []string{"--conf", p}, io.Discard, io.Discard); code != 2 {
				t.Fatalf("exit %d", code)
			}
		})
	}
	p := filepath.Join(t.TempDir(), "absent.conf")
	if code := run(context.Background(), []string{"--conf", p}, io.Discard, io.Discard); code != 2 {
		t.Fatalf("missing file exit %d", code)
	}
}

func TestConfPasswordWarning(t *testing.T) {
	p := writeConf(t, baseConf+"sasl_user=your-user\nsasl_pass=your-secret=a#b\n")
	var stderr bytes.Buffer
	o, err := parseOptions([]string{"--conf", p}, &stderr)
	if err != nil || o.pass != "your-secret=a#b" {
		t.Fatalf("password parse: %v", err)
	}
	if !strings.Contains(stderr.String(), "sasl_pass_env") || strings.Contains(stderr.String(), "your-secret") {
		t.Fatal("unsafe or missing warning")
	}
	t.Setenv("KAFSCOUT_TEST_PASS", "env-secret")
	if _, err := parseOptions([]string{"--conf", p, "--sasl-pass-env=KAFSCOUT_TEST_PASS"}, io.Discard); err == nil {
		t.Fatal("mixed password conflict accepted")
	}
	stderr.Reset()
	o, err = parseOptions([]string{"--conf", p, "--sasl-pass=", "--sasl-pass-env=KAFSCOUT_TEST_PASS"}, &stderr)
	if err != nil || o.pass != "env-secret" || !strings.Contains(stderr.String(), "sasl_pass_env") {
		t.Fatalf("password source override: %v", err)
	}
}
