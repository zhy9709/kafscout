package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

type confEntry struct {
	key, value string
	line       int
}

// Only whole-line comments are stripped: # and = may be query/password bytes.
func parseConf(r io.Reader) ([]confEntry, error) {
	allowed := map[string]string{
		"brokers": "brokers", "topic": "topic", "start": "start", "end": "end",
		"query": "query", "partition": "partition", "limit": "limit", "tz": "tz",
		"max_bytes": "max-bytes", "sasl_user": "sasl-user", "sasl_pass": "sasl-pass",
		"sasl_pass_env": "sasl-pass-env", "hex": "hex", "quiet": "quiet",
	}
	var entries []confEntry
	seen := make(map[string]bool)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for line := 1; scanner.Scan(); line++ {
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}
		key, value, ok := strings.Cut(raw, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return nil, fmt.Errorf("conf line %d: expected key=value", line)
		}
		name, ok := allowed[key]
		if !ok {
			return nil, fmt.Errorf("conf line %d: unknown key %q", line, key)
		}
		if seen[key] {
			return nil, fmt.Errorf("conf line %d: duplicate key %q", line, key)
		}
		seen[key] = true
		entries = append(entries, confEntry{name, strings.TrimSpace(value), line})
	}
	if e := scanner.Err(); e != nil {
		return nil, fmt.Errorf("conf read: %w", e)
	}
	return entries, nil
}
