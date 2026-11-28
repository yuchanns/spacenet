package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	stdruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRunVersionAndHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"-version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("-version exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "spacenet ") {
		t.Fatalf("unexpected -version output: %q", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"-h"}, &stdout, &stderr); code != 0 {
		t.Fatalf("-h exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "Usage: spacenet") {
		t.Fatalf("unexpected -h output: %q", stderr.String())
	}
}

func TestRunValidationErrors(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		wantErr string
		code    int
	}{
		{
			name:    "unknown flag",
			args:    []string{"-no-such-flag"},
			wantErr: "flag provided but not defined",
			code:    2,
		},
		{
			name:    "negative threads",
			args:    []string{"-threads", "-1"},
			wantErr: "-threads must be >= 0",
			code:    2,
		},
		{
			name:    "too many config files",
			args:    []string{"a.lua", "b.lua"},
			wantErr: "expected at most one config file",
			code:    2,
		},
		{
			name:    "missing config file",
			args:    []string{filepath.Join(t.TempDir(), "missing.lua")},
			wantErr: "spacenet:",
			code:    1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(context.Background(), tc.args, &stdout, &stderr)
			if code != tc.code {
				t.Fatalf("exit code = %d, want %d; stderr = %q", code, tc.code, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantErr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantErr)
			}
		})
	}
}

func TestRunConfigFileToCompletion(t *testing.T) {
	dir := t.TempDir()
	svcDir := filepath.Join(dir, "service")
	if err := os.MkdirAll(svcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	svcScript := `
local spacenet = require "spacenet"
require "spacenet.manager"
local crypt = require "spacenet.crypt"
spacenet.start(function()
	local greeting = spacenet.getenv("greeting") or ""
	spacenet.error("CLI_HELLO:" .. greeting .. ":" .. crypt.base64encode("ok"))
	spacenet.abort()
end)
`
	if err := os.WriteFile(filepath.Join(svcDir, "cli_hello.lua"), []byte(svcScript), 0o644); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "config.lua")
	cfgContent := `
thread = 2
start = "cli_hello"
greeting = "spacenet-cli"
`
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var stdout, stderr syncBuf
	code := run(ctx, []string{"-basedir", dir, cfgPath}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run() exit code = %d, stdout = %q, stderr = %q", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "CLI_HELLO:spacenet-cli:b2s=") {
		t.Fatalf("expected CLI_HELLO log, got stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	}
}

func TestRunGracefulCancel(t *testing.T) {
	dir := t.TempDir()
	svcDir := filepath.Join(dir, "service")
	if err := os.MkdirAll(svcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	svcScript := `
local spacenet = require "spacenet"
spacenet.start(function()
	spacenet.error("CLI_WAITING")
	while true do
		spacenet.sleep(100)
	end
end)
`
	if err := os.WriteFile(filepath.Join(svcDir, "cli_wait.lua"), []byte(svcScript), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout, stderr syncBuf
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"-threads", "2", "-basedir", dir, "-start", "cli_wait"}, &stdout, &stderr)
	}()

	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(stdout.String(), "CLI_WAITING") {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("timeout waiting for CLI_WAITING; stdout=%q stderr=%q", stdout.String(), stderr.String())
		}
		time.Sleep(15 * time.Millisecond)
	}

	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("expected clean exit code 0 on cancel, got %d; stderr=%q", code, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for run() to exit after cancel")
	}
}

func TestCLISpacenetBuildsAndRunsVersion(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "spacenet")
	if stdruntime.GOOS == "windows" {
		bin += ".exe"
	}
	build := exec.Command("go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build cmd/spacenet failed: %v\n%s", err, out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "-version").CombinedOutput()
	if err != nil {
		t.Fatalf("spacenet -version failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "spacenet") {
		t.Fatalf("unexpected -version output: %q", string(out))
	}
}
