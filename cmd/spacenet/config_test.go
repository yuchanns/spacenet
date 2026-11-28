package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yuchanns/spacenet"
)

func TestParseConfigFileIncludeAndEnv(t *testing.T) {
	t.Setenv("SPACENET_TEST_HOST", "127.0.0.1")

	dir := t.TempDir()
	subDir := filepath.Join(dir, "conf")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatal(err)
	}
	baseCfg := `
thread = 4
harbor = 2
host = "$SPACENET_TEST_HOST"
enabled = true
`
	if err := os.WriteFile(filepath.Join(subDir, "base.lua"), []byte(baseCfg), 0o644); err != nil {
		t.Fatal(err)
	}
	mainCfg := `
include "conf/base.lua"
start = "app_main"
ratio = 1.5
`
	mainPath := filepath.Join(dir, "config.lua")
	if err := os.WriteFile(mainPath, []byte(mainCfg), 0o644); err != nil {
		t.Fatal(err)
	}

	env, err := parseConfigFile(mainPath)
	if err != nil {
		t.Fatalf("parseConfigFile failed: %v", err)
	}
	want := map[string]string{
		"thread":  "4",
		"harbor":  "2",
		"host":    "127.0.0.1",
		"enabled": "true",
		"start":   "app_main",
		"ratio":   "1.5",
	}
	for k, w := range want {
		if got := env[k]; got != w {
			t.Errorf("env[%q] = %q, want %q", k, got, w)
		}
	}
}

func TestLoadConfigFileAppliesOptions(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.lua")
	cfg := `
thread = 3
harbor = 1
start = "custom_start"
custom_key = "custom_val"
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	opts, err := loadConfigFile(cfgPath)
	if err != nil {
		t.Fatalf("loadConfigFile failed: %v", err)
	}
	node, err := spacenet.New(opts...)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer node.Close()

	if v, ok := node.GetEnv("start"); !ok || v != "custom_start" {
		t.Fatalf("start = %q (%v), want custom_start", v, ok)
	}
	if v, ok := node.GetEnv("custom_key"); !ok || v != "custom_val" {
		t.Fatalf("custom_key = %q (%v), want custom_val", v, ok)
	}
}
