package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigGeneratesCompleteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	cfg, generated, err := LoadConfig(path)
	if err != nil || !generated {
		t.Fatalf("LoadConfig() = %#v, %v, generated=%v", cfg, err, generated)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != len(configFields) {
		t.Fatalf("generated %d fields, want %d", len(fields), len(configFields))
	}
	loaded, generated, err := LoadConfig(path)
	if err != nil || generated {
		t.Fatalf("second LoadConfig() = %#v, %v, generated=%v", loaded, err, generated)
	}
	if loaded != cfg {
		t.Fatalf("loaded config differs from generated config")
	}
}

func TestLoadConfigRejectsMissingAndUnknownFields(t *testing.T) {
	base := DefaultConfig()
	data, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	delete(fields, "napcatWsUrl")
	path := filepath.Join(t.TempDir(), "config.json")
	missing, _ := json.Marshal(fields)
	if err := os.WriteFile(path, missing, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadConfig(path); err == nil {
		t.Fatal("missing field was accepted")
	}
	fields["napcatWsUrl"] = base.NapcatWSURL
	fields["unexpected"] = true
	unknown, _ := json.Marshal(fields)
	if err := os.WriteFile(path, unknown, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadConfig(path); err == nil {
		t.Fatal("unknown field was accepted")
	}
}
