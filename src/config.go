package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
)

const configFilename = "config.json"

type Config struct {
	UpstreamBaseURL        string `json:"upstreamBaseUrl"`
	UpstreamTimeoutMs      int64  `json:"upstreamTimeoutMs"`
	InfoCacheDir           string `json:"infoCacheDir"`
	MaxTaskQueued          int    `json:"maxTaskQueued"`
	PDFCacheDir            string `json:"pdfCacheDir"`
	PDFCacheMaxSize        int64  `json:"pdfCacheMaxSize"`
	WorkerPoolSize         int    `json:"workerPoolSize"`
	MaxRetries             int    `json:"maxRetries"`
	ErrorTTLMS             int64  `json:"errorTtlMs"`
	ImageDownloadTimeoutMS int64  `json:"imageDownloadTimeoutMs"`
	ImageDownloadRetries   int    `json:"imageDownloadRetries"`
	NetworkConcurrency     int    `json:"networkConcurrency"`
	CPUConcurrency         int    `json:"cpuConcurrency"`
	NapcatWSURL            string `json:"napcatWsUrl"`
	NapcatAccessToken      string `json:"napcatAccessToken"`
	ActionTimeoutMs        int64  `json:"actionTimeoutMs"`
	FileActionTimeoutMs    int64  `json:"fileActionTimeoutMs"`
	PollIntervalMS         int64  `json:"pollIntervalMs"`
	MaxPollAttempts        int    `json:"maxPollAttempts"`
	RateLimitWindowMS      int64  `json:"rateLimitWindowMs"`
	RateLimitMaxRequests   int    `json:"rateLimitMaxRequests"`
}

var configFields = []string{
	"upstreamBaseUrl",
	"upstreamTimeoutMs",
	"infoCacheDir",
	"maxTaskQueued",
	"pdfCacheDir",
	"pdfCacheMaxSize",
	"workerPoolSize",
	"maxRetries",
	"errorTtlMs",
	"imageDownloadTimeoutMs",
	"imageDownloadRetries",
	"networkConcurrency",
	"cpuConcurrency",
	"napcatWsUrl",
	"napcatAccessToken",
	"actionTimeoutMs",
	"fileActionTimeoutMs",
	"pollIntervalMs",
	"maxPollAttempts",
	"rateLimitWindowMs",
	"rateLimitMaxRequests",
}

func DefaultConfig() Config {
	cpu := runtime.GOMAXPROCS(0)
	if cpu < 1 {
		cpu = 1
	}
	return Config{
		UpstreamBaseURL:        "https://jmserver.2kb.fish",
		UpstreamTimeoutMs:      10_000,
		InfoCacheDir:           "./cache/info",
		MaxTaskQueued:          100,
		PDFCacheDir:            "./cache/pdf",
		PDFCacheMaxSize:        10 * 1024 * 1024 * 1024,
		WorkerPoolSize:         3,
		MaxRetries:             3,
		ErrorTTLMS:             60 * 60 * 1000,
		ImageDownloadTimeoutMS: 120_000,
		ImageDownloadRetries:   3,
		NetworkConcurrency:     15,
		CPUConcurrency:         cpu,
		NapcatWSURL:            "ws://localhost:3001",
		NapcatAccessToken:      "",
		ActionTimeoutMs:        10_000,
		FileActionTimeoutMs:    60_000,
		PollIntervalMS:         2_000,
		MaxPollAttempts:        150,
		RateLimitWindowMS:      10_000,
		RateLimitMaxRequests:   3,
	}
}

func LoadConfig(path string) (Config, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		cfg := DefaultConfig()
		encoded, marshalErr := json.MarshalIndent(cfg, "", "  ")
		if marshalErr != nil {
			return Config{}, false, fmt.Errorf("marshal default config: %w", marshalErr)
		}
		encoded = append(encoded, '\n')
		file, createErr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if createErr != nil {
			return Config{}, false, fmt.Errorf("create %s: %w", path, createErr)
		}
		if _, writeErr := file.Write(encoded); writeErr != nil {
			_ = file.Close()
			return Config{}, false, fmt.Errorf("write %s: %w", path, writeErr)
		}
		if closeErr := file.Close(); closeErr != nil {
			return Config{}, false, fmt.Errorf("close %s: %w", path, closeErr)
		}
		return cfg, true, nil
	}
	if err != nil {
		return Config{}, false, fmt.Errorf("read %s: %w", path, err)
	}

	var fields map[string]json.RawMessage
	if err := decodeStrict(data, &fields); err != nil {
		return Config{}, false, fmt.Errorf("parse %s: %w", path, err)
	}
	for _, field := range configFields {
		if _, ok := fields[field]; !ok {
			return Config{}, false, fmt.Errorf("config field %q is required", field)
		}
	}

	var cfg Config
	if err := decodeStrict(data, &cfg); err != nil {
		return Config{}, false, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, false, err
	}
	return cfg, false, nil
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func (c Config) Validate() error {
	positive := []struct {
		name  string
		value int64
	}{
		{"upstreamTimeoutMs", c.UpstreamTimeoutMs},
		{"maxTaskQueued", int64(c.MaxTaskQueued)},
		{"pdfCacheMaxSize", c.PDFCacheMaxSize},
		{"workerPoolSize", int64(c.WorkerPoolSize)},
		{"errorTtlMs", c.ErrorTTLMS},
		{"imageDownloadTimeoutMs", c.ImageDownloadTimeoutMS},
		{"imageDownloadRetries", int64(c.ImageDownloadRetries)},
		{"networkConcurrency", int64(c.NetworkConcurrency)},
		{"cpuConcurrency", int64(c.CPUConcurrency)},
		{"actionTimeoutMs", c.ActionTimeoutMs},
		{"fileActionTimeoutMs", c.FileActionTimeoutMs},
		{"pollIntervalMs", c.PollIntervalMS},
		{"maxPollAttempts", int64(c.MaxPollAttempts)},
		{"rateLimitWindowMs", c.RateLimitWindowMS},
		{"rateLimitMaxRequests", int64(c.RateLimitMaxRequests)},
	}
	for _, item := range positive {
		if item.value <= 0 {
			return fmt.Errorf("config field %q must be positive", item.name)
		}
	}
	if c.MaxRetries < 0 {
		return errors.New("config field \"maxRetries\" must not be negative")
	}
	if c.InfoCacheDir == "" || c.PDFCacheDir == "" {
		return errors.New("cache directories must not be empty")
	}
	if err := validateURL(c.UpstreamBaseURL, "http", "https"); err != nil {
		return fmt.Errorf("upstreamBaseUrl: %w", err)
	}
	if err := validateURL(c.NapcatWSURL, "ws", "wss"); err != nil {
		return fmt.Errorf("napcatWsUrl: %w", err)
	}
	return nil
}

func validateURL(value string, schemes ...string) error {
	u, err := url.Parse(value)
	if err != nil || u.Host == "" {
		return errors.New("must be an absolute URL")
	}
	for _, scheme := range schemes {
		if u.Scheme == scheme {
			return nil
		}
	}
	return fmt.Errorf("unsupported scheme %q", u.Scheme)
}

func resolvePath(value string) (string, error) {
	path, err := filepath.Abs(value)
	if err != nil {
		return "", fmt.Errorf("resolve path %q: %w", value, err)
	}
	return path, nil
}
