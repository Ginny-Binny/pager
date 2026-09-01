// Package config loads runtime configuration from environment variables and
// the checks file, and validates both before the process starts serving.
//
// Validation is deliberately strict and fails fast: a monitor that boots with a
// bad topic or an unreachable-by-construction check is worse than one that
// refuses to boot, because it looks healthy while paging nobody.
package config

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// nameRE constrains check names because they become both Redis key segments
// and URL path segments in /ack/{check}.
var nameRE = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// DefaultTimeoutMS is the per-probe timeout when a check does not set its own.
const DefaultTimeoutMS = 5000

// Check is one HTTP endpoint to poll. Only Name and URL are required; the
// remaining fields are optional assertions layered on top of the status code.
type Check struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"`

	// ExpectedStatus defaults to 200 when zero.
	ExpectedStatus int `yaml:"expected_status"`
	// BodyContains, when set, must appear as a substring of the response body.
	BodyContains string `yaml:"body_contains"`
	// JSONPath is a dot path (e.g. "status", "data.db.ok", "items.0.name"),
	// not full JSONPath syntax. Requires ExpectedValue.
	JSONPath string `yaml:"json_path"`
	// ExpectedValue is compared against the JSONPath lookup, stringified.
	ExpectedValue string `yaml:"expected_value"`
	// MaxLatencyMS fails the probe when the response takes longer, even on 200.
	MaxLatencyMS int `yaml:"max_latency_ms"`
	// TimeoutMS bounds the whole request. Defaults to DefaultTimeoutMS.
	TimeoutMS int `yaml:"timeout_ms"`
	// FollowRedirects is off by default: a redirect on a status endpoint is a
	// misconfiguration worth seeing, not something to silently chase to a 200.
	FollowRedirects bool `yaml:"follow_redirects"`
}

// Timeout returns the effective per-probe timeout.
func (c Check) Timeout() time.Duration {
	if c.TimeoutMS > 0 {
		return time.Duration(c.TimeoutMS) * time.Millisecond
	}
	return DefaultTimeoutMS * time.Millisecond
}

// WantStatus returns the effective expected HTTP status code.
func (c Check) WantStatus() int {
	if c.ExpectedStatus > 0 {
		return c.ExpectedStatus
	}
	return 200
}

type checksFile struct {
	Checks []Check `yaml:"checks"`
}

// Config is the fully resolved runtime configuration.
type Config struct {
	NTFYURL   string
	NTFYTopic string
	NTFYToken string

	RedisAddr     string
	PublicBaseURL string
	ListenAddr    string

	PollInterval       time.Duration
	EscalationInterval time.Duration
	FailureThreshold   int
	RecoveryThreshold  int

	ChecksFile string
	Checks     []Check
}

// Load reads the environment and the checks file, applies defaults, and
// validates the result.
func Load() (*Config, error) {
	c := &Config{
		NTFYURL:            os.Getenv("NTFY_URL"),
		NTFYTopic:          os.Getenv("NTFY_TOPIC"),
		NTFYToken:          os.Getenv("NTFY_TOKEN"),
		RedisAddr:          envStr("REDIS_ADDR", "localhost:6379"),
		PublicBaseURL:      envStr("PUBLIC_BASE_URL", "http://localhost:8080"),
		ListenAddr:         envStr("LISTEN_ADDR", ":8080"),
		ChecksFile:         envStr("CHECKS_FILE", "checks.yaml"),
		FailureThreshold:   envInt("FAILURE_THRESHOLD", 3),
		RecoveryThreshold:  envInt("RECOVERY_THRESHOLD", 2),
		PollInterval:       envDur("POLL_INTERVAL", 60*time.Second),
		EscalationInterval: envDur("ESCALATION_INTERVAL", 5*time.Minute),
	}

	// Required: without these the service runs blind, paging into the void.
	if c.NTFYURL == "" {
		return nil, fmt.Errorf("NTFY_URL is required")
	}
	if c.NTFYTopic == "" {
		return nil, fmt.Errorf("NTFY_TOPIC is required")
	}
	if _, err := url.Parse(c.NTFYURL); err != nil {
		return nil, fmt.Errorf("NTFY_URL is not a valid URL: %w", err)
	}
	if c.FailureThreshold < 1 {
		return nil, fmt.Errorf("FAILURE_THRESHOLD must be >= 1, got %d", c.FailureThreshold)
	}
	if c.RecoveryThreshold < 1 {
		return nil, fmt.Errorf("RECOVERY_THRESHOLD must be >= 1, got %d", c.RecoveryThreshold)
	}
	if c.PollInterval < time.Second {
		return nil, fmt.Errorf("POLL_INTERVAL must be >= 1s, got %s", c.PollInterval)
	}
	if c.EscalationInterval < time.Second {
		return nil, fmt.Errorf("ESCALATION_INTERVAL must be >= 1s, got %s", c.EscalationInterval)
	}

	checks, err := LoadChecks(c.ChecksFile)
	if err != nil {
		return nil, err
	}
	c.Checks = checks
	return c, nil
}

// LoadChecks reads and validates the checks file.
func LoadChecks(path string) ([]Check, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read checks file %s: %w", path, err)
	}

	var f checksFile
	// KnownFields catches typos like "body_contain", which would otherwise be
	// silently ignored and quietly weaken the check.
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("parse checks file %s: %w", path, err)
	}
	if len(f.Checks) == 0 {
		return nil, fmt.Errorf("checks file %s defines no checks", path)
	}

	seen := make(map[string]bool, len(f.Checks))
	for i, ck := range f.Checks {
		if !nameRE.MatchString(ck.Name) {
			return nil, fmt.Errorf("check %d: name %q must match %s", i, ck.Name, nameRE)
		}
		if seen[ck.Name] {
			return nil, fmt.Errorf("duplicate check name %q", ck.Name)
		}
		seen[ck.Name] = true

		u, err := url.Parse(ck.URL)
		if err != nil {
			return nil, fmt.Errorf("check %q: invalid url: %w", ck.Name, err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, fmt.Errorf("check %q: url scheme must be http or https, got %q", ck.Name, u.Scheme)
		}
		if u.Host == "" {
			return nil, fmt.Errorf("check %q: url has no host", ck.Name)
		}
		if (ck.JSONPath == "") != (ck.ExpectedValue == "") {
			return nil, fmt.Errorf("check %q: json_path and expected_value must be set together", ck.Name)
		}
		if ck.TimeoutMS < 0 || ck.MaxLatencyMS < 0 {
			return nil, fmt.Errorf("check %q: timeout_ms and max_latency_ms must not be negative", ck.Name)
		}
		// A max_latency above the timeout can never trip: the request is
		// aborted first, so the assertion would be dead config.
		if ck.MaxLatencyMS > 0 && time.Duration(ck.MaxLatencyMS)*time.Millisecond > ck.Timeout() {
			return nil, fmt.Errorf("check %q: max_latency_ms (%d) exceeds timeout (%s) and can never trigger",
				ck.Name, ck.MaxLatencyMS, ck.Timeout())
		}
	}
	return f.Checks, nil
}

// Names returns the configured check names in file order.
func (c *Config) Names() []string {
	names := make([]string, len(c.Checks))
	for i, ck := range c.Checks {
		names[i] = ck.Name
	}
	return names
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envDur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
