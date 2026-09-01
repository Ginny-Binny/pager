package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeChecks(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "checks.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write checks file: %v", err)
	}
	return path
}

func TestLoadChecksMinimal(t *testing.T) {
	path := writeChecks(t, `
checks:
  - name: connect-backend
    url: https://connect.pabbly.com/backend/status
  - name: scheduler
    url: https://pc-workflow-runner.pabbly.com/prod/api/v1/status
    max_latency_ms: 2000
    body_contains: "Container is up"
`)
	checks, err := LoadChecks(path)
	if err != nil {
		t.Fatalf("LoadChecks: %v", err)
	}
	if len(checks) != 2 {
		t.Fatalf("got %d checks, want 2", len(checks))
	}
	if got := checks[0].WantStatus(); got != 200 {
		t.Fatalf("default expected status = %d, want 200", got)
	}
	if got := checks[0].Timeout(); got != 5*time.Second {
		t.Fatalf("default timeout = %s, want 5s", got)
	}
	if checks[0].FollowRedirects {
		t.Fatal("follow_redirects defaulted to true; a redirecting status endpoint would look healthy")
	}
	if checks[1].MaxLatencyMS != 2000 || checks[1].BodyContains != "Container is up" {
		t.Fatalf("optional fields not parsed: %+v", checks[1])
	}
}

func TestLoadChecksRejectsBadConfig(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "empty file",
			body:    "checks: []\n",
			wantErr: "no checks",
		},
		{
			name:    "name with a slash would break the ack route",
			body:    "checks:\n  - name: a/b\n    url: https://x.test/s\n",
			wantErr: "must match",
		},
		{
			name:    "duplicate names would collide in redis",
			body:    "checks:\n  - name: a\n    url: https://x.test/s\n  - name: a\n    url: https://y.test/s\n",
			wantErr: "duplicate",
		},
		{
			name:    "non-http scheme",
			body:    "checks:\n  - name: a\n    url: ftp://x.test/s\n",
			wantErr: "scheme",
		},
		{
			name:    "url with no host",
			body:    "checks:\n  - name: a\n    url: /just/a/path\n",
			wantErr: "scheme",
		},
		{
			name:    "json_path without expected_value",
			body:    "checks:\n  - name: a\n    url: https://x.test/s\n    json_path: status\n",
			wantErr: "must be set together",
		},
		{
			name:    "expected_value without json_path",
			body:    "checks:\n  - name: a\n    url: https://x.test/s\n    expected_value: ok\n",
			wantErr: "must be set together",
		},
		{
			// Dead config: the request is aborted before the assertion can fire.
			name:    "max_latency above timeout",
			body:    "checks:\n  - name: a\n    url: https://x.test/s\n    timeout_ms: 1000\n    max_latency_ms: 5000\n",
			wantErr: "can never trigger",
		},
		{
			// A silently ignored typo would quietly weaken the check.
			name:    "unknown field",
			body:    "checks:\n  - name: a\n    url: https://x.test/s\n    body_contain: oops\n",
			wantErr: "field body_contain not found",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadChecks(writeChecks(t, tc.body))
			if err == nil {
				t.Fatalf("LoadChecks accepted invalid config")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadChecksMissingFile(t *testing.T) {
	if _, err := LoadChecks(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Fatal("LoadChecks succeeded on a missing file")
	}
}

// Paging into the void is worse than refusing to start, so the ntfy settings
// are required rather than defaulted.
func TestLoadRequiresNtfySettings(t *testing.T) {
	path := writeChecks(t, "checks:\n  - name: a\n    url: https://x.test/s\n")
	t.Setenv("CHECKS_FILE", path)

	t.Setenv("NTFY_URL", "")
	t.Setenv("NTFY_TOPIC", "alerts")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "NTFY_URL") {
		t.Fatalf("error = %v, want it to name NTFY_URL", err)
	}

	t.Setenv("NTFY_URL", "https://ntfy.example.com")
	t.Setenv("NTFY_TOPIC", "")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "NTFY_TOPIC") {
		t.Fatalf("error = %v, want it to name NTFY_TOPIC", err)
	}
}

func TestLoadDefaultsAndOverrides(t *testing.T) {
	path := writeChecks(t, "checks:\n  - name: a\n    url: https://x.test/s\n")
	t.Setenv("CHECKS_FILE", path)
	t.Setenv("NTFY_URL", "https://ntfy.example.com")
	t.Setenv("NTFY_TOPIC", "alerts")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PollInterval != time.Minute || cfg.EscalationInterval != 5*time.Minute {
		t.Fatalf("intervals = %s / %s, want 1m / 5m", cfg.PollInterval, cfg.EscalationInterval)
	}
	if cfg.FailureThreshold != 3 || cfg.RecoveryThreshold != 2 {
		t.Fatalf("thresholds = %d / %d, want 3 / 2", cfg.FailureThreshold, cfg.RecoveryThreshold)
	}
	if cfg.ListenAddr != ":8080" || cfg.RedisAddr != "localhost:6379" {
		t.Fatalf("addrs = %q / %q", cfg.ListenAddr, cfg.RedisAddr)
	}
	if got := cfg.Names(); len(got) != 1 || got[0] != "a" {
		t.Fatalf("Names() = %v, want [a]", got)
	}

	t.Setenv("POLL_INTERVAL", "30s")
	t.Setenv("FAILURE_THRESHOLD", "5")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PollInterval != 30*time.Second || cfg.FailureThreshold != 5 {
		t.Fatalf("overrides not applied: %s / %d", cfg.PollInterval, cfg.FailureThreshold)
	}
}

func TestLoadRejectsNonsenseThresholds(t *testing.T) {
	path := writeChecks(t, "checks:\n  - name: a\n    url: https://x.test/s\n")
	t.Setenv("CHECKS_FILE", path)
	t.Setenv("NTFY_URL", "https://ntfy.example.com")
	t.Setenv("NTFY_TOPIC", "alerts")

	t.Setenv("FAILURE_THRESHOLD", "0")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "FAILURE_THRESHOLD") {
		t.Fatalf("error = %v, want it to reject a zero failure threshold", err)
	}
	t.Setenv("FAILURE_THRESHOLD", "3")

	t.Setenv("POLL_INTERVAL", "100ms")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "POLL_INTERVAL") {
		t.Fatalf("error = %v, want it to reject a sub-second poll interval", err)
	}
}
