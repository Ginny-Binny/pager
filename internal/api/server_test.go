package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Ginny-Binny/pager/internal/config"
	"github.com/Ginny-Binny/pager/internal/store"
)

var baseTime = time.Unix(1_700_000_000, 0)

// TestMain silences go-redis's internal logger: one test deliberately kills
// Redis, and its reconnect chatter would otherwise bury real failures.
func TestMain(m *testing.M) {
	redis.SetLogger(discardLogger{})
	os.Exit(m.Run())
}

type discardLogger struct{}

func (discardLogger) Printf(context.Context, string, ...any) {}

type fixture struct {
	srv   *Server
	store *store.Store
	mr    *miniredis.Miniredis
	clock time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	cfg := &config.Config{
		PollInterval: time.Minute,
		Checks: []config.Check{
			{Name: "web", URL: "https://example.com/status"},
			{Name: "api", URL: "https://example.com/api/status"},
		},
	}
	f := &fixture{store: store.New(rdb), mr: mr, clock: baseTime}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	f.srv = New(cfg, f.store, log, func() time.Time { return f.clock })
	return f
}

func (f *fixture) do(method, target string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	f.srv.Routes().ServeHTTP(rec, req)
	return rec
}

// openIncident drives a check down and returns its ack token.
func (f *fixture) openIncident(t *testing.T, name, token string) string {
	t.Helper()
	var out store.Outcome
	for i := 1; i <= 3; i++ {
		var err error
		out, err = f.store.RecordProbe(context.Background(), name, false, 100,
			"expected HTTP 200, got 500", baseTime.Add(time.Duration(i)*time.Minute), 3, 2, token)
		if err != nil {
			t.Fatalf("RecordProbe: %v", err)
		}
	}
	if out.Action != store.ActionPageNew {
		t.Fatalf("setup: incident was not opened (action %q)", out.Action)
	}
	return out.AckToken
}

func TestHealthUnhealthyBeforeFirstCycle(t *testing.T) {
	f := newFixture(t)
	rec := f.do(http.MethodGet, "/health", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 before any cycle completes", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "no poll cycle") {
		t.Fatalf("body does not explain the failure:\n%s", rec.Body.String())
	}
}

func TestHealthOKAfterRecentCycle(t *testing.T) {
	f := newFixture(t)
	if err := f.store.SetCycleCompleted(context.Background(), f.clock); err != nil {
		t.Fatalf("SetCycleCompleted: %v", err)
	}

	rec := f.do(http.MethodGet, "/health", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("status field = %q, want ok", resp.Status)
	}
	if resp.LastCycle != f.clock.UTC().Format(time.RFC3339) {
		t.Fatalf("last_cycle = %q, want the recorded cycle time", resp.LastCycle)
	}
}

// The whole point of /health: a poller that has stopped looping must become
// externally visible, because nothing else can detect it.
func TestHealthDetectsStuckPoller(t *testing.T) {
	f := newFixture(t)
	if err := f.store.SetCycleCompleted(context.Background(), f.clock); err != nil {
		t.Fatalf("SetCycleCompleted: %v", err)
	}

	// Two intervals late is tolerated; a slow cycle should not cry wolf.
	f.clock = baseTime.Add(2 * time.Minute)
	if rec := f.do(http.MethodGet, "/health", nil); rec.Code != http.StatusOK {
		t.Fatalf("status = %d at 2 intervals, want 200", rec.Code)
	}

	// Past three intervals the poller is considered stuck.
	f.clock = baseTime.Add(3*time.Minute + time.Second)
	rec := f.do(http.MethodGet, "/health", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d at >3 intervals, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "stale") {
		t.Fatalf("body does not mention staleness:\n%s", rec.Body.String())
	}
}

// With Redis gone the monitor knows nothing, so it must not report health.
func TestHealthFailsWhenRedisIsDown(t *testing.T) {
	f := newFixture(t)
	if err := f.store.SetCycleCompleted(context.Background(), f.clock); err != nil {
		t.Fatalf("SetCycleCompleted: %v", err)
	}
	f.mr.Close()

	rec := f.do(http.MethodGet, "/health", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d with redis down, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "redis") {
		t.Fatalf("body does not name redis:\n%s", rec.Body.String())
	}
}

func TestStatusListsChecksInConfigOrder(t *testing.T) {
	f := newFixture(t)
	f.openIncident(t, "web", "secret-token-value")

	rec := f.do(http.MethodGet, "/status", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp statusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Checks) != 2 {
		t.Fatalf("got %d checks, want 2", len(resp.Checks))
	}
	if resp.Checks[0].Name != "web" || resp.Checks[1].Name != "api" {
		t.Fatalf("checks out of config order: %s, %s", resp.Checks[0].Name, resp.Checks[1].Name)
	}
	if resp.Checks[0].URL != "https://example.com/status" {
		t.Fatalf("check URL = %q, want it filled in from config", resp.Checks[0].URL)
	}
	if resp.Checks[0].Incident == nil || resp.Checks[0].Incident.Status != store.IncidentFiring {
		t.Fatalf("web incident = %+v, want firing", resp.Checks[0].Incident)
	}
	if resp.Checks[1].Incident != nil {
		t.Fatalf("api has an incident it should not: %+v", resp.Checks[1].Incident)
	}
}

// /status is reachable by anyone who can reach the service, so it must never
// serialise the secret that silences a page.
func TestStatusDoesNotLeakAckToken(t *testing.T) {
	f := newFixture(t)
	f.openIncident(t, "web", "super-secret-token")

	body := f.do(http.MethodGet, "/status", nil).Body.String()
	for _, secret := range []string{"super-secret-token", "ack_token"} {
		if strings.Contains(body, secret) {
			t.Fatalf("/status leaks %q:\n%s", secret, body)
		}
	}
}

func TestAckWithTokenInQuery(t *testing.T) {
	f := newFixture(t)
	token := f.openIncident(t, "web", "good-token")

	rec := f.do(http.MethodGet, "/ack/web?t="+token, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("Content-Type = %q, want HTML for a phone browser", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Acknowledged") {
		t.Fatalf("page does not confirm the ack:\n%s", body)
	}
	if !strings.Contains(body, `name="viewport"`) {
		t.Fatalf("page has no viewport meta; it will render badly on a phone:\n%s", body)
	}

	inc, err := f.store.GetIncident(context.Background(), "web")
	if err != nil {
		t.Fatalf("GetIncident: %v", err)
	}
	if inc.Status != store.IncidentAcked {
		t.Fatalf("incident status = %q, want %q", inc.Status, store.IncidentAcked)
	}
}

// The ntfy action button sends the token in a header, over POST, so a bot that
// prefetches links in the notification cannot silence a live incident.
func TestAckWithTokenInHeaderOverPOST(t *testing.T) {
	f := newFixture(t)
	token := f.openIncident(t, "web", "good-token")

	rec := f.do(http.MethodPost, "/ack/web", map[string]string{"X-Ack-Token": token})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	inc, _ := f.store.GetIncident(context.Background(), "web")
	if inc.Status != store.IncidentAcked {
		t.Fatalf("incident status = %q, want %q", inc.Status, store.IncidentAcked)
	}
}

func TestAckRejectsBadOrMissingToken(t *testing.T) {
	f := newFixture(t)
	f.openIncident(t, "web", "good-token")

	for _, target := range []string{"/ack/web", "/ack/web?t=", "/ack/web?t=wrong-token"} {
		rec := f.do(http.MethodGet, target, nil)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("GET %s: status = %d, want 403", target, rec.Code)
		}
		inc, _ := f.store.GetIncident(context.Background(), "web")
		if inc.Status != store.IncidentFiring {
			t.Fatalf("GET %s acked the incident anyway (status %q)", target, inc.Status)
		}
	}
}

func TestAckUnknownCheck(t *testing.T) {
	f := newFixture(t)
	rec := f.do(http.MethodGet, "/ack/not-configured?t=x", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Unknown check") {
		t.Fatalf("body does not explain the 404:\n%s", rec.Body.String())
	}
}

func TestAckWhenNoIncidentIsOpen(t *testing.T) {
	f := newFixture(t)
	rec := f.do(http.MethodGet, "/ack/web?t=anything", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Nothing to acknowledge") {
		t.Fatalf("body does not explain that there is nothing to ack:\n%s", rec.Body.String())
	}
}

func TestAckPageEscapesCheckName(t *testing.T) {
	f := newFixture(t)
	// Path segments are URL-escaped by the client; html/template escapes on
	// render. Confirm no raw markup survives into the page.
	rec := f.do(http.MethodGet, "/ack/%3Cscript%3E?t=x", nil)
	if strings.Contains(rec.Body.String(), "<script>") {
		t.Fatalf("check name rendered unescaped:\n%s", rec.Body.String())
	}
}

func TestUnroutedPathsAndMethods(t *testing.T) {
	f := newFixture(t)
	if rec := f.do(http.MethodGet, "/", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("GET /: status = %d, want 404", rec.Code)
	}
	if rec := f.do(http.MethodPost, "/status", nil); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /status: status = %d, want 405", rec.Code)
	}
}
