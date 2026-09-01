package engine

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Ginny-Binny/pager/internal/config"
	"github.com/Ginny-Binny/pager/internal/notify"
	"github.com/Ginny-Binny/pager/internal/probe"
	"github.com/Ginny-Binny/pager/internal/store"
)

// fakeNotifier captures pages instead of sending them.
type fakeNotifier struct {
	mu   sync.Mutex
	msgs []notify.Message
	err  error
}

func (f *fakeNotifier) Publish(_ context.Context, m notify.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.msgs = append(f.msgs, m)
	return nil
}

func (f *fakeNotifier) all() []notify.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]notify.Message(nil), f.msgs...)
}

func (f *fakeNotifier) count() int { return len(f.all()) }

func (f *fakeNotifier) last() notify.Message {
	m := f.all()
	if len(m) == 0 {
		return notify.Message{}
	}
	return m[len(m)-1]
}

func (f *fakeNotifier) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs = nil
}

// harness wires an engine against miniredis, a controllable upstream and a
// clock the test advances by hand.
type harness struct {
	eng      *Engine
	store    *store.Store
	notifier *fakeNotifier
	healthy  *atomic.Bool
	clock    time.Time
	mu       sync.Mutex
}

func (h *harness) now() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.clock
}

func (h *harness) advance(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clock = h.clock.Add(d)
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	healthy := &atomic.Bool{}
	healthy.Store(true)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Write([]byte(`{"status":"success"}`))
	}))
	t.Cleanup(upstream.Close)

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	cfg := &config.Config{
		PublicBaseURL:      "https://pager.example.com",
		PollInterval:       time.Minute,
		EscalationInterval: 5 * time.Minute,
		FailureThreshold:   3,
		RecoveryThreshold:  2,
		Checks:             []config.Check{{Name: "web", URL: upstream.URL, TimeoutMS: 2000}},
	}

	h := &harness{
		store:    store.New(rdb),
		notifier: &fakeNotifier{},
		healthy:  healthy,
		clock:    time.Unix(1_700_000_000, 0),
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h.eng = New(cfg, h.store, probe.New(), h.notifier, log, h.now)
	return h
}

// poll runs n cycles, advancing the clock by one interval between them.
func (h *harness) poll(n int) {
	for i := 0; i < n; i++ {
		h.eng.PollOnce(context.Background())
		h.advance(time.Minute)
	}
}

func TestPagesOnceAfterThresholdAndDedupes(t *testing.T) {
	h := newHarness(t)

	h.poll(2)
	if n := h.notifier.count(); n != 0 {
		t.Fatalf("sent %d notifications while healthy, want 0", n)
	}

	h.healthy.Store(false)

	h.poll(2)
	if n := h.notifier.count(); n != 0 {
		t.Fatalf("paged after %d failures, want silence below the threshold of 3", n)
	}

	h.poll(1)
	if n := h.notifier.count(); n != 1 {
		t.Fatalf("sent %d notifications on the threshold failure, want exactly 1", n)
	}

	m := h.notifier.last()
	if m.Priority != notify.PriorityPage {
		t.Fatalf("page priority = %d, want %d", m.Priority, notify.PriorityPage)
	}
	if m.Title != "web is DOWN" {
		t.Fatalf("page title = %q, want %q", m.Title, "web is DOWN")
	}
	if !strings.Contains(m.Body, "500") {
		t.Fatalf("page body omits the failing status code:\n%s", m.Body)
	}
	if !strings.Contains(m.Body, "Acknowledge: https://pager.example.com/ack/web?t=") {
		t.Fatalf("page body has no ack link:\n%s", m.Body)
	}
	if m.ActionToken == "" || m.ActionURL != "https://pager.example.com/ack/web" {
		t.Fatalf("page has no POST ack action: url=%q token set=%v", m.ActionURL, m.ActionToken != "")
	}

	// Dedupe: five more failing cycles must stay silent.
	h.poll(5)
	if n := h.notifier.count(); n != 1 {
		t.Fatalf("sent %d notifications while already firing, want 1 — the incident re-paged", n)
	}
}

func TestEscalationLadderAndCap(t *testing.T) {
	h := newHarness(t)
	h.healthy.Store(false)
	h.poll(3)
	if h.notifier.count() != 1 {
		t.Fatalf("setup: expected the opening page, got %d", h.notifier.count())
	}
	h.notifier.reset()

	// Not due yet: the opening page counts as level 1 and reset the timer.
	h.eng.EscalateOnce(context.Background())
	if n := h.notifier.count(); n != 0 {
		t.Fatalf("escalated %d times before the interval elapsed, want 0", n)
	}

	// Level 2 at +5m.
	h.advance(5 * time.Minute)
	h.eng.EscalateOnce(context.Background())
	if n := h.notifier.count(); n != 1 {
		t.Fatalf("sent %d pages at the first escalation, want 1", n)
	}
	if got := h.notifier.last().Title; got != "[L2] web is STILL DOWN" {
		t.Fatalf("escalation title = %q, want the level-2 title", got)
	}
	// The escalation loop runs no probe of its own, so it must carry the last
	// observed latency forward rather than reporting a misleading 0ms.
	if body := h.notifier.last().Body; strings.Contains(body, "Latency: 0ms") {
		t.Fatalf("escalation page reports a zero latency:\n%s", body)
	}

	// A second tick at the same instant must not double-page.
	h.eng.EscalateOnce(context.Background())
	if n := h.notifier.count(); n != 1 {
		t.Fatalf("a repeated tick sent another page (total %d), want 1", n)
	}

	// Level 3 at +10m.
	h.advance(5 * time.Minute)
	h.eng.EscalateOnce(context.Background())
	if n := h.notifier.count(); n != 2 {
		t.Fatalf("sent %d pages after the second escalation, want 2", n)
	}
	if got := h.notifier.last().Title; got != "[L3] web is STILL DOWN" {
		t.Fatalf("escalation title = %q, want the level-3 title", got)
	}

	// Capped: hours of ticks, no more pages.
	for i := 0; i < 20; i++ {
		h.advance(30 * time.Minute)
		h.eng.EscalateOnce(context.Background())
	}
	if n := h.notifier.count(); n != 2 {
		t.Fatalf("sent %d pages after the cap, want 2 — escalation must go quiet at level 3", n)
	}
}

// Going quiet must not mean losing the incident: polls keep its TTL alive, so
// it never expires and reappears as a fresh, fully-escalating outage.
func TestSilencedIncidentSurvivesADayOfPolling(t *testing.T) {
	h := newHarness(t)
	h.healthy.Store(false)
	h.poll(3)

	h.advance(5 * time.Minute)
	h.eng.EscalateOnce(context.Background())
	h.advance(5 * time.Minute)
	h.eng.EscalateOnce(context.Background())
	if h.notifier.count() != 3 {
		t.Fatalf("setup: want 3 pages (open + two escalations), got %d", h.notifier.count())
	}

	// A silent day and a half of polling and ticking.
	for i := 0; i < 36; i++ {
		h.advance(time.Hour)
		h.eng.PollOnce(context.Background())
		h.eng.EscalateOnce(context.Background())
	}

	if n := h.notifier.count(); n != 3 {
		t.Fatalf("sent %d pages over 36h of silence, want 3 — the incident expired and re-paged", n)
	}
	inc, err := h.store.GetIncident(context.Background(), "web")
	if err != nil {
		t.Fatalf("GetIncident: %v", err)
	}
	if inc == nil {
		t.Fatal("incident vanished after 36h; the poller should keep it alive while down")
	}
	if inc.EscalationLevel != 3 {
		t.Fatalf("escalation level = %d, want 3 to be preserved", inc.EscalationLevel)
	}
}

func TestAckStopsEscalationButNotRecovery(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.healthy.Store(false)
	h.poll(3)
	token := h.notifier.last().ActionToken
	if token == "" {
		t.Fatal("opening page carried no ack token")
	}
	h.notifier.reset()

	ok, err := h.store.Ack(ctx, "web", token)
	if err != nil || !ok {
		t.Fatalf("Ack = (%v, %v), want (true, nil)", ok, err)
	}

	for i := 0; i < 10; i++ {
		h.advance(5 * time.Minute)
		h.eng.PollOnce(ctx)
		h.eng.EscalateOnce(ctx)
	}
	if n := h.notifier.count(); n != 0 {
		t.Fatalf("acked incident sent %d pages, want 0", n)
	}

	// Recovery still fires — acking silences escalation, not the all-clear.
	h.healthy.Store(true)
	h.poll(2)

	if n := h.notifier.count(); n != 1 {
		t.Fatalf("sent %d notifications on recovery, want exactly 1", n)
	}
	m := h.notifier.last()
	if m.Priority != notify.PriorityRecovery {
		t.Fatalf("recovery priority = %d, want %d", m.Priority, notify.PriorityRecovery)
	}
	if m.Title != "web recovered" {
		t.Fatalf("recovery title = %q, want %q", m.Title, "web recovered")
	}
	if !strings.Contains(m.Body, "Back up after") {
		t.Fatalf("recovery body omits the downtime:\n%s", m.Body)
	}
	if inc, _ := h.store.GetIncident(ctx, "web"); inc != nil {
		t.Fatalf("incident survived recovery: %+v", inc)
	}
}

func TestRecoveryRequiresTwoSuccesses(t *testing.T) {
	h := newHarness(t)
	h.healthy.Store(false)
	h.poll(3)
	h.notifier.reset()

	h.healthy.Store(true)
	h.poll(1)
	if n := h.notifier.count(); n != 0 {
		t.Fatalf("recovered after a single success (%d notifications), want 0", n)
	}
	h.poll(1)
	if n := h.notifier.count(); n != 1 {
		t.Fatalf("sent %d notifications after the second success, want 1", n)
	}
}

// A flap that recovers and fails again is a genuinely new outage, so it pages
// again with a fresh ack token rather than reusing the resolved incident's.
func TestReopenedIncidentPagesWithNewToken(t *testing.T) {
	h := newHarness(t)
	h.healthy.Store(false)
	h.poll(3)
	first := h.notifier.last().ActionToken

	h.healthy.Store(true)
	h.poll(2)
	h.healthy.Store(false)
	h.poll(3)

	second := h.notifier.last().ActionToken
	if second == "" || second == first {
		t.Fatalf("reopened incident token = %q, want a fresh token (first was %q)", second, first)
	}
	if got := h.notifier.count(); got != 3 {
		t.Fatalf("sent %d notifications across down/up/down, want 3 (page, recover, page)", got)
	}
}

func TestPollRecordsCycleCompletion(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	if last, err := h.store.LastCycleCompleted(ctx); err != nil || !last.IsZero() {
		t.Fatalf("last cycle before any poll = (%v, %v), want zero", last, err)
	}
	h.eng.PollOnce(ctx)
	last, err := h.store.LastCycleCompleted(ctx)
	if err != nil {
		t.Fatalf("LastCycleCompleted: %v", err)
	}
	if !last.Equal(h.now()) {
		t.Fatalf("last cycle = %s, want the injected clock time %s", last, h.now())
	}
}

// A page that cannot be delivered must not corrupt state: the incident stays
// open so the escalation loop keeps trying.
func TestNotifierFailureLeavesIncidentOpen(t *testing.T) {
	h := newHarness(t)
	h.notifier.err = errNotify

	h.healthy.Store(false)
	h.poll(3)

	inc, err := h.store.GetIncident(context.Background(), "web")
	if err != nil {
		t.Fatalf("GetIncident: %v", err)
	}
	if inc == nil || inc.Status != store.IncidentFiring {
		t.Fatalf("incident = %+v, want a firing incident despite the delivery failure", inc)
	}
}

var errNotify = &notifyError{}

type notifyError struct{}

func (*notifyError) Error() string { return "ntfy unavailable" }

func TestAckURLEscapesAndTrimsBase(t *testing.T) {
	h := newHarness(t)
	h.eng.cfg.PublicBaseURL = "https://pager.example.com///"
	got := h.eng.ackURL("web-1", "abc def")
	want := "https://pager.example.com/ack/web-1?t=abc+def"
	if got != want {
		t.Fatalf("ackURL = %q, want %q", got, want)
	}
}

func TestNewAckTokenIsRandom(t *testing.T) {
	seen := make(map[string]bool, 64)
	for i := 0; i < 64; i++ {
		tok, err := newAckToken()
		if err != nil {
			t.Fatalf("newAckToken: %v", err)
		}
		if len(tok) != 32 {
			t.Fatalf("token %q has length %d, want 32 hex chars (128 bits)", tok, len(tok))
		}
		if seen[tok] {
			t.Fatalf("newAckToken repeated %q", tok)
		}
		seen[tok] = true
	}
}
