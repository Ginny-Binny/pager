package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const (
	failThreshold = 3
	recThreshold  = 2
)

var baseTime = time.Unix(1_700_000_000, 0)

func newTestStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return New(rdb), mr
}

// fail records one failed probe at the given offset from baseTime.
func fail(t *testing.T, s *Store, name string, offset time.Duration, token string) Outcome {
	t.Helper()
	out, err := s.RecordProbe(context.Background(), name, false, 120, "expected HTTP 200, got 500",
		baseTime.Add(offset), failThreshold, recThreshold, token)
	if err != nil {
		t.Fatalf("RecordProbe(fail): %v", err)
	}
	return out
}

// succeed records one successful probe at the given offset from baseTime.
func succeed(t *testing.T, s *Store, name string, offset time.Duration) Outcome {
	t.Helper()
	out, err := s.RecordProbe(context.Background(), name, true, 42, "",
		baseTime.Add(offset), failThreshold, recThreshold, "unused-token")
	if err != nil {
		t.Fatalf("RecordProbe(success): %v", err)
	}
	return out
}

func TestFailureThresholdOpensIncidentOnThirdFailure(t *testing.T) {
	s, _ := newTestStore(t)

	for i := 1; i <= failThreshold-1; i++ {
		out := fail(t, s, "web", time.Duration(i)*time.Minute, "tok")
		if out.Action != ActionNone {
			t.Fatalf("failure %d: action = %q, want %q", i, out.Action, ActionNone)
		}
		if out.Status != StatusUp {
			t.Fatalf("failure %d: status = %q, want %q (below threshold)", i, out.Status, StatusUp)
		}
	}

	out := fail(t, s, "web", failThreshold*time.Minute, "tok-abc")
	if out.Action != ActionPageNew {
		t.Fatalf("action on threshold failure = %q, want %q", out.Action, ActionPageNew)
	}
	if out.Status != StatusDown {
		t.Fatalf("status = %q, want %q", out.Status, StatusDown)
	}
	if out.AckToken != "tok-abc" {
		t.Fatalf("ack token = %q, want the token supplied on the opening call", out.AckToken)
	}
	if out.EscalationLevel != 1 {
		t.Fatalf("escalation level = %d, want 1 for the opening page", out.EscalationLevel)
	}
}

func TestRepeatedFailuresDoNotRepage(t *testing.T) {
	s, _ := newTestStore(t)
	for i := 1; i <= failThreshold; i++ {
		fail(t, s, "web", time.Duration(i)*time.Minute, "tok-first")
	}

	// Dedupe: the incident already exists, so nothing else should page.
	for i := failThreshold + 1; i <= failThreshold+5; i++ {
		out := fail(t, s, "web", time.Duration(i)*time.Minute, "tok-later")
		if out.Action != ActionNone {
			t.Fatalf("failure %d after incident opened: action = %q, want %q", i, out.Action, ActionNone)
		}
		if out.AckToken != "tok-first" {
			t.Fatalf("failure %d: ack token = %q, want the original %q — a rotating token would "+
				"invalidate the ack link already sent to the phone", i, out.AckToken, "tok-first")
		}
	}
}

func TestRecoveryRequiresConsecutiveSuccesses(t *testing.T) {
	s, _ := newTestStore(t)
	for i := 1; i <= failThreshold; i++ {
		fail(t, s, "web", time.Duration(i)*time.Minute, "tok")
	}

	if out := succeed(t, s, "web", 10*time.Minute); out.Action != ActionNone || out.Status != StatusDown {
		t.Fatalf("first success: action=%q status=%q, want none/down (recovery needs %d)",
			out.Action, out.Status, recThreshold)
	}

	out := succeed(t, s, "web", 11*time.Minute)
	if out.Action != ActionResolved {
		t.Fatalf("second success: action = %q, want %q", out.Action, ActionResolved)
	}
	if out.Status != StatusUp {
		t.Fatalf("second success: status = %q, want %q", out.Status, StatusUp)
	}
	if out.FirstSeen != baseTime.Add(failThreshold*time.Minute).Unix() {
		t.Fatalf("first_seen = %d, want the moment the incident opened", out.FirstSeen)
	}

	inc, err := s.GetIncident(context.Background(), "web")
	if err != nil {
		t.Fatalf("GetIncident: %v", err)
	}
	if inc != nil {
		t.Fatalf("incident still present after resolve: %+v", inc)
	}
}

// A single success part-way through a failure run must reset the counter, so a
// flapping endpoint does not accumulate failures across recoveries.
func TestSuccessResetsFailureRun(t *testing.T) {
	s, _ := newTestStore(t)
	fail(t, s, "web", 1*time.Minute, "tok")
	fail(t, s, "web", 2*time.Minute, "tok")
	succeed(t, s, "web", 3*time.Minute)

	for i := 4; i <= 5; i++ {
		if out := fail(t, s, "web", time.Duration(i)*time.Minute, "tok"); out.Action != ActionNone {
			t.Fatalf("failure %d after reset: action = %q, want %q", i, out.Action, ActionNone)
		}
	}
	if out := fail(t, s, "web", 6*time.Minute, "tok"); out.Action != ActionPageNew {
		t.Fatalf("third failure after reset: action = %q, want %q", out.Action, ActionPageNew)
	}
}

// The design principle under test: state lives in Redis, so a fresh process
// picks up mid-incident without losing or duplicating anything.
func TestStateSurvivesRestart(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb1 := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s1 := New(rdb1)

	for i := 1; i <= failThreshold; i++ {
		out, err := s1.RecordProbe(context.Background(), "web", false, 100, "boom",
			baseTime.Add(time.Duration(i)*time.Minute), failThreshold, recThreshold, "tok-original")
		if err != nil {
			t.Fatalf("RecordProbe: %v", err)
		}
		if i == failThreshold && out.Action != ActionPageNew {
			t.Fatalf("expected page on failure %d", i)
		}
	}
	rdb1.Close() // simulate the process dying mid-incident

	rdb2 := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb2.Close()
	s2 := New(rdb2)

	inc, err := s2.GetIncident(context.Background(), "web")
	if err != nil {
		t.Fatalf("GetIncident after restart: %v", err)
	}
	if inc == nil {
		t.Fatal("incident lost across restart")
	}
	if inc.Status != IncidentFiring || inc.EscalationLevel != 1 {
		t.Fatalf("after restart incident = %+v, want firing at level 1", inc)
	}

	out, err := s2.RecordProbe(context.Background(), "web", false, 100, "boom",
		baseTime.Add(10*time.Minute), failThreshold, recThreshold, "tok-new")
	if err != nil {
		t.Fatalf("RecordProbe after restart: %v", err)
	}
	if out.Action != ActionNone {
		t.Fatalf("action after restart = %q, want %q — the restarted process re-paged", out.Action, ActionNone)
	}
	if out.AckToken != "tok-original" {
		t.Fatalf("ack token after restart = %q, want %q", out.AckToken, "tok-original")
	}
}

func TestClaimEscalationTiming(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	const threshold = 5 * time.Minute

	// Open the incident at baseTime+3m; last_notified is set to that moment.
	for i := 1; i <= failThreshold; i++ {
		fail(t, s, "web", time.Duration(i)*time.Minute, "tok")
	}
	opened := baseTime.Add(failThreshold * time.Minute)

	// Not yet due.
	for _, d := range []time.Duration{0, time.Minute, 4*time.Minute + 59*time.Second} {
		got, err := s.ClaimEscalation(ctx, "web", opened.Add(d), threshold, 3)
		if err != nil {
			t.Fatalf("ClaimEscalation at +%s: %v", d, err)
		}
		if got != 0 {
			t.Fatalf("ClaimEscalation at +%s returned level %d, want 0 (not due yet)", d, got)
		}
	}

	// Due at exactly the threshold.
	got, err := s.ClaimEscalation(ctx, "web", opened.Add(threshold), threshold, 3)
	if err != nil {
		t.Fatalf("ClaimEscalation: %v", err)
	}
	if got != 2 {
		t.Fatalf("first escalation returned %d, want 2 (the opening page was level 1)", got)
	}

	// The claim is atomic, so an immediately repeated tick must not also win.
	got, err = s.ClaimEscalation(ctx, "web", opened.Add(threshold), threshold, 3)
	if err != nil {
		t.Fatalf("ClaimEscalation (repeat): %v", err)
	}
	if got != 0 {
		t.Fatalf("repeat claim at the same instant returned %d, want 0 — this would double-page", got)
	}

	// Level 3 at +10m, then capped: the ladder goes quiet.
	if got, _ := s.ClaimEscalation(ctx, "web", opened.Add(2*threshold), threshold, 3); got != 3 {
		t.Fatalf("second escalation returned %d, want 3", got)
	}
	for _, d := range []time.Duration{3 * threshold, 10 * threshold, 48 * time.Hour} {
		if got, _ := s.ClaimEscalation(ctx, "web", opened.Add(d), threshold, 3); got != 0 {
			t.Fatalf("claim at +%s returned %d, want 0 — escalation must stop at the cap", d, got)
		}
	}
}

func TestAckedIncidentStopsEscalating(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	const threshold = 5 * time.Minute

	for i := 1; i <= failThreshold; i++ {
		fail(t, s, "web", time.Duration(i)*time.Minute, "tok-ack")
	}
	opened := baseTime.Add(failThreshold * time.Minute)

	ok, err := s.Ack(ctx, "web", "tok-ack")
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if !ok {
		t.Fatal("Ack returned false for a live incident with the right token")
	}

	for _, d := range []time.Duration{threshold, 2 * threshold, 12 * time.Hour} {
		if got, _ := s.ClaimEscalation(ctx, "web", opened.Add(d), threshold, 3); got != 0 {
			t.Fatalf("acked incident escalated to %d at +%s, want 0", got, d)
		}
	}

	// Acking silences escalation, not recovery.
	succeed(t, s, "web", 20*time.Minute)
	out := succeed(t, s, "web", 21*time.Minute)
	if out.Action != ActionResolved {
		t.Fatalf("acked incident recovery: action = %q, want %q", out.Action, ActionResolved)
	}
}

func TestAckRejectsWrongToken(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	for i := 1; i <= failThreshold; i++ {
		fail(t, s, "web", time.Duration(i)*time.Minute, "correct-token")
	}

	if ok, _ := s.Ack(ctx, "web", "wrong-token"); ok {
		t.Fatal("Ack accepted a wrong token")
	}
	inc, _ := s.GetIncident(ctx, "web")
	if inc.Status != IncidentFiring {
		t.Fatalf("incident status = %q after a rejected ack, want %q", inc.Status, IncidentFiring)
	}

	// A token from a resolved incident must not ack a later one: it dies with
	// the hash, so the reopened incident has a different secret.
	succeed(t, s, "web", 10*time.Minute)
	succeed(t, s, "web", 11*time.Minute)
	for i := 12; i <= 14; i++ {
		fail(t, s, "web", time.Duration(i)*time.Minute, "second-token")
	}
	if ok, _ := s.Ack(ctx, "web", "correct-token"); ok {
		t.Fatal("a stale token from the previous incident acked the new one")
	}
}

func TestAckOnMissingIncident(t *testing.T) {
	s, _ := newTestStore(t)
	if ok, err := s.Ack(context.Background(), "web", "whatever"); err != nil || ok {
		t.Fatalf("Ack with no incident = (%v, %v), want (false, nil)", ok, err)
	}
	tok, err := s.AckToken(context.Background(), "web")
	if err != nil || tok != "" {
		t.Fatalf("AckToken with no incident = (%q, %v), want (\"\", nil)", tok, err)
	}
}

// After the ladder caps out nothing notifies, so only the poll path is left to
// keep the incident alive. If the TTL were refreshed on notification writes
// alone, the hash would expire and the next poll would read it as a brand-new
// outage and page again at full urgency.
func TestPollRefreshesIncidentTTLWhileDown(t *testing.T) {
	s, mr := newTestStore(t)
	ctx := context.Background()

	for i := 1; i <= failThreshold; i++ {
		fail(t, s, "web", time.Duration(i)*time.Minute, "tok")
	}

	// A silent day passes, polled hourly, with no notifications at all.
	for hour := 1; hour <= 26; hour++ {
		mr.FastForward(time.Hour)
		out := fail(t, s, "web", time.Duration(hour)*time.Hour, "tok-fresh")
		if out.Action != ActionNone {
			t.Fatalf("hour %d: action = %q, want %q — the incident expired and re-paged",
				hour, out.Action, ActionNone)
		}
	}

	inc, err := s.GetIncident(ctx, "web")
	if err != nil {
		t.Fatalf("GetIncident: %v", err)
	}
	if inc == nil {
		t.Fatal("incident expired after 26h of polling; it should be kept alive while down")
	}
	if inc.FirstSeen.Time() != baseTime.Add(failThreshold*time.Minute) {
		t.Fatalf("first_seen = %s, want the original outage start — the incident was recreated",
			inc.FirstSeen.Time())
	}
}

func TestFiringNamesExcludesAckedAndHealthy(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	for _, name := range []string{"down-firing", "down-acked"} {
		for i := 1; i <= failThreshold; i++ {
			fail(t, s, name, time.Duration(i)*time.Minute, "tok-"+name)
		}
	}
	succeed(t, s, "healthy", time.Minute)
	if _, err := s.Ack(ctx, "down-acked", "tok-down-acked"); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	got, err := s.FiringNames(ctx, []string{"down-firing", "down-acked", "healthy", "never-seen"})
	if err != nil {
		t.Fatalf("FiringNames: %v", err)
	}
	if len(got) != 1 || got[0] != "down-firing" {
		t.Fatalf("FiringNames = %v, want [down-firing]", got)
	}
}

func TestSnapshotAndCycleTracking(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	succeed(t, s, "healthy", time.Minute)
	for i := 1; i <= failThreshold; i++ {
		fail(t, s, "broken", time.Duration(i)*time.Minute, "tok")
	}

	snap, err := s.Snapshot(ctx, []string{"healthy", "broken", "unseen"})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap["healthy"].Status != StatusUp || snap["healthy"].Incident != nil {
		t.Fatalf("healthy snapshot = %+v, want up with no incident", snap["healthy"])
	}
	if snap["broken"].Status != StatusDown || snap["broken"].Incident == nil {
		t.Fatalf("broken snapshot = %+v, want down with an incident", snap["broken"])
	}
	if snap["broken"].ConsecutiveFailures != failThreshold {
		t.Fatalf("broken consecutive_failures = %d, want %d",
			snap["broken"].ConsecutiveFailures, failThreshold)
	}
	if snap["unseen"].Status != "unknown" {
		t.Fatalf("never-probed check status = %q, want %q", snap["unseen"].Status, "unknown")
	}

	if last, err := s.LastCycleCompleted(ctx); err != nil || !last.IsZero() {
		t.Fatalf("LastCycleCompleted before any cycle = (%v, %v), want zero time", last, err)
	}
	if err := s.SetCycleCompleted(ctx, baseTime); err != nil {
		t.Fatalf("SetCycleCompleted: %v", err)
	}
	last, err := s.LastCycleCompleted(ctx)
	if err != nil {
		t.Fatalf("LastCycleCompleted: %v", err)
	}
	if !last.Equal(baseTime) {
		t.Fatalf("LastCycleCompleted = %s, want %s", last, baseTime)
	}
}

// The ack token is a secret; serialising it through /status would hand it to
// anyone who can reach the endpoint.
func TestIncidentJSONOmitsAckToken(t *testing.T) {
	s, _ := newTestStore(t)
	for i := 1; i <= failThreshold; i++ {
		fail(t, s, "web", time.Duration(i)*time.Minute, "super-secret-token")
	}
	snap, err := s.Snapshot(context.Background(), []string{"web"})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	blob, err := json.Marshal(snap["web"])
	if err != nil {
		t.Fatalf("marshal check state: %v", err)
	}
	for _, secret := range []string{"super-secret-token", "ack_token"} {
		if strings.Contains(string(blob), secret) {
			t.Fatalf("serialised check state leaks %q: %s", secret, blob)
		}
	}
}
