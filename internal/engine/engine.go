// Package engine runs the poll and escalation loops and turns state
// transitions into pages.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/Ginny-Binny/pager/internal/config"
	"github.com/Ginny-Binny/pager/internal/notify"
	"github.com/Ginny-Binny/pager/internal/probe"
	"github.com/Ginny-Binny/pager/internal/store"
)

// MaxEscalationLevel caps the ladder. The opening page is level 1, so pages
// land at roughly 0, 5 and 10 minutes and then stop. The incident stays open
// and visible in /status; the poller keeps refreshing its TTL so going quiet
// never turns into a surprise re-page a day later.
const MaxEscalationLevel = 3

// escalationTick is how often the escalation loop looks for work. It is
// deliberately much shorter than the escalation threshold so a due escalation
// fires promptly rather than up to a whole interval late.
const escalationTick = 30 * time.Second

// maxConcurrentProbes bounds fan-out so a long check list cannot open an
// unbounded number of sockets at once.
const maxConcurrentProbes = 16

// Notifier is the subset of the ntfy client the engine needs, so tests can
// capture pages instead of sending them.
type Notifier interface {
	Publish(ctx context.Context, m notify.Message) error
}

// Engine ties config, probing, state and notification together.
type Engine struct {
	cfg      *config.Config
	store    *store.Store
	prober   *probe.Prober
	notifier Notifier
	log      *slog.Logger

	// now is injected so escalation timing is testable without sleeping.
	now func() time.Time
}

// New builds an Engine. Pass nil for now to use the wall clock.
func New(cfg *config.Config, st *store.Store, pr *probe.Prober, n Notifier, log *slog.Logger, now func() time.Time) *Engine {
	if now == nil {
		now = time.Now
	}
	return &Engine{cfg: cfg, store: st, prober: pr, notifier: n, log: log, now: now}
}

// RunPoller probes every check on an interval until ctx is cancelled. The
// first cycle runs immediately so a fresh start does not look stuck to
// /health for a whole interval.
func (e *Engine) RunPoller(ctx context.Context) {
	e.PollOnce(ctx)

	t := time.NewTicker(e.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.PollOnce(ctx)
		}
	}
}

// PollOnce probes every check concurrently and records the cycle completion.
func (e *Engine) PollOnce(ctx context.Context) {
	start := e.now()

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrentProbes)
	for _, ck := range e.cfg.Checks {
		wg.Add(1)
		go func(ck config.Check) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			e.probeAndRecord(ctx, ck)
		}(ck)
	}
	wg.Wait()

	if ctx.Err() != nil {
		return
	}
	// Written only after every probe has been recorded, so /health reflects a
	// genuinely completed cycle rather than one that started and stalled.
	if err := e.store.SetCycleCompleted(ctx, e.now()); err != nil {
		e.log.Error("failed to record poll cycle completion", "error", err)
		return
	}
	e.log.Debug("poll cycle complete",
		"checks", len(e.cfg.Checks),
		"duration_ms", e.now().Sub(start).Milliseconds())
}

func (e *Engine) probeAndRecord(ctx context.Context, ck config.Check) {
	res := e.prober.Probe(ctx, ck)

	if !res.OK {
		e.log.Warn("probe failed",
			"check", ck.Name,
			"url", ck.URL,
			"reason", res.Reason,
			"status_code", res.StatusCode,
			"latency_ms", res.LatencyMS)
	}

	token, err := newAckToken()
	if err != nil {
		e.log.Error("failed to generate ack token", "check", ck.Name, "error", err)
		return
	}

	out, err := e.store.RecordProbe(ctx, ck.Name, res.OK, res.LatencyMS, res.Reason,
		e.now(), e.cfg.FailureThreshold, e.cfg.RecoveryThreshold, token)
	if err != nil {
		e.log.Error("failed to record probe result", "check", ck.Name, "error", err)
		return
	}

	switch out.Action {
	case store.ActionPageNew:
		e.log.Warn("state transition: UP -> DOWN",
			"check", ck.Name, "url", ck.URL, "reason", res.Reason,
			"consecutive_failures", e.cfg.FailureThreshold)
		e.page(ctx, ck, res, out, 1)

	case store.ActionResolved:
		downtime := e.now().Sub(time.Unix(out.FirstSeen, 0))
		e.log.Info("state transition: DOWN -> UP",
			"check", ck.Name, "url", ck.URL,
			"downtime", downtime.Round(time.Second).String())
		e.recover(ctx, ck, res, downtime)
	}
}

// page sends a priority-5 notification for a new or escalated incident.
func (e *Engine) page(ctx context.Context, ck config.Check, res probe.Result, out store.Outcome, level int) {
	title := fmt.Sprintf("%s is DOWN", ck.Name)
	if level > 1 {
		title = fmt.Sprintf("[L%d] %s is STILL DOWN", level, ck.Name)
	}

	body := fmt.Sprintf("Reason: %s\nURL: %s", res.Reason, ck.URL)
	if res.StatusCode > 0 {
		body += fmt.Sprintf("\nHTTP status: %d", res.StatusCode)
	}
	// Omitted rather than reported as zero: a timed-out or refused probe has
	// no meaningful latency, and "Latency: 0ms" reads as suspiciously fast.
	if res.LatencyMS > 0 {
		body += fmt.Sprintf("\nLatency: %dms", res.LatencyMS)
	}
	if out.FirstSeen > 0 {
		since := e.now().Sub(time.Unix(out.FirstSeen, 0)).Round(time.Second)
		body += fmt.Sprintf("\nDown for: %s (since %s)",
			since, time.Unix(out.FirstSeen, 0).UTC().Format(time.RFC3339))
	}

	ackURL := e.ackURL(ck.Name, out.AckToken)
	body += "\n\nAcknowledge: " + ackURL

	msg := notify.Message{
		Title:       title,
		Body:        body,
		Priority:    notify.PriorityPage,
		Tags:        []string{"rotating_light"},
		ActionURL:   e.ackURL(ck.Name, ""),
		ActionToken: out.AckToken,
	}
	if err := e.notifier.Publish(ctx, msg); err != nil {
		e.log.Error("failed to send page", "check", ck.Name, "level", level, "error", err)
		return
	}
	e.log.Info("page sent", "check", ck.Name, "level", level, "priority", notify.PriorityPage)
}

// recover sends the priority-2 all-clear. It fires for acked incidents too:
// acking silences escalation, not the news that the outage ended.
func (e *Engine) recover(ctx context.Context, ck config.Check, res probe.Result, downtime time.Duration) {
	msg := notify.Message{
		Title:    fmt.Sprintf("%s recovered", ck.Name),
		Priority: notify.PriorityRecovery,
		Tags:     []string{"white_check_mark"},
		Body: fmt.Sprintf("Back up after %s\nURL: %s\nHTTP status: %d\nLatency: %dms",
			downtime.Round(time.Second), ck.URL, res.StatusCode, res.LatencyMS),
	}
	if err := e.notifier.Publish(ctx, msg); err != nil {
		e.log.Error("failed to send recovery notice", "check", ck.Name, "error", err)
		return
	}
	e.log.Info("recovery sent", "check", ck.Name, "downtime", downtime.Round(time.Second).String())
}

// RunEscalation re-pages firing incidents that have gone unacknowledged.
func (e *Engine) RunEscalation(ctx context.Context) {
	t := time.NewTicker(escalationTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.EscalateOnce(ctx)
		}
	}
}

// EscalateOnce advances every firing incident that is due.
func (e *Engine) EscalateOnce(ctx context.Context) {
	firing, err := e.store.FiringNames(ctx, e.cfg.Names())
	if err != nil {
		e.log.Error("failed to list firing incidents", "error", err)
		return
	}
	for _, name := range firing {
		ck, ok := e.checkByName(name)
		if !ok {
			continue
		}
		// The claim is atomic: a non-zero level means this tick, and only this
		// tick, owns the page for that level.
		level, err := e.store.ClaimEscalation(ctx, name, e.now(), e.cfg.EscalationInterval, MaxEscalationLevel)
		if err != nil {
			e.log.Error("failed to claim escalation", "check", name, "error", err)
			continue
		}
		if level == 0 {
			continue
		}

		inc, err := e.store.GetIncident(ctx, name)
		if err != nil || inc == nil {
			e.log.Error("escalation claimed but incident is gone", "check", name, "error", err)
			continue
		}

		e.log.Warn("escalating incident",
			"check", name, "level", level, "reason", inc.Reason,
			"down_since", inc.FirstSeen.Time().UTC().Format(time.RFC3339))

		if level == 2 {
			// ─── PHONE CALL ESCALATION STUB ──────────────────────────────
			// Level 2 is where a voice call would be placed, because a push
			// notification has already failed to get a response once.
			//
			//   Twilio: POST https://api.twilio.com/2010-04-01/Accounts/{sid}/Calls.json
			//           (To, From, Url or Twiml)
			//   Exotel: POST https://api.exotel.com/v1/Accounts/{sid}/Calls/connect
			//           (From, To, CallerId)
			//
			// Deliberately not implemented: no provider is configured, and a
			// half-wired call path that silently fails is worse than none.
			e.log.Warn("phone call escalation point reached (stub, no provider configured)",
				"check", name, "level", level)
		}

		token, err := e.store.AckToken(ctx, name)
		if err != nil {
			e.log.Error("failed to read ack token for escalation", "check", name, "error", err)
			continue
		}
		out := store.Outcome{
			AckToken:        token,
			FirstSeen:       int64(inc.FirstSeen),
			EscalationLevel: level,
		}
		// Re-page with the latest observed latency rather than a zero value —
		// the escalation loop has no probe of its own to report.
		res := probe.Result{Reason: inc.Reason}
		if snap, err := e.store.Snapshot(ctx, []string{name}); err == nil {
			res.LatencyMS = snap[name].LastLatencyMS
		}
		e.page(ctx, ck, res, out, level)
	}
}

func (e *Engine) checkByName(name string) (config.Check, bool) {
	for _, ck := range e.cfg.Checks {
		if ck.Name == name {
			return ck, true
		}
	}
	return config.Check{}, false
}

// ackURL builds the acknowledgement link. An empty token yields the bare URL,
// used for the POST action button that carries the token in a header instead.
func (e *Engine) ackURL(name, token string) string {
	base := e.cfg.PublicBaseURL
	for len(base) > 0 && base[len(base)-1] == '/' {
		base = base[:len(base)-1]
	}
	u := base + "/ack/" + url.PathEscape(name)
	if token != "" {
		u += "?t=" + url.QueryEscape(token)
	}
	return u
}

// newAckToken returns 128 bits of randomness, hex encoded. Unguessable, and
// scoped to a single incident: it is stored in the incident hash and dies with
// it, so a token from a resolved incident cannot ack a later one.
func newAckToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate ack token: %w", err)
	}
	return hex.EncodeToString(b), nil
}
