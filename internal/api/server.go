// Package api serves the health, status and acknowledgement endpoints.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"html/template"
	"log/slog"
	"net/http"
	"time"

	"github.com/Ginny-Binny/pager/internal/config"
	"github.com/Ginny-Binny/pager/internal/store"
)

// staleCycleMultiplier decides when a poller counts as stuck. Three intervals
// tolerates one slow cycle without crying wolf, while still surfacing a wedged
// loop to an external heartbeat service well inside five minutes at defaults.
const staleCycleMultiplier = 3

// Server holds the HTTP handlers.
type Server struct {
	cfg   *config.Config
	store *store.Store
	log   *slog.Logger
	now   func() time.Time
}

// New builds a Server. Pass nil for now to use the wall clock.
func New(cfg *config.Config, st *store.Store, log *slog.Logger, now func() time.Time) *Server {
	if now == nil {
		now = time.Now
	}
	return &Server{cfg: cfg, store: st, log: log, now: now}
}

// Routes returns the mux. Method-and-path patterns are stdlib as of Go 1.22,
// so no router dependency is needed.
func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /ack/{check}", s.handleAck)
	mux.HandleFunc("POST /ack/{check}", s.handleAck)
	return mux
}

type healthResponse struct {
	Status          string `json:"status"`
	LastCycle       string `json:"last_cycle,omitempty"`
	LastCycleAgeSec int64  `json:"last_cycle_age_seconds,omitempty"`
	Detail          string `json:"detail,omitempty"`
}

// handleHealth is the endpoint an external heartbeat service watches. It is
// the only thing that can detect a wedged poller, so it reports unhealthy on
// anything it cannot positively verify — including an unreachable Redis, where
// the monitor knows nothing and must not claim otherwise.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	if err := s.store.Ping(ctx); err != nil {
		s.log.Error("health check: redis unreachable", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, healthResponse{
			Status: "unhealthy",
			Detail: "redis unreachable: " + err.Error(),
		})
		return
	}

	last, err := s.store.LastCycleCompleted(ctx)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, healthResponse{
			Status: "unhealthy",
			Detail: "cannot read last cycle: " + err.Error(),
		})
		return
	}
	if last.IsZero() {
		writeJSON(w, http.StatusServiceUnavailable, healthResponse{
			Status: "unhealthy",
			Detail: "no poll cycle has completed yet",
		})
		return
	}

	age := s.now().Sub(last)
	resp := healthResponse{
		Status:          "ok",
		LastCycle:       last.UTC().Format(time.RFC3339),
		LastCycleAgeSec: int64(age.Seconds()),
	}
	if age > staleCycleMultiplier*s.cfg.PollInterval {
		resp.Status = "unhealthy"
		resp.Detail = "last poll cycle is stale; poller may be stuck"
		s.log.Error("health check: poller appears stuck",
			"last_cycle", last.UTC().Format(time.RFC3339), "age", age.String())
		writeJSON(w, http.StatusServiceUnavailable, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

type statusResponse struct {
	GeneratedAt string             `json:"generated_at"`
	LastCycle   string             `json:"last_cycle,omitempty"`
	Checks      []store.CheckState `json:"checks"`
}

// handleStatus reports every check and its incident. store.Incident carries no
// ack_token field, so the ack secrets cannot leak through this endpoint.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	snap, err := s.store.Snapshot(ctx, s.cfg.Names())
	if err != nil {
		s.log.Error("status: snapshot failed", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}

	resp := statusResponse{GeneratedAt: s.now().UTC().Format(time.RFC3339)}
	if last, err := s.store.LastCycleCompleted(ctx); err == nil && !last.IsZero() {
		resp.LastCycle = last.UTC().Format(time.RFC3339)
	}
	// Config order, not map order, so the output is stable between requests.
	for _, ck := range s.cfg.Checks {
		st := snap[ck.Name]
		st.Name = ck.Name
		st.URL = ck.URL
		resp.Checks = append(resp.Checks, st)
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleAck acknowledges an incident. Served on both GET (tapping the link in
// the notification body) and POST (the ntfy action button).
func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	name := r.PathValue("check")
	if _, ok := s.checkExists(name); !ok {
		s.renderAck(w, http.StatusNotFound, ackPage{
			Heading: "Unknown check",
			Detail:  "No check named " + name + " is configured.",
			Bad:     true,
		})
		return
	}

	token := r.URL.Query().Get("t")
	if token == "" {
		token = r.Header.Get("X-Ack-Token")
	}

	want, err := s.store.AckToken(ctx, name)
	if err != nil {
		s.log.Error("ack: failed to read token", "check", name, "error", err)
		s.renderAck(w, http.StatusServiceUnavailable, ackPage{
			Heading: "Temporarily unavailable",
			Detail:  "Could not reach the state store. Try again.",
			Bad:     true,
		})
		return
	}
	if want == "" {
		s.renderAck(w, http.StatusNotFound, ackPage{
			Heading: "Nothing to acknowledge",
			Detail:  name + " has no open incident. It may have already recovered.",
		})
		return
	}

	// Constant-time so the token cannot be recovered by timing the responses.
	if subtle.ConstantTimeCompare([]byte(token), []byte(want)) != 1 {
		s.log.Warn("ack: rejected bad token", "check", name, "remote", r.RemoteAddr)
		s.renderAck(w, http.StatusForbidden, ackPage{
			Heading: "Invalid link",
			Detail:  "This acknowledgement link is not valid for the current incident.",
			Bad:     true,
		})
		return
	}

	ok, err := s.store.Ack(ctx, name, token)
	if err != nil {
		s.log.Error("ack: failed to write", "check", name, "error", err)
		s.renderAck(w, http.StatusServiceUnavailable, ackPage{
			Heading: "Temporarily unavailable",
			Detail:  "Could not reach the state store. Try again.",
			Bad:     true,
		})
		return
	}
	if !ok {
		// The incident resolved between the read and the write.
		s.renderAck(w, http.StatusNotFound, ackPage{
			Heading: "Nothing to acknowledge",
			Detail:  name + " has no open incident. It may have already recovered.",
		})
		return
	}

	s.log.Info("incident acknowledged", "check", name, "remote", r.RemoteAddr)
	s.renderAck(w, http.StatusOK, ackPage{
		Heading: "Acknowledged",
		Detail:  name + " will stop escalating. You will still get the recovery message.",
	})
}

func (s *Server) checkExists(name string) (config.Check, bool) {
	for _, ck := range s.cfg.Checks {
		if ck.Name == name {
			return ck, true
		}
	}
	return config.Check{}, false
}

type ackPage struct {
	Heading string
	Detail  string
	Bad     bool
}

// ackTmpl is fully self-contained: a phone opening this mid-incident may be on
// a bad connection, and there is no reason to depend on external assets.
var ackTmpl = template.Must(template.New("ack").Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Heading}}</title>
<style>
  :root { color-scheme: light dark; }
  body {
    margin: 0; min-height: 100vh;
    display: flex; align-items: center; justify-content: center;
    font: 16px/1.5 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
    background: #f6f7f9; color: #1b1d21;
  }
  .card {
    max-width: 30rem; margin: 1.5rem; padding: 2rem 1.75rem;
    background: #fff; border-radius: 14px;
    box-shadow: 0 1px 3px rgba(0,0,0,.08), 0 8px 24px rgba(0,0,0,.06);
    text-align: center;
  }
  .mark { font-size: 2.75rem; line-height: 1; }
  h1 { margin: .75rem 0 .5rem; font-size: 1.35rem; }
  p { margin: 0; color: #5a6069; }
  @media (prefers-color-scheme: dark) {
    body { background: #16181c; color: #e8eaed; }
    .card { background: #212429; box-shadow: none; }
    p { color: #a2a8b3; }
  }
</style>
</head>
<body>
  <div class="card">
    <div class="mark">{{if .Bad}}&#9888;&#65039;{{else}}&#9989;{{end}}</div>
    <h1>{{.Heading}}</h1>
    <p>{{.Detail}}</p>
  </div>
</body>
</html>
`))

func (s *Server) renderAck(w http.ResponseWriter, code int, p ackPage) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	if err := ackTmpl.Execute(w, p); err != nil {
		s.log.Error("failed to render ack page", "error", err)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
