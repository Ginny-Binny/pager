// Package store owns every piece of monitor state. Nothing lives in process
// memory, so a restart never loses a pending escalation and never re-pages an
// acked incident.
//
// The three decisions that must not race — open an incident, claim an
// escalation, ack an incident — are each a single Lua script. Doing the test
// and the write in one round trip is what makes "page exactly once" hold when
// two ticks overlap, when a restart lands mid-cycle, or when a second instance
// is briefly running during a deploy.
package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// IncidentTTL bounds how long a forgotten incident can linger. The poller
// refreshes it on every cycle while the check is DOWN, so a live outage never
// expires out from under us — which would otherwise look like a brand-new
// incident and re-page at full urgency.
const IncidentTTL = 24 * time.Hour

// Action is what the caller should do about a probe result.
type Action string

const (
	// ActionNone covers both "still healthy" and "already firing" — the dedupe
	// case falls out of the script rather than needing a Go-side check.
	ActionNone Action = "none"
	// ActionPageNew means an incident was just opened by this call.
	ActionPageNew Action = "page_new"
	// ActionResolved means the incident closed and was deleted by this call.
	ActionResolved Action = "resolved"
)

// Status values for a check.
const (
	StatusUp   = "up"
	StatusDown = "down"
)

// Incident status values.
const (
	IncidentFiring = "firing"
	IncidentAcked  = "acked"
)

// Outcome is the result of recording one probe.
type Outcome struct {
	Action Action
	// Status is the check status after applying thresholds: "up" or "down".
	Status string
	// AckToken is the live incident's token, empty when there is no incident.
	AckToken string
	// FirstSeen is the incident start, unix seconds. Zero when no incident.
	FirstSeen int64
	// EscalationLevel is the incident's level; the opening page is level 1.
	EscalationLevel int
}

// CheckState is the snapshot of one check for the /status endpoint.
type CheckState struct {
	Name                 string    `json:"name"`
	URL                  string    `json:"url"`
	Status               string    `json:"status"`
	LastLatencyMS        int64     `json:"last_latency_ms"`
	LastChecked          *UnixTime `json:"last_checked,omitempty"`
	LastReason           string    `json:"last_reason,omitempty"`
	ConsecutiveFailures  int       `json:"consecutive_failures"`
	ConsecutiveSuccesses int       `json:"consecutive_successes"`
	Incident             *Incident `json:"incident,omitempty"`
}

// Incident is the public view of a firing or acked incident. It deliberately
// has no ack_token field: /status is reachable by anyone who can reach the
// service, and serialising the token there would hand out the ack secret.
type Incident struct {
	Status          string   `json:"status"`
	FirstSeen       UnixTime `json:"first_seen"`
	LastNotified    UnixTime `json:"last_notified"`
	EscalationLevel int      `json:"escalation_level"`
	Reason          string   `json:"reason,omitempty"`
}

// Store is the Redis-backed state store.
type Store struct {
	rdb *redis.Client
}

// New wraps an existing client.
func New(rdb *redis.Client) *Store { return &Store{rdb: rdb} }

func checkKey(name string) string    { return "pager:check:" + name }
func incidentKey(name string) string { return "pager:incident:" + name }

const metaKey = "pager:meta"

// recordProbeScript applies one probe result and reports what changed.
//
// KEYS[1] check hash, KEYS[2] incident hash
// ARGV[1] ok ("1"/"0")   ARGV[2] latency_ms      ARGV[3] reason
// ARGV[4] now (unix s)   ARGV[5] fail threshold  ARGV[6] recovery threshold
// ARGV[7] candidate ack token                    ARGV[8] incident ttl seconds
//
// Returns {action, status, ack_token, first_seen, escalation_level}.
var recordProbeScript = redis.NewScript(`
local ok      = ARGV[1] == '1'
local now     = tonumber(ARGV[4])
local failT   = tonumber(ARGV[5])
local recT    = tonumber(ARGV[6])
local token   = ARGV[7]
local ttl     = tonumber(ARGV[8])

local prev = redis.call('HGET', KEYS[1], 'last_status')
if not prev then prev = 'up' end

local cf = tonumber(redis.call('HGET', KEYS[1], 'consecutive_failures') or '0')
local cs = tonumber(redis.call('HGET', KEYS[1], 'consecutive_successes') or '0')
if ok then cf = 0; cs = cs + 1 else cs = 0; cf = cf + 1 end

-- Thresholds are one-directional: only a healthy check can fall down, and only
-- a down check can come back up. Flapping inside a threshold changes nothing.
local status = prev
if prev == 'up'   and cf >= failT then status = 'down' end
if prev == 'down' and cs >= recT then status = 'up'   end

-- Timestamps are written straight from ARGV rather than from the Lua number,
-- to sidestep Lua's %.14g number-to-string conversion entirely.
redis.call('HSET', KEYS[1],
  'last_status', status,
  'last_latency_ms', ARGV[2],
  'last_checked', ARGV[4],
  'last_reason', ARGV[3],
  'consecutive_failures', cf,
  'consecutive_successes', cs)

local action    = 'none'
local ackToken  = ''
local firstSeen = 0
local level     = 0

if status == 'down' then
  if redis.call('EXISTS', KEYS[2]) == 0 then
    -- Creating the hash is itself the dedupe: whoever wins this branch is the
    -- only caller that gets told to page.
    redis.call('HSET', KEYS[2],
      'status', 'firing',
      'first_seen', ARGV[4],
      'last_notified', ARGV[4],
      'escalation_level', '1',
      'ack_token', token,
      'reason', ARGV[3])
    action    = 'page_new'
    ackToken  = token
    firstSeen = now
    level     = 1
  else
    ackToken  = redis.call('HGET', KEYS[2], 'ack_token')
    firstSeen = tonumber(redis.call('HGET', KEYS[2], 'first_seen') or '0')
    level     = tonumber(redis.call('HGET', KEYS[2], 'escalation_level') or '0')
    redis.call('HSET', KEYS[2], 'reason', ARGV[3])
  end
  -- Refreshed every poll while down, not merely on notification writes, so a
  -- silenced level-3 incident cannot expire and resurface as a fresh page.
  redis.call('EXPIRE', KEYS[2], ttl)
elseif prev == 'down' and status == 'up' then
  if redis.call('EXISTS', KEYS[2]) == 1 then
    firstSeen = tonumber(redis.call('HGET', KEYS[2], 'first_seen') or '0')
    action    = 'resolved'
    redis.call('DEL', KEYS[2])
  end
end

return {action, status, ackToken, string.format('%d', firstSeen), string.format('%d', level)}
`)

// RecordProbe stores one probe result and returns what the caller should do.
// newToken is generated per call but only consumed when an incident is opened.
func (s *Store) RecordProbe(ctx context.Context, name string, ok bool, latencyMS int64,
	reason string, now time.Time, failThreshold, recoveryThreshold int, newToken string,
) (Outcome, error) {
	okArg := "0"
	if ok {
		okArg = "1"
	}
	raw, err := recordProbeScript.Run(ctx, s.rdb,
		[]string{checkKey(name), incidentKey(name)},
		okArg, latencyMS, reason, now.Unix(),
		failThreshold, recoveryThreshold, newToken, int(IncidentTTL.Seconds()),
	).Slice()
	if err != nil {
		return Outcome{}, fmt.Errorf("record probe for %s: %w", name, err)
	}
	if len(raw) < 5 {
		return Outcome{}, fmt.Errorf("record probe for %s: unexpected reply %v", name, raw)
	}
	return Outcome{
		Action:          Action(asString(raw[0])),
		Status:          asString(raw[1]),
		AckToken:        asString(raw[2]),
		FirstSeen:       asInt64(raw[3]),
		EscalationLevel: int(asInt64(raw[4])),
	}, nil
}

// claimEscalationScript advances one incident's escalation, or reports 0.
//
// KEYS[1] incident hash
// ARGV[1] now (unix s)  ARGV[2] threshold seconds
// ARGV[3] max level     ARGV[4] incident ttl seconds
//
// Returns the new level, or 0 when this tick must not page.
var claimEscalationScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end

-- Acked incidents stop escalating. They still get their recovery message,
-- which is handled on the poll path, not here.
if redis.call('HGET', KEYS[1], 'status') ~= 'firing' then return 0 end

local now  = tonumber(ARGV[1])
local last = tonumber(redis.call('HGET', KEYS[1], 'last_notified') or '0')
if now - last < tonumber(ARGV[2]) then return 0 end

local level = tonumber(redis.call('HGET', KEYS[1], 'escalation_level') or '0')
if level >= tonumber(ARGV[3]) then return 0 end

level = level + 1
redis.call('HSET', KEYS[1],
  'escalation_level', string.format('%d', level),
  'last_notified', ARGV[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
return level
`)

// ClaimEscalation atomically decides whether this tick escalates, and if so
// records it. A non-zero return is a licence to page exactly once: the read,
// the comparison and the write happen together, so two overlapping ticks
// cannot both come back with the same level.
func (s *Store) ClaimEscalation(ctx context.Context, name string, now time.Time,
	threshold time.Duration, maxLevel int,
) (int, error) {
	level, err := claimEscalationScript.Run(ctx, s.rdb,
		[]string{incidentKey(name)},
		now.Unix(), int64(threshold.Seconds()), maxLevel, int(IncidentTTL.Seconds()),
	).Int()
	if err != nil {
		return 0, fmt.Errorf("claim escalation for %s: %w", name, err)
	}
	return level, nil
}

// ackScript marks an incident acked, re-verifying the token so a race with a
// resolve-and-reopen cannot ack the wrong incident.
//
// KEYS[1] incident hash. ARGV[1] token, ARGV[2] ttl seconds.
var ackScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if redis.call('HGET', KEYS[1], 'ack_token') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'status', 'acked')
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
`)

// AckToken returns the live incident's token, or "" when there is no incident.
// The caller compares it in constant time before deciding to accept the ack.
func (s *Store) AckToken(ctx context.Context, name string) (string, error) {
	tok, err := s.rdb.HGet(ctx, incidentKey(name), "ack_token").Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read ack token for %s: %w", name, err)
	}
	return tok, nil
}

// Ack marks the incident acknowledged. It reports false when the incident is
// gone or the token no longer matches.
func (s *Store) Ack(ctx context.Context, name, token string) (bool, error) {
	n, err := ackScript.Run(ctx, s.rdb,
		[]string{incidentKey(name)}, token, int(IncidentTTL.Seconds()),
	).Int()
	if err != nil {
		return false, fmt.Errorf("ack %s: %w", name, err)
	}
	return n == 1, nil
}

// FiringNames returns the names, among those given, with an open incident that
// is still firing (not acked).
func (s *Store) FiringNames(ctx context.Context, names []string) ([]string, error) {
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(names))
	for i, n := range names {
		cmds[i] = pipe.HGet(ctx, incidentKey(n), "status")
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("list firing incidents: %w", err)
	}
	var out []string
	for i, cmd := range cmds {
		if v, err := cmd.Result(); err == nil && v == IncidentFiring {
			out = append(out, names[i])
		}
	}
	return out, nil
}

// GetIncident returns the incident for a check, or nil when there is none.
func (s *Store) GetIncident(ctx context.Context, name string) (*Incident, error) {
	h, err := s.rdb.HGetAll(ctx, incidentKey(name)).Result()
	if err != nil {
		return nil, fmt.Errorf("read incident for %s: %w", name, err)
	}
	if len(h) == 0 {
		return nil, nil
	}
	return &Incident{
		Status:          h["status"],
		FirstSeen:       UnixTime(atoi64(h["first_seen"])),
		LastNotified:    UnixTime(atoi64(h["last_notified"])),
		EscalationLevel: int(atoi64(h["escalation_level"])),
		Reason:          h["reason"],
	}, nil
}

// Snapshot returns the current state of every named check for /status.
func (s *Store) Snapshot(ctx context.Context, names []string) (map[string]CheckState, error) {
	pipe := s.rdb.Pipeline()
	checkCmds := make([]*redis.MapStringStringCmd, len(names))
	incCmds := make([]*redis.MapStringStringCmd, len(names))
	for i, n := range names {
		checkCmds[i] = pipe.HGetAll(ctx, checkKey(n))
		incCmds[i] = pipe.HGetAll(ctx, incidentKey(n))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}

	out := make(map[string]CheckState, len(names))
	for i, n := range names {
		h, _ := checkCmds[i].Result()
		st := CheckState{Name: n, Status: "unknown"}
		if v, ok := h["last_status"]; ok && v != "" {
			st.Status = v
		}
		st.LastLatencyMS = atoi64(h["last_latency_ms"])
		if ts := atoi64(h["last_checked"]); ts > 0 {
			t := UnixTime(ts)
			st.LastChecked = &t
		}
		st.LastReason = h["last_reason"]
		st.ConsecutiveFailures = int(atoi64(h["consecutive_failures"]))
		st.ConsecutiveSuccesses = int(atoi64(h["consecutive_successes"]))

		if ih, _ := incCmds[i].Result(); len(ih) > 0 {
			st.Incident = &Incident{
				Status:          ih["status"],
				FirstSeen:       UnixTime(atoi64(ih["first_seen"])),
				LastNotified:    UnixTime(atoi64(ih["last_notified"])),
				EscalationLevel: int(atoi64(ih["escalation_level"])),
				Reason:          ih["reason"],
			}
		}
		out[n] = st
	}
	return out, nil
}

// SetCycleCompleted records that a full poll cycle finished.
func (s *Store) SetCycleCompleted(ctx context.Context, at time.Time) error {
	if err := s.rdb.HSet(ctx, metaKey, "last_cycle_completed", at.Unix()).Err(); err != nil {
		return fmt.Errorf("record cycle completion: %w", err)
	}
	return nil
}

// LastCycleCompleted returns when the last full poll cycle finished. The zero
// time means no cycle has completed yet.
func (s *Store) LastCycleCompleted(ctx context.Context) (time.Time, error) {
	v, err := s.rdb.HGet(ctx, metaKey, "last_cycle_completed").Result()
	if err == redis.Nil {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("read last cycle: %w", err)
	}
	ts := atoi64(v)
	if ts == 0 {
		return time.Time{}, nil
	}
	return time.Unix(ts, 0), nil
}

// Ping reports whether Redis is reachable. /health depends on this: with no
// state backend the monitor cannot know anything, and must not claim health.
func (s *Store) Ping(ctx context.Context) error {
	return s.rdb.Ping(ctx).Err()
}

// UnixTime serialises as RFC3339 for humans while staying a unix second count
// internally, which is what the Lua scripts compare against.
type UnixTime int64

// MarshalJSON renders the timestamp as an RFC3339 string.
func (t UnixTime) MarshalJSON() ([]byte, error) {
	if t == 0 {
		return []byte("null"), nil
	}
	return []byte(strconv.Quote(time.Unix(int64(t), 0).UTC().Format(time.RFC3339))), nil
}

// UnmarshalJSON accepts what MarshalJSON produces, plus a bare unix second
// count. Without this the type marshals but cannot round-trip, which trips up
// anything that reads /status back — tests included.
func (t *UnixTime) UnmarshalJSON(b []byte) error {
	s := string(b)
	if s == "null" || s == `""` {
		*t = 0
		return nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		*t = UnixTime(n)
		return nil
	}
	unquoted, err := strconv.Unquote(s)
	if err != nil {
		return fmt.Errorf("parse timestamp %s: %w", s, err)
	}
	parsed, err := time.Parse(time.RFC3339, unquoted)
	if err != nil {
		return fmt.Errorf("parse timestamp %q: %w", unquoted, err)
	}
	*t = UnixTime(parsed.Unix())
	return nil
}

// Time converts back to a time.Time.
func (t UnixTime) Time() time.Time { return time.Unix(int64(t), 0) }

func atoi64(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}

func asInt64(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case string:
		return atoi64(t)
	case []byte:
		return atoi64(string(t))
	default:
		return 0
	}
}
