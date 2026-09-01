// Package probe performs a single HTTP health probe and decides whether the
// response counts as healthy.
package probe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Ginny-Binny/pager/internal/config"
)

// UserAgent identifies this monitor to the endpoints it polls.
const UserAgent = "psyduck-monitor/1.0"

// maxBody caps how much of a response we read. A status endpoint that returns
// megabytes is already broken; reading it all would just tie up the probe.
const maxBody = 64 << 10

// Result is the outcome of one probe.
type Result struct {
	OK         bool
	StatusCode int
	LatencyMS  int64
	// Reason is a human-readable failure cause, empty when OK.
	Reason string
}

// Prober runs probes over a shared connection pool.
type Prober struct {
	follow   *http.Client
	noFollow *http.Client
}

// New returns a Prober with sane pooling. Per-probe deadlines come from the
// request context, not from http.Client.Timeout, so each check can differ.
func New() *Prober {
	tr := &http.Transport{
		MaxIdleConns:        32,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     90 * time.Second,
	}
	return &Prober{
		follow: &http.Client{Transport: tr},
		noFollow: &http.Client{
			Transport: tr,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// Probe issues the request and evaluates every configured assertion.
//
// Failure reasons are reported in a fixed precedence — transport, status,
// latency, body substring, dot path — so the page always names the first thing
// that went wrong rather than an incidental later mismatch.
func (p *Prober) Probe(ctx context.Context, ck config.Check) Result {
	ctx, cancel := context.WithTimeout(ctx, ck.Timeout())
	defer cancel()

	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ck.URL, nil)
	if err != nil {
		return Result{Reason: fmt.Sprintf("bad request: %v", err)}
	}
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("Accept", "*/*")

	client := p.noFollow
	if ck.FollowRedirects {
		client = p.follow
	}

	resp, err := client.Do(req)
	if err != nil {
		latency := time.Since(start).Milliseconds()
		// A deadline hit is the common case and deserves its own wording;
		// anything else (DNS, refused, TLS) carries the transport error.
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return Result{LatencyMS: latency, Reason: fmt.Sprintf("timeout after %s", ck.Timeout())}
		}
		return Result{LatencyMS: latency, Reason: fmt.Sprintf("request failed: %v", unwrapURLErr(err))}
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	latency := time.Since(start).Milliseconds()

	res := Result{StatusCode: resp.StatusCode, LatencyMS: latency}

	if want := ck.WantStatus(); resp.StatusCode != want {
		res.Reason = fmt.Sprintf("expected HTTP %d, got %d", want, resp.StatusCode)
		return res
	}
	if readErr != nil {
		if errors.Is(readErr, context.DeadlineExceeded) {
			res.Reason = fmt.Sprintf("timeout after %s reading body", ck.Timeout())
		} else {
			res.Reason = fmt.Sprintf("reading body failed: %v", readErr)
		}
		return res
	}
	if ck.MaxLatencyMS > 0 && latency > int64(ck.MaxLatencyMS) {
		res.Reason = fmt.Sprintf("latency %dms exceeded max_latency_ms %d", latency, ck.MaxLatencyMS)
		return res
	}
	if ck.BodyContains != "" && !strings.Contains(string(body), ck.BodyContains) {
		res.Reason = fmt.Sprintf("body does not contain %q", ck.BodyContains)
		return res
	}
	if ck.JSONPath != "" {
		var doc any
		if err := json.Unmarshal(body, &doc); err != nil {
			res.Reason = fmt.Sprintf("response is not valid JSON: %v", err)
			return res
		}
		got, ok := LookupPath(doc, ck.JSONPath)
		if !ok {
			res.Reason = fmt.Sprintf("json path %q not found in response", ck.JSONPath)
			return res
		}
		if s := stringify(got); s != ck.ExpectedValue {
			res.Reason = fmt.Sprintf("json path %q is %q, expected %q", ck.JSONPath, s, ck.ExpectedValue)
			return res
		}
	}

	res.OK = true
	return res
}

// LookupPath resolves a dot path against decoded JSON. It supports object keys
// and numeric slice indices ("items.0.name"). This is deliberately not full
// JSONPath — no filters, wildcards or recursive descent — which keeps the
// dependency list at go-redis plus a YAML parser.
func LookupPath(doc any, path string) (any, bool) {
	cur := doc
	for _, seg := range strings.Split(path, ".") {
		if seg == "" {
			return nil, false
		}
		switch node := cur.(type) {
		case map[string]any:
			v, ok := node[seg]
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(node) {
				return nil, false
			}
			cur = node[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

// stringify renders a decoded JSON value for comparison against the configured
// expected_value, which YAML always hands us as a string.
func stringify(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case float64:
		// -1 precision avoids turning 200 into "200.000000".
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprintf("%v", t)
		}
		return string(b)
	}
}

// unwrapURLErr strips the *url.Error wrapper so the reason line reads as the
// underlying cause rather than repeating the method and URL already logged.
func unwrapURLErr(err error) error {
	var ue interface{ Unwrap() error }
	if errors.As(err, &ue) {
		if inner := ue.Unwrap(); inner != nil {
			return inner
		}
	}
	return err
}
