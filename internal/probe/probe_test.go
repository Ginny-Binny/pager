package probe

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Ginny-Binny/pager/internal/config"
)

func serve(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestHealthyResponse(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != UserAgent {
			t.Errorf("User-Agent = %q, want %q", got, UserAgent)
		}
		w.Write([]byte(`{"status":"success","message":"OK"}`))
	})

	res := New().Probe(context.Background(), config.Check{Name: "ok", URL: url})
	if !res.OK {
		t.Fatalf("probe failed unexpectedly: %s", res.Reason)
	}
	if res.StatusCode != 200 {
		t.Fatalf("status code = %d, want 200", res.StatusCode)
	}
}

func TestUnexpectedStatusCode(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "database connection failed", http.StatusServiceUnavailable)
	})

	res := New().Probe(context.Background(), config.Check{Name: "down", URL: url})
	if res.OK {
		t.Fatal("probe passed on a 503")
	}
	if res.StatusCode != 503 {
		t.Fatalf("status code = %d, want 503", res.StatusCode)
	}
	if !strings.Contains(res.Reason, "503") {
		t.Fatalf("reason = %q, want it to name the status code", res.Reason)
	}
}

func TestExpectedStatusOverride(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	ck := config.Check{Name: "204", URL: url, ExpectedStatus: 204}
	if res := New().Probe(context.Background(), ck); !res.OK {
		t.Fatalf("probe failed with expected_status 204: %s", res.Reason)
	}
}

func TestTimeout(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.Write([]byte("too late"))
	})

	ck := config.Check{Name: "slow", URL: url, TimeoutMS: 100}
	res := New().Probe(context.Background(), ck)
	if res.OK {
		t.Fatal("probe passed despite exceeding its timeout")
	}
	if !strings.Contains(res.Reason, "timeout") {
		t.Fatalf("reason = %q, want it to mention the timeout", res.Reason)
	}
}

func TestUnreachableHost(t *testing.T) {
	// Reserved TEST-NET-1; guaranteed not to answer.
	ck := config.Check{Name: "gone", URL: "http://192.0.2.1:9/status", TimeoutMS: 300}
	res := New().Probe(context.Background(), ck)
	if res.OK {
		t.Fatal("probe passed against an unreachable host")
	}
	if res.Reason == "" {
		t.Fatal("no failure reason recorded for an unreachable host")
	}
}

func TestMaxLatency(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.Write([]byte("slow but fine"))
	})

	ck := config.Check{Name: "laggy", URL: url, MaxLatencyMS: 50, TimeoutMS: 3000}
	res := New().Probe(context.Background(), ck)
	if res.OK {
		t.Fatal("probe passed despite exceeding max_latency_ms")
	}
	if !strings.Contains(res.Reason, "max_latency_ms") {
		t.Fatalf("reason = %q, want it to name max_latency_ms", res.Reason)
	}
	// A latency failure is still a 200; the page should say so.
	if res.StatusCode != 200 {
		t.Fatalf("status code = %d, want the real 200 to be reported", res.StatusCode)
	}
}

func TestBodyContains(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"degraded"}`))
	})
	pr := New()

	ck := config.Check{Name: "body", URL: url, BodyContains: "success"}
	res := pr.Probe(context.Background(), ck)
	if res.OK {
		t.Fatal("probe passed despite the body substring being absent")
	}
	if !strings.Contains(res.Reason, "success") {
		t.Fatalf("reason = %q, want it to name the missing substring", res.Reason)
	}

	ck.BodyContains = "degraded"
	if res := pr.Probe(context.Background(), ck); !res.OK {
		t.Fatalf("probe failed when the substring was present: %s", res.Reason)
	}
}

func TestJSONDotPath(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"Success","data":{"db":{"ok":true}},"nodes":[{"name":"a"},{"name":"b"}]}`))
	})
	pr := New()

	cases := []struct {
		name    string
		path    string
		want    string
		wantOK  bool
		inError string
	}{
		{name: "top level string", path: "status", want: "Success", wantOK: true},
		{name: "nested bool", path: "data.db.ok", want: "true", wantOK: true},
		{name: "array index", path: "nodes.1.name", want: "b", wantOK: true},
		{name: "value mismatch", path: "status", want: "Failure", inError: "expected"},
		{name: "missing path", path: "data.cache.ok", want: "true", inError: "not found"},
		{name: "index out of range", path: "nodes.9.name", want: "z", inError: "not found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ck := config.Check{Name: "json", URL: url, JSONPath: tc.path, ExpectedValue: tc.want}
			res := pr.Probe(context.Background(), ck)
			if res.OK != tc.wantOK {
				t.Fatalf("OK = %v, want %v (reason %q)", res.OK, tc.wantOK, res.Reason)
			}
			if tc.inError != "" && !strings.Contains(res.Reason, tc.inError) {
				t.Fatalf("reason = %q, want it to contain %q", res.Reason, tc.inError)
			}
		})
	}
}

func TestJSONPathOnNonJSONBody(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("plain text, not json"))
	})
	ck := config.Check{Name: "notjson", URL: url, JSONPath: "status", ExpectedValue: "ok"}
	res := New().Probe(context.Background(), ck)
	if res.OK {
		t.Fatal("probe passed on a non-JSON body with a json_path assertion")
	}
	if !strings.Contains(res.Reason, "not valid JSON") {
		t.Fatalf("reason = %q, want it to say the body is not JSON", res.Reason)
	}
}

// A status endpoint that starts redirecting is misconfigured. Following the
// redirect would report a healthy 200 from somewhere else entirely.
func TestRedirectsNotFollowedByDefault(t *testing.T) {
	target := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("healthy elsewhere"))
	})
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target, http.StatusMovedPermanently)
	})
	pr := New()

	res := pr.Probe(context.Background(), config.Check{Name: "redir", URL: url})
	if res.OK {
		t.Fatal("probe followed a redirect and reported healthy")
	}
	if res.StatusCode != 301 {
		t.Fatalf("status code = %d, want the 301 itself to be reported", res.StatusCode)
	}

	ck := config.Check{Name: "redir", URL: url, FollowRedirects: true}
	if res := pr.Probe(context.Background(), ck); !res.OK {
		t.Fatalf("probe failed with follow_redirects enabled: %s", res.Reason)
	}
}

func TestLookupPathEdgeCases(t *testing.T) {
	doc := map[string]any{"a": map[string]any{"b": []any{"x", "y"}}}
	if v, ok := LookupPath(doc, "a.b.0"); !ok || v != "x" {
		t.Fatalf("LookupPath(a.b.0) = (%v, %v), want (x, true)", v, ok)
	}
	for _, bad := range []string{"", "a.b.c", "a.b.-1", "a.b.2", "a.b.0.deeper"} {
		if _, ok := LookupPath(doc, bad); ok {
			t.Fatalf("LookupPath(%q) succeeded, want not found", bad)
		}
	}
}

func TestStringify(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{nil, "null"},
		{"ok", "ok"},
		{true, "true"},
		{float64(200), "200"},               // not "200.000000"
		{float64(1.5), "1.5"},               //
		{[]any{"a"}, `["a"]`},               //
		{map[string]any{"k": 1}, `{"k":1}`}, //
	}
	for _, tc := range cases {
		if got := stringify(tc.in); got != tc.want {
			t.Errorf("stringify(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
