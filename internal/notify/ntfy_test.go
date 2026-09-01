package notify

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type captured struct {
	path   string
	method string
	header http.Header
	body   string
}

func serve(t *testing.T, status int) (*captured, string) {
	t.Helper()
	got := &captured{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.path, got.method, got.header, got.body = r.URL.Path, r.Method, r.Header.Clone(), string(b)
		if status != http.StatusOK {
			http.Error(w, "nope", status)
			return
		}
		w.Write([]byte(`{"id":"x"}`))
	}))
	t.Cleanup(srv.Close)
	return got, srv.URL
}

func TestPublishPage(t *testing.T) {
	got, url := serve(t, http.StatusOK)
	c := New(url+"/", "my-alerts", "tk_secret")

	err := c.Publish(context.Background(), Message{
		Title:       "web is DOWN",
		Body:        "Reason: expected HTTP 200, got 500\nLatency: 91ms",
		Priority:    PriorityPage,
		Tags:        []string{"rotating_light"},
		ActionURL:   "https://pager.example.com/ack/web",
		ActionToken: "abc123",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if got.method != http.MethodPost {
		t.Fatalf("method = %s, want POST", got.method)
	}
	if got.path != "/my-alerts" {
		t.Fatalf("path = %q, want /my-alerts (trailing slash on base URL not trimmed)", got.path)
	}
	if h := got.header.Get("Title"); h != "web is DOWN" {
		t.Fatalf("Title = %q", h)
	}
	if h := got.header.Get("Priority"); h != "5" {
		t.Fatalf("Priority = %q, want 5 — anything lower will not sound an alarm", h)
	}
	if h := got.header.Get("Tags"); h != "rotating_light" {
		t.Fatalf("Tags = %q", h)
	}
	if h := got.header.Get("Authorization"); h != "Bearer tk_secret" {
		t.Fatalf("Authorization = %q", h)
	}
	if !strings.Contains(got.body, "Latency: 91ms") {
		t.Fatalf("body = %q, want the multi-line detail preserved", got.body)
	}

	// The action must be a POST carrying the token in a header: a GET link is
	// prefetchable, and a prefetch would silently ack a live incident.
	action := got.header.Get("Actions")
	for _, want := range []string{"http", "Ack", "https://pager.example.com/ack/web",
		"method=POST", "headers.X-Ack-Token=abc123"} {
		if !strings.Contains(action, want) {
			t.Fatalf("Actions = %q, want it to contain %q", action, want)
		}
	}
}

func TestPublishWithoutTokenOrAction(t *testing.T) {
	got, url := serve(t, http.StatusOK)
	c := New(url, "topic", "")

	err := c.Publish(context.Background(), Message{
		Title:    "web recovered",
		Body:     "Back up after 4m30s",
		Priority: PriorityRecovery,
		Tags:     []string{"white_check_mark"},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if h := got.header.Get("Authorization"); h != "" {
		t.Fatalf("Authorization = %q, want none on an unauthenticated server", h)
	}
	if h := got.header.Get("Actions"); h != "" {
		t.Fatalf("Actions = %q, want none when no ack action is set", h)
	}
	if h := got.header.Get("Priority"); h != "2" {
		t.Fatalf("Priority = %q, want 2 for a recovery", h)
	}
}

// An ack action without its token would render a button that always 403s.
func TestActionOmittedWhenTokenMissing(t *testing.T) {
	got, url := serve(t, http.StatusOK)
	c := New(url, "topic", "")
	err := c.Publish(context.Background(), Message{
		Title:     "web is DOWN",
		ActionURL: "https://pager.example.com/ack/web",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if h := got.header.Get("Actions"); h != "" {
		t.Fatalf("Actions = %q, want none when the token is empty", h)
	}
}

func TestPublishReportsServerError(t *testing.T) {
	_, url := serve(t, http.StatusForbidden)
	c := New(url, "topic", "wrong-token")

	err := c.Publish(context.Background(), Message{Title: "x", Priority: PriorityPage})
	if err == nil {
		t.Fatal("Publish succeeded against a 403")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Fatalf("error = %v, want it to surface the status code", err)
	}
}

func TestPublishReportsTransportError(t *testing.T) {
	// A server that is closed before use gives an immediate connection refusal
	// rather than making the test wait out the client timeout.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	c := New(url, "topic", "")
	if err := c.Publish(context.Background(), Message{Title: "x"}); err == nil {
		t.Fatal("Publish succeeded against an unreachable server")
	}
}

// Failure reasons are built from upstream error strings, which can contain
// newlines. An unsanitised header would corrupt the request.
func TestHeaderSafeStripsControlCharacters(t *testing.T) {
	got, url := serve(t, http.StatusOK)
	c := New(url, "topic", "")

	err := c.Publish(context.Background(), Message{
		Title:    "web is DOWN\r\nX-Injected: yes",
		Priority: PriorityPage,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got.header.Get("X-Injected") != "" {
		t.Fatal("a newline in the title injected an extra header")
	}
	if strings.ContainsAny(got.header.Get("Title"), "\r\n") {
		t.Fatalf("Title = %q, want newlines stripped", got.header.Get("Title"))
	}
}
