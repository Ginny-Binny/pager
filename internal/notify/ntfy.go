// Package notify publishes pages to a self-hosted ntfy server.
package notify

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Priority levels used by this monitor. ntfy maps 5 to its "max" priority,
// which is what the Android app needs in order to sound an alarm and bypass
// Do Not Disturb.
const (
	PriorityPage     = 5
	PriorityRecovery = 2
)

// Message is one notification.
type Message struct {
	Title    string
	Body     string
	Priority int
	Tags     []string

	// ActionURL and ActionToken, when both set, add a native Ack button that
	// fires an HTTP POST. POST matters: link-preview and prefetch bots follow
	// GET links, and a prefetched ack would silence a live incident.
	ActionURL   string
	ActionToken string
}

// Client publishes to one ntfy topic.
type Client struct {
	baseURL string
	topic   string
	token   string
	hc      *http.Client
}

// New returns a Client for the given server and topic. token may be empty on
// an unauthenticated server.
func New(baseURL, topic, token string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		topic:   topic,
		token:   token,
		hc:      &http.Client{Timeout: 10 * time.Second},
	}
}

// Publish sends the message. The body carries the detail; ntfy metadata rides
// in headers, which cannot contain newlines.
func (c *Client) Publish(ctx context.Context, m Message) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	url := c.baseURL + "/" + c.topic
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString(m.Body))
	if err != nil {
		return fmt.Errorf("build ntfy request: %w", err)
	}

	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Title", headerSafe(m.Title))
	if m.Priority > 0 {
		req.Header.Set("Priority", strconv.Itoa(m.Priority))
	}
	if len(m.Tags) > 0 {
		req.Header.Set("Tags", strings.Join(m.Tags, ","))
	}
	if m.ActionURL != "" && m.ActionToken != "" {
		// action=http, label, url, then key=value settings, comma separated.
		req.Header.Set("Actions", strings.Join([]string{
			"http",
			"Ack",
			m.ActionURL,
			"method=POST",
			"headers.X-Ack-Token=" + m.ActionToken,
			"clear=true",
		}, ", "))
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("publish to ntfy: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<10))
		return fmt.Errorf("ntfy returned %d: %s", resp.StatusCode, strings.TrimSpace(string(detail)))
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	return nil
}

// headerSafe strips characters that would corrupt an HTTP header. Check names
// are already restricted, but reasons are built from upstream error strings.
func headerSafe(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' {
			return ' '
		}
		if r < 0x20 || r > 0x7e {
			return -1
		}
		return r
	}, s)
}
