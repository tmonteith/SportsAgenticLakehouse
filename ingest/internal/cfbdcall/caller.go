// Package cfbdcall wraps the CFBD client for batch and polling workloads: it paces
// requests, retries rate-limit (429) and server (5xx) errors with backoff, and counts
// and logs every HTTP request it sends, because every request draws on the monthly
// API quota shared with the live MCP tools.
package cfbdcall

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// QuotaHeader is the response header in which the API reports the calls left this month.
const QuotaHeader = "X-CallLimit-Remaining"

// Getter sends one GET request. *cfbd.Client implements it.
type Getter interface {
	RawGet(ctx context.Context, endpoint string, query url.Values) (*http.Response, error)
}

// Response is a successful (2xx) API response with its body fully read.
type Response struct {
	Endpoint  string
	Query     url.Values
	Status    int
	Body      []byte
	FetchedAt time.Time // when the successful request was sent, UTC
}

// StatusError is a non-retryable API response (a 4xx other than 429).
type StatusError struct {
	Endpoint string
	Status   int
	Message  string // start of the response body
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("CFBD %s returned %d: %s", e.Endpoint, e.Status, e.Message)
}

// IsAuthError reports whether err is a 401 or 403 from the API (bad or missing key).
func IsAuthError(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && (se.Status == http.StatusUnauthorized || se.Status == http.StatusForbidden)
}

// Options tune a Caller. Zero values take the defaults noted on each field.
type Options struct {
	MinInterval  time.Duration // minimum time between request starts (default 1s)
	MaxRetries   int           // retries after the first attempt (default 5; negative means none)
	BaseBackoff  time.Duration // first retry delay, doubled each retry (default 2s)
	MaxBackoff   time.Duration // cap on a single delay, Retry-After included (default 60s)
	MaxBodyBytes int64         // largest body accepted (default 128 MiB)
	Logger       *slog.Logger  // default slog.Default()
	Now          func() time.Time
	Sleep        func(ctx context.Context, d time.Duration) error
}

// Stats counts what a Caller sent. Calls counts every HTTP request, retries included.
type Stats struct {
	Calls          int            `json:"api_calls"`
	CallsByPath    map[string]int `json:"api_calls_by_endpoint"`
	Retries        int            `json:"api_retries"`
	Failures       int            `json:"api_failures"`
	QuotaRemaining string         `json:"api_quota_remaining,omitempty"`
}

// Caller is safe for concurrent use, though the workloads use it from one goroutine.
type Caller struct {
	getter Getter
	opts   Options

	mu       sync.Mutex
	lastSent time.Time
	stats    Stats
}

// New returns a Caller sending requests through g.
func New(g Getter, opts Options) *Caller {
	if opts.MinInterval == 0 {
		opts.MinInterval = time.Second
	}
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 5
	} else if opts.MaxRetries < 0 {
		opts.MaxRetries = 0
	}
	if opts.BaseBackoff == 0 {
		opts.BaseBackoff = 2 * time.Second
	}
	if opts.MaxBackoff == 0 {
		opts.MaxBackoff = 60 * time.Second
	}
	if opts.MaxBodyBytes == 0 {
		opts.MaxBodyBytes = 128 << 20
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Sleep == nil {
		opts.Sleep = SleepContext
	}
	return &Caller{getter: g, opts: opts, stats: Stats{CallsByPath: map[string]int{}}}
}

// SleepContext waits for d or until ctx is done.
func SleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Stats returns a copy of the counters.
func (c *Caller) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.CallsByPath = make(map[string]int, len(c.stats.CallsByPath))
	for k, v := range c.stats.CallsByPath {
		s.CallsByPath[k] = v
	}
	return s
}

// Get calls endpoint with query, retrying 429, 5xx and transport errors with
// exponential backoff (honoring Retry-After). Other 4xx responses return *StatusError
// without retrying.
func (c *Caller) Get(ctx context.Context, endpoint string, query url.Values) (*Response, error) {
	params := query.Encode()
	var lastErr error

	for attempt := 1; attempt <= c.opts.MaxRetries+1; attempt++ {
		if attempt > 1 {
			c.bump(func(s *Stats) { s.Retries++ })
		}
		if err := c.pace(ctx); err != nil {
			return nil, err
		}

		sentAt := c.opts.Now().UTC()
		status, body, header, err := c.do(ctx, endpoint, query)
		c.bump(func(s *Stats) {
			s.Calls++
			s.CallsByPath[endpoint]++
			if q := header.Get(QuotaHeader); q != "" {
				s.QuotaRemaining = q
			}
		})
		duration := c.opts.Now().Sub(sentAt)

		log := c.opts.Logger.With(
			"endpoint", endpoint, "params", params, "attempt", attempt,
			"duration_ms", duration.Milliseconds(),
		)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			log.Warn("cfbd_call", "error", err.Error())
			lastErr = err
			if waitErr := c.backoff(ctx, attempt, ""); waitErr != nil {
				return nil, waitErr
			}
			continue
		}

		log.Info("cfbd_call", "status", status, "bytes", len(body), "quota_remaining", header.Get(QuotaHeader))

		switch {
		case status >= 200 && status < 300:
			return &Response{Endpoint: endpoint, Query: query, Status: status, Body: body, FetchedAt: sentAt}, nil
		case status == http.StatusTooManyRequests || status >= 500:
			lastErr = &StatusError{Endpoint: endpoint, Status: status, Message: snippet(body)}
			if waitErr := c.backoff(ctx, attempt, header.Get("Retry-After")); waitErr != nil {
				return nil, waitErr
			}
		default:
			c.bump(func(s *Stats) { s.Failures++ })
			return nil, &StatusError{Endpoint: endpoint, Status: status, Message: snippet(body)}
		}
	}

	c.bump(func(s *Stats) { s.Failures++ })
	return nil, fmt.Errorf("CFBD %s?%s: giving up after %d attempts: %w", endpoint, params, c.opts.MaxRetries+1, lastErr)
}

func (c *Caller) do(ctx context.Context, endpoint string, query url.Values) (int, []byte, http.Header, error) {
	resp, err := c.getter.RawGet(ctx, endpoint, query)
	if err != nil {
		return 0, nil, http.Header{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, c.opts.MaxBodyBytes+1))
	if err != nil {
		return 0, nil, resp.Header, fmt.Errorf("reading response body: %w", err)
	}
	if int64(len(body)) > c.opts.MaxBodyBytes {
		return 0, nil, resp.Header, fmt.Errorf("response body larger than %d bytes", c.opts.MaxBodyBytes)
	}
	return resp.StatusCode, body, resp.Header, nil
}

// pace waits until MinInterval has passed since the previous request started.
func (c *Caller) pace(ctx context.Context) error {
	c.mu.Lock()
	var wait time.Duration
	if !c.lastSent.IsZero() {
		wait = c.opts.MinInterval - c.opts.Now().Sub(c.lastSent)
	}
	c.mu.Unlock()
	if wait > 0 {
		if err := c.opts.Sleep(ctx, wait); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.lastSent = c.opts.Now()
	c.mu.Unlock()
	return nil
}

// backoff sleeps before retry number attempt: Retry-After seconds when the server sent
// them, else BaseBackoff * 2^(attempt-1), capped at MaxBackoff. Pacing still applies
// to the retry itself.
func (c *Caller) backoff(ctx context.Context, attempt int, retryAfter string) error {
	if attempt > c.opts.MaxRetries {
		return nil // no retry follows
	}
	d := c.opts.BaseBackoff << (attempt - 1)
	if secs, err := strconv.Atoi(retryAfter); err == nil && secs >= 0 {
		d = time.Duration(secs) * time.Second
	}
	if d > c.opts.MaxBackoff || d < 0 {
		d = c.opts.MaxBackoff
	}
	return c.opts.Sleep(ctx, d)
}

func (c *Caller) bump(f func(*Stats)) {
	c.mu.Lock()
	f(&c.stats)
	c.mu.Unlock()
}

func snippet(b []byte) string {
	const max = 200
	if len(b) > max {
		return string(b[:max]) + "..."
	}
	return string(b)
}
