package cfbdcall

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/clock"
)

// scripted returns canned responses in order and records each request.
type scripted struct {
	responses []func() (*http.Response, error)
	requests  []string
}

func (s *scripted) RawGet(_ context.Context, endpoint string, q url.Values) (*http.Response, error) {
	s.requests = append(s.requests, endpoint+"?"+q.Encode())
	next := s.responses[0]
	s.responses = s.responses[1:]
	return next()
}

func reply(status int, body string, header map[string]string) func() (*http.Response, error) {
	return func() (*http.Response, error) {
		h := http.Header{}
		for k, v := range header {
			h.Set(k, v)
		}
		return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(body))}, nil
	}
}

func newTestCaller(g Getter, fc *clock.Fake) *Caller {
	return New(g, Options{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:    fc.Now,
		Sleep:  fc.Sleep,
	})
}

func TestGetRetriesRateLimitAndServerErrors(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC))
	g := &scripted{responses: []func() (*http.Response, error){
		reply(429, `{"message":"rate limited"}`, map[string]string{"Retry-After": "7"}),
		reply(502, `bad gateway`, nil),
		func() (*http.Response, error) { return nil, errors.New("connection reset") },
		reply(200, `[{"id":1}]`, map[string]string{QuotaHeader: "2981"}),
	}}
	c := newTestCaller(g, fc)

	resp, err := c.Get(context.Background(), "/games", url.Values{"year": {"2024"}, "week": {"1"}})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(resp.Body) != `[{"id":1}]` || resp.Status != 200 {
		t.Fatalf("response = %d %s", resp.Status, resp.Body)
	}
	// Retry-After 7s, then exponential 4s (2s<<1), then 8s (2s<<2).
	want := []time.Duration{7 * time.Second, 4 * time.Second, 8 * time.Second}
	if len(fc.Slept) != len(want) {
		t.Fatalf("slept %v, want %v", fc.Slept, want)
	}
	for i := range want {
		if fc.Slept[i] != want[i] {
			t.Errorf("sleep %d = %v, want %v", i, fc.Slept[i], want[i])
		}
	}
	s := c.Stats()
	if s.Calls != 4 || s.Retries != 3 || s.Failures != 0 || s.CallsByPath["/games"] != 4 || s.QuotaRemaining != "2981" {
		t.Errorf("stats = %+v", s)
	}
	if !resp.FetchedAt.Equal(fc.Now()) {
		t.Errorf("FetchedAt = %v, want time of the successful request %v", resp.FetchedAt, fc.Now())
	}
}

func TestGetDoesNotRetryClientErrors(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC))
	g := &scripted{responses: []func() (*http.Response, error){reply(401, `Unauthorized`, nil)}}
	c := newTestCaller(g, fc)

	_, err := c.Get(context.Background(), "/teams", url.Values{"year": {"2024"}})
	if !IsAuthError(err) {
		t.Fatalf("err = %v, want auth error", err)
	}
	if s := c.Stats(); s.Calls != 1 || s.Failures != 1 || s.Retries != 0 {
		t.Errorf("stats = %+v", s)
	}
}

func TestGetGivesUpAfterMaxRetries(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC))
	var rs []func() (*http.Response, error)
	for i := 0; i < 6; i++ {
		rs = append(rs, reply(503, `down`, nil))
	}
	c := newTestCaller(&scripted{responses: rs}, fc)

	_, err := c.Get(context.Background(), "/plays", nil)
	if err == nil || !strings.Contains(err.Error(), "giving up after 6 attempts") {
		t.Fatalf("err = %v", err)
	}
	// 2, 4, 8, 16, 32 seconds; no sleep after the last attempt.
	if len(fc.Slept) != 5 || fc.Slept[4] != 32*time.Second {
		t.Errorf("slept %v", fc.Slept)
	}
	if s := c.Stats(); s.Calls != 6 || s.Failures != 1 {
		t.Errorf("stats = %+v", s)
	}
}

func TestGetPacesRequests(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC))
	g := &scripted{responses: []func() (*http.Response, error){
		reply(200, `[]`, nil), reply(200, `[]`, nil),
	}}
	c := newTestCaller(g, fc)

	r1, _ := c.Get(context.Background(), "/games", nil)
	fc.Advance(300 * time.Millisecond)
	r2, _ := c.Get(context.Background(), "/games", nil)
	if gap := r2.FetchedAt.Sub(r1.FetchedAt); gap != time.Second {
		t.Errorf("gap between requests = %v, want 1s", gap)
	}
}

func TestGetRejectsOversizedBody(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC))
	g := &scripted{responses: []func() (*http.Response, error){reply(200, `0123456789`, nil)}}
	c := New(g, Options{
		MaxBodyBytes: 4, MaxRetries: -1,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: fc.Now, Sleep: fc.Sleep,
	})
	if _, err := c.Get(context.Background(), "/plays", nil); err == nil || !strings.Contains(err.Error(), "larger than 4 bytes") {
		t.Fatalf("err = %v", err)
	}
}
