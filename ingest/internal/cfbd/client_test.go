package cfbd

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestRawGetSendsAuthAndQuery(t *testing.T) {
	var gotAuth, gotAccept, gotPath, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotAccept = r.Header.Get("Accept")
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"message":"slow down"}`))
	}))
	defer srv.Close()

	c := NewClientWithHTTPClient("test-key", srv.URL, srv.Client())
	resp, err := c.RawGet(context.Background(), "/plays", url.Values{"year": {"2024"}, "week": {"3"}})
	if err != nil {
		t.Fatalf("RawGet: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429 returned as is", resp.StatusCode)
	}
	if string(body) != `{"message":"slow down"}` {
		t.Errorf("body = %q", body)
	}
	if gotAuth != "Bearer test-key" || gotAccept != "application/json" {
		t.Errorf("headers: Authorization=%q Accept=%q", gotAuth, gotAccept)
	}
	if gotPath != "/plays" || gotQuery != "week=3&year=2024" {
		t.Errorf("request = %s?%s", gotPath, gotQuery)
	}
}

func TestNewClientWithHTTPClientDefaults(t *testing.T) {
	c := NewClientWithHTTPClient("", "", nil)
	if c.baseURL != defaultBaseURL || c.httpClient == nil {
		t.Fatalf("defaults not applied: %+v", c)
	}
}

func TestSeasonForDate(t *testing.T) {
	cases := map[string]int{
		"2026-01-19T23:00:00Z": 2025, // playoff final
		"2026-02-28T12:00:00Z": 2025,
		"2026-03-01T00:00:00Z": 2026,
		"2026-08-29T16:00:00Z": 2026,
		"2026-12-31T23:59:59Z": 2026,
	}
	for in, want := range cases {
		ts, _ := time.Parse(time.RFC3339, in)
		if got := SeasonForDate(ts); got != want {
			t.Errorf("SeasonForDate(%s) = %d, want %d", in, got, want)
		}
	}
}

func TestParseAPITime(t *testing.T) {
	for _, in := range []string{"2024-11-16T19:00:00.000Z", "2024-11-16T19:00:00Z"} {
		got, err := ParseAPITime(in)
		if err != nil || !got.Equal(time.Date(2024, 11, 16, 19, 0, 0, 0, time.UTC)) {
			t.Errorf("ParseAPITime(%q) = %v, %v", in, got, err)
		}
	}
}
