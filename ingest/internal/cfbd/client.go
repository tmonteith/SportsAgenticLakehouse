package cfbd

// Package cfbd is the minimal collegefootballdata.com client the extractor and poller
// need: authenticated raw GETs (the responses are landed as-is) plus the few typed
// fields used to plan requests and polling windows.

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// Season types accepted by the API's seasonType parameter.
const (
	SeasonTypeRegular    = "regular"
	SeasonTypePostseason = "postseason"
	SeasonTypeBoth       = "both"
)

// Game status values reported by the /scoreboard endpoint.
const (
	GameStatusScheduled  = "scheduled"
	GameStatusInProgress = "in_progress"
	GameStatusCompleted  = "completed"
)

const defaultBaseURL = "https://api.collegefootballdata.com"

// Client sends authenticated requests to the CFBD API.
type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

// NewClientWithBaseURL returns a client for baseURL (the public API when empty) with a
// 15-second timeout.
func NewClientWithBaseURL(apiKey string, baseURL string) *Client {
	return NewClientWithHTTPClient(apiKey, baseURL, nil)
}

// NewClientWithHTTPClient returns a client that sends requests through hc, for callers
// that need a different timeout or transport than NewClient's 15-second default.
// An empty baseURL means the public API.
func NewClientWithHTTPClient(apiKey string, baseURL string, hc *http.Client) *Client {
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	if hc == nil {
		hc = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{apiKey: apiKey, baseURL: baseURL, httpClient: hc}
}

// RawGet sends one authenticated GET request to endpoint (an API path such as "/games")
// and returns the response whatever its status. The caller must close the body.
// The API key travels only in the Authorization header, never in the URL, so errors and
// logs that include the URL do not leak it.
func (c *Client) RawGet(ctx context.Context, endpoint string, query url.Values) (*http.Response, error) {
	reqURL := c.baseURL + endpoint
	if len(query) > 0 {
		reqURL += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	req.Header.Set("Accept", "application/json")

	return c.httpClient.Do(req)
}

// CalendarWeek is one entry of the /calendar response.
type CalendarWeek struct {
	Season         int    `json:"season"`
	Week           int    `json:"week"`
	SeasonType     string `json:"seasonType"`
	StartDate      string `json:"startDate"`
	EndDate        string `json:"endDate"`
	FirstGameStart string `json:"firstGameStart"`
	LastGameStart  string `json:"lastGameStart"`
}

// ScheduledGame holds the /games fields the poller needs to plan its polling windows.
type ScheduledGame struct {
	ID           int    `json:"id"`
	StartDate    string `json:"startDate"`
	StartTimeTBD bool   `json:"startTimeTBD"`
	Completed    bool   `json:"completed"`
}

// ScoreboardGame holds the /scoreboard fields the poller reads. The poller publishes
// the API's raw game object, not this struct.
type ScoreboardGame struct {
	ID        int    `json:"id"`
	StartDate string `json:"startDate"`
	Status    string `json:"status"`
}

// SeasonForDate returns the season a date belongs to: January and February belong to
// the previous season (bowls and the playoff final), March onward to the new one.
func SeasonForDate(t time.Time) int {
	t = t.UTC()
	if t.Month() < time.March {
		return t.Year() - 1
	}
	return t.Year()
}

// ParseAPITime parses the timestamps the API returns (RFC 3339, with or without
// fractional seconds).
func ParseAPITime(s string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, s)
}
