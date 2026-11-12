package poller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/cfbd"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/cfbdcall"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/clock"
)

// gameDay replays testdata/gameday: the 2024 schedule and scoreboard snapshots of
// Saturday 2024-11-16 in the CFBD v2 format. The scoreboard answers with the latest
// snapshot taken at or before the virtual time of the request.
type gameDay struct {
	t        *testing.T
	clock    *clock.Fake
	schedule []byte
	frames   []frame

	mu       sync.Mutex
	requests map[string][]time.Time
}

type frame struct {
	From     time.Time       `json:"from"`
	Response json.RawMessage `json:"response"`
}

func loadGameDay(t *testing.T, fc *clock.Fake) *gameDay {
	t.Helper()
	g := &gameDay{t: t, clock: fc, requests: map[string][]time.Time{}}
	var err error
	if g.schedule, err = os.ReadFile(filepath.Join("testdata", "gameday", "schedule_2024.json")); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join("testdata", "gameday", "scoreboard_frames.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &g.frames); err != nil {
		t.Fatal(err)
	}
	return g
}

func (g *gameDay) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	now := g.clock.Now()
	key := r.URL.Path + "?" + r.URL.Query().Encode()
	g.mu.Lock()
	g.requests[r.URL.Path] = append(g.requests[r.URL.Path], now)
	g.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer test-key" {
		g.t.Errorf("request without the API key header: %s", key)
	}

	switch key {
	case "/games?classification=fbs&seasonType=both&year=2024":
		_, _ = w.Write(g.schedule)
	case "/scoreboard?classification=fbs":
		body := json.RawMessage(`[]`)
		for _, f := range g.frames {
			if !f.From.After(now) {
				body = f.Response
			}
		}
		_, _ = w.Write(body)
	default:
		g.t.Errorf("unexpected CFBD request %s", key)
		w.WriteHeader(http.StatusNotFound)
	}
}

func (g *gameDay) times(path string) []time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]time.Time(nil), g.requests[path]...)
}

// recorder is a fake Kafka producer. fail decides, from the virtual time, whether
// the broker is down.
type recorder struct {
	clock    *clock.Fake
	fail     func(now time.Time) bool
	messages []Message
	attempts int
}

func (r *recorder) Publish(_ context.Context, msgs []Message) error {
	r.attempts++
	if r.fail != nil && r.fail(r.clock.Now()) {
		return errors.New("kafka: broker not available")
	}
	r.messages = append(r.messages, msgs...)
	return nil
}

type expectedMessage struct {
	Key        string  `json:"key"`
	FetchedAt  string  `json:"fetched_at"`
	Status     string  `json:"status"`
	Period     *int    `json:"period"`
	Clock      *string `json:"clock"`
	HomePoints *int    `json:"home_points"`
	AwayPoints *int    `json:"away_points"`
}

func at(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func between(from, to string) func(time.Time) bool {
	return func(now time.Time) bool { return !now.Before(at(from)) && now.Before(at(to)) }
}

func TestGameDayReplayPublishesExactlyTheExpectedMessages(t *testing.T) {
	cases := map[string]func(time.Time) bool{
		"kafka up": nil,
		// The 19:04 kickoff message waits in the queue and goes out at 19:06 unchanged.
		"kafka down mid-game": between("2024-11-16T19:04:00Z", "2024-11-16T19:06:00Z"),
		// The last final score cannot be published as the window closes; the idle loop
		// retries every minute without polling the API.
		"kafka down at window close": between("2024-11-16T23:40:00Z", "2024-11-16T23:50:00Z"),
	}
	for name, fail := range cases {
		t.Run(name, func(t *testing.T) { replay(t, fail) })
	}
}

func replay(t *testing.T, fail func(time.Time) bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fc := clock.NewFake(at("2024-11-16T12:00:00Z"))
	fc.StopAt, fc.Stop = at("2024-11-17T06:00:00Z"), cancel // SIGTERM the next morning

	day := loadGameDay(t, fc)
	srv := httptest.NewServer(day)
	defer srv.Close()

	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	caller := cfbdcall.New(cfbd.NewClientWithBaseURL("test-key", srv.URL), cfbdcall.Options{
		Logger: discard, Now: fc.Now, Sleep: fc.Sleep,
	})
	pub := &recorder{clock: fc, fail: fail}
	p := New(caller, pub, fc, discard, Config{})
	p.CallStats = caller.Stats

	if err := p.Run(ctx); err != nil {
		t.Fatalf("Run returned %v, want nil on shutdown", err)
	}

	// Exactly the expected messages, in order.
	var want []expectedMessage
	b, _ := os.ReadFile(filepath.Join("testdata", "gameday", "expected_messages.json"))
	if err := json.Unmarshal(b, &want); err != nil {
		t.Fatal(err)
	}
	if len(pub.messages) != len(want) {
		t.Fatalf("published %d messages, want %d", len(pub.messages), len(want))
	}
	for i, m := range pub.messages {
		got := decodeMessage(t, m)
		if !reflect.DeepEqual(got, want[i]) {
			t.Errorf("message %d:\n got  %+v\n want %+v", i, got, want[i])
		}
	}
	checkPayloadsAreRawGames(t, day, pub.messages)

	// API calls: the schedule at noon and after midnight, and one scoreboard poll a
	// minute from 18:45 (15 minutes before the first kickoff) to 23:40 (last final).
	polls := day.times("/scoreboard")
	if len(polls) != 296 {
		t.Errorf("scoreboard polls = %d, want 296", len(polls))
	}
	if len(polls) > 0 && (!polls[0].Equal(at("2024-11-16T18:45:00Z")) || !polls[len(polls)-1].Equal(at("2024-11-16T23:40:00Z"))) {
		t.Errorf("polled from %s to %s, want 18:45 to 23:40", polls[0], polls[len(polls)-1])
	}
	for i := 1; i < len(polls); i++ {
		if polls[i].Sub(polls[i-1]) != time.Minute {
			t.Errorf("poll %d came %s after the previous one, want 1m", i, polls[i].Sub(polls[i-1]))
			break
		}
	}
	scheds := day.times("/games")
	if len(scheds) != 2 || !scheds[0].Equal(at("2024-11-16T12:00:00Z")) || !scheds[1].Equal(at("2024-11-17T00:00:00Z")) {
		t.Errorf("schedule reads at %v, want 12:00 and 00:00", scheds)
	}
	if s := caller.Stats(); s.Calls != 298 {
		t.Errorf("API calls = %d, want 298", s.Calls)
	}
	if st := p.Stats(); st.Published != len(want) || st.Windows != 1 {
		t.Errorf("poller stats = %+v", st)
	}
	if fail != nil && p.Stats().PublishErrors == 0 {
		t.Error("broker outage was not exercised")
	}
}

func decodeMessage(t *testing.T, m Message) expectedMessage {
	t.Helper()
	var v struct {
		GameID    json.Number `json:"game_id"`
		FetchedAt string      `json:"fetched_at"`
		Payload   struct {
			Status   string  `json:"status"`
			Period   *int    `json:"period"`
			Clock    *string `json:"clock"`
			HomeTeam struct {
				Points *int `json:"points"`
			} `json:"homeTeam"`
			AwayTeam struct {
				Points *int `json:"points"`
			} `json:"awayTeam"`
		} `json:"payload"`
	}
	dec := json.NewDecoder(bytes.NewReader(m.Value))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("message value is not JSON: %v", err)
	}
	var top map[string]json.RawMessage
	_ = json.Unmarshal(m.Value, &top)
	if len(top) != 3 {
		t.Errorf("message has fields %v, want game_id, fetched_at, payload", keys(top))
	}
	if string(m.Key) != v.GameID.String() {
		t.Errorf("key %q does not match game_id %s", m.Key, v.GameID)
	}
	return expectedMessage{
		Key: string(m.Key), FetchedAt: v.FetchedAt, Status: v.Payload.Status, Period: v.Payload.Period,
		Clock: v.Payload.Clock, HomePoints: v.Payload.HomeTeam.Points, AwayPoints: v.Payload.AwayTeam.Points,
	}
}

// checkPayloadsAreRawGames proves each payload is the API's game object unchanged.
func checkPayloadsAreRawGames(t *testing.T, day *gameDay, msgs []Message) {
	t.Helper()
	for i, m := range msgs {
		var v struct {
			FetchedAt string          `json:"fetched_at"`
			Payload   json.RawMessage `json:"payload"`
		}
		_ = json.Unmarshal(m.Value, &v)
		var frameGames []json.RawMessage
		for _, f := range day.frames {
			if !f.From.After(at(v.FetchedAt)) {
				_ = json.Unmarshal(f.Response, &frameGames)
			}
		}
		found := false
		for _, g := range frameGames {
			if jsonEqual(g, v.Payload) {
				found = true
			}
		}
		if !found {
			t.Errorf("message %d payload is not a raw game object of the %s scoreboard", i, v.FetchedAt)
		}
	}
}

func TestCanonicalHashIgnoresKeyOrderAndWhitespace(t *testing.T) {
	a := json.RawMessage(`{"id":1,"status":"in_progress","homeTeam":{"points":7,"name":"Texas A&M"}}`)
	b := json.RawMessage("{\n  \"homeTeam\": {\"name\": \"Texas A&M\", \"points\": 7},\n  \"status\": \"in_progress\", \"id\": 1\n}")
	c := json.RawMessage(`{"id":1,"status":"in_progress","homeTeam":{"points":10,"name":"Texas A&M"}}`)
	ha, pa, _ := canonical(a)
	hb, _, _ := canonical(b)
	hc, _, _ := canonical(c)
	if ha != hb {
		t.Error("key order or whitespace changed the hash")
	}
	if ha == hc {
		t.Error("a score change did not change the hash")
	}
	msg, _ := encode(liveMessage{GameID: 1, FetchedAt: "2024-11-16T19:00:00Z", Payload: pa})
	if want := `{"game_id":1,"fetched_at":"2024-11-16T19:00:00Z","payload":{"id":1,"status":"in_progress","homeTeam":{"points":7,"name":"Texas A&M"}}}`; string(msg) != want {
		t.Errorf("message = %s\nwant      %s", msg, want)
	}
}

func TestShutdownWhileIdleIsImmediate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fc := clock.NewFake(at("2024-11-16T12:00:00Z"))
	day := loadGameDay(t, fc)
	srv := httptest.NewServer(day)
	defer srv.Close()
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	caller := cfbdcall.New(cfbd.NewClientWithBaseURL("test-key", srv.URL), cfbdcall.Options{Logger: discard, Now: fc.Now, Sleep: fc.Sleep})

	// SIGTERM arrives during the first idle sleep.
	fc.StopAt, fc.Stop = at("2024-11-16T12:00:01Z"), cancel
	if err := New(caller, &recorder{clock: fc}, fc, discard, Config{}).Run(ctx); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if n := len(day.times("/scoreboard")); n != 0 {
		t.Errorf("scoreboard polled %d times outside any window", n)
	}
}

func jsonEqual(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

func keys(m map[string]json.RawMessage) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestScheduleFailuresBackOff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fc := clock.NewFake(at("2024-11-16T12:00:00Z"))
	fc.StopAt, fc.Stop = at("2024-11-16T16:00:00Z"), cancel

	var mu sync.Mutex
	var calls []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls = append(calls, fc.Now())
		mu.Unlock()
		if r.URL.Path != "/games" {
			t.Errorf("unexpected request %s", r.URL)
		}
		w.WriteHeader(http.StatusUnauthorized) // bad key: not retried by the caller
	}))
	defer srv.Close()

	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	caller := cfbdcall.New(cfbd.NewClientWithBaseURL("test-key", srv.URL), cfbdcall.Options{Logger: discard, Now: fc.Now, Sleep: fc.Sleep})
	if err := New(caller, &recorder{clock: fc}, fc, discard, Config{}).Run(ctx); err != nil {
		t.Fatal(err)
	}
	want := []string{"12:00", "12:05", "12:15", "12:35", "13:15", "14:15", "15:15"}
	if len(calls) != len(want) {
		t.Fatalf("schedule attempts at %v, want %v", calls, want)
	}
	for i, c := range calls {
		if got := c.Format("15:04"); got != want[i] {
			t.Errorf("attempt %d at %s, want %s", i, got, want[i])
		}
	}
}
