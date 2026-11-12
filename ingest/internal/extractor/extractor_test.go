package extractor

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/cfbd"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/cfbdcall"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/clock"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/landing"
)

// fixtureRoutes maps each request the extractor may send (path + canonical query) to a
// recorded response in testdata/cfbd. Anything else is a test failure: no live calls.
var fixtureRoutes = map[string]string{
	"/calendar?year=2024": "calendar_2024.json",
	"/teams?year=2024":    "teams_2024.json",
}

func init() {
	for _, st := range []string{"regular", "postseason"} {
		for wk := 1; wk <= 3; wk++ {
			if st == "postseason" && wk > 1 {
				continue
			}
			q := fmt.Sprintf("seasonType=%s&week=%d&year=2024", st, wk)
			fixtureRoutes["/games?"+q] = fmt.Sprintf("games_2024_%s_%02d.json", st, wk)
			fixtureRoutes["/plays?"+q] = fmt.Sprintf("plays_2024_%s_%02d.json", st, wk)
		}
	}
}

// fakeCFBD serves the fixtures and counts requests. override, when set, answers
// instead (return 0 to fall through to the fixture).
type fakeCFBD struct {
	t        *testing.T
	mu       sync.Mutex
	requests []string
	override func(n int, key string) (status int, body string)
}

func (f *fakeCFBD) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path
	if r.URL.RawQuery != "" {
		key += "?" + r.URL.Query().Encode()
	}
	f.mu.Lock()
	f.requests = append(f.requests, key)
	n := len(f.requests)
	f.mu.Unlock()

	if r.Header.Get("Authorization") != "Bearer test-key" {
		f.t.Errorf("request %s without the API key header", key)
	}
	w.Header().Set(cfbdcall.QuotaHeader, fmt.Sprint(5000-n))
	if f.override != nil {
		if status, body := f.override(n, key); status != 0 {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, body)
			return
		}
	}
	file, ok := fixtureRoutes[key]
	if !ok {
		f.t.Errorf("unexpected CFBD request %s", key)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	b, err := os.ReadFile(filepath.Join("testdata", "cfbd", file))
	if err != nil {
		f.t.Fatal(err)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

func (f *fakeCFBD) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

type harness struct {
	api   *fakeCFBD
	store *landing.MemStore
	clock *clock.Fake
	url   string
}

func newHarness(t *testing.T, now time.Time) *harness {
	h := &harness{api: &fakeCFBD{t: t}, store: landing.NewMemStore(), clock: clock.NewFake(now)}
	srv := httptest.NewServer(h.api)
	t.Cleanup(srv.Close)
	h.url = srv.URL
	return h
}

// run executes one extractor run with a fresh caller (as a new process would).
func (h *harness) run(t *testing.T, cfg Config) (Summary, cfbdcall.Stats, error) {
	t.Helper()
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	caller := cfbdcall.New(cfbd.NewClientWithBaseURL("test-key", h.url), cfbdcall.Options{
		Logger: discard, Now: h.clock.Now, Sleep: h.clock.Sleep,
	})
	sum, err := New(caller, h.store, h.clock, discard).Run(context.Background(), cfg)
	return sum, caller.Stats(), err
}

func backfillConfig(t *testing.T) Config {
	eps, _ := ParseEndpoints(DefaultEndpoints)
	types, _ := ParseSeasonTypes(DefaultSeasonTypes)
	return Config{Seasons: []int{2024}, Weeks: WeekSpec{All: true}, SeasonTypes: types, Endpoints: eps}
}

var objectNameRE = regexp.MustCompile(`^cfb/(games|plays|teams)/season=2024/week=(\d{2})/\d{8}T\d{6}Z\.json\.gz$`)

func TestBackfillLandsSeasonAndSecondRunLandsNothing(t *testing.T) {
	h := newHarness(t, time.Date(2026, 11, 12, 10, 30, 0, 0, time.UTC))
	cfg := backfillConfig(t)

	sum, stats, err := h.run(t, cfg)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	// teams (week=00) + 3 regular weeks and 1 postseason week x (games, plays).
	if sum.Total.Landed != 9 || sum.Total.Skipped != 0 || sum.Total.Failed != 0 {
		t.Fatalf("first run summary = %+v", sum.Total)
	}
	if stats.Calls != 10 || stats.CallsByPath["/calendar"] != 1 || stats.CallsByPath["/plays"] != 4 {
		t.Errorf("first run stats = %+v", stats)
	}
	if stats.QuotaRemaining != "4990" {
		t.Errorf("quota remaining = %q, want the last header value", stats.QuotaRemaining)
	}

	names := h.store.Names()
	if len(names) != 9 {
		t.Fatalf("landed %d objects: %v", len(names), names)
	}
	weeks := map[string]int{}
	for _, name := range names {
		m := objectNameRE.FindStringSubmatch(name)
		if m == nil {
			t.Errorf("object name %q does not follow the landing layout", name)
			continue
		}
		weeks[m[1]+"/"+m[2]]++
		data, ct, md, _ := h.store.Get(name)
		if ct != landing.ContentType {
			t.Errorf("%s content type = %q", name, ct)
		}
		if md["endpoint"] != m[1] || md["http_status"] != "200" || md["params_hash"] != landing.ParamsHash(md["params"]) {
			t.Errorf("%s metadata = %v", name, md)
		}
		if _, err := time.Parse(time.RFC3339, md["fetched_at"]); err != nil {
			t.Errorf("%s fetched_at %q is not RFC 3339", name, md["fetched_at"])
		}
		// The object is the API response byte for byte, gzipped.
		raw := gunzip(t, data)
		want, _ := os.ReadFile(filepath.Join("testdata", "cfbd", fixtureRoutes["/"+md["endpoint"]+"?"+md["params"]]))
		if !bytes.Equal(raw, want) {
			t.Errorf("%s content differs from the API response", name)
		}
	}
	// Postseason week 1 shares week=01 with regular week 1; params (seasonType) differ.
	if weeks["teams/00"] != 1 || weeks["games/01"] != 2 || weeks["plays/02"] != 1 || weeks["games/03"] != 1 {
		t.Errorf("objects per endpoint/week = %v", weeks)
	}

	// Second run: everything is already landed, so nothing new lands and the only API
	// call is the calendar.
	h.clock.Advance(time.Hour)
	sum, stats, err = h.run(t, cfg)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if sum.Total.Landed != 0 || sum.Total.Skipped != 9 || len(h.store.Names()) != 9 {
		t.Errorf("second run landed something: %+v, %d objects", sum.Total, len(h.store.Names()))
	}
	if stats.Calls != 1 || stats.CallsByPath["/calendar"] != 1 {
		t.Errorf("second run stats = %+v, want only the calendar call", stats)
	}

	// --force re-lands everything as new objects; nothing is overwritten.
	h.clock.Advance(time.Hour)
	cfg.Force = true
	sum, _, err = h.run(t, cfg)
	if err != nil || sum.Total.Landed != 9 || len(h.store.Names()) != 18 {
		t.Errorf("forced run: err=%v summary=%+v objects=%d", err, sum.Total, len(h.store.Names()))
	}
}

func TestNightlyRecentWeeks(t *testing.T) {
	// Tuesday 2024-09-10 03:30 America/Denver: week 2 ended 2.5 hours ago, week 3 began.
	h := newHarness(t, time.Date(2024, 9, 10, 9, 30, 0, 0, time.UTC))
	eps, _ := ParseEndpoints(DefaultEndpoints)
	types, _ := ParseSeasonTypes(DefaultSeasonTypes)
	cfg := Config{Seasons: []int{2024}, Weeks: WeekSpec{Recent: true}, SeasonTypes: types, Endpoints: eps, Force: true}

	sum, stats, err := h.run(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Total.Landed != 5 || stats.Calls != 6 {
		t.Fatalf("nightly: summary=%+v calls=%d", sum.Total, stats.Calls)
	}
	for _, name := range h.store.Names() {
		if strings.Contains(name, "week=01") {
			t.Errorf("week 1 is outside the recent window but landed: %s", name)
		}
	}

	// The next night re-lands the open weeks (--force), as the CronJob does.
	h.clock.Advance(24 * time.Hour)
	sum, _, err = h.run(t, cfg)
	if err != nil || sum.Total.Landed != 5 {
		t.Errorf("second night: err=%v summary=%+v", err, sum.Total)
	}
}

func TestOffseasonNightlyLandsNothing(t *testing.T) {
	h := newHarness(t, time.Date(2024, 6, 15, 9, 30, 0, 0, time.UTC))
	eps, _ := ParseEndpoints(DefaultEndpoints)
	types, _ := ParseSeasonTypes(DefaultSeasonTypes)
	cfg := Config{Seasons: []int{2024}, Weeks: WeekSpec{Recent: true}, SeasonTypes: types, Endpoints: eps, Force: true}

	sum, stats, err := h.run(t, cfg)
	if err != nil || sum.Total.Landed != 0 || stats.Calls != 1 {
		t.Errorf("offseason: err=%v summary=%+v calls=%d", err, sum.Total, stats.Calls)
	}
}

func TestExplicitWeeksAndSeasonTypes(t *testing.T) {
	h := newHarness(t, time.Date(2026, 11, 12, 10, 30, 0, 0, time.UTC))
	eps, _ := ParseEndpoints("games")
	weeks, _ := ParseWeeks("2-3")
	cfg := Config{Seasons: []int{2024}, Weeks: weeks, SeasonTypes: []string{"regular"}, Endpoints: eps}

	sum, stats, err := h.run(t, cfg)
	if err != nil || sum.Total.Landed != 2 || stats.Calls != 3 {
		t.Fatalf("err=%v summary=%+v calls=%d", err, sum.Total, stats.Calls)
	}
	for _, name := range h.store.Names() {
		if !strings.HasPrefix(name, "cfb/games/season=2024/week=02/") && !strings.HasPrefix(name, "cfb/games/season=2024/week=03/") {
			t.Errorf("unexpected object %s", name)
		}
	}
}

func TestRetriesRateLimitAndCountsEveryCall(t *testing.T) {
	h := newHarness(t, time.Date(2026, 11, 12, 10, 30, 0, 0, time.UTC))
	h.api.override = func(n int, key string) (int, string) {
		if key == "/teams?year=2024" && n == 2 {
			return http.StatusTooManyRequests, `{"message":"Too many requests"}`
		}
		if key == "/games?seasonType=regular&week=1&year=2024" && n == 4 {
			return http.StatusBadGateway, `<html>bad gateway</html>`
		}
		return 0, ""
	}
	sum, stats, err := h.run(t, backfillConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Total.Landed != 9 || stats.Calls != 12 || stats.Retries != 2 || h.api.count() != 12 {
		t.Errorf("summary=%+v stats=%+v requests=%d", sum.Total, stats, h.api.count())
	}
}

func TestNameCollisionRefetchesInsteadOfOverwriting(t *testing.T) {
	start := time.Date(2026, 11, 12, 10, 30, 0, 0, time.UTC)
	h := newHarness(t, start)
	// Another run already landed a teams response in the very second this run fetches.
	taken := landing.ObjectName("teams", 2024, 0, start)
	if err := h.store.Create(context.Background(), taken, []byte("other"), landing.ContentType, map[string]string{}); err != nil {
		t.Fatal(err)
	}
	eps, _ := ParseEndpoints("teams")
	cfg := Config{Seasons: []int{2024}, Weeks: WeekSpec{All: true}, SeasonTypes: []string{"regular"}, Endpoints: eps}

	sum, stats, err := h.run(t, cfg)
	if err != nil || sum.Total.Landed != 1 || stats.CallsByPath["/teams"] != 2 {
		t.Fatalf("err=%v summary=%+v stats=%+v", err, sum.Total, stats)
	}
	if d, _, _, _ := h.store.Get(taken); string(d) != "other" {
		t.Error("existing object was overwritten")
	}
}

func TestAuthErrorStopsRun(t *testing.T) {
	h := newHarness(t, time.Date(2026, 11, 12, 10, 30, 0, 0, time.UTC))
	h.api.override = func(int, string) (int, string) { return http.StatusUnauthorized, "Unauthorized" }
	_, stats, err := h.run(t, backfillConfig(t))
	if !cfbdcall.IsAuthError(err) || stats.Calls != 1 {
		t.Errorf("err=%v calls=%d, want auth error after one call", err, stats.Calls)
	}
}

func TestFailedTargetDoesNotStopOthers(t *testing.T) {
	h := newHarness(t, time.Date(2026, 11, 12, 10, 30, 0, 0, time.UTC))
	h.api.override = func(_ int, key string) (int, string) {
		if strings.HasPrefix(key, "/plays?seasonType=regular&week=2&") {
			return http.StatusBadRequest, `{"message":"bad week"}`
		}
		return 0, ""
	}
	sum, _, err := h.run(t, backfillConfig(t))
	if err == nil || sum.Total.Failed != 1 || sum.Total.Landed != 8 || sum.ByEndpoint["plays"].Failed != 1 {
		t.Errorf("err=%v summary=%+v", err, sum.Total)
	}
}

func TestDryRunWritesNothing(t *testing.T) {
	h := newHarness(t, time.Date(2026, 11, 12, 10, 30, 0, 0, time.UTC))
	cfg := backfillConfig(t)
	cfg.DryRun = true
	sum, stats, err := h.run(t, cfg)
	if err != nil || sum.Total.WouldFetch != 9 || len(h.store.Names()) != 0 || stats.Calls != 1 {
		t.Errorf("err=%v summary=%+v objects=%d calls=%d", err, sum.Total, len(h.store.Names()), stats.Calls)
	}
}

func TestParseFlags(t *testing.T) {
	now := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	if s, _ := ParseSeasons("current", now); len(s) != 1 || s[0] != 2025 {
		t.Errorf("current season in January = %v", s)
	}
	if s, _ := ParseSeasons("2024,2022-2023", now); fmt.Sprint(s) != "[2022 2023 2024]" {
		t.Errorf("seasons = %v", s)
	}
	if _, err := ParseSeasons("20x4", now); err == nil {
		t.Error("bad season accepted")
	}
	if w, _ := ParseWeeks("1-3,7"); w.String() != "1,2,3,7" {
		t.Errorf("weeks = %v", w)
	}
	if w, _ := ParseWeeks("all"); !w.All {
		t.Error("all not parsed")
	}
	if _, err := ParseWeeks("5-2"); err == nil {
		t.Error("reversed range accepted")
	}
	if _, err := ParseEndpoints("games,drives"); err == nil {
		t.Error("unknown endpoint accepted")
	}
	if _, err := ParseSeasonTypes("both"); err == nil {
		t.Error("unknown season type accepted")
	}
}

func gunzip(t *testing.T, b []byte) []byte {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
