// Package extractor lands raw CFBD API responses in the landing bucket: one gzipped
// object per response, idempotent on (endpoint, params_hash), never overwriting.
package extractor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/cfbd"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/cfbdcall"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/clock"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/landing"
)

// DefaultRecentWindow is how far back --weeks=recent looks: weeks whose calendar dates
// overlap the last three days. A nightly run then covers the current week and, until
// the Thursday after it ends, the previous one (late games and stat corrections).
const DefaultRecentWindow = 72 * time.Hour

// API is the counted, rate-limited CFBD caller (*cfbdcall.Caller).
type API interface {
	Get(ctx context.Context, endpoint string, query url.Values) (*cfbdcall.Response, error)
}

// Config is one extractor run.
type Config struct {
	Seasons      []int
	Weeks        WeekSpec
	SeasonTypes  []string
	Endpoints    []Endpoint
	Force        bool          // re-land even if (endpoint, params_hash) already landed
	DryRun       bool          // list and plan only: no data calls, no writes
	RecentWindow time.Duration // for Weeks.Recent; default DefaultRecentWindow
}

// Counts per endpoint.
type Counts struct {
	Landed     int   `json:"landed"`
	Skipped    int   `json:"skipped"`
	WouldFetch int   `json:"would_fetch,omitempty"`
	Failed     int   `json:"failed"`
	Bytes      int64 `json:"bytes_landed"`
}

// Summary of a run.
type Summary struct {
	Total      Counts             `json:"total"`
	ByEndpoint map[string]*Counts `json:"by_endpoint"`
}

func (s *Summary) counts(endpoint string) *Counts {
	if s.ByEndpoint == nil {
		s.ByEndpoint = map[string]*Counts{}
	}
	c, ok := s.ByEndpoint[endpoint]
	if !ok {
		c = &Counts{}
		s.ByEndpoint[endpoint] = c
	}
	return c
}

func (s *Summary) add(endpoint string, f func(*Counts)) {
	f(s.counts(endpoint))
	f(&s.Total)
}

// Extractor runs extraction against an API and a landing Store.
type Extractor struct {
	api   API
	store landing.Store
	clock clock.Clock
	log   *slog.Logger

	landed map[string]map[string]bool // "<endpoint>/<season>" -> params_hash set
}

// New returns an Extractor.
func New(api API, store landing.Store, clk clock.Clock, log *slog.Logger) *Extractor {
	if clk == nil {
		clk = clock.Real{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Extractor{api: api, store: store, clock: clk, log: log, landed: map[string]map[string]bool{}}
}

type target struct {
	ep         Endpoint
	season     int
	week       int // 0 for season-level endpoints
	seasonType string
	query      url.Values
}

// Run lands every selected (endpoint, season, week). It keeps going after a failed
// target and returns an error naming how many failed; an auth error (401/403) or a
// cancelled context stops the run at once.
func (e *Extractor) Run(ctx context.Context, cfg Config) (Summary, error) {
	if cfg.RecentWindow == 0 {
		cfg.RecentWindow = DefaultRecentWindow
	}
	var sum Summary

	needCalendar := cfg.Weeks.Recent
	for _, ep := range cfg.Endpoints {
		if !ep.SeasonLevel {
			needCalendar = true
		}
	}

	for _, season := range cfg.Seasons {
		var weeks []cfbd.CalendarWeek
		if needCalendar {
			cal, err := e.calendar(ctx, season)
			if err != nil {
				if cfbdcall.IsAuthError(err) || ctx.Err() != nil {
					return sum, err
				}
				e.log.Error("calendar_failed", "season", season, "error", err.Error())
				sum.add("calendar", func(c *Counts) { c.Failed++ })
				continue
			}
			weeks = selectWeeks(cal, cfg, e.clock.Now())
		}
		if cfg.Weeks.Recent && len(weeks) == 0 {
			e.log.Info("no_recent_weeks", "season", season, "window_hours", cfg.RecentWindow.Hours())
			continue
		}

		for _, t := range plan(cfg, season, weeks) {
			if err := e.process(ctx, cfg, t, &sum); err != nil {
				if cfbdcall.IsAuthError(err) || ctx.Err() != nil {
					return sum, err
				}
			}
		}
	}

	if sum.Total.Failed > 0 {
		return sum, fmt.Errorf("%d request(s) failed", sum.Total.Failed)
	}
	return sum, nil
}

// calendar fetches the season's week calendar. It is a planning call and is not
// landed (it feeds no silver table), but it is counted like every other call.
func (e *Extractor) calendar(ctx context.Context, season int) ([]cfbd.CalendarWeek, error) {
	resp, err := e.api.Get(ctx, "/calendar", url.Values{"year": {strconv.Itoa(season)}})
	if err != nil {
		return nil, err
	}
	var weeks []cfbd.CalendarWeek
	if err := json.Unmarshal(resp.Body, &weeks); err != nil {
		return nil, fmt.Errorf("decoding calendar %d: %w", season, err)
	}
	return weeks, nil
}

// selectWeeks returns the calendar weeks the run covers, regular season first.
func selectWeeks(cal []cfbd.CalendarWeek, cfg Config, now time.Time) []cfbd.CalendarWeek {
	types := map[string]bool{}
	for _, st := range cfg.SeasonTypes {
		types[st] = true
	}
	windowStart := now.Add(-cfg.RecentWindow)

	seen := map[string]bool{}
	var out []cfbd.CalendarWeek
	for _, w := range cal {
		if !types[w.SeasonType] {
			continue
		}
		switch {
		case cfg.Weeks.All:
		case cfg.Weeks.Recent:
			start, end, ok := weekBounds(w)
			if !ok || start.After(now) || end.Before(windowStart) {
				continue
			}
		default:
			if !cfg.Weeks.Numbers[w.Week] {
				continue
			}
		}
		key := w.SeasonType + "/" + strconv.Itoa(w.Week)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, w)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].SeasonType != out[j].SeasonType {
			return out[i].SeasonType == cfbd.SeasonTypeRegular
		}
		return out[i].Week < out[j].Week
	})
	return out
}

// weekBounds reads a calendar week's start and end (startDate/endDate, falling back to
// the first and last kickoff).
func weekBounds(w cfbd.CalendarWeek) (time.Time, time.Time, bool) {
	pick := func(a, b string) (time.Time, bool) {
		for _, s := range []string{a, b} {
			if s == "" {
				continue
			}
			if t, err := cfbd.ParseAPITime(s); err == nil {
				return t, true
			}
		}
		return time.Time{}, false
	}
	start, ok1 := pick(w.StartDate, w.FirstGameStart)
	end, ok2 := pick(w.EndDate, w.LastGameStart)
	return start, end, ok1 && ok2
}

// plan lists the targets of one season: season-level endpoints, then each week's
// week-level endpoints in --endpoints order.
func plan(cfg Config, season int, weeks []cfbd.CalendarWeek) []target {
	year := strconv.Itoa(season)
	var out []target
	for _, ep := range cfg.Endpoints {
		if ep.SeasonLevel {
			out = append(out, target{ep: ep, season: season, query: url.Values{"year": {year}}})
		}
	}
	for _, w := range weeks {
		for _, ep := range cfg.Endpoints {
			if ep.SeasonLevel {
				continue
			}
			out = append(out, target{
				ep: ep, season: season, week: w.Week, seasonType: w.SeasonType,
				query: url.Values{"year": {year}, "week": {strconv.Itoa(w.Week)}, "seasonType": {w.SeasonType}},
			})
		}
	}
	return out
}

func (e *Extractor) process(ctx context.Context, cfg Config, t target, sum *Summary) error {
	params := landing.CanonicalParams(t.query)
	hash := landing.ParamsHash(params)
	log := e.log.With("endpoint", t.ep.Name, "params", params, "season", t.season, "week", t.week)

	known, err := e.landedHashes(ctx, t.ep.Name, t.season)
	if err != nil {
		log.Error("list_failed", "error", err.Error())
		sum.add(t.ep.Name, func(c *Counts) { c.Failed++ })
		return err
	}
	if known[hash] && !cfg.Force {
		log.Debug("skip_already_landed")
		sum.add(t.ep.Name, func(c *Counts) { c.Skipped++ })
		return nil
	}
	if cfg.DryRun {
		log.Info("would_fetch", "already_landed", known[hash])
		sum.add(t.ep.Name, func(c *Counts) { c.WouldFetch++ })
		return nil
	}

	const maxNameAttempts = 3
	for attempt := 1; attempt <= maxNameAttempts; attempt++ {
		resp, err := e.api.Get(ctx, t.ep.Path, t.query)
		if err != nil {
			log.Error("fetch_failed", "error", err.Error())
			sum.add(t.ep.Name, func(c *Counts) { c.Failed++ })
			return err
		}

		gz, err := gzipBytes(resp.Body)
		if err != nil {
			sum.add(t.ep.Name, func(c *Counts) { c.Failed++ })
			return err
		}
		name := landing.ObjectName(t.ep.Name, t.season, t.week, resp.FetchedAt)
		md := landing.Metadata(t.ep.Name, t.query, resp.FetchedAt, resp.Status)

		err = e.store.Create(ctx, name, gz, landing.ContentType, md)
		if errors.Is(err, landing.ErrExists) {
			// Another run landed a response for this endpoint, season and week in the
			// same second. Fetch again in a later second rather than overwrite.
			log.Warn("object_name_taken", "object", name, "attempt", attempt)
			if serr := e.clock.Sleep(ctx, time.Second); serr != nil {
				return serr
			}
			continue
		}
		if err != nil {
			log.Error("write_failed", "object", name, "error", err.Error())
			sum.add(t.ep.Name, func(c *Counts) { c.Failed++ })
			return err
		}

		known[hash] = true
		log.Info("landed", "object", name, "bytes", len(gz), "raw_bytes", len(resp.Body))
		sum.add(t.ep.Name, func(c *Counts) { c.Landed++; c.Bytes += int64(len(gz)) })
		return nil
	}

	err = fmt.Errorf("%s %s: object name taken %d times", t.ep.Name, params, maxNameAttempts)
	log.Error("write_failed", "error", err.Error())
	sum.add(t.ep.Name, func(c *Counts) { c.Failed++ })
	return err
}

// landedHashes lists cfb/<endpoint>/season=<YYYY>/ once per run and returns the set of
// params_hash values already landed there.
func (e *Extractor) landedHashes(ctx context.Context, endpoint string, season int) (map[string]bool, error) {
	key := endpoint + "/" + strconv.Itoa(season)
	if set, ok := e.landed[key]; ok {
		return set, nil
	}
	objs, err := e.store.List(ctx, landing.SeasonPrefix(endpoint, season))
	if err != nil {
		return nil, err
	}
	set := map[string]bool{}
	for _, o := range objs {
		if o.Metadata[landing.MetaEndpoint] == endpoint && o.Metadata[landing.MetaParamsHash] != "" {
			set[o.Metadata[landing.MetaParamsHash]] = true
		}
	}
	e.landed[key] = set
	return set, nil
}

func gzipBytes(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
