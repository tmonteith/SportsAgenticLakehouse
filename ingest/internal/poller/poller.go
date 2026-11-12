// Package poller publishes live college football game state to Kafka on game days.
//
// It reads the season schedule (/games) to find kickoff windows and sleeps outside
// them, so no API calls are made when no game can be live. Inside a window it polls
// the live scoreboard (/scoreboard) every Interval and publishes a game to
// cfb.live_games only when the hash of its raw state changed.
package poller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"time"

	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/cfbd"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/cfbdcall"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/clock"
)

// Topic is the shared-contract topic the poller produces to.
const Topic = "cfb.live_games"

// API is the counted, rate-limited CFBD caller (*cfbdcall.Caller).
type API interface {
	Get(ctx context.Context, endpoint string, query url.Values) (*cfbdcall.Response, error)
}

// Message is one Kafka record: key game_id (decimal), value the JSON live message.
type Message struct {
	Key   []byte
	Value []byte
}

// Publisher writes messages to the topic, all or nothing from the poller's view.
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

// liveMessage is the cfb.live_games payload (shared contract).
type liveMessage struct {
	GameID    int             `json:"game_id"`
	FetchedAt string          `json:"fetched_at"`
	Payload   json.RawMessage `json:"payload"`
}

// Config tunes the poller. Zero values take the defaults below.
type Config struct {
	Interval        time.Duration // scoreboard poll interval while games are live (60s)
	Lead            time.Duration // start polling this long before a kickoff (15m)
	ScheduleRefresh time.Duration // re-read the schedule this often (12h)
	MaxGameDuration time.Duration // a game reported in progress counts as live at most this long after kickoff (8h)
	ScheduledGrace  time.Duration // a game still "scheduled" after kickoff keeps the window open this long (3h)
	RetryDelay      time.Duration // first wait after a failed schedule read, doubled up to 1h (5m)
	Classification  string        // division filter for schedule and scoreboard ("fbs")
}

// Defaults fills unset fields.
func (c Config) Defaults() Config {
	if c.Interval == 0 {
		c.Interval = 60 * time.Second
	}
	if c.Lead == 0 {
		c.Lead = 15 * time.Minute
	}
	if c.ScheduleRefresh == 0 {
		c.ScheduleRefresh = 12 * time.Hour
	}
	if c.MaxGameDuration == 0 {
		c.MaxGameDuration = 8 * time.Hour
	}
	if c.ScheduledGrace == 0 {
		c.ScheduledGrace = 3 * time.Hour
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = 5 * time.Minute
	}
	if c.Classification == "" {
		c.Classification = "fbs"
	}
	return c
}

// Stats counts what the poller did since it started.
type Stats struct {
	ScheduleFetches int `json:"schedule_fetches"`
	Polls           int `json:"polls"`
	Published       int `json:"published"`
	PublishErrors   int `json:"publish_errors"`
	Windows         int `json:"windows"`
}

type game struct {
	kickoff   time.Time
	tbd       bool
	completed bool
	status    string // last scoreboard status
	lastHash  string // hash of the last published state
	lastSeen  time.Time
}

type pending struct {
	gameID int
	hash   string
	msg    Message
}

// Poller is not safe for concurrent use; Run drives it from one goroutine.
type Poller struct {
	api   API
	pub   Publisher
	clock clock.Clock
	log   *slog.Logger
	cfg   Config
	// CallStats, when set, is logged with window and shutdown summaries.
	CallStats func() cfbdcall.Stats

	games            map[int]*game
	scheduleAt       time.Time // last successful schedule read
	nextSchedule     time.Time // next schedule read (zero: now)
	scheduleFailures int
	pending          []pending
	inWindow         bool
	windowPolls      int
	windowPubs       int
	stats            Stats
}

// New returns a Poller.
func New(api API, pub Publisher, clk clock.Clock, log *slog.Logger, cfg Config) *Poller {
	if clk == nil {
		clk = clock.Real{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Poller{api: api, pub: pub, clock: clk, log: log, cfg: cfg.Defaults(), games: map[int]*game{}}
}

// Stats returns the counters.
func (p *Poller) Stats() Stats { return p.stats }

// Run polls until ctx is cancelled (SIGTERM), then tries once more, briefly, to
// publish anything still pending and returns nil.
func (p *Poller) Run(ctx context.Context) error {
	defer p.shutdown()
	for {
		if ctx.Err() != nil {
			return nil
		}
		if len(p.pending) > 0 {
			p.flush(ctx)
		}

		now := p.clock.Now()
		if !now.Before(p.nextSchedule) {
			if err := p.refreshSchedule(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				// Back off 5m, 10m, 20m, 40m, then hourly, so a bad key or an API outage
				// does not turn into a call every few minutes.
				p.scheduleFailures++
				delay := min(p.cfg.RetryDelay<<min(p.scheduleFailures-1, 4), time.Hour)
				p.nextSchedule = now.Add(delay)
				p.log.Error("schedule_failed", "error", err.Error(), "retry_in", delay.String())
				if p.scheduleAt.IsZero() {
					// No schedule to work from yet.
					if p.clock.Sleep(ctx, delay) != nil {
						return nil
					}
					continue
				}
				// Otherwise keep polling from the last schedule.
			} else {
				p.scheduleFailures = 0
				p.nextSchedule = p.clock.Now().Add(p.cfg.ScheduleRefresh)
			}
			now = p.clock.Now()
		}

		if p.active(now) {
			p.openWindow(now)
			p.poll(ctx)
			if p.clock.Sleep(ctx, p.cfg.Interval) != nil {
				return nil
			}
			continue
		}
		p.closeWindow(now)

		wake := p.nextSchedule
		next, ok := p.nextWindow(now)
		if ok && next.Before(wake) {
			wake = next
		}
		if len(p.pending) > 0 && wake.Sub(now) > p.cfg.Interval {
			wake = now.Add(p.cfg.Interval) // retry the publish soon, without polling
		}
		p.log.Info("idle", "until", wake.UTC().Format(time.RFC3339), "next_window", formatTime(next, ok))
		if p.clock.Sleep(ctx, wake.Sub(now)) != nil {
			return nil
		}
	}
}

// refreshSchedule reads every game of the season (both season types) in one call.
func (p *Poller) refreshSchedule(ctx context.Context) error {
	now := p.clock.Now()
	season := cfbd.SeasonForDate(now)
	resp, err := p.api.Get(ctx, "/games", url.Values{
		"year":           {strconv.Itoa(season)},
		"seasonType":     {cfbd.SeasonTypeBoth},
		"classification": {p.cfg.Classification},
	})
	if err != nil {
		return err
	}
	var sched []cfbd.ScheduledGame
	if err := json.Unmarshal(resp.Body, &sched); err != nil {
		return fmt.Errorf("decoding schedule: %w", err)
	}
	p.stats.ScheduleFetches++

	for _, s := range sched {
		if s.ID == 0 {
			continue
		}
		g := p.game(s.ID)
		if t, err := cfbd.ParseAPITime(s.StartDate); err == nil {
			g.kickoff = t
		}
		g.tbd = s.StartTimeTBD
		g.completed = g.completed || s.Completed // never reopen a finished game
	}
	// Forget long-finished games so the map stays the size of one season.
	for id, g := range p.games {
		if g.kickoff.Before(now.Add(-7*24*time.Hour)) && now.Sub(g.lastSeen) > 48*time.Hour {
			delete(p.games, id)
		}
	}
	p.scheduleAt = now
	p.log.Info("schedule_loaded", "season", season, "games", len(sched))
	return nil
}

func (p *Poller) game(id int) *game {
	g, ok := p.games[id]
	if !ok {
		g = &game{}
		p.games[id] = g
	}
	return g
}

// live reports whether game g may be live at now.
func (p *Poller) live(g *game, now time.Time) bool {
	if g.completed || g.kickoff.IsZero() {
		return false
	}
	if g.status == cfbd.GameStatusInProgress {
		return now.Before(g.kickoff.Add(p.cfg.MaxGameDuration))
	}
	if g.tbd {
		return false // a placeholder kickoff time never opens a window
	}
	return !now.Before(g.kickoff.Add(-p.cfg.Lead)) && now.Before(g.kickoff.Add(p.cfg.ScheduledGrace))
}

func (p *Poller) active(now time.Time) bool {
	for _, g := range p.games {
		if p.live(g, now) {
			return true
		}
	}
	return false
}

// nextWindow returns when the next window opens: the earliest future kickoff minus Lead.
func (p *Poller) nextWindow(now time.Time) (time.Time, bool) {
	var next time.Time
	for _, g := range p.games {
		if g.completed || g.tbd || g.kickoff.IsZero() {
			continue
		}
		start := g.kickoff.Add(-p.cfg.Lead)
		if start.After(now) && (next.IsZero() || start.Before(next)) {
			next = start
		}
	}
	return next, !next.IsZero()
}

// poll reads the scoreboard once and queues every game whose state hash changed.
func (p *Poller) poll(ctx context.Context) {
	p.stats.Polls++
	p.windowPolls++
	resp, err := p.api.Get(ctx, "/scoreboard", url.Values{"classification": {p.cfg.Classification}})
	if err != nil {
		if ctx.Err() == nil {
			p.log.Error("scoreboard_failed", "error", err.Error())
		}
		return
	}
	var raws []json.RawMessage
	if err := json.Unmarshal(resp.Body, &raws); err != nil {
		p.log.Error("scoreboard_decode_failed", "error", err.Error())
		return
	}

	now := p.clock.Now()
	fetchedAt := resp.FetchedAt.UTC().Format(time.RFC3339)
	for _, raw := range raws {
		var sg cfbd.ScoreboardGame
		if err := json.Unmarshal(raw, &sg); err != nil || sg.ID == 0 {
			p.log.Warn("scoreboard_game_skipped", "reason", "no id")
			continue
		}
		g := p.game(sg.ID)
		g.lastSeen = now
		g.status = sg.Status
		if t, err := cfbd.ParseAPITime(sg.StartDate); err == nil {
			g.kickoff = t
		} else if g.kickoff.IsZero() {
			g.kickoff = now
		}
		if sg.Status == cfbd.GameStatusCompleted {
			g.completed = true
		}

		hash, payload, err := canonical(raw)
		if err != nil {
			p.log.Warn("scoreboard_game_skipped", "game_id", sg.ID, "reason", err.Error())
			continue
		}
		if hash == p.knownHash(sg.ID) {
			continue
		}
		value, err := encode(liveMessage{GameID: sg.ID, FetchedAt: fetchedAt, Payload: payload})
		if err != nil {
			p.log.Warn("scoreboard_game_skipped", "game_id", sg.ID, "reason", err.Error())
			continue
		}
		p.queue(pending{gameID: sg.ID, hash: hash, msg: Message{Key: []byte(strconv.Itoa(sg.ID)), Value: value}})
	}
	p.flush(ctx)
}

// knownHash is the newest state already published or waiting to be.
func (p *Poller) knownHash(id int) string {
	for _, pd := range p.pending {
		if pd.gameID == id {
			return pd.hash
		}
	}
	return p.games[id].lastHash
}

// queue adds a state, replacing an unpublished older state of the same game.
func (p *Poller) queue(pd pending) {
	for i := range p.pending {
		if p.pending[i].gameID == pd.gameID {
			p.pending[i] = pd
			return
		}
	}
	p.pending = append(p.pending, pd)
}

// flush publishes everything pending. On failure it keeps the queue for the next
// tick, so a Kafka outage costs no extra API calls.
func (p *Poller) flush(ctx context.Context) {
	if len(p.pending) == 0 {
		return
	}
	msgs := make([]Message, len(p.pending))
	for i, pd := range p.pending {
		msgs[i] = pd.msg
	}
	if err := p.pub.Publish(ctx, msgs); err != nil {
		p.stats.PublishErrors++
		if ctx.Err() == nil {
			p.log.Error("publish_failed", "messages", len(msgs), "error", err.Error())
		}
		return
	}
	for _, pd := range p.pending {
		if g, ok := p.games[pd.gameID]; ok {
			g.lastHash = pd.hash
		}
	}
	p.stats.Published += len(msgs)
	p.windowPubs += len(msgs)
	p.log.Info("published", "topic", Topic, "messages", len(msgs))
	p.pending = p.pending[:0]
}

func (p *Poller) openWindow(now time.Time) {
	if p.inWindow {
		return
	}
	p.inWindow = true
	p.windowPolls, p.windowPubs = 0, 0
	p.stats.Windows++
	p.log.Info("window_opened", "at", now.UTC().Format(time.RFC3339))
}

func (p *Poller) closeWindow(now time.Time) {
	if !p.inWindow {
		return
	}
	p.inWindow = false
	args := []any{"at", now.UTC().Format(time.RFC3339), "polls", p.windowPolls, "published", p.windowPubs}
	if p.CallStats != nil {
		args = append(args, "api_calls_total", p.CallStats().Calls)
	}
	p.log.Info("window_closed", args...)
}

func (p *Poller) shutdown() {
	if len(p.pending) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		p.flush(ctx)
		cancel()
	}
	args := []any{"stats", p.stats, "unpublished", len(p.pending)}
	if p.CallStats != nil {
		args = append(args, "api", p.CallStats())
	}
	p.log.Info("poller_stopped", args...)
}

// canonical returns the SHA-256 of the game object with keys sorted and whitespace
// removed (so key order and formatting never count as a change), and the object
// compacted for the payload.
func canonical(raw json.RawMessage) (string, json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", nil, err
	}
	canon, err := json.Marshal(v) // object keys come out sorted
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(canon)
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return "", nil, err
	}
	return hex.EncodeToString(sum[:]), compact.Bytes(), nil
}

func encode(m liveMessage) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(m); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

func formatTime(t time.Time, ok bool) string {
	if !ok {
		return "none scheduled"
	}
	return t.UTC().Format(time.RFC3339)
}
