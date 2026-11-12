// Command poller publishes live game state to the Kafka topic cfb.live_games while
// games are in progress, and sleeps (no API calls) between kickoff windows.
//
// Environment: CFBD_API_KEY (required), KAFKA_BOOTSTRAP (required unless
// --kafka-bootstrap is set). The poller has no GCP access.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/cfbd"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/cfbdcall"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/clock"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/poller"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	def := poller.Config{}.Defaults()
	fs := flag.NewFlagSet("poller", flag.ContinueOnError)
	interval := fs.Duration("interval", def.Interval, "scoreboard poll interval while games are in progress")
	lead := fs.Duration("lead", def.Lead, "start polling this long before a kickoff")
	refresh := fs.Duration("schedule-refresh", def.ScheduleRefresh, "how often to re-read the season schedule")
	maxGame := fs.Duration("max-game-duration", def.MaxGameDuration, "longest a game reported in progress counts as live after kickoff")
	grace := fs.Duration("scheduled-grace", def.ScheduledGrace, "how long a game still scheduled after kickoff keeps polling going")
	classification := fs.String("classification", def.Classification, "division of the schedule and scoreboard (fbs, fcs, ii, iii)")
	bootstrap := fs.String("kafka-bootstrap", os.Getenv("KAFKA_BOOTSTRAP"), "Kafka bootstrap servers (default $KAFKA_BOOTSTRAP)")
	timeout := fs.Duration("timeout", 30*time.Second, "HTTP timeout per CFBD request")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	apiKey := os.Getenv("CFBD_API_KEY")
	if apiKey == "" {
		log.Error("config", "error", "CFBD_API_KEY is not set")
		return 2
	}
	if *bootstrap == "" {
		log.Error("config", "error", "set KAFKA_BOOTSTRAP or --kafka-bootstrap")
		return 2
	}
	if *interval < 10*time.Second {
		log.Error("config", "error", fmt.Sprintf("--interval %s is below 10s", *interval))
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	client := cfbd.NewClientWithHTTPClient(apiKey, "", &http.Client{Timeout: *timeout})
	// Short retry budget: a poll that keeps failing is simply retried next interval.
	caller := cfbdcall.New(client, cfbdcall.Options{MaxRetries: 3, MaxBackoff: 30 * time.Second, MaxBodyBytes: 16 << 20, Logger: log})
	pub := poller.NewKafkaPublisher(*bootstrap, poller.Topic)
	defer pub.Close()

	cfg := poller.Config{
		Interval: *interval, Lead: *lead, ScheduleRefresh: *refresh, MaxGameDuration: *maxGame,
		ScheduledGrace: *grace, Classification: *classification,
	}
	log.Info("poller_start", "topic", poller.Topic, "kafka_bootstrap", *bootstrap, "interval", cfg.Interval.String(),
		"lead", cfg.Lead.String(), "classification", cfg.Classification)

	p := poller.New(caller, pub, clock.Real{}, log, cfg)
	p.CallStats = caller.Stats
	if err := p.Run(ctx); err != nil {
		log.Error("poller_failed", "error", err.Error())
		return 1
	}
	return 0
}
