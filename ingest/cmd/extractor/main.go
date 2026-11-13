// Command extractor lands raw CFBD API responses (games, plays, teams) in the
// lakehouse landing bucket, gzipped, one object per response, never overwriting.
//
// Nightly (CronJob):  extractor --season=current --weeks=recent --force
// Backfill (Job):     extractor --season=2023,2024,2025 --weeks=all
//
// With --completion-marker=<name> (the nightly CronJob passes extractor-nightly), a
// successful run also writes gs://<bucket>/_markers/<name>/<YYYY-MM-DD>.json (the
// America/Denver date the run started), which Airflow's cfb_nightly DAG waits for.
//
// Environment: CFBD_API_KEY (required), GCP_PROJECT (landing bucket
// ${GCP_PROJECT}-sa-landing unless --bucket is set), GOOGLE_APPLICATION_CREDENTIALS
// (Application Default Credentials), IMAGE_TAG (optional; recorded in the marker).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/cfbd"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/cfbdcall"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/clock"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/extractor"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/landing"
	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/marker"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	fs := flag.NewFlagSet("extractor", flag.ContinueOnError)
	season := fs.String("season", "current", `season(s): "current", a year, or a list such as "2023,2024" or "2022-2024"`)
	weeks := fs.String("weeks", "recent", `weeks: "all", "recent" (calendar weeks overlapping the last --recent-days), or a list such as "1-15"`)
	seasonTypes := fs.String("season-types", extractor.DefaultSeasonTypes, "season types: regular, postseason")
	endpoints := fs.String("endpoints", extractor.DefaultEndpoints, "endpoints to land: games, plays, teams")
	force := fs.Bool("force", false, "re-land responses already landed (new objects; nothing is overwritten)")
	dryRun := fs.Bool("dry-run", false, "list what would be fetched; no data calls, no writes")
	bucket := fs.String("bucket", "", "landing bucket (default ${GCP_PROJECT}-sa-landing)")
	recentDays := fs.Int("recent-days", int(extractor.DefaultRecentWindow.Hours()/24), "look-back of --weeks=recent, in days")
	minInterval := fs.Duration("min-interval", time.Second, "minimum time between CFBD requests")
	maxRetries := fs.Int("max-retries", 5, "retries per request on 429, 5xx or network errors")
	timeout := fs.Duration("timeout", 2*time.Minute, "HTTP timeout per CFBD request (play-by-play weeks are large)")
	completionMarker := fs.String("completion-marker", "", "after a successful run, write _markers/<name>/<America/Denver date>.json (empty: no marker)")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	fail := func(msg string, err error) int {
		log.Error(msg, "error", err.Error())
		return 2
	}

	apiKey := os.Getenv("CFBD_API_KEY")
	if apiKey == "" {
		return fail("config", fmt.Errorf("CFBD_API_KEY is not set"))
	}
	if *bucket == "" {
		project := os.Getenv("GCP_PROJECT")
		if project == "" {
			return fail("config", fmt.Errorf("set GCP_PROJECT or --bucket"))
		}
		*bucket = project + "-sa-landing"
	}

	seasons, err := extractor.ParseSeasons(*season, time.Now())
	if err != nil {
		return fail("config", err)
	}
	weekSpec, err := extractor.ParseWeeks(*weeks)
	if err != nil {
		return fail("config", err)
	}
	types, err := extractor.ParseSeasonTypes(*seasonTypes)
	if err != nil {
		return fail("config", err)
	}
	eps, err := extractor.ParseEndpoints(*endpoints)
	if err != nil {
		return fail("config", err)
	}
	if *completionMarker != "" {
		if err := marker.ValidateName(*completionMarker); err != nil {
			return fail("config", err)
		}
	}
	if *recentDays < 1 {
		return fail("config", fmt.Errorf("--recent-days must be at least 1"))
	}
	retries := *maxRetries
	if retries == 0 {
		retries = -1 // cfbdcall treats 0 as "default"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	store, err := landing.NewGCS(ctx, *bucket)
	if err != nil {
		return fail("gcs", err)
	}
	client := cfbd.NewClientWithHTTPClient(apiKey, "", &http.Client{Timeout: *timeout})
	// A week of play-by-play is ~15 MB; the cap keeps a runaway response from
	// exceeding the 256Mi memory limit.
	caller := cfbdcall.New(client, cfbdcall.Options{
		MinInterval: *minInterval, MaxRetries: retries, MaxBodyBytes: 64 << 20, Logger: log,
	})

	cfg := extractor.Config{
		Seasons: seasons, Weeks: weekSpec, SeasonTypes: types, Endpoints: eps,
		Force: *force, DryRun: *dryRun, RecentWindow: time.Duration(*recentDays) * 24 * time.Hour,
	}
	names := make([]string, len(eps))
	for i, ep := range eps {
		names[i] = ep.Name
	}
	log.Info("extractor_start", "bucket", *bucket, "seasons", seasons, "weeks", weekSpec.String(),
		"season_types", types, "endpoints", strings.Join(names, ","), "force", *force, "dry_run", *dryRun,
		"completion_marker", *completionMarker)

	started := time.Now()
	sum, runErr := extractor.New(caller, store, clock.Real{}, log).Run(ctx, cfg)
	finished := time.Now()
	stats := caller.Stats()
	log.Info("extractor_summary", "summary", sum, "api", stats, "duration_s", int(finished.Sub(started).Seconds()))
	if runErr != nil {
		log.Error("extractor_failed", "error", runErr.Error())
	}

	// Never a marker for a failed or dry run (marker.AfterRun checks both).
	body := marker.NewBody(started, finished, sum.Total.Landed, stats.Calls, os.Getenv("IMAGE_TAG"))
	if _, err := marker.AfterRun(ctx, store, *completionMarker, started, *dryRun, runErr, body, log); err != nil {
		log.Error("marker_failed", "error", err.Error())
		return 1
	}
	if runErr != nil {
		return 1
	}
	return 0
}
