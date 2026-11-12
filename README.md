# SportsAgenticLakehouse

Batch and streaming jobs that build the SportsAgentic Iceberg lakehouse on Google Cloud, and the Airflow DAGs that schedule them.

College football data and agent telemetry flow from the SportsAgentic cluster on Oracle Kubernetes Engine into bronze, silver and gold Iceberg tables. The SportsAgentic MCP server reads the gold tables through BigQuery.

## Layout

| Path | Holds |
| --- | --- |
| `jobs/` | PySpark jobs: landing to bronze, bronze to silver, silver to gold, table maintenance, and the Kafka drain job |
| `dags/` | Airflow DAGs. Airflow on the cluster syncs this directory from the `main` branch |
| `ingest/` | Go module with the CFB extractor and the game-day poller, and their Dockerfiles |
| `docker/drain/` | Container image for the drain job (Spark in local mode, built for amd64 and arm64) |

## Related repositories

- [SportsAgenticInfra](https://github.com/tmonteith/SportsAgenticInfra): OpenTofu for the cluster, Kafka, Airflow and the Google Cloud resources
- [SportsAgentic](https://github.com/tmonteith/SportsAgentic): Go MCP server
- [SportsAgenticAgent](https://github.com/tmonteith/SportsAgenticAgent): FastAPI orchestrator, which emits agent telemetry
- [SportsAgenticUI](https://github.com/tmonteith/SportsAgenticUI): web UI

## Ingestion: extractor and poller

Two small Go commands in `ingest/` (its own module, `github.com/tmonteith/SportsAgenticLakehouse/ingest`) feed the lakehouse. Both count every CFBD request in their logs, because the monthly quota is shared with the MCP server's live tools, and retry `429` and `5xx` responses with backoff.

| Command | Runs as | Does |
| --- | --- | --- |
| `ingest/cmd/extractor` | nightly CronJob and backfill Job (namespace `data`) | Lands raw `/games`, `/plays` and `/teams` responses, gzipped, at `gs://${GCP_PROJECT}-sa-landing/cfb/<endpoint>/season=<YYYY>/week=<WW>/<fetched_at>.json.gz`. Never overwrites; skips a response already landed (same endpoint and query) unless `--force`. |
| `ingest/cmd/poller` | Deployment, 1 replica | Reads the schedule, sleeps until 15 minutes before the next kickoff, then polls `/scoreboard` every 60 s and publishes a game to Kafka topic `cfb.live_games` only when its state changed. |

```bash
cd ingest
go test ./...                                       # recorded fixtures only; no live API, GCS or Kafka

# Extractor (Application Default Credentials for GCS)
export CFBD_API_KEY=... GCP_PROJECT=... GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
go run ./cmd/extractor --season=2024 --weeks=all --dry-run      # what would be fetched
go run ./cmd/extractor --season=2024 --weeks=all                # backfill one season
go run ./cmd/extractor --season=current --weeks=recent --force  # what the nightly CronJob runs

# Poller
export CFBD_API_KEY=... KAFKA_BOOTSTRAP=localhost:9092
go run ./cmd/poller
```

Images: `ingest/cmd/extractor/Dockerfile` and `ingest/cmd/poller/Dockerfile` (distroless, non-root, `linux/amd64` and `linux/arm64`, build context `ingest/`). `.github/workflows/ingest-images.yml` builds both and pushes them to OCIR as `sportsagentic/extractor` and `sportsagentic/poller` on every push to `main` that touches `ingest/`, with the same short-SHA tag. The Kubernetes manifests live in the infra repo (`k8s/data/ingest`, variable `ingest_image_tag`).

## Status

This repository is a skeleton. Jobs and DAGs land as the lakehouse build reaches them.
