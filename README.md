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
go run ./cmd/extractor --season=current --weeks=recent --force --completion-marker=extractor-nightly  # what the nightly CronJob runs

# Poller
export CFBD_API_KEY=... KAFKA_BOOTSTRAP=localhost:9092
go run ./cmd/poller
```

With `--completion-marker=<name>`, a successful run (not a dry run) also writes `gs://${GCP_PROJECT}-sa-landing/_markers/<name>/<YYYY-MM-DD>.json`, dated by the America/Denver day the run started, with `started_at`, `finished_at`, `files_written`, `api_calls` and `image_tag` (env `IMAGE_TAG`). It is written with `ifGenerationMatch=0`, so a second run the same day keeps the first marker. The nightly CronJob passes `--completion-marker=extractor-nightly`; the backfill Job passes nothing. The `cfb_nightly` DAG waits for this marker.

Images: `ingest/cmd/extractor/Dockerfile` and `ingest/cmd/poller/Dockerfile` (distroless, non-root, `linux/amd64` and `linux/arm64`, build context `ingest/`). `.github/workflows/ingest-images.yml` builds both and pushes them to OCIR as `sportsagentic/extractor` and `sportsagentic/poller` on every push to `main` that touches `ingest/`, with the same short-SHA tag. The Kubernetes manifests live in the infra repo (`k8s/data/ingest`, variable `ingest_image_tag`).

## Batch jobs on Serverless for Apache Spark

`scripts/publish-jobs.sh` uploads the jobs to `gs://${GCP_PROJECT}-sa-jobs/jobs/<short-sha>/` and copies the same files to `jobs/current/` (with `current/VERSION` holding the SHA). The DAGs always submit from `jobs/current/`, so **run `publish-jobs.sh` after every merge to `main`**; to roll back, run it from an older checkout. `scripts/submit-batch.sh <job>` submits one batch by hand (`JOBS_VERSION=current` to run what the DAGs run).

Both the script and the DAGs build the batch from `jobs/sa_lakehouse/batch_spec.py`: runtime 2.3, the Iceberg jars, `sa_lakehouse.zip`, subnet `sa-dataproc`, service account `sa-spark-runner`, labels, catalog properties, driver env and `spark.dynamicAllocation.maxExecutors=2`. Airflow cannot import `sa_lakehouse`, so `scripts/gen-batch-spec.py` writes a copy to `dags/lakehouse/batch_spec.json`; rerun it after changing `batch_spec.py` or `config.py` (a test fails while the copy is stale).

`jobs/nightly.py` runs `load_landing` → `build_silver` → `build_gold` in one Spark session, prints each step's summary line and a final `nightly_summary` line, and exits non-zero if a step fails. `build_gold.py --tables gold.game_summaries` rebuilds only the listed gold tables.

## Airflow DAGs

Airflow 3.2 on the cluster (namespace `data`, LocalExecutor, no triggerer) syncs `dags/` from `main`. Every DAG runs in America/Denver with `catchup=False` and `max_active_runs=1`; batch tasks retry twice with exponential backoff (from 5 minutes, doubling), and each try submits a new batch with its own ID. Failures show in the Airflow UI only.

| DAG | Schedule (America/Denver) | Does |
| --- | --- | --- |
| `cfb_nightly` | `45 3 * * *`, daily 03:45 | Waits (up to 3 h, checking every 5 min) for the extractor's marker `gs://<proj>-sa-landing/_markers/extractor-nightly/<date>.json`, then one batch runs `nightly.py`. Run timeout 4 h. |
| `stream_drain` | `*/15 * * * *` | A pod in `data` runs the spark-drain image (same pod spec as the infra chart `k8s/data/drain/`) and drains `cfb.live_games` and `agent.events` into bronze and silver. Succeeded pods are deleted. |
| `gold_after_live` | `30 23 * 1,8-12 4-6`, Thu/Fri/Sat 23:30, August to January | One batch runs `build_gold.py --tables gold.game_summaries --seasons <season>` (January and February belong to the previous season). |
| `lakehouse_maintenance` | `0 5 * * 0`, Sundays 05:00 | One batch runs `maintain.py` (compaction, snapshot expiry, orphan files). |

Shared helpers are in `dags/lakehouse/` (ignored by the DAG parser through `dags/.airflowignore`).

**Before unpausing** (all four DAGs start paused):

1. The Airflow pods have `GCP_PROJECT` and `GOOGLE_APPLICATION_CREDENTIALS` (the `gcp-key-airflow` key) and an empty Google connection, `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=google-cloud-platform://`, so the Google operators use those Application Default Credentials. The infra repo sets all three.
2. The drain image is set as Airflow Variable `spark_drain_image`, the full reference `ord.ocir.io/<tenancy-namespace>/sportsagentic/spark-drain:<tag>`. The infra repo sets it as env `AIRFLOW_VAR_SPARK_DRAIN_IMAGE`; for a quick test, set it in the UI (Admin → Variables) or with `airflow variables set spark_drain_image <ref>`. Without it, `stream_drain` fails and says so.
3. `scripts/publish-jobs.sh` has run, so `gs://<proj>-sa-jobs/jobs/current/` exists.
4. The extractor CronJob passes `--completion-marker=extractor-nightly`.

Then unpause `stream_drain`, `cfb_nightly`, `lakehouse_maintenance` and, during the season, `gold_after_live`.

**Run one by hand:** Trigger DAG in the UI, or from the scheduler pod:

```bash
kubectl -n data exec deploy/airflow-scheduler -c scheduler -- airflow dags trigger cfb_nightly
```

A manual `cfb_nightly` run waits for today's marker (Denver date). To run a job without Airflow, use `scripts/submit-batch.sh`.

**Serverless runs per month:** 30–31 nightly + 4–5 maintenance + 12–15 game-day (Thursdays to Saturdays, August to January) = 46 to 50 in season, 32 to 35 from February to July, about 41 a month over a year. That is well under the 80-run budget, at about 6 cents per 5-minute, 12-DCU batch (roughly $3 a month in season). Retries add a batch each; the drain runs on the cluster and adds no Serverless runs.

## Tests

```bash
scripts/test.sh        # PySpark jobs, in apache/spark 3.5.7 with a local Iceberg catalog
scripts/test-dags.sh   # DAGs, in apache/airflow:3.2.2 (unittest; nothing installed)
cd ingest && go test ./...
```
