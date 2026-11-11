# SportsAgenticLakehouse

Batch and streaming jobs that build the SportsAgentic Iceberg lakehouse on Google Cloud, and the Airflow DAGs that schedule them.

College football data and agent telemetry flow from the SportsAgentic cluster on Oracle Kubernetes Engine into bronze, silver and gold Iceberg tables. The SportsAgentic MCP server reads the gold tables through BigQuery.

## Layout

| Path | Holds |
| --- | --- |
| `jobs/` | PySpark jobs: landing to bronze, bronze to silver, silver to gold, table maintenance, and the Kafka drain job |
| `dags/` | Airflow DAGs. Airflow on the cluster syncs this directory from the `main` branch |
| `docker/drain/` | Container image for the drain job (Spark in local mode, built for amd64 and arm64) |

## Related repositories

- [SportsAgenticInfra](https://github.com/tmonteith/SportsAgenticInfra): OpenTofu for the cluster, Kafka, Airflow and the Google Cloud resources
- [SportsAgentic](https://github.com/tmonteith/SportsAgentic): Go MCP server, the extractor and the game-day poller
- [SportsAgenticAgent](https://github.com/tmonteith/SportsAgenticAgent): FastAPI orchestrator, which emits agent telemetry
- [SportsAgenticUI](https://github.com/tmonteith/SportsAgenticUI): web UI

## Status

This repository is a skeleton. Jobs and DAGs land as the lakehouse build reaches them.
