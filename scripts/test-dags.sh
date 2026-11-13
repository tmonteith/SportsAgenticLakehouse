#!/usr/bin/env bash
# Test the Airflow DAGs in dags/ inside the image the cluster runs (apache/airflow:3.2.2,
# providers google 22.0.0 and cncf-kubernetes 10.17.1). Needs Docker. The worktree is
# mounted read-only and nothing is installed: the tests use the standard library's unittest.
# No GCP, Kubernetes or Airflow database is touched.
#
# Usage: scripts/test-dags.sh [unittest args, e.g. -k sensor]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${AIRFLOW_TEST_IMAGE:-apache/airflow:3.2.2}"

exec docker run --rm \
  -v "$ROOT:/work:ro" \
  -w /work \
  -e AIRFLOW_HOME=/tmp/airflow \
  -e AIRFLOW__CORE__DAGS_FOLDER=/work/dags \
  -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -e GCP_PROJECT=test-proj \
  -e AIRFLOW_VAR_SPARK_DRAIN_IMAGE=ord.ocir.io/test-namespace/sportsagentic/spark-drain:abc1234 \
  --entrypoint python \
  "$IMAGE" -W ignore::DeprecationWarning -m unittest discover -s tests/dags -t . "$@"
