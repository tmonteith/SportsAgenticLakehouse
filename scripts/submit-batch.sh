#!/usr/bin/env bash
# Submit one batch job to Serverless for Apache Spark (contracts H1, H2, H9). Run by the user.
#
# Usage: GCP_PROJECT=<proj> scripts/submit-batch.sh [--dry-run] <job> [job args...]
#   <job>        create_tables | load_landing | build_silver | build_gold | maintain | nightly
#   --dry-run    print the gcloud command instead of running it
# Environment:
#   GCP_PROJECT        required
#   JOBS_VERSION       published code version (default: this checkout's git short SHA;
#                      'current' is what the Airflow DAGs submit)
#   ICEBERG_CATALOG    catalog name (default sa_catalog)
#   SA_STAGING_BUCKET  optional --staging-bucket (default: Dataproc's own staging bucket)
#   SA_MAX_EXECUTORS   executor cap (default 2: 12 DCUs with the driver)
#
# The batch (runtime 2.3, jars, py-files, subnet, service account, labels, catalog properties,
# driverEnv, executor cap) comes from sa_lakehouse.batch_spec (contract I4), the same spec the
# Airflow DAGs submit; GCP_PROJECT and ICEBERG_CATALOG reach the driver through driverEnv.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REGION="us-central1"
JOBS=(create_tables load_landing build_silver build_gold maintain nightly)

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then DRY_RUN=1; shift; fi
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -eq 0 ]]; then sed -n '2,17p' "$0"; [[ $# -eq 0 ]] && exit 2; exit 0; fi
JOB="$1"; shift
if [[ ! " ${JOBS[*]} " == *" ${JOB} "* ]]; then
  echo "unknown job: ${JOB} (one of: ${JOBS[*]})" >&2
  exit 2
fi

: "${GCP_PROJECT:?GCP_PROJECT is not set}"
VERSION="${JOBS_VERSION:-$(git -C "$ROOT" rev-parse --short HEAD)}"

# Main file, runtime, py-files, jars, subnet, service account, labels and properties all come
# from sa_lakehouse.batch_spec, the batch the Airflow DAGs submit too (one per line).
spec_args=(--job "$JOB" --jobs-ref "$VERSION")
if [[ -n "${SA_STAGING_BUCKET:-}" ]]; then spec_args+=(--staging-bucket "${SA_STAGING_BUCKET}"); fi
SPEC="$(PYTHONPATH="$ROOT/jobs" python3 -m sa_lakehouse.batch_spec "${spec_args[@]}")"
spec=()
while IFS= read -r line; do spec+=("$line"); done <<<"$SPEC"

BATCH_ID="sa-${JOB//_/-}-$(date -u +%Y%m%d-%H%M%S)"
cmd=(
  gcloud dataproc batches submit pyspark "${spec[0]}"
  --project "${GCP_PROJECT}"
  --region "${REGION}"
  --batch "${BATCH_ID}"
  "${spec[@]:1}"
)
if [[ $# -gt 0 ]]; then cmd+=(-- "$@"); fi

if [[ $DRY_RUN -eq 1 ]]; then
  printf '+'; printf ' %q' "${cmd[@]}"; printf '\n'
else
  "${cmd[@]}"
fi
