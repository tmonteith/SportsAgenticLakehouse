#!/usr/bin/env bash
# Submit one batch job to Serverless for Apache Spark (contracts H1, H2, H9). Run by the user.
#
# Usage: GCP_PROJECT=<proj> scripts/submit-batch.sh [--dry-run] <job> [job args...]
#   <job>        create_tables | load_landing | build_silver | build_gold | maintain
#   --dry-run    print the gcloud command instead of running it
# Environment:
#   GCP_PROJECT        required
#   JOBS_VERSION       published code version (default: this checkout's git short SHA)
#   ICEBERG_CATALOG    catalog name (default sa_catalog)
#   SA_STAGING_BUCKET  optional --staging-bucket (default: Dataproc's own staging bucket)
#   SA_MAX_EXECUTORS   executor cap (default 2: 12 DCUs with the driver)
#
# Catalog properties come from sa_lakehouse.config.catalog_conf (the single H2 source);
# GCP_PROJECT and ICEBERG_CATALOG reach the driver through spark.dataproc.driverEnv.*.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REGION="us-central1"
RUNTIME="2.3"
SUBNET="sa-dataproc"
ICEBERG_VERSION="1.10.2"
JOBS=(create_tables load_landing build_silver build_gold maintain)

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then DRY_RUN=1; shift; fi
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -eq 0 ]]; then sed -n '2,15p' "$0"; [[ $# -eq 0 ]] && exit 2; exit 0; fi
JOB="$1"; shift
if [[ ! " ${JOBS[*]} " == *" ${JOB} "* ]]; then
  echo "unknown job: ${JOB} (one of: ${JOBS[*]})" >&2
  exit 2
fi

: "${GCP_PROJECT:?GCP_PROJECT is not set}"
VERSION="${JOBS_VERSION:-$(git -C "$ROOT" rev-parse --short HEAD)}"
BUCKET="gs://${GCP_PROJECT}-sa-jobs"
BASE="${BUCKET}/jobs/${VERSION}"

# '^#^' switches gcloud's list delimiter to '#' (property values contain ':' and '/').
PROPERTIES="$(PYTHONPATH="$ROOT/jobs" python3 - <<'PY'
import os
from sa_lakehouse.config import catalog_conf, settings_from_env

settings = settings_from_env()
props = dict(catalog_conf(settings))
props["spark.dataproc.driverEnv.GCP_PROJECT"] = settings.gcp_project
props["spark.dataproc.driverEnv.ICEBERG_CATALOG"] = settings.catalog
# Hold every batch at the plan's 12 DCUs (driver + 2 executors, 4 vCPUs each). Without a cap,
# dynamic allocation scales to the project's 32-vCPU CPUS_ALL_REGIONS quota, and the next batch
# cannot start until those workers are released. Override with SA_MAX_EXECUTORS.
props["spark.dynamicAllocation.maxExecutors"] = os.environ.get("SA_MAX_EXECUTORS", "2")
bad = [k for k, v in props.items() if "#" in k + v]
if bad:
    raise SystemExit(f"'#' in properties: {bad}")
print("^#^" + "#".join(f"{k}={v}" for k, v in props.items()))
PY
)"

BATCH_ID="sa-${JOB//_/-}-$(date -u +%Y%m%d-%H%M%S)"
cmd=(
  gcloud dataproc batches submit pyspark "${BASE}/${JOB}.py"
  --project "${GCP_PROJECT}"
  --region "${REGION}"
  --batch "${BATCH_ID}"
  --version "${RUNTIME}"
  --py-files "${BASE}/sa_lakehouse.zip"
  --jars "${BUCKET}/jars/iceberg-spark-runtime-3.5_2.13-${ICEBERG_VERSION}.jar,${BUCKET}/jars/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar"
  --subnet "${SUBNET}"
  --service-account "sa-spark-runner@${GCP_PROJECT}.iam.gserviceaccount.com"
  --labels "app=sportsagentic,job=${JOB}"
  --properties "${PROPERTIES}"
)
if [[ -n "${SA_STAGING_BUCKET:-}" ]]; then cmd+=(--staging-bucket "${SA_STAGING_BUCKET}"); fi
if [[ $# -gt 0 ]]; then cmd+=(-- "$@"); fi

if [[ $DRY_RUN -eq 1 ]]; then
  printf '+'; printf ' %q' "${cmd[@]}"; printf '\n'
else
  "${cmd[@]}"
fi
