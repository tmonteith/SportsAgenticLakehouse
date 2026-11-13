#!/usr/bin/env bash
# Publish the batch jobs to the jobs bucket (contracts H9, I3). Run by the user, never by CI or agents.
#
#   gs://<proj>-sa-jobs/jobs/<git-short-sha>/sa_lakehouse.zip   (jobs/sa_lakehouse + jobs/resources
#                                                                  + the entry scripts, which
#                                                                  nightly.py imports)
#   gs://<proj>-sa-jobs/jobs/<git-short-sha>/<entry>.py          (the six entry scripts)
#   gs://<proj>-sa-jobs/jars/<iceberg jars>                       (uploaded only if missing; from
#                                                                  Maven Central, SHA-1 checked)
#   gs://<proj>-sa-jobs/jobs/current/...                          (the same zip and entry scripts,
#                                                                  overwritten, then VERSION = the
#                                                                  short SHA; the Airflow DAGs
#                                                                  always submit from current/)
#
# Rollback: run this script from an older checkout. Avoid publishing while a DAG batch is
# starting (cfb_nightly 03:45, gold_after_live 23:30, lakehouse_maintenance Sun 05:00 Denver):
# the files in current/ are replaced one by one.
#
# Usage: GCP_PROJECT=<proj> scripts/publish-jobs.sh [--dry-run] [--allow-dirty]
#   --dry-run      build the zip locally and print every gcloud command instead of running it
#   --allow-dirty  publish from a working tree with uncommitted changes under jobs/
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ICEBERG_VERSION="1.10.2"
SCALA="2.13" # Serverless runtime 2.3 (contract H1)
MAVEN="https://repo1.maven.org/maven2/org/apache/iceberg"
JARS=(
  "iceberg-spark-runtime-3.5_${SCALA}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA}-${ICEBERG_VERSION}.jar"
  "iceberg-gcp-bundle/${ICEBERG_VERSION}/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar"
)
ENTRIES=(create_tables load_landing build_silver build_gold maintain nightly)

DRY_RUN=0
ALLOW_DIRTY=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    --allow-dirty) ALLOW_DIRTY=1 ;;
    -h|--help) sed -n '2,23p' "$0"; exit 0 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

: "${GCP_PROJECT:?GCP_PROJECT is not set}"
SHA="$(git -C "$ROOT" rev-parse --short HEAD)"
if [[ -n "$(git -C "$ROOT" status --porcelain -- jobs)" ]]; then
  if [[ $ALLOW_DIRTY -eq 1 || $DRY_RUN -eq 1 ]]; then
    echo "warning: jobs/ has uncommitted changes; the published code will not match $SHA" >&2
  else
    echo "jobs/ has uncommitted changes; commit first or pass --allow-dirty" >&2
    exit 1
  fi
fi

BUCKET="gs://${GCP_PROJECT}-sa-jobs"
DEST="${BUCKET}/jobs/${SHA}"

run() {
  if [[ $DRY_RUN -eq 1 ]]; then
    printf '+'; printf ' %q' "$@"; printf '\n'
  else
    "$@"
  fi
}

BUILD="$(mktemp -d)"
trap 'rm -rf "$BUILD"' EXIT

# Zip with the package and resources at the root, so --py-files imports sa_lakehouse and
# sa_lakehouse.teams finds resources/team_aliases.json inside the archive. The entry scripts go
# in at the root too, so nightly.py can import load_landing, build_silver and build_gold.
python3 - "$ROOT/jobs" "$BUILD/sa_lakehouse.zip" "${ENTRIES[@]}" <<'PY'
import os, sys, zipfile
src, out, entries = sys.argv[1], sys.argv[2], sys.argv[3:]
with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
    for entry in sorted(entries):
        zf.write(os.path.join(src, f"{entry}.py"), f"{entry}.py")
    for top in ("sa_lakehouse", "resources"):
        for dirpath, dirnames, filenames in os.walk(os.path.join(src, top)):
            dirnames[:] = sorted(d for d in dirnames if d != "__pycache__")
            for name in sorted(filenames):
                if name.endswith((".pyc", ".pyo")):
                    continue
                path = os.path.join(dirpath, name)
                zf.write(path, os.path.relpath(path, src))
    print(f"built {out}: {len(zf.namelist())} files", file=sys.stderr)
PY
if [[ $DRY_RUN -eq 1 ]]; then
  python3 -m zipfile -l "$BUILD/sa_lakehouse.zip" >&2
fi

run gcloud storage cp "$BUILD/sa_lakehouse.zip" "${DEST}/sa_lakehouse.zip"
entry_paths=()
for e in "${ENTRIES[@]}"; do entry_paths+=("$ROOT/jobs/$e.py"); done
run gcloud storage cp "${entry_paths[@]}" "${DEST}/"

for rel in "${JARS[@]}"; do
  jar="$(basename "$rel")"
  if [[ $DRY_RUN -eq 1 ]]; then
    echo "# if ${BUCKET}/jars/${jar} is missing (gcloud storage ls): download ${MAVEN}/${rel}, check its .sha1, then:"
    run gcloud storage cp "$BUILD/$jar" "${BUCKET}/jars/${jar}"
    continue
  fi
  if gcloud storage ls "${BUCKET}/jars/${jar}" >/dev/null 2>&1; then
    echo "${jar} already in ${BUCKET}/jars/"
    continue
  fi
  curl -fsSL -o "$BUILD/$jar" "${MAVEN}/${rel}"
  expected="$(curl -fsSL "${MAVEN}/${rel}.sha1" | cut -c1-40)"
  actual="$(shasum -a 1 "$BUILD/$jar" | cut -d' ' -f1)"
  [[ "$expected" == "$actual" ]] || { echo "checksum mismatch for ${jar}" >&2; exit 1; }
  run gcloud storage cp "$BUILD/$jar" "${BUCKET}/jars/${jar}"
done

# jobs/current/ last, once the jars exist: the same files, overwritten, then VERSION.
CURRENT="${BUCKET}/jobs/current"
printf '%s\n' "$SHA" >"$BUILD/VERSION"
run gcloud storage cp "$BUILD/sa_lakehouse.zip" "${CURRENT}/sa_lakehouse.zip"
run gcloud storage cp "${entry_paths[@]}" "${CURRENT}/"
run gcloud storage cp "$BUILD/VERSION" "${CURRENT}/VERSION"

echo "published ${DEST} and ${CURRENT} (VERSION ${SHA}); submit with JOBS_VERSION=${SHA} or JOBS_VERSION=current scripts/submit-batch.sh <job>"
