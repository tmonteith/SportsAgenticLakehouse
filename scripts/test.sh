#!/usr/bin/env bash
# Run the jobs test suite in the same Spark the drain image uses (contract H1):
# apache/spark 3.5.7 (Java 17, Python 3), plus the Iceberg Spark runtime jar, against a
# local Iceberg catalog. Needs Docker. Jars are cached in .cache/jars (git-ignored).
#
# Usage: scripts/test.sh [pytest args...]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${SPARK_TEST_IMAGE:-apache/spark:3.5.7-scala2.12-java17-python3-ubuntu}"
ICEBERG_VERSION="1.10.2"
JAR_DIR="$ROOT/.cache/jars"
RUNTIME_JAR="iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"
MAVEN="https://repo1.maven.org/maven2/org/apache/iceberg"

mkdir -p "$JAR_DIR"
if [[ ! -s "$JAR_DIR/$RUNTIME_JAR" ]]; then
  url="$MAVEN/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/${RUNTIME_JAR}"
  curl -fsSL -o "$JAR_DIR/$RUNTIME_JAR.tmp" "$url"
  expected="$(curl -fsSL "$url.sha1")"
  actual="$(shasum -a 1 "$JAR_DIR/$RUNTIME_JAR.tmp" | cut -d' ' -f1)"
  [[ "$expected" == "$actual" ]] || { echo "checksum mismatch for $RUNTIME_JAR" >&2; exit 1; }
  mv "$JAR_DIR/$RUNTIME_JAR.tmp" "$JAR_DIR/$RUNTIME_JAR"
fi

# Runs as the image's own spark user: Hadoop needs a uid with a passwd entry.
exec docker run --rm \
  -e HOME=/tmp \
  -e SPARK_SUBMIT_OPTS=-Duser.home=/tmp \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -e SA_TEST_JARS="/jars/$RUNTIME_JAR" \
  -v "$ROOT/jobs:/work/jobs" \
  -v "$JAR_DIR:/jars:ro" \
  -w /work/jobs \
  --entrypoint bash \
  "$IMAGE" -c '
    set -e
    python3 -m pip install --quiet --user --disable-pip-version-check --no-warn-script-location "pytest>=8" >/dev/null
    export PYTHONPATH="/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*.zip):/work/jobs"
    python3 -m pytest -p no:cacheprovider "$@"
  ' bash "$@"
