# syntax=docker/dockerfile:1
# Streaming drain (plan WS4b, contract H1/H8): local-mode Spark 3.5.7 with the Iceberg
# runtime, the Iceberg GCP bundle (BigLake REST catalog auth, GCSFileIO), the Kafka source
# and the shaded GCS connector (gs:// checkpoints). Jars are downloaded and checksum-checked
# at build time; nothing is downloaded at run time. No credentials in the image: the key is
# mounted at run time (GOOGLE_APPLICATION_CREDENTIALS).
#
# Build from the repo root:  docker build -f docker/drain/Dockerfile -t spark-drain .
# The base image is multi-arch (linux/amd64, linux/arm64) and nothing added here is
# arch-specific (jars and Python sources only).
FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root
COPY docker/drain/jars.txt /tmp/jars.txt
RUN set -eu; \
    cd /opt/spark/jars; \
    while read -r sha path; do \
      case "$sha" in ''|'#'*) continue ;; esac; \
      file="$(basename "$path")"; \
      curl -fsSL --retry 3 -o "$file" "https://repo1.maven.org/maven2/$path"; \
      echo "$sha  $file" | sha256sum -c --quiet -; \
      chmod 0644 "$file"; \
    done < /tmp/jars.txt; \
    rm /tmp/jars.txt

# Only what the drain imports: the entry script and the sa_lakehouse package.
COPY --chown=root:root --chmod=0755 jobs/drain.py /opt/sa/jobs/drain.py
COPY --chown=root:root --chmod=0755 jobs/sa_lakehouse /opt/sa/jobs/sa_lakehouse

# /tmp is the only path Spark writes (spark.local.dir, JVM temp files); the pod mounts an
# emptyDir there so the root filesystem can be read-only. SPARK_DRIVER_MEMORY is read by
# spark-submit (contract H8: 1400m inside a 2Gi limit).
ENV HOME=/tmp \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_DRIVER_MEMORY=1400m
WORKDIR /tmp

# The base image's non-root "spark" user.
USER 185:185
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--conf", "spark.ui.enabled=false", "/opt/sa/jobs/drain.py"]
CMD []
