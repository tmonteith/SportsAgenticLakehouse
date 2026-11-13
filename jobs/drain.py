"""Streaming drain: Kafka -> bronze -> silver, one ``availableNow`` pass (plan WS4b, contract H8).

Two Structured Streaming queries, each one foreachBatch per micro-batch:

- ``live_game_events``: ``cfb.live_games`` -> append ``bronze.live_game_events`` ->
  ``merge_games(spark, updates, mode="live")`` into ``silver.games`` (rejects to
  ``silver._rejects``).
- ``agent_events``: ``agent.events`` -> append ``bronze.agent_events`` -> MERGE into
  ``silver.agent_requests``.

An empty micro-batch does no silver work (and no reads from GCS). Checkpoints live in
``gs://<proj>-sa-warehouse/_checkpoints/<stream>/``. The process exits 0 when every query
finished and non-zero on failure, after printing one JSON line (``"event": "drain_summary"``).

Bronze appends are safe to replay: every append records the highest Kafka offset per
partition it wrote as the snapshot property ``sa.drain.max-offsets``; a replayed batch
(the job died after the Iceberg commit, before the checkpoint commit) drops offsets at or
below that watermark. Silver merges are idempotent, so the batch's silver work always runs.

Run in the drain image as:
    spark-submit --master local[2] /opt/sa/jobs/drain.py [--streams ...] [--parallel]
Env: GCP_PROJECT, ICEBERG_CATALOG, KAFKA_BOOTSTRAP, GOOGLE_APPLICATION_CREDENTIALS, KNOWN_TOOLS (optional).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from typing import Callable

from sa_lakehouse import agent_events, live_games
from sa_lakehouse.config import DEFAULT_CATALOG, Settings, build_spark, catalog_conf, settings_from_env, table
from sa_lakehouse.tables import ensure_tables

WATERMARK_PROPERTY = "sa.drain.max-offsets"
MASTER = "local[2]"
DEFAULT_MAX_OFFSETS_PER_TRIGGER = 100_000

DRAIN_TABLES = [
    "bronze.live_game_events",
    "bronze.agent_events",
    "silver.games",
    "silver.agent_requests",
    "silver._rejects",
]

BRONZE_COLUMNS = ["kafka_key", "value_json", "_kafka_partition", "_kafka_offset", "_kafka_timestamp", "_ingested_at"]


def _f():
    from pyspark.sql import functions as F

    return F


# ---- bronze ------------------------------------------------------------------------------


def to_bronze(kafka_df):
    """Kafka source rows (key, value, partition, offset, timestamp, ...) -> bronze columns.

    Records with a null value (tombstones; neither producer sends them) are dropped,
    since ``value_json`` is NOT NULL.
    """
    F = _f()
    return kafka_df.where(F.col("value").isNotNull()).select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("value_json"),
        F.col("partition").cast("int").alias("_kafka_partition"),
        F.col("offset").cast("bigint").alias("_kafka_offset"),
        F.col("timestamp").cast("timestamp").alias("_kafka_timestamp"),
        F.current_timestamp().alias("_ingested_at"),
    )


def committed_offsets(spark, bronze_table: str) -> dict[int, int]:
    """Highest offset per partition recorded by earlier appends (snapshot metadata only)."""
    rows = spark.sql(
        f"SELECT summary['{WATERMARK_PROPERTY}'] AS w FROM {bronze_table}.snapshots "
        f"WHERE summary['{WATERMARK_PROPERTY}'] IS NOT NULL"
    ).collect()
    out: dict[int, int] = {}
    for row in rows:
        for part, off in json.loads(row["w"]).items():
            out[int(part)] = max(out.get(int(part), -1), int(off))
    return out


def append_bronze(spark, bronze_df, bronze_table: str) -> int:
    """Append rows not already in the table (by the offset watermark). Returns rows appended."""
    F = _f()
    done = committed_offsets(spark, bronze_table)
    for part, off in done.items():
        bronze_df = bronze_df.where(~((F.col("_kafka_partition") == part) & (F.col("_kafka_offset") <= off)))
    stats = bronze_df.groupBy("_kafka_partition").agg(F.count("*").alias("n"), F.max("_kafka_offset").alias("max")).collect()
    rows = sum(r["n"] for r in stats)
    if rows == 0:
        return 0
    watermark = json.dumps({str(r["_kafka_partition"]): r["max"] for r in stats}, sort_keys=True)
    (
        bronze_df.select(*BRONZE_COLUMNS)
        .writeTo(bronze_table)
        .option(f"snapshot-property.{WATERMARK_PROPERTY}", watermark)
        .append()
    )
    return rows


def append_rejects(rejects_df, catalog: str = DEFAULT_CATALOG) -> int:
    rejects_df = rejects_df.cache()
    try:
        n = rejects_df.count()
        if n:
            rejects_df.writeTo(table("silver._rejects", catalog)).append()
        return n
    finally:
        rejects_df.unpersist()


# ---- foreachBatch functions ----------------------------------------------------------------


def _default_merge_games():
    # WS4a's sa_lakehouse.games is the only writer of silver.games (contract H5).
    from sa_lakehouse.games import merge_games

    return merge_games


def process_live_games_batch(
    spark, batch_df, batch_id: int, *, merge_games: Callable | None = None, catalog: str = DEFAULT_CATALOG
) -> dict:
    """One ``cfb.live_games`` micro-batch: bronze append, then silver.games (live mode)."""
    bronze = to_bronze(batch_df).cache()
    try:
        if bronze.isEmpty():
            return {"batch_id": batch_id, "empty": True}
        result = {"batch_id": batch_id, "kafka_records": bronze.count()}
        result["bronze_rows"] = append_bronze(spark, bronze, table("bronze.live_game_events", catalog))
        updates, rejects = live_games.parse_live_games(bronze)
        updates = updates.cache()
        try:
            result["silver_rows"] = updates.count()
            if result["silver_rows"]:
                (merge_games or _default_merge_games())(spark, updates, mode="live")
        finally:
            updates.unpersist()
        result["rejects"] = append_rejects(rejects, catalog)
        return result
    finally:
        bronze.unpersist()


def process_agent_events_batch(
    spark, batch_df, batch_id: int, *, tools: frozenset[str] | None = None, catalog: str = DEFAULT_CATALOG
) -> dict:
    """One ``agent.events`` micro-batch: bronze append, then MERGE into silver.agent_requests."""
    bronze = to_bronze(batch_df).cache()
    try:
        if bronze.isEmpty():
            return {"batch_id": batch_id, "empty": True}
        result = {"batch_id": batch_id, "kafka_records": bronze.count()}
        result["bronze_rows"] = append_bronze(spark, bronze, table("bronze.agent_events", catalog))
        events, rejects = agent_events.parse_events(bronze)
        events = events.cache()
        try:
            result["silver_rows"] = agent_events.merge_agent_requests(
                spark, events, tools, target=table(agent_events.TABLE_NAME, catalog)
            )
        finally:
            events.unpersist()
        result["rejects"] = append_rejects(rejects, catalog)
        return result
    finally:
        bronze.unpersist()


# ---- streams -------------------------------------------------------------------------------


@dataclass(frozen=True)
class StreamSpec:
    name: str  # checkpoint directory and query name
    topic: str
    process: Callable


STREAMS = {
    s.name: s
    for s in [
        StreamSpec("live_game_events", "cfb.live_games", process_live_games_batch),
        StreamSpec("agent_events", "agent.events", process_agent_events_batch),
    ]
}

_COUNTERS = ("batches", "empty_batches", "kafka_records", "bronze_rows", "silver_rows", "rejects")


def drain_conf(settings: Settings) -> dict[str, str]:
    conf = catalog_conf(settings)
    conf.update(
        {
            "spark.ui.enabled": "false",
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",
            # gs:// checkpoints through the GCS connector, with Application Default Credentials.
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            "spark.hadoop.fs.gs.auth.type": "APPLICATION_DEFAULT",
            "spark.hadoop.fs.gs.project.id": settings.gcp_project,
        }
    )
    return conf


def kafka_source(spark, settings: Settings, topic: str, max_offsets: int | None):
    """The topic as a streaming DataFrame, from the earliest retained offset (contract H8)."""
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
    )
    if max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", str(max_offsets))
    return reader.load()


def start_stream(
    source_df, spec: StreamSpec, checkpoint: str, stats: dict, catalog: str = DEFAULT_CATALOG, process: Callable | None = None
):
    """Start one availableNow query running ``spec.process`` (or ``process``) per micro-batch.

    Per-stream counters are accumulated into ``stats[spec.name]``.
    """
    process = process or spec.process
    counters = stats.setdefault(spec.name, {c: 0 for c in _COUNTERS})

    def on_batch(batch_df, batch_id):
        result = process(batch_df.sparkSession, batch_df, batch_id, catalog=catalog)
        counters["batches"] += 1
        counters["empty_batches"] += 1 if result.get("empty") else 0
        for c in _COUNTERS[2:]:
            counters[c] += result.get(c, 0)

    return (
        source_df.writeStream.queryName(spec.name)
        .foreachBatch(on_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )


def _start(spark, settings: Settings, spec: StreamSpec, stats: dict, max_offsets: int | None):
    source = kafka_source(spark, settings, spec.topic, max_offsets)
    return start_stream(source, spec, settings.checkpoint_dir(spec.name), stats, settings.catalog)


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument(
        "--streams",
        default=",".join(STREAMS),
        help="comma list of streams to run (default: %(default)s)",
    )
    p.add_argument("--parallel", action="store_true", help="run the streams together instead of one after the other")
    p.add_argument(
        "--max-offsets-per-trigger",
        type=int,
        default=DEFAULT_MAX_OFFSETS_PER_TRIGGER,
        help="cap per micro-batch (availableNow still drains everything); 0 = no cap",
    )
    args = p.parse_args(argv)
    names = [s.strip() for s in args.streams.split(",") if s.strip()]
    unknown = [n for n in names if n not in STREAMS]
    if unknown or not names:
        p.error(f"--streams must name some of {', '.join(STREAMS)}")
    args.stream_names = names
    return args


def main(argv=None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    started = time.time()
    stats: dict = {}
    summary = {"event": "drain_summary", "status": "ok", "streams": stats}
    spark = None
    try:
        settings = settings_from_env()
        if not settings.kafka_bootstrap:
            raise RuntimeError("KAFKA_BOOTSTRAP is not set")
        spark = build_spark("sa-drain", drain_conf(settings), master=MASTER)
        summary["tables_created"] = ensure_tables(spark, DRAIN_TABLES, catalog=settings.catalog)
        specs = [STREAMS[n] for n in args.stream_names]
        if args.parallel:
            queries = [_start(spark, settings, s, stats, args.max_offsets_per_trigger) for s in specs]
            try:
                for q in queries:
                    q.awaitTermination()
            finally:
                for q in queries:
                    if q.isActive:
                        q.stop()
        else:
            for s in specs:
                _start(spark, settings, s, stats, args.max_offsets_per_trigger).awaitTermination()
    except Exception as exc:  # report and exit non-zero
        summary["status"] = "failed"
        summary["error"] = f"{type(exc).__name__}: {str(exc).splitlines()[0][:500] if str(exc) else ''}"
    finally:
        if spark is not None:
            spark.stop()
    summary["seconds"] = round(time.time() - started, 1)
    print(json.dumps(summary, sort_keys=True), flush=True)
    return 0 if summary["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
