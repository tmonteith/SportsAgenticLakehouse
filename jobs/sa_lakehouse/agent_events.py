"""``agent.events`` -> ``silver.agent_requests`` (plan WS4b, contract H8).

One row per ``request_id``, assembled from the events in a micro-batch plus the row
already in the table, so events may arrive out of order and across drain runs:

- ``requested_at``: the ``request_started`` event_time; without one, the earliest event
  seen so far (the stored value or the batch's earliest, whichever is earlier).
- ``completed_at``/``status``/``latency_ms``/``error_code``/``model``/tokens: from the
  ``request_completed`` (status ``completed``) or ``request_failed`` (``failed``) event;
  ``in_flight`` until one arrives. A stored terminal status is kept when the batch has none.
- Every field is ``coalesce(batch value, stored value)``: a known value is never replaced
  by null.
- ``tool_calls``: stored calls plus one per ``tool_called`` event (deduplicated by
  ``event_id`` within the batch, and by ``(tool, started_at, latency_ms, status)`` against
  stored calls, since the stored struct carries no event id), sorted by ``started_at``.
  ``known_tool`` = tool in :data:`DEFAULT_KNOWN_TOOLS` (env ``KNOWN_TOOLS``, comma list, overrides).
  Unknown tools are kept and flagged.

The table is merge-on-read; :func:`merge_agent_requests` writes it with one MERGE INTO.
Existing rows are read (and matched) only inside a ``requested_at`` window around the
batch's event times (:data:`LOOKBACK`), so a run reads a few daily partitions, not the
whole table.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Mapping

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import table

SOURCE = "kafka:agent.events"
TABLE_NAME = "silver.agent_requests"
TARGET = table(TABLE_NAME)

DEFAULT_KNOWN_TOOLS = frozenset(
    {"get_cfb_rankings", "get_cfb_scores", "get_team_schedule", "get_team_analytics", "get_game_summary"}
)

REQUEST_STARTED = "request_started"
TOOL_CALLED = "tool_called"
REQUEST_COMPLETED = "request_completed"
REQUEST_FAILED = "request_failed"
EVENT_TYPES = (REQUEST_STARTED, TOOL_CALLED, REQUEST_COMPLETED, REQUEST_FAILED)
TERMINAL = {REQUEST_COMPLETED: "completed", REQUEST_FAILED: "failed"}
IN_FLIGHT = "in_flight"

# A request's events all fall within seconds of each other. Stored rows are looked up
# (and MERGE-matched) only with requested_at within this distance of the batch's event
# times, which lets Iceberg prune the days(requested_at) partitions it reads.
LOOKBACK = timedelta(days=2)

REASON_UNPARSEABLE = "unparseable"
# from_json is permissive: malformed JSON sets this field instead of failing.
CORRUPT = "_corrupt_record"
REASON_MISSING_REQUIRED = "missing_required"
REASON_UNKNOWN_EVENT_TYPE = "unknown_event_type"

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("request_id", T.StringType()),
        T.StructField("session_id", T.StringType()),
        T.StructField("user_key", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("tool", T.StringType()),
        T.StructField("latency_ms", T.IntegerType()),
        T.StructField("status", T.StringType()),
        T.StructField("error_code", T.StringType()),
        T.StructField("model", T.StringType()),
        T.StructField("input_tokens", T.IntegerType()),
        T.StructField("output_tokens", T.IntegerType()),
        T.StructField("prompt_chars", T.IntegerType()),
        T.StructField("client", T.StringType()),
        T.StructField("app_version", T.StringType()),
    ]
)
_PARSE_SCHEMA = T.StructType(EVENT_SCHEMA.fields + [T.StructField(CORRUPT, T.StringType())])

# silver.agent_requests columns in table order (sa_lakehouse.tables).
REQUEST_COLUMNS = [
    "request_id", "session_id", "user_key", "requested_at", "completed_at", "status", "error_code",
    "latency_ms", "model", "input_tokens", "output_tokens", "prompt_chars", "tool_calls", "client",
    "app_version", "_last_event_at", "_kafka_partition", "_kafka_offset", "_ingested_at",
]
# Constant per request; taken from the earliest event that carries them.
_CONSTANT_FIELDS = ("session_id", "user_key", "prompt_chars", "client", "app_version")
# Only meaningful on the terminal event (tool_called reuses latency_ms/status/error_code).
_TERMINAL_FIELDS = ("latency_ms", "error_code", "model", "input_tokens", "output_tokens")

_CALL_FIELDS = ("tool", "started_at", "latency_ms", "status", "known_tool")


def known_tools(env: Mapping[str, str] | None = None) -> frozenset[str]:
    """The default tool list, or env ``KNOWN_TOOLS`` (comma separated) when set and non-empty."""
    env = os.environ if env is None else env
    names = frozenset(t.strip() for t in env.get("KNOWN_TOOLS", "").split(",") if t.strip())
    return names or DEFAULT_KNOWN_TOOLS


def parse_events(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Bronze rows -> (valid events, one per event_id; silver._rejects rows)."""
    m = bronze.select(
        "value_json", "_kafka_partition", "_kafka_offset", "_ingested_at",
        F.from_json("value_json", _PARSE_SCHEMA, {"columnNameOfCorruptRecord": CORRUPT}).alias("e"),
    )
    ev = m.select(
        "value_json", "_kafka_partition", "_kafka_offset", "_ingested_at",
        (F.col("e").isNull() | F.col(f"e.{CORRUPT}").isNotNull()).alias("_unparseable"),
        *[F.col(f"e.{f.name}").alias(f.name) for f in EVENT_SCHEMA.fields if f.name != "event_time"],
        F.to_timestamp("e.event_time").alias("event_time"),
    )
    reason = (
        F.when(F.col("_unparseable"), F.lit(REASON_UNPARSEABLE))
        .when(F.col("request_id").isNull() | F.col("event_time").isNull(), F.lit(REASON_MISSING_REQUIRED))
        .when(~F.col("event_type").isin(*EVENT_TYPES) | F.col("event_type").isNull(), F.lit(REASON_UNKNOWN_EVENT_TYPE))
    )
    ev = ev.withColumn("_reason", reason)

    rejects = ev.filter(F.col("_reason").isNotNull()).select(
        F.current_timestamp().alias("rejected_at"),
        F.lit(TABLE_NAME).alias("table_name"),
        F.col("_reason").alias("reason"),
        F.coalesce(
            F.col("request_id"),
            F.format_string("kafka:%d:%d", F.col("_kafka_partition"), F.col("_kafka_offset")),
        ).alias("record_key"),
        F.col("value_json").alias("raw"),
        F.lit(SOURCE).alias("_source"),
        F.lit(None).cast("timestamp").alias("_fetched_at"),
    )

    # A redelivered event keeps its first offset.
    event_key = F.coalesce(
        F.col("event_id"), F.format_string("kafka:%d:%d", F.col("_kafka_partition"), F.col("_kafka_offset"))
    )
    first = Window.partitionBy(event_key).orderBy("_kafka_partition", "_kafka_offset")
    valid = (
        ev.filter(F.col("_reason").isNull())
        .withColumn("_rn", F.row_number().over(first))
        .filter("_rn = 1")
        .drop("_rn", "_reason", "_unparseable", "value_json")
    )
    return valid, rejects


def _first_non_null(c: str, order: Column) -> Column:
    # min_by skips rows whose ordering value is null.
    return F.min_by(F.col(c), F.when(F.col(c).isNotNull(), order))


def aggregate_events(events: DataFrame) -> DataFrame:
    """One row per request_id from the batch's events alone (``b_*`` columns, no tool calls)."""
    order = F.struct("event_time", "_kafka_partition", "_kafka_offset")
    term_order = F.when(F.col("event_type").isin(*TERMINAL), order)
    terminal_status = F.when(F.col("event_type") == REQUEST_COMPLETED, F.lit(TERMINAL[REQUEST_COMPLETED])).when(
        F.col("event_type") == REQUEST_FAILED, F.lit(TERMINAL[REQUEST_FAILED])
    )
    aggs = [
        F.min(F.when(F.col("event_type") == REQUEST_STARTED, F.col("event_time"))).alias("b_started_at"),
        F.min("event_time").alias("b_earliest"),
        F.max("event_time").alias("b_last_event_at"),
        F.max_by(terminal_status, term_order).alias("b_status"),
        F.max_by(F.col("event_time"), term_order).alias("b_completed_at"),
        F.max_by(F.col("_kafka_partition"), order).alias("b_kafka_partition"),
        F.max_by(F.col("_kafka_offset"), order).alias("b_kafka_offset"),
        F.max("_ingested_at").alias("b_ingested_at"),
    ]
    aggs += [_first_non_null(c, order).alias(f"b_{c}") for c in _CONSTANT_FIELDS]
    aggs += [F.max_by(F.col(c), term_order).alias(f"b_{c}") for c in _TERMINAL_FIELDS]
    return events.groupBy("request_id").agg(*aggs)


def merge_tool_calls(events: DataFrame, stored: DataFrame, tools: frozenset[str]) -> DataFrame:
    """(request_id, tool_calls): stored calls plus the batch's tool_called events.

    Deduplicated on (tool, started_at, latency_ms, status), sorted by started_at.
    """
    new = events.filter(F.col("event_type") == TOOL_CALLED).select(
        "request_id",
        "tool",
        F.col("event_time").alias("started_at"),
        "latency_ms",
        "status",
        F.col("tool").isin(*sorted(tools)).alias("known_tool"),
    )
    old = stored.select("request_id", F.explode("tool_calls").alias("c")).select(
        "request_id", *[F.col(f"c.{f}").alias(f) for f in _CALL_FIELDS]
    )
    calls = new.unionByName(old).dropDuplicates(["request_id", "tool", "started_at", "latency_ms", "status"])
    # sort_array orders structs field by field, so started_at goes first, then back in table order.
    sorted_calls = F.sort_array(F.collect_list(F.struct("started_at", "tool", "latency_ms", "status", "known_tool")))
    return calls.groupBy("request_id").agg(
        F.transform(sorted_calls, lambda c: F.struct(*[c[f].alias(f) for f in _CALL_FIELDS])).alias("tool_calls")
    )


def assemble_requests(events: DataFrame, stored: DataFrame, tools: frozenset[str]) -> DataFrame:
    """Combine the batch's events with the stored rows of the same requests -> table rows."""
    batch = aggregate_events(events)
    calls = merge_tool_calls(events, stored, tools)
    s = stored.select(*[F.col(c).alias(f"s_{c}") for c in REQUEST_COLUMNS if c != "tool_calls"])
    j = batch.join(s, batch["request_id"] == s["s_request_id"], "left").join(calls, "request_id", "left")

    def keep(c: str) -> Column:
        return F.coalesce(F.col(f"b_{c}"), F.col(f"s_{c}")).alias(c)

    requested_at = F.coalesce(F.col("b_started_at"), F.least(F.col("s_requested_at"), F.col("b_earliest")))
    empty_calls = F.array().cast(stored.schema["tool_calls"].dataType)
    return j.select(
        F.col("request_id"),
        keep("session_id"),
        keep("user_key"),
        requested_at.alias("requested_at"),
        keep("completed_at"),
        F.coalesce(F.col("b_status"), F.col("s_status"), F.lit(IN_FLIGHT)).alias("status"),
        keep("error_code"),
        keep("latency_ms"),
        keep("model"),
        keep("input_tokens"),
        keep("output_tokens"),
        keep("prompt_chars"),
        F.coalesce(F.col("tool_calls"), empty_calls).alias("tool_calls"),
        keep("client"),
        keep("app_version"),
        F.greatest(F.col("b_last_event_at"), F.col("s__last_event_at")).alias("_last_event_at"),
        F.col("b_kafka_partition").alias("_kafka_partition"),
        F.col("b_kafka_offset").alias("_kafka_offset"),
        F.col("b_ingested_at").alias("_ingested_at"),
    )


def lookup_window(events: DataFrame) -> tuple[str, str] | None:
    """SQL timestamp literals (low, high) bounding ``requested_at`` of stored rows that can
    match this batch. Formatted in Spark (session time zone UTC), never in Python's local zone."""
    fmt = "yyyy-MM-dd HH:mm:ss.SSSSSS"
    secs = int(LOOKBACK.total_seconds())
    row = events.agg(
        F.date_format(F.min("event_time") - F.expr(f"INTERVAL {secs} SECONDS"), fmt).alias("lo"),
        F.date_format(F.max("event_time") + F.expr(f"INTERVAL {secs} SECONDS"), fmt).alias("hi"),
    ).first()
    if row is None or row["lo"] is None:
        return None
    return f"TIMESTAMP '{row['lo']}'", f"TIMESTAMP '{row['hi']}'"


def merge_agent_requests(spark, events: DataFrame, tools: frozenset[str] | None = None, target: str = TARGET) -> int:
    """Assemble and MERGE the batch's events into silver.agent_requests. Returns rows merged."""
    tools = known_tools() if tools is None else tools
    window = lookup_window(events)
    if window is None:
        return 0
    lo, hi = window
    in_window = f"requested_at >= {lo} AND requested_at <= {hi}"

    ids = events.select("request_id").distinct()
    stored = spark.table(target).where(in_window).join(ids, "request_id", "left_semi")
    rows = assemble_requests(events, stored, tools).cache()
    try:
        n = rows.count()
        if n == 0:
            return 0
        view = "_sa_drain_agent_requests"
        rows.createOrReplaceTempView(view)
        t_window = in_window.replace("requested_at", "t.requested_at")
        cols = ", ".join(REQUEST_COLUMNS)
        spark.sql(
            f"""
            MERGE INTO {target} t
            USING {view} s
            ON t.request_id = s.request_id AND {t_window}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({", ".join("s." + c for c in REQUEST_COLUMNS)})
            """
        )
        spark.catalog.dropTempView(view)
        return n
    finally:
        rows.unpersist()
