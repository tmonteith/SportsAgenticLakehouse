"""``cfb.live_games`` messages -> ``silver.games`` rows (contract H5, live mapping).

Input is bronze-shaped rows (``bronze.live_game_events`` columns): the Kafka value as
``value_json`` plus ``_kafka_partition``, ``_kafka_offset`` and ``_ingested_at``. Each
message is ``{"game_id", "fetched_at", "payload"}`` with the CFBD ``/scoreboard`` game
object under ``payload``.

:func:`parse_live_games` returns the update rows (exactly the ``silver.games`` columns,
deduplicated to the latest ``_fetched_at`` per game) and the rows for ``silver._rejects``.
``merge_games(spark, updates, mode="live")`` (``sa_lakehouse.games``, WS4a) writes them.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .hashing import with_record_hash

SOURCE = "kafka:cfb.live_games"
TABLE_NAME = "silver.games"

# CFBD scoreboard status -> silver.games status. Anything else is rejected.
STATUS_MAP = {
    "scheduled": "scheduled",
    "in_progress": "in_progress",
    "completed": "final",
    "postponed": "postponed",
    "canceled": "canceled",
    "cancelled": "canceled",
}

# silver.games columns in table order (sa_lakehouse.tables).
GAMES_COLUMNS = [
    "game_id", "season", "week", "season_type", "start_time_utc", "status", "period", "clock_seconds",
    "home_team_id", "away_team_id", "home_conference", "away_conference", "home_points", "away_points",
    "neutral_site", "conference_game", "venue_id", "attendance",
    "_source", "_fetched_at", "_ingested_at", "_record_hash",
]
BUSINESS_COLUMNS = [c for c in GAMES_COLUMNS if not c.startswith("_")]

# Reject reasons written to silver._rejects.reason.
REASON_UNPARSEABLE = "unparseable"
# from_json is permissive: malformed JSON sets this field instead of failing.
CORRUPT = "_corrupt_record"
REASON_MISSING_REQUIRED = "missing_required"
REASON_UNKNOWN_STATUS = "unknown_status"

_TEAM = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("conference", T.StringType()),
        T.StructField("points", T.IntegerType()),
    ]
)
MESSAGE_SCHEMA = T.StructType(
    [
        T.StructField("game_id", T.LongType()),
        T.StructField("fetched_at", T.StringType()),
        T.StructField(
            "payload",
            T.StructType(
                [
                    T.StructField("startDate", T.StringType()),
                    T.StructField("status", T.StringType()),
                    T.StructField("period", T.IntegerType()),
                    T.StructField("clock", T.StringType()),
                    T.StructField("neutralSite", T.BooleanType()),
                    T.StructField("conferenceGame", T.BooleanType()),
                    T.StructField("homeTeam", _TEAM),
                    T.StructField("awayTeam", _TEAM),
                ]
            ),
        ),
    ]
)
_PARSE_SCHEMA = T.StructType(MESSAGE_SCHEMA.fields + [T.StructField(CORRUPT, T.StringType())])

def clock_seconds(clock: Column) -> Column:
    """``"MM:SS"`` -> seconds; null when absent or not in that form."""
    pattern = r"^(\d{1,2}):(\d{2})$"
    return F.when(
        clock.rlike(pattern),
        F.regexp_extract(clock, pattern, 1).cast("int") * 60 + F.regexp_extract(clock, pattern, 2).cast("int"),
    )


def season_from_start(start: Column) -> Column:
    """The payload has no season: UTC year of ``startDate`` if month >= 6, else year - 1."""
    return F.when(F.month(start) >= 6, F.year(start)).otherwise(F.year(start) - 1)


def _status_expr(raw: Column) -> Column:
    expr = None
    for src, dst in STATUS_MAP.items():
        expr = (F.when(raw == src, F.lit(dst))) if expr is None else expr.when(raw == src, F.lit(dst))
    return expr


def parse_live_games(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Map bronze rows to (silver.games updates, silver._rejects rows)."""
    m = bronze.select(
        "value_json", "_kafka_partition", "_kafka_offset", "_ingested_at",
        F.from_json("value_json", _PARSE_SCHEMA, {"columnNameOfCorruptRecord": CORRUPT}).alias("m"),
    )
    p = F.col("m.payload")
    raw_status = F.lower(F.trim(p["status"]))
    start = F.to_timestamp(p["startDate"])
    rows = m.select(
        "value_json", "_kafka_partition", "_kafka_offset",
        (F.col("m").isNull() | F.col(f"m.{CORRUPT}").isNotNull()).alias("_unparseable"),
        F.col("m.game_id").alias("game_id"),
        season_from_start(start).alias("season"),
        F.lit(None).cast("int").alias("week"),
        F.lit(None).cast("string").alias("season_type"),
        start.alias("start_time_utc"),
        _status_expr(raw_status).alias("status"),
        p["period"].alias("period"),
        clock_seconds(p["clock"]).alias("clock_seconds"),
        p["homeTeam"]["id"].cast("string").alias("home_team_id"),
        p["awayTeam"]["id"].cast("string").alias("away_team_id"),
        p["homeTeam"]["conference"].alias("home_conference"),
        p["awayTeam"]["conference"].alias("away_conference"),
        p["homeTeam"]["points"].alias("home_points"),
        p["awayTeam"]["points"].alias("away_points"),
        p["neutralSite"].alias("neutral_site"),
        p["conferenceGame"].alias("conference_game"),
        F.lit(None).cast("bigint").alias("venue_id"),
        F.lit(None).cast("int").alias("attendance"),
        F.lit(SOURCE).alias("_source"),
        F.to_timestamp("m.fetched_at").alias("_fetched_at"),
        F.col("_ingested_at"),
    )

    missing = (
        F.col("game_id").isNull() | F.col("season").isNull() | F.col("_fetched_at").isNull()
        | F.col("home_team_id").isNull() | F.col("away_team_id").isNull()
    )
    reason = (
        F.when(F.col("_unparseable"), F.lit(REASON_UNPARSEABLE))
        .when(missing, F.lit(REASON_MISSING_REQUIRED))
        .when(F.col("status").isNull(), F.lit(REASON_UNKNOWN_STATUS))
    )
    rows = rows.withColumn("_reason", reason)

    rejects = rows.filter(F.col("_reason").isNotNull()).select(
        F.current_timestamp().alias("rejected_at"),
        F.lit(TABLE_NAME).alias("table_name"),
        F.col("_reason").alias("reason"),
        F.coalesce(
            F.col("game_id").cast("string"),
            F.format_string("kafka:%d:%d", F.col("_kafka_partition"), F.col("_kafka_offset")),
        ).alias("record_key"),
        F.col("value_json").alias("raw"),
        F.lit(SOURCE).alias("_source"),
        F.col("_fetched_at"),
    )

    valid = rows.filter(F.col("_reason").isNull())
    latest = Window.partitionBy("game_id").orderBy(F.col("_fetched_at").desc(), F.col("_kafka_offset").desc())
    latest_rows = (
        valid.withColumn("_rn", F.row_number().over(latest))
        .filter("_rn = 1")
        .select(*[c for c in GAMES_COLUMNS if c != "_record_hash"])
    )
    updates = with_record_hash(latest_rows).select(*GAMES_COLUMNS)
    return updates, rejects
