"""silver.games: batch mapping from ``/games`` and the one writer, :func:`merge_games` (contract H5).

``merge_games`` is shared with the drain (WS4b, ``mode='live'``). Rules:

- dedupe the updates per ``game_id``, keeping the latest ``_fetched_at``;
- a matched row is updated only when the update is newer (``s._fetched_at > t._fetched_at``),
  would change the row (merged ``_record_hash`` differs), and does not move a ``final`` game to
  a non-final status;
- ``mode='batch'`` sets every column; ``mode='live'`` sets ``start_time_utc, status, period,
  clock_seconds, home_points, away_points``, fills ``home_conference, away_conference,
  neutral_site, conference_game`` only where the target is null, and keeps every other column;
- inserts take the update as is; ``_record_hash`` is recomputed on the merged row.

The rules sit in the MERGE's WHEN condition, and a pre-check with the same condition skips
the MERGE (and the snapshot) when no row qualifies.
"""

from __future__ import annotations

import itertools

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import DEFAULT_CATALOG, table
from .hashing import quote, record_hash_sql, with_record_hash
from .landing import explode_payload
from .quality import GAME_CHECKS, apply_checks
from .rejects import reject_rows

GAMES = "silver.games"
SOURCE = "cfbd:games"
MODES = ("batch", "live")
LIVE_SET = ("start_time_utc", "status", "period", "clock_seconds", "home_points", "away_points")
LIVE_FILL = ("home_conference", "away_conference", "neutral_site", "conference_game")

GAME_PAYLOAD = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("season", T.IntegerType()),
        T.StructField("week", T.IntegerType()),
        T.StructField("seasonType", T.StringType()),
        T.StructField("startDate", T.StringType()),
        T.StructField("completed", T.BooleanType()),
        T.StructField("neutralSite", T.BooleanType()),
        T.StructField("conferenceGame", T.BooleanType()),
        T.StructField("attendance", T.IntegerType()),
        T.StructField("venueId", T.LongType()),
        T.StructField("homeId", T.LongType()),
        T.StructField("homeTeam", T.StringType()),
        T.StructField("homeConference", T.StringType()),
        T.StructField("homePoints", T.IntegerType()),
        T.StructField("awayId", T.LongType()),
        T.StructField("awayTeam", T.StringType()),
        T.StructField("awayConference", T.StringType()),
        T.StructField("awayPoints", T.IntegerType()),
    ]
)

_view_ids = itertools.count()


def games_payload(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Explode ``/games`` bronze rows. Returns (records with an ``r`` struct + bronze columns, bad-file rejects)."""
    return explode_payload(bronze, GAME_PAYLOAD, table_name=GAMES, source=SOURCE)


def map_games(records: DataFrame) -> DataFrame:
    """``/games`` records -> silver.games columns (H5 batch mapping), plus ``__raw``."""
    g = F.col("r")
    return records.select(
        g["id"].alias("game_id"),
        g["season"].alias("season"),
        g["week"].alias("week"),
        g["seasonType"].alias("season_type"),
        F.to_timestamp(g["startDate"]).alias("start_time_utc"),
        F.when(g["completed"], F.lit("final")).otherwise(F.lit("scheduled")).alias("status"),
        F.lit(None).cast("int").alias("period"),
        F.lit(None).cast("int").alias("clock_seconds"),
        g["homeId"].cast("string").alias("home_team_id"),
        g["awayId"].cast("string").alias("away_team_id"),
        g["homeConference"].alias("home_conference"),
        g["awayConference"].alias("away_conference"),
        g["homePoints"].alias("home_points"),
        g["awayPoints"].alias("away_points"),
        g["neutralSite"].alias("neutral_site"),
        g["conferenceGame"].alias("conference_game"),
        g["venueId"].alias("venue_id"),
        g["attendance"].alias("attendance"),
        F.lit(SOURCE).alias("_source"),
        F.col("fetched_at").alias("_fetched_at"),
        F.lit(None).cast("timestamp").alias("_ingested_at"),
        F.lit(None).cast("string").alias("_record_hash"),
        F.to_json(g).alias("__raw"),
    )


def latest_per_game(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("game_id").orderBy(
        F.col("_fetched_at").desc(),
        F.when(F.col("status") == "final", 0).otherwise(1),
        F.col("_record_hash").desc_nulls_last(),
    )
    return df.withColumn("__rn", F.row_number().over(w)).where("__rn = 1").drop("__rn")


def build_games(spark, bronze: DataFrame, catalog: str = DEFAULT_CATALOG) -> tuple[dict, DataFrame]:
    """Bronze ``/games`` rows -> silver.games (batch). Returns (summary, rejects)."""
    records, bad = games_payload(bronze)
    mapped = apply_checks(latest_per_game(map_games(records)), GAME_CHECKS).cache()
    try:
        input_rows = mapped.count()
        good = mapped.where("__reason IS NULL").drop("__reason", "__raw")
        failed = mapped.where("__reason IS NOT NULL")
        rejects = reject_rows(
            failed,
            table_name=GAMES,
            reason=F.col("__reason"),
            record_key=F.col("game_id"),
            raw=F.col("__raw"),
            source=SOURCE,
        ).unionByName(bad)
        result = merge_games(spark, good, "batch", catalog)
        rejects = rejects.localCheckpoint()  # materialize before the cache goes away
    finally:
        mapped.unpersist()
    result["input_rows"] = input_rows
    return result, rejects


def merge_games(spark, updates_df: DataFrame, mode: str, catalog: str = DEFAULT_CATALOG) -> dict:
    """Merge ``updates_df`` (exactly the silver.games columns) into silver.games.

    ``_record_hash`` and ``_ingested_at`` in ``updates_df`` are ignored: the hash is recomputed
    and ``_ingested_at`` is the time of this write. Returns ``{"inserted", "updated",
    "touched_seasons"}``; no MERGE runs (no snapshot) when nothing qualifies.
    """
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, got {mode!r}")
    target = table(GAMES, catalog)
    schema = spark.table(target).schema
    cols = schema.names
    needed = [c for c in cols if c not in ("_record_hash", "_ingested_at")]
    missing = [c for c in needed if c not in updates_df.columns]
    if missing:
        raise ValueError(f"updates_df is missing silver.games columns: {missing}")

    src = updates_df.select(*[F.col(c).cast(schema[c].dataType).alias(c) for c in needed])
    src = with_record_hash(src.withColumn("_ingested_at", F.current_timestamp()))
    src = latest_per_game(src.select(*cols)).cache()
    view = f"__games_src_{next(_view_ids)}"
    src.createOrReplaceTempView(view)
    try:
        business = [f for f in schema.fields if not f.name.startswith("_")]
        if mode == "batch":
            merged = {f.name: (f"s.{quote(f.name)}", f.dataType) for f in business}
        else:
            merged = {}
            for f in business:
                c = quote(f.name)
                if f.name in LIVE_SET:
                    expr = f"s.{c}"
                elif f.name in LIVE_FILL:
                    expr = f"coalesce(t.{c}, s.{c})"
                else:
                    expr = f"t.{c}"
                merged[f.name] = (expr, f.dataType)
        merged_hash = record_hash_sql(merged)
        cond = (
            "s._fetched_at > t._fetched_at"
            " AND NOT (coalesce(t.status, '') = 'final' AND coalesce(s.status, '') <> 'final')"
            f" AND NOT ({merged_hash} <=> t._record_hash)"
        )
        pre = spark.sql(
            f"""
            SELECT
              coalesce(sum(CASE WHEN t.game_id IS NULL THEN 1 ELSE 0 END), 0) AS inserted,
              coalesce(sum(CASE WHEN t.game_id IS NOT NULL THEN 1 ELSE 0 END), 0) AS updated,
              collect_set(s.season) AS seasons
            FROM {view} s LEFT JOIN {target} t ON t.game_id = s.game_id
            WHERE t.game_id IS NULL OR ({cond})
            """
        ).collect()[0]
        result = {"inserted": pre.inserted, "updated": pre.updated, "touched_seasons": sorted(pre.seasons)}
        if not (pre.inserted or pre.updated):
            return result

        sets = [f"t.{quote(n)} = {expr}" for n, (expr, _) in merged.items()]
        sets += [f"t.{c} = s.{c}" for c in ("_source", "_fetched_at", "_ingested_at")]
        sets.append(f"t._record_hash = {merged_hash}")
        spark.sql(
            f"""
            MERGE INTO {target} t
            USING {view} s
            ON t.game_id = s.game_id
            WHEN MATCHED AND {cond} THEN UPDATE SET {", ".join(sets)}
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        return result
    finally:
        spark.catalog.dropTempView(view)
        src.unpersist()
