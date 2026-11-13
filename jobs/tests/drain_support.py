"""Test helpers for the drain: Kafka-shaped static DataFrames and a stand-in merge_games.

``merge_games`` here is a minimal stand-in with the exact signature of WS4a's
``sa_lakehouse.games.merge_games(spark, updates_df, mode)`` and the contract H5 rules, so
the drain's foreachBatch functions can be tested before WS4a merges. The lead replaces it
with the real function at integration (the drain imports ``sa_lakehouse.games`` lazily).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import types as T

FIXTURES = Path(__file__).parent / "fixtures"

KAFKA_SCHEMA = T.StructType(
    [
        T.StructField("key", T.BinaryType()),
        T.StructField("value", T.BinaryType()),
        T.StructField("topic", T.StringType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("offset", T.LongType()),
        T.StructField("timestamp", T.TimestampType()),
        T.StructField("timestampType", T.IntegerType()),
    ]
)

_TS = datetime(2026, 11, 13, 4, 0, tzinfo=timezone.utc)


def fixture_lines(name: str) -> list[str]:
    return [line for line in (FIXTURES / name).read_text().splitlines() if line.strip()]


def kafka_df(spark, topic: str, values: list, key_field: str | None = None, start_offset: int = 0, partition: int = 0):
    """A static DataFrame shaped like the Kafka source. ``values`` are dicts or raw strings."""
    rows = []
    for i, v in enumerate(values):
        raw = v if isinstance(v, str) else json.dumps(v, separators=(",", ":"))
        key = None
        if key_field:
            try:
                obj = json.loads(raw)
            except ValueError:
                obj = None
            if isinstance(obj, dict) and obj.get(key_field) is not None:
                key = str(obj[key_field]).encode()
        rows.append((key, raw.encode(), topic, partition, start_offset + i, _TS, 0))
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def empty_kafka_df(spark):
    return spark.createDataFrame([], KAFKA_SCHEMA)


_LIVE_SET = ["start_time_utc", "status", "period", "clock_seconds", "home_points", "away_points"]
_LIVE_FILL = ["home_conference", "away_conference", "neutral_site", "conference_game"]
_META = ["_source", "_fetched_at", "_ingested_at", "_record_hash"]


def merge_games(spark, updates_df, mode: str) -> None:
    """Stand-in for sa_lakehouse.games.merge_games (contract H5). Does not recompute the hash."""
    from sa_lakehouse.config import table

    cols = updates_df.columns
    if mode == "live":
        sets = [f"{c} = s.{c}" for c in _LIVE_SET] + [f"{c} = coalesce(t.{c}, s.{c})" for c in _LIVE_FILL]
    else:
        sets = [f"{c} = s.{c}" for c in cols if c not in _META and c != "game_id"]
    sets += [f"{c} = s.{c}" for c in _META]
    updates_df.createOrReplaceTempView("_stub_games_updates")
    spark.sql(
        f"""
        MERGE INTO {table("silver.games")} t
        USING (
          SELECT * FROM (
            SELECT *, row_number() OVER (PARTITION BY game_id ORDER BY _fetched_at DESC) AS _rn
            FROM _stub_games_updates) WHERE _rn = 1
        ) s
        ON t.game_id = s.game_id
        WHEN MATCHED
          AND NOT (s._record_hash <=> t._record_hash)
          AND s._fetched_at > t._fetched_at
          AND NOT (t.status = 'final' AND s.status <> 'final')
          THEN UPDATE SET {", ".join(sets)}
        WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}) VALUES ({", ".join("s." + c for c in cols)})
        """
    )
