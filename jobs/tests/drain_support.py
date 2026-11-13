"""Test helpers for the drain: Kafka-shaped static DataFrames and fixture records."""

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


