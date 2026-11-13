"""silver._rejects: rows that failed mapping, team resolution or a quality check (contract H4)."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .config import DEFAULT_CATALOG, table

REJECTS = "silver._rejects"
COLUMNS = ["rejected_at", "table_name", "reason", "record_key", "raw", "_source", "_fetched_at"]


def reject_rows(
    df: DataFrame,
    *,
    table_name: str,
    reason: Column | str,
    record_key: Column,
    raw: Column,
    source: str,
    fetched_at: Column | str = "_fetched_at",
) -> DataFrame:
    """Shape ``df`` into silver._rejects rows. ``source`` is the ``_source`` value (e.g. ``cfbd:plays``)."""
    return df.select(
        F.current_timestamp().alias("rejected_at"),
        F.lit(table_name).alias("table_name"),
        (F.lit(reason) if isinstance(reason, str) else reason).alias("reason"),
        record_key.cast("string").alias("record_key"),
        raw.cast("string").alias("raw"),
        F.lit(source).alias("_source"),
        (F.col(fetched_at) if isinstance(fetched_at, str) else fetched_at).cast("timestamp").alias("_fetched_at"),
    )


def count_by_reason(rejects: DataFrame) -> dict[str, int]:
    return {r["reason"]: r["count"] for r in rejects.groupBy("reason").count().collect()}


def write_rejects(spark, rejects: DataFrame, catalog: str = DEFAULT_CATALOG) -> int:
    """Append rejects; no write (and no snapshot) when there are none. Returns the count."""
    rejects = rejects.select(*COLUMNS).cache()
    try:
        n = rejects.count()
        if n:
            rejects.writeTo(table(REJECTS, catalog)).append()
        return n
    finally:
        rejects.unpersist()
