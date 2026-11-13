"""bronze.api_responses_raw -> silver.games, silver.plays, silver.teams (+ silver._rejects).

Only bronze rows not yet built are read: each silver table keeps a watermark (the highest
bronze ``_ingested_at`` it has consumed, in microseconds) in its Iceberg table property
``sa.build_silver.bronze_watermark``. A rerun with no new bronze rows reads no payloads and
writes nothing. Steps run in order games -> plays (plays resolve teams through silver.games)
-> teams; each step writes its rows, then its rejects, then advances its watermark.

Exit codes: 0 ok, 3 a quality check failed on more than 1% of a table's input rows (data and
rejects are written first), 1 unexpected error, 2 bad arguments.
"""

from __future__ import annotations

import argparse
import json
import sys

from pyspark.sql import functions as F

from sa_lakehouse.config import build_spark, catalog_conf, settings_from_env, table
from sa_lakehouse.games import GAMES, build_games
from sa_lakehouse.hashing import get_table_property, set_table_property
from sa_lakehouse.landing import RAW
from sa_lakehouse.plays import PLAYS, build_plays
from sa_lakehouse.quality import evaluate
from sa_lakehouse.rejects import REJECTS, count_by_reason, write_rejects
from sa_lakehouse.tables import ensure_tables
from sa_lakehouse.teams import TEAMS, build_teams

WATERMARK = "sa.build_silver.bronze_watermark"
STEPS = (("games", GAMES, build_games), ("plays", PLAYS, build_plays), ("teams", TEAMS, build_teams))
EXIT_QUALITY = 3


def run(spark, catalog: str) -> tuple[dict, int]:
    ensure_tables(spark, [RAW, GAMES, PLAYS, TEAMS, REJECTS], catalog)
    raw = spark.table(table(RAW, catalog))
    summary = {"tables": {}, "touched_seasons": [], "over_threshold": []}
    touched: set[int] = set()

    for endpoint, name, build in STEPS:
        target = table(name, catalog)
        watermark = get_table_property(spark, target, WATERMARK)
        new = raw.where(F.col("endpoint") == endpoint)
        if watermark:
            new = new.where(F.col("_ingested_at") > F.expr(f"timestamp_micros({int(watermark)})"))
        stats = new.agg(
            F.count("*").alias("files"), F.max(F.expr("unix_micros(_ingested_at)")).alias("high")
        ).collect()[0]
        step = {"bronze_files": stats.files}
        summary["tables"][name] = step
        if not stats.files:
            continue

        result, rejects = build(spark, new, catalog)
        reasons = count_by_reason(rejects)
        step.update(result)
        step["rejects"] = write_rejects(spark, rejects, catalog)
        quality = evaluate(result["input_rows"], reasons)
        step["checks"] = quality["checks"]
        summary["over_threshold"] += [f"{name}:{r}" for r in quality["over_threshold"]]
        touched.update(result.get("touched_seasons", []))
        set_table_property(spark, target, WATERMARK, str(stats.high))

    summary["touched_seasons"] = sorted(touched)
    summary["status"] = "quality_failed" if summary["over_threshold"] else "ok"
    return summary, EXIT_QUALITY if summary["over_threshold"] else 0


def main(argv=None, spark=None) -> int:
    ap = argparse.ArgumentParser(description="Build silver games, plays and teams from bronze.")
    ap.parse_args(argv)
    settings = settings_from_env()
    if spark is None:
        spark = build_spark("sa-build-silver", catalog_conf(settings))
    summary, code = run(spark, settings.catalog)
    print(json.dumps({"event": "build_silver_summary", **summary}, sort_keys=True, default=str), flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
