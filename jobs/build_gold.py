"""silver -> gold.team_efficiency_weekly, gold.game_summaries (per season), gold.agent_usage_daily (per day).

Usage: build_gold.py [--seasons 2024,2025 ...] [--days YYYY-MM-DD ...]

Default seasons: those whose silver rows changed since the last default build (watermark in the
``sa.build_gold.silver_watermark`` property of gold.game_summaries; every season on the first
build). Explicit ``--seasons`` rebuild exactly those and leave the watermark alone.
Default days: the last 3 UTC days. Exit codes: 0 ok, 1 unexpected error, 2 bad arguments.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date

from sa_lakehouse.config import build_spark, catalog_conf, settings_from_env, table
from sa_lakehouse.games import GAMES
from sa_lakehouse.gold import (
    AGENT_REQUESTS, GAME_SUMMARIES, GOLD_TABLES, WATERMARK, build_days, build_seasons, default_days, touched_seasons,
)
from sa_lakehouse.hashing import get_table_property, set_table_property
from sa_lakehouse.plays import PLAYS
from sa_lakehouse.tables import ensure_tables
from sa_lakehouse.teams import TEAMS


def _seasons(values):
    out = []
    for v in values or []:
        out += [int(x) for x in v.split(",") if x.strip()]
    return sorted(set(out))


def _days(values):
    out = []
    for v in values or []:
        out += [date.fromisoformat(x.strip()) for x in v.split(",") if x.strip()]
    return sorted(set(out))


def run(spark, catalog: str, seasons=None, days=None) -> dict:
    ensure_tables(spark, [GAMES, PLAYS, TEAMS, AGENT_REQUESTS, *GOLD_TABLES], catalog)
    summary = {"seasons_mode": "explicit" if seasons else "touched"}
    high = None
    if not seasons:
        seasons, high = touched_seasons(spark, catalog)
    days = days or default_days()
    summary["seasons"] = seasons
    summary["days"] = [d.isoformat() for d in days]
    summary["tables"] = {**build_seasons(spark, seasons, catalog), **build_days(spark, days, catalog)}
    target = table(GAME_SUMMARIES, catalog)
    if high is not None and get_table_property(spark, target, WATERMARK) != str(high):
        set_table_property(spark, target, WATERMARK, str(high))
    return summary


def main(argv=None, spark=None) -> int:
    ap = argparse.ArgumentParser(description="Build the gold tables from silver.")
    ap.add_argument("--seasons", nargs="*", help="seasons to rebuild (space or comma separated)")
    ap.add_argument("--days", nargs="*", help="UTC days (YYYY-MM-DD) to rebuild in agent_usage_daily")
    args = ap.parse_args(argv)
    try:
        seasons, days = _seasons(args.seasons), _days(args.days)
    except ValueError as exc:
        ap.error(str(exc))
    settings = settings_from_env()
    if spark is None:
        spark = build_spark("sa-build-gold", catalog_conf(settings))
    summary = run(spark, settings.catalog, seasons, days)
    print(json.dumps({"event": "build_gold_summary", **summary}, sort_keys=True, default=str), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
