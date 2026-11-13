"""silver -> gold.team_efficiency_weekly, gold.game_summaries (per season), gold.agent_usage_daily (per day).

Usage: build_gold.py [--tables gold.game_summaries,...] [--seasons 2024,2025 ...] [--days YYYY-MM-DD ...]

Default tables: all three gold tables. Default seasons: those whose silver rows changed since the
last default build (watermark in the ``sa.build_gold.silver_watermark`` property of
gold.game_summaries; every season on the first build). Explicit ``--seasons`` rebuild exactly
those and leave the watermark alone; so does a ``--tables`` list without both per-season tables,
so the next default build still rebuilds the seasons it skipped.
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
    AGENT_REQUESTS, GAME_SUMMARIES, GOLD_TABLES, SEASON_TABLES, USAGE, WATERMARK, build_days, build_seasons,
    default_days, touched_seasons,
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


def _tables(values):
    names = [n.strip() for v in values or [] for n in v.split(",") if n.strip()]
    unknown = sorted(set(names) - set(GOLD_TABLES))
    if unknown:
        raise ValueError(f"unknown gold tables: {', '.join(unknown)} (known: {', '.join(GOLD_TABLES)})")
    return [t for t in GOLD_TABLES if t in names] or list(GOLD_TABLES)


def run(spark, catalog: str, seasons=None, days=None, tables=GOLD_TABLES) -> dict:
    ensure_tables(spark, [GAMES, PLAYS, TEAMS, AGENT_REQUESTS, *GOLD_TABLES], catalog)
    season_tables = [t for t in SEASON_TABLES if t in tables]
    summary = {"seasons_mode": "explicit" if seasons else "touched", "gold_tables": list(tables)}
    high = None
    if not seasons and season_tables:
        seasons, high = touched_seasons(spark, catalog)
    days = (days or default_days()) if USAGE in tables else []
    summary["seasons"] = (seasons or []) if season_tables else []
    summary["days"] = [d.isoformat() for d in days]
    summary["tables"] = {
        **build_seasons(spark, summary["seasons"], catalog, season_tables),
        **build_days(spark, days, catalog),
    }
    # The watermark covers both per-season tables; a partial default build leaves it alone.
    target = table(GAME_SUMMARIES, catalog)
    full = all(t in tables for t in SEASON_TABLES)
    if high is not None and full and get_table_property(spark, target, WATERMARK) != str(high):
        set_table_property(spark, target, WATERMARK, str(high))
    return summary


def main(argv=None, spark=None) -> int:
    ap = argparse.ArgumentParser(description="Build the gold tables from silver.")
    ap.add_argument("--seasons", nargs="*", help="seasons to rebuild (space or comma separated)")
    ap.add_argument("--days", nargs="*", help="UTC days (YYYY-MM-DD) to rebuild in agent_usage_daily")
    ap.add_argument("--tables", nargs="*", help="gold tables to build (space or comma separated; default all)")
    args = ap.parse_args(argv)
    try:
        seasons, days, tables = _seasons(args.seasons), _days(args.days), _tables(args.tables)
    except ValueError as exc:
        ap.error(str(exc))
    settings = settings_from_env()
    if spark is None:
        spark = build_spark("sa-build-gold", catalog_conf(settings))
    summary = run(spark, settings.catalog, seasons, days, tables)
    print(json.dumps({"event": "build_gold_summary", **summary}, sort_keys=True, default=str), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
