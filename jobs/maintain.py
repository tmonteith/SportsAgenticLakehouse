"""Iceberg maintenance for lakehouse tables.

Usage: maintain.py [--tables bronze.agent_events silver.games ...] [--dry-run]

Default: every table in sa_lakehouse.tables. ``--dry-run`` prints the CALL statements in the
summary and runs nothing. Exit codes: 0 ok, 1 unexpected error, 2 bad arguments.
"""

from __future__ import annotations

import argparse
import json
import sys

from sa_lakehouse.config import build_spark, catalog_conf, settings_from_env
from sa_lakehouse.maintenance import maintain
from sa_lakehouse.tables import TABLES


def main(argv=None, spark=None) -> int:
    ap = argparse.ArgumentParser(description="Compact, expire snapshots and remove orphan files.")
    ap.add_argument("--tables", nargs="*", help="<namespace>.<table> names (space or comma separated; default all)")
    ap.add_argument("--dry-run", action="store_true", help="print the statements, run nothing")
    args = ap.parse_args(argv)
    names = [n for v in args.tables or [] for n in v.split(",") if n.strip()]
    unknown = [n for n in names if n not in TABLES]
    if unknown:
        ap.error(f"unknown tables: {', '.join(unknown)}")
    settings = settings_from_env()
    if spark is None and not args.dry_run:
        spark = build_spark("sa-maintain", catalog_conf(settings))
    summary = maintain(spark, names, dry_run=args.dry_run, catalog=settings.catalog)
    print(json.dumps({"event": "maintain_summary", **summary}, sort_keys=True, default=str), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
