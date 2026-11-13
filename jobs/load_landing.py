"""New landing files -> bronze.api_responses_raw and bronze._load_state.

Usage: load_landing.py [--since YYYY-MM-DD | --reload] [--landing-root URI]

``--since`` loads only objects fetched on or after that UTC date (from the object name;
the landing layout has no date partition). ``--landing-root`` defaults to
``gs://<GCP_PROJECT>-sa-landing/cfb``. ``--reload`` deletes every row of the two bronze landing
tables first and loads all landing files again. Exit codes: 0 ok, 1 unexpected error, 2 bad arguments.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date

from sa_lakehouse.config import build_spark, catalog_conf, settings_from_env
from sa_lakehouse.landing import default_landing_root, load_landing


def main(argv=None, spark=None) -> int:
    ap = argparse.ArgumentParser(description="Load new landing files into bronze.")
    ap.add_argument("--since", type=date.fromisoformat, help="only objects fetched on/after this UTC date")
    ap.add_argument("--landing-root", help="landing root URI (default gs://<proj>-sa-landing/cfb)")
    ap.add_argument("--reload", action="store_true", help="delete both bronze landing tables' rows first, then load every file")
    args = ap.parse_args(argv)
    if args.reload and args.since:
        ap.error("--reload loads every file; it cannot be combined with --since")
    settings = settings_from_env()
    if spark is None:
        spark = build_spark("sa-load-landing", catalog_conf(settings))
    root = args.landing_root or default_landing_root(settings)
    summary = load_landing(spark, root, since=args.since, catalog=settings.catalog, reload=args.reload)
    print(json.dumps({"event": "load_landing_summary", **summary}, sort_keys=True, default=str), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
