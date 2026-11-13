"""Create every lakehouse Iceberg table that does not exist yet (idempotent).

The namespaces bronze, silver and gold already exist in the real catalog (OpenTofu).
Exit codes: 0 ok, 1 unexpected error, 2 bad arguments.
"""

from __future__ import annotations

import argparse
import json
import sys

from sa_lakehouse.config import build_spark, catalog_conf, settings_from_env
from sa_lakehouse.tables import TABLES, ensure_tables


def main(argv=None, spark=None) -> int:
    ap = argparse.ArgumentParser(description="Create missing lakehouse tables.")
    ap.parse_args(argv)
    settings = settings_from_env()
    if spark is None:
        spark = build_spark("sa-create-tables", catalog_conf(settings))
    created = ensure_tables(spark, catalog=settings.catalog)
    summary = {"created": created, "existing": sorted(set(TABLES) - set(created))}
    print(json.dumps({"event": "create_tables_summary", **summary}, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
