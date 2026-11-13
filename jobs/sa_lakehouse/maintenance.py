"""Iceberg table maintenance (plan WS4a): compaction, delete-file rewrite, snapshot expiry, orphans.

For each table, in order: ``rewrite_data_files``, ``rewrite_position_delete_files``,
``expire_snapshots`` (older than 7 days, keeping at least the current one) and
``remove_orphan_files`` (older than 3 days, Iceberg's own safety default). Each procedure is a
no-op without work to do, so a rerun adds no snapshot.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .config import DEFAULT_CATALOG
from .tables import TABLES

EXPIRE_AFTER = timedelta(days=7)
ORPHAN_AFTER = timedelta(days=3)
PROCEDURES = ("rewrite_data_files", "rewrite_position_delete_files", "expire_snapshots", "remove_orphan_files")


def _ts(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime("TIMESTAMP '%Y-%m-%d %H:%M:%S'")


def statements(name: str, now: datetime, catalog: str = DEFAULT_CATALOG) -> list[str]:
    """The CALL statements for one ``<namespace>.<table>``."""
    if name not in TABLES:
        raise KeyError(f"unknown table {name!r}")
    call = f"CALL {catalog}.system"
    t = f"table => '{name}'"
    return [
        f"{call}.rewrite_data_files({t})",
        f"{call}.rewrite_position_delete_files({t})",
        f"{call}.expire_snapshots({t}, older_than => {_ts(now - EXPIRE_AFTER)}, retain_last => 1)",
        f"{call}.remove_orphan_files({t}, older_than => {_ts(now - ORPHAN_AFTER)})",
    ]


def maintain(spark, names=None, dry_run: bool = False, catalog: str = DEFAULT_CATALOG, now: datetime | None = None) -> dict:
    """Run (or with ``dry_run`` only list) maintenance for ``names`` (default: every table)."""
    now = now or datetime.now(timezone.utc)
    names = list(TABLES) if not names else list(names)
    summary = {"dry_run": dry_run, "tables": {}}
    for name in names:
        stmts = statements(name, now, catalog)
        if dry_run:
            summary["tables"][name] = {"statements": stmts}
            continue
        if not spark.catalog.tableExists(f"{catalog}.{name}"):
            summary["tables"][name] = {"skipped": "missing"}
            continue
        results = {}
        for proc, stmt in zip(PROCEDURES, stmts):
            rows = spark.sql(stmt).collect()
            results[proc] = _result(proc, rows)
        summary["tables"][name] = results
    return summary


def _result(proc: str, rows) -> dict:
    if proc == "remove_orphan_files":
        return {"orphan_files": len(rows)}
    if not rows:
        return {}
    return {k: v for k, v in rows[0].asDict().items() if isinstance(v, (int, float))}
