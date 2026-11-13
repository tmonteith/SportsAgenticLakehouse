"""Test helpers: landing files built from the trimmed fixtures, snapshot counting."""

from __future__ import annotations

import copy
import gzip
import json
import os
from datetime import datetime, timezone

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def fixture(name: str):
    with open(os.path.join(FIXTURES, name)) as fh:
        return copy.deepcopy(json.load(fh))


def write_landing(root: str, endpoint: str, season: int, week: int, fetched_at: datetime, payload) -> str:
    """Write one landing object the way the extractor names it; returns its path."""
    name = fetched_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    d = os.path.join(root, endpoint, f"season={season:04d}", f"week={week:02d}")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, f"{name}.json.gz")
    with gzip.open(path, "wt") as fh:
        json.dump(payload, fh)
    return path


def ts(s: str) -> datetime:
    """'2025-09-01T03:30:00' -> aware UTC datetime."""
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def snapshots(spark, name: str) -> int:
    return spark.sql(f"SELECT count(*) FROM sa_catalog.{name}.snapshots").collect()[0][0]


def all_snapshots(spark, names) -> dict:
    return {n: snapshots(spark, n) for n in names}
