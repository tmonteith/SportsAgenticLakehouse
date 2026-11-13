import json
from datetime import datetime, timezone

import pytest
from helpers import snapshots

import create_tables
import maintain
from sa_lakehouse.maintenance import statements
from sa_lakehouse.tables import TABLES


def _summary(capsys, event):
    return json.loads([l for l in capsys.readouterr().out.splitlines() if event in l][-1])


def test_create_tables_is_idempotent(clean_catalog, monkeypatch, capsys):
    monkeypatch.setenv("GCP_PROJECT", "test-proj")
    assert create_tables.main([], spark=clean_catalog) == 0
    assert set(_summary(capsys, "create_tables_summary")["created"]) == set(TABLES)
    assert create_tables.main([], spark=clean_catalog) == 0
    assert _summary(capsys, "create_tables_summary")["created"] == []


def test_statements():
    s = statements("silver.games", datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc))
    assert s[0] == "CALL sa_catalog.system.rewrite_data_files(table => 'silver.games')"
    assert "older_than => TIMESTAMP '2026-11-06 12:00:00', retain_last => 1" in s[2]
    assert "remove_orphan_files(table => 'silver.games', older_than => TIMESTAMP '2026-11-10 12:00:00')" in s[3]
    with pytest.raises(KeyError):
        statements("silver.nope", datetime.now(timezone.utc))


def test_dry_run_needs_no_spark(monkeypatch, capsys):
    monkeypatch.setenv("GCP_PROJECT", "test-proj")
    assert maintain.main(["--tables", "silver.games,gold.game_summaries", "--dry-run"]) == 0
    s = _summary(capsys, "maintain_summary")
    assert s["dry_run"] and set(s["tables"]) == {"silver.games", "gold.game_summaries"}
    assert len(s["tables"]["silver.games"]["statements"]) == 4


def test_unknown_table_is_a_usage_error(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "test-proj")
    with pytest.raises(SystemExit) as e:
        maintain.main(["--tables", "silver.nope", "--dry-run"])
    assert e.value.code == 2


def test_maintain_compacts_then_rerun_adds_no_snapshot(clean_catalog, monkeypatch, capsys):
    spark = clean_catalog
    monkeypatch.setenv("GCP_PROJECT", "test-proj")
    create_tables.main([], spark=spark)
    for i in range(6):  # six small files in one partition: enough for bin-pack compaction
        spark.sql(
            "INSERT INTO sa_catalog.silver._rejects VALUES "
            f"(TIMESTAMP '2026-11-12 0{i}:00:00', 'silver.plays', 'test', 'k{i}', '{{}}', 'cfbd:plays', NULL)"
        )
    spark.sql("DELETE FROM sa_catalog.silver.games WHERE game_id = 1")  # no-op on an empty table
    tables = ["silver._rejects", "silver.games", "gold.agent_usage_daily"]
    assert maintain.main(["--tables", *tables], spark=spark) == 0
    s = _summary(capsys, "maintain_summary")
    assert s["tables"]["silver._rejects"]["rewrite_data_files"]["rewritten_data_files_count"] == 6
    assert spark.table("sa_catalog.silver._rejects").count() == 6
    before = {t: snapshots(spark, t) for t in tables}
    assert maintain.main(["--tables", *tables], spark=spark) == 0
    assert {t: snapshots(spark, t) for t in tables} == before
