"""nightly.py (contract I5) and build_gold.py --tables."""

import json

import pytest
from helpers import fixture, ts, write_landing

import build_gold
import build_silver
import load_landing
import nightly


@pytest.fixture
def env(clean_catalog, tmp_path, monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "test-proj")
    path = str(tmp_path / "landing" / "cfb")
    write_landing(path, "teams", 2025, 0, ts("2025-09-01T03:29:00"), fixture("teams_2025.json"))
    write_landing(path, "games", 2025, 1, ts("2025-09-01T03:30:00"), fixture("games_2025_w01.json"))
    write_landing(path, "plays", 2025, 1, ts("2025-09-01T03:30:05"), fixture("plays_2025_w01.json"))
    return clean_catalog, "file://" + path


def events(capsys):
    out = {}
    for line in capsys.readouterr().out.splitlines():
        if line.startswith("{") and '"event"' in line:
            rec = json.loads(line)
            out[rec["event"]] = rec
    return out


def test_nightly_runs_three_steps_in_one_session(env, capsys):
    spark, root = env
    assert nightly.main(["--landing-root", root], spark=spark) == 0
    ev = events(capsys)
    assert set(ev) == {"load_landing_summary", "build_silver_summary", "build_gold_summary", "nightly_summary"}
    s = ev["nightly_summary"]
    assert s["status"] == "ok" and s["exit_code"] == 0
    assert [(x["step"], x["status"], x["exit_code"]) for x in s["steps"]] == [
        ("load_landing", "ok", 0), ("build_silver", "ok", 0), ("build_gold", "ok", 0)]
    assert ev["build_silver_summary"]["tables"]["silver.games"]["inserted"] == 4
    assert ev["build_gold_summary"]["seasons"] == [2025]
    assert spark.table("sa_catalog.gold.game_summaries").count() == 4

    # A rerun is idempotent: nothing new to load, build or rewrite.
    assert nightly.main(["--landing-root", root], spark=spark) == 0
    ev = events(capsys)
    assert ev["nightly_summary"]["status"] == "ok"
    assert ev["build_gold_summary"]["seasons"] == []


def test_nightly_stops_and_exits_non_zero_when_a_step_raises(env, capsys, monkeypatch):
    spark, root = env

    def boom(argv=None, spark=None):
        raise RuntimeError("catalog unavailable")

    monkeypatch.setattr(build_silver, "main", boom)
    called = []
    monkeypatch.setattr(build_gold, "main", lambda argv=None, spark=None: called.append(1) or 0)
    assert nightly.main(["--landing-root", root], spark=spark) == 1
    s = events(capsys)["nightly_summary"]
    assert s["status"] == "failed" and s["exit_code"] == 1
    assert [(x["step"], x["status"]) for x in s["steps"]] == [
        ("load_landing", "ok"), ("build_silver", "failed"), ("build_gold", "skipped")]
    assert s["steps"][1]["error"] == "RuntimeError: catalog unavailable"
    assert not called


def test_nightly_builds_gold_after_a_silver_quality_failure_then_exits_3(env, capsys, monkeypatch):
    spark, root = env
    monkeypatch.setattr(build_silver, "main", lambda argv=None, spark=None: build_silver.EXIT_QUALITY)
    called = []
    monkeypatch.setattr(build_gold, "main", lambda argv=None, spark=None: called.append(argv) or 0)
    assert nightly.main(["--landing-root", root], spark=spark) == 3
    s = events(capsys)["nightly_summary"]
    assert s["status"] == "quality_failed"
    assert [x["status"] for x in s["steps"]] == ["ok", "quality_failed", "ok"]
    assert called == [[]]


def test_nightly_reports_a_step_exit_code(env, capsys, monkeypatch):
    spark, root = env
    monkeypatch.setattr(load_landing, "main", lambda argv=None, spark=None: 2)
    assert nightly.main(["--landing-root", root], spark=spark) == 2
    s = events(capsys)["nightly_summary"]
    assert [x["status"] for x in s["steps"]] == ["failed", "skipped", "skipped"]


def test_build_gold_tables_builds_only_the_listed_tables(env, capsys):
    spark, root = env
    assert load_landing.main(["--landing-root", root], spark=spark) == 0
    assert build_silver.main([], spark=spark) == 0
    capsys.readouterr()

    # What gold_after_live submits; explicit seasons leave the watermark alone.
    assert build_gold.main(["--tables", "gold.game_summaries", "--seasons", "2025"], spark=spark) == 0
    s = events(capsys)["build_gold_summary"]
    assert s["gold_tables"] == ["gold.game_summaries"] and s["days"] == []
    assert list(s["tables"]) == ["gold.game_summaries"]
    assert spark.table("sa_catalog.gold.game_summaries").count() == 4
    assert spark.table("sa_catalog.gold.team_efficiency_weekly").count() == 0
    assert spark.table("sa_catalog.gold.agent_usage_daily").count() == 0

    # A partial default build does not advance the watermark, so the full build still sees 2025.
    assert build_gold.main(["--tables", "gold.game_summaries"], spark=spark) == 0
    assert events(capsys)["build_gold_summary"]["seasons"] == [2025]
    assert build_gold.main([], spark=spark) == 0
    s = events(capsys)["build_gold_summary"]
    assert s["seasons"] == [2025] and s["gold_tables"] == list(build_gold.GOLD_TABLES)
    assert spark.table("sa_catalog.gold.team_efficiency_weekly").count() > 0
    assert build_gold.main([], spark=spark) == 0
    assert events(capsys)["build_gold_summary"]["seasons"] == []


def test_build_gold_rejects_unknown_tables():
    with pytest.raises(SystemExit) as exc:
        build_gold.main(["--tables", "gold.nope"])
    assert exc.value.code == 2
