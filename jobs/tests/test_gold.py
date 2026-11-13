"""Gold math on a small hand-checked example.

Game 1 (week 3, final): A 14 - B 7.  Game 2 (week 3, final): A 3 - C 0.  Game 3 (week 4): scheduled.

A's offense, game 1 (scrimmage unless noted):
  1 rush  d1&10 +6   epa  0.5  success  drive a1
  2 pass  d2&4  +2   epa -0.2  fail     a1   (needs 2.8)
  3 sack  d3&2  -5   epa -1.0  fail     a1
  4 punt              (not scrimmage)    a1
  5 Passing Touchdown d3&50 +40 scoring epa 3.0 success (touchdown) a2
  6 rush  d2&10 +7   epa null success   a2
B's offense, game 1:
  7 rush  d1&10 +2   epa -0.3 fail      b1
  8 Interception d2&8 +0 epa -2.0 fail turnover b1
  9 pass  d3&5  +5   epa  1.2 success   b2
A's offense, game 2:
 10 rush  d1&10 +10  epa  1.0 success   a3
"""

import json
from datetime import date

import pytest
from helpers import all_snapshots, ts

import build_gold
from sa_lakehouse.gold import GOLD_TABLES
from sa_lakehouse.tables import ensure_tables


def put(spark, name, rows):
    schema = spark.table(f"sa_catalog.{name}").schema
    spark.createDataFrame([tuple(r.get(c) for c in schema.names) for r in rows], schema).writeTo(
        f"sa_catalog.{name}"
    ).append()


def game(gid, home, away, hp, ap, week=3, status="final"):
    return dict(game_id=gid, season=2025, week=week, season_type="regular", start_time_utc=ts("2025-09-13T16:00:00"),
                status=status, home_team_id=home, away_team_id=away, home_points=hp, away_points=ap,
                _source="cfbd:games", _fetched_at=ts("2025-09-14T03:30:00"), _ingested_at=ts("2025-09-14T03:40:00"))


def play(n, gid, off, dfn, ptype, raw, down, dist, gained, epa, drive, scoring=False, turnover=False):
    return dict(play_id=f"p{n}", game_id=gid, drive_id=drive, season=2025, week=3, play_sequence=n,
                offense_team_id=off, defense_team_id=dfn, down=down, distance=dist, play_type=ptype,
                play_type_raw=raw, yards_gained=gained, is_scoring=scoring, is_turnover=turnover, source_epa=epa,
                _source="cfbd:plays", _fetched_at=ts("2025-09-14T03:30:00"), _ingested_at=ts("2025-09-14T03:40:00"))


def tool(name, status, ms, known=True):
    return {"tool": name, "started_at": ts("2026-11-12T10:00:00"), "latency_ms": ms, "status": status, "known_tool": known}


def request(rid, at, status, ms, calls):
    return dict(request_id=rid, session_id="00000000-0000-4000-8000-000000000000", user_key="made-up-key",
                requested_at=ts(at), status=status, latency_ms=ms, tool_calls=calls, _last_event_at=ts(at))


@pytest.fixture
def silver(clean_catalog, monkeypatch):
    spark = clean_catalog
    monkeypatch.setenv("GCP_PROJECT", "test-proj")
    ensure_tables(spark)
    put(spark, "silver.games", [game(1, "A", "B", 14, 7), game(2, "A", "C", 3, 0), game(3, "B", "C", None, None, 4, "scheduled")])
    put(spark, "silver.teams", [dict(team_id=t, display_name=f"Team {t}", _fetched_at=ts("2025-09-01T00:00:00")) for t in "ABC"])
    put(spark, "silver.plays", [
        play(1, 1, "A", "B", "rush", "Rush", 1, 10, 6, 0.5, "a1"),
        play(2, 1, "A", "B", "pass", "Pass Reception", 2, 4, 2, -0.2, "a1"),
        play(3, 1, "A", "B", "sack", "Sack", 3, 2, -5, -1.0, "a1"),
        play(4, 1, "A", "B", "punt", "Punt", 4, 8, 0, None, "a1"),
        play(5, 1, "A", "B", "pass", "Passing Touchdown", 3, 50, 40, 3.0, "a2", scoring=True),
        play(6, 1, "A", "B", "rush", "Rush", 2, 10, 7, None, "a2"),
        play(7, 1, "B", "A", "rush", "Rush", 1, 10, 2, -0.3, "b1"),
        play(8, 1, "B", "A", "pass", "Interception", 2, 8, 0, -2.0, "b1", turnover=True),
        play(9, 1, "B", "A", "pass", "Pass Reception", 3, 5, 5, 1.2, "b2"),
        play(10, 2, "A", "C", "rush", "Rush", 1, 10, 10, 1.0, "a3"),
    ])
    put(spark, "silver.agent_requests", [
        request("r1", "2026-11-12T10:00:00", "completed", 1000,
                [tool("get_cfb_rankings", "ok", 200), tool("get_team_schedule", "ok", 400)]),
        request("r2", "2026-11-12T11:00:00", "failed", 3000, [tool("get_cfb_rankings", "error", 600)]),
        request("r3", "2026-11-12T12:00:00", "completed", 2000, [tool("mystery_tool", "ok", 100, known=False)]),
        request("r4", "2026-11-11T23:59:59", "completed", 500, None),
    ])
    return spark


def run(spark, capsys, *args):
    assert build_gold.main(list(args), spark=spark) == 0
    out = [l for l in capsys.readouterr().out.splitlines() if "build_gold_summary" in l][-1]
    return json.loads(out)


DAYS = ("--days", "2026-11-11", "2026-11-12")


def test_team_efficiency_weekly(silver, capsys):
    spark = silver
    run(spark, capsys, "--seasons", "2025", *DAYS)
    rows = {(r.team_id, r.week): r for r in spark.table("sa_catalog.gold.team_efficiency_weekly").collect()}
    assert set(rows) == {("A", 3), ("B", 3), ("C", 3)}  # the scheduled week-4 game is not counted
    a = rows[("A", 3)]
    assert (a.games, a.season_type, a.off_plays, a.off_epa_plays, a.off_successes, a.off_drives, a.off_points) == (
        2, "regular", 6, 5, 4, 3, 17)
    assert a.off_epa_sum == pytest.approx(3.3) and a.off_success_epa_sum == pytest.approx(4.5)
    assert a.off_epa_per_play == pytest.approx(3.3 / 5) and a.off_success_rate == pytest.approx(4 / 6)
    assert a.off_explosiveness == pytest.approx(4.5 / 4) and a.off_points_per_drive == pytest.approx(17 / 3)
    assert (a.def_plays, a.def_epa_plays, a.def_successes, a.def_drives, a.def_points) == (3, 3, 1, 2, 7)
    assert a.def_epa_sum == pytest.approx(-1.1) and a.def_epa_per_play == pytest.approx(-1.1 / 3)
    assert a.def_explosiveness == pytest.approx(1.2) and a.def_points_per_drive == pytest.approx(3.5)
    b = rows[("B", 3)]
    assert (b.off_plays, b.off_points, b.def_plays, b.def_points, b.def_drives) == (3, 7, 5, 14, 2)
    c = rows[("C", 3)]
    assert (c.off_plays, c.off_epa_per_play, c.off_points_per_drive, c.def_points) == (0, None, None, 3)


def test_game_summaries(silver, capsys):
    spark = silver
    run(spark, capsys, "--seasons", "2025", *DAYS)
    g = {r.game_id: r for r in spark.table("sa_catalog.gold.game_summaries").collect()}
    one = g[1]
    assert (one.home_team_name, one.away_team_name, one.winner_team_id) == ("Team A", "Team B", "A")
    assert (one.home_plays, one.away_plays, one.home_yards, one.away_yards) == (5, 3, 50, 7)
    assert one.home_epa_per_play == pytest.approx(2.3 / 4) and one.away_epa_per_play == pytest.approx(-1.1 / 3)
    assert one.home_success_rate == pytest.approx(0.6) and one.away_success_rate == pytest.approx(1 / 3)
    assert (one.home_turnovers, one.away_turnovers) == (0, 1)
    three = g[3]
    assert three.winner_team_id is None and three.home_plays is None and three.status == "scheduled"


def test_agent_usage_daily(silver, capsys):
    spark = silver
    run(spark, capsys, "--seasons", "2025", *DAYS)
    tbl = spark.table("sa_catalog.gold.agent_usage_daily")
    assert not {"user_key", "session_id", "request_id"} & set(tbl.columns)
    rows = {(r.day, r.tool): r for r in tbl.collect()}
    d12, d11 = date(2026, 11, 12), date(2026, 11, 11)
    got = lambda k: tuple(rows[k][c] for c in ("requests", "calls", "errors", "p50_latency_ms", "p95_latency_ms", "unknown_tool"))  # noqa: E731
    assert got((d12, "get_cfb_rankings")) == (2, 2, 1, 400, 580, False)
    assert got((d12, "get_team_schedule")) == (1, 1, 0, 400, 400, False)
    assert got((d12, "mystery_tool")) == (1, 1, 0, 100, 100, True)
    assert got((d12, "_all")) == (3, 4, 1, 2000, 2900, False)
    assert got((d11, "_all")) == (1, 0, 0, 500, 500, False)
    assert len(rows) == 5


def test_rebuild_unchanged_adds_no_snapshot_and_touched_seasons(silver, capsys):
    spark = silver
    s = run(spark, capsys, *DAYS)  # first default build: every season
    assert s["seasons"] == [2025] and s["seasons_mode"] == "touched"
    before = all_snapshots(spark, GOLD_TABLES)
    assert run(spark, capsys, *DAYS)["seasons"] == []
    assert run(spark, capsys, "--seasons", "2025", *DAYS)["tables"]["gold.game_summaries"] == {
        "inserted": 0, "updated": 0, "deleted": 0}
    assert all_snapshots(spark, GOLD_TABLES) == before
    # A silver change in 2025 makes 2025 touched; only the changed rows are rewritten.
    spark.sql("UPDATE sa_catalog.silver.games SET home_points = 21, _ingested_at = current_timestamp() WHERE game_id = 1")
    s = run(spark, capsys, *DAYS)
    assert s["seasons"] == [2025]
    assert s["tables"]["gold.game_summaries"] == {"inserted": 0, "updated": 1, "deleted": 0}
    assert s["tables"]["gold.team_efficiency_weekly"]["updated"] == 2  # A and B
    assert all_snapshots(spark, ["gold.agent_usage_daily"]) == {"gold.agent_usage_daily": before["gold.agent_usage_daily"]}
    # Replacing a day removes rows that are no longer produced.
    spark.sql("DELETE FROM sa_catalog.silver.agent_requests WHERE request_id = 'r3'")
    s = run(spark, capsys, "--seasons", "2025", *DAYS)
    assert s["tables"]["gold.agent_usage_daily"]["deleted"] == 1
