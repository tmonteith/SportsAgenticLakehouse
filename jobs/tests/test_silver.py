import json

import pytest
from helpers import all_snapshots, fixture, ts, write_landing

import build_silver
import load_landing
from sa_lakehouse import teams as teams_mod
from sa_lakehouse.plays import play_type_sql

SILVER = ["silver.games", "silver.plays", "silver.teams", "silver._rejects"]
RUTGERS_GAME, NORFOLK_GAME = 401752794, 401767327


@pytest.fixture
def env(clean_catalog, tmp_path, monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "test-proj")
    root = "file://" + str(tmp_path / "landing" / "cfb")
    return clean_catalog, root, str(tmp_path / "landing" / "cfb")


def load(spark, root):
    assert load_landing.main(["--landing-root", root], spark=spark) == 0


def build(spark, capsys):
    code = build_silver.main([], spark=spark)
    line = [l for l in capsys.readouterr().out.splitlines() if "build_silver_summary" in l][-1]
    return code, json.loads(line)


def land_week1(path):
    write_landing(path, "teams", 2025, 0, ts("2025-09-01T03:29:00"), fixture("teams_2025.json"))
    write_landing(path, "games", 2025, 1, ts("2025-09-01T03:30:00"), fixture("games_2025_w01.json"))
    write_landing(path, "plays", 2025, 1, ts("2025-09-01T03:30:05"), fixture("plays_2025_w01.json"))


def test_bronze_to_silver_and_rerun(env, capsys):
    spark, root, path = env
    land_week1(path)
    load(spark, root)
    code, s = build(spark, capsys)
    assert code == 0, s
    assert s["tables"]["silver.games"]["inserted"] == 4
    assert s["tables"]["silver.plays"]["inserted"] == 60
    assert s["tables"]["silver.teams"]["inserted"] == 8
    assert s["touched_seasons"] == [2025] and s["over_threshold"] == []
    assert spark.table("sa_catalog.silver._rejects").count() == 0

    g = spark.table("sa_catalog.silver.games").where(f"game_id = {RUTGERS_GAME}").collect()[0]
    assert (g.home_team_id, g.away_team_id, g.status, g.home_points, g.away_points) == ("164", "195", "final", 34, 31)
    assert g._source == "cfbd:games" and g.season_type == "regular"

    plays = spark.table("sa_catalog.silver.plays").where(f"game_id = {RUTGERS_GAME}").orderBy("play_sequence").collect()
    src = sorted((p for p in fixture("plays_2025_w01.json") if p["gameId"] == RUTGERS_GAME),
                 key=lambda p: (p["driveNumber"], p["playNumber"], p["id"]))
    assert [p.play_sequence for p in plays] == list(range(1, 31))
    assert [p.play_id for p in plays] == [p["id"] for p in src]
    first = {p["id"]: p for p in src}
    ids = {"Rutgers": "164", "Ohio": "195"}
    for p in plays:
        raw = first[p.play_id]
        assert p.offense_team_id == ids[raw["offense"]] and p.defense_team_id == ids[raw["defense"]]
        assert p.clock_seconds == raw["clock"]["minutes"] * 60 + raw["clock"]["seconds"]
        assert p.down == (raw["down"] or None)
        assert p.season == 2025 and p.week == 1 and p._record_hash
    kinds = {p.play_type_raw: p.play_type for p in spark.table("sa_catalog.silver.plays").collect()}
    assert kinds["Rush"] == "rush" and kinds["Sack"] == "sack" and kinds["Kickoff Return (Offense)"] == "kickoff"
    assert kinds["End Period"] == "administrative" and kinds["Field Goal Good"] == "field_goal"

    t = spark.table("sa_catalog.silver.teams").where("team_id = '164'").collect()[0]
    assert t.display_name == "Rutgers" and t.conference_by_season == {2025: "Big Ten"}
    assert "rutgers" in t.aliases and t.aliases == sorted(set(t.aliases))

    # Rerun with no new landing files: nothing written anywhere.
    before = all_snapshots(spark, SILVER + ["bronze.api_responses_raw", "bronze._load_state"])
    load(spark, root)
    code, s = build(spark, capsys)
    assert code == 0 and all(v["bronze_files"] == 0 for v in s["tables"].values())
    assert all_snapshots(spark, before) == before

    # Same content landed again later: bronze grows, silver does not change.
    write_landing(path, "games", 2025, 1, ts("2025-09-02T03:30:00"), fixture("games_2025_w01.json"))
    write_landing(path, "plays", 2025, 1, ts("2025-09-02T03:30:05"), fixture("plays_2025_w01.json"))
    write_landing(path, "teams", 2025, 0, ts("2025-09-02T03:29:00"), fixture("teams_2025.json"))
    before = all_snapshots(spark, SILVER)
    load(spark, root)
    code, s = build(spark, capsys)
    assert code == 0 and s["tables"]["silver.plays"]["bronze_files"] == 1
    assert all_snapshots(spark, SILVER) == before
    assert s["touched_seasons"] == []


def test_plays_are_replaced_per_game(env, capsys):
    spark, root, path = env
    land_week1(path)
    load(spark, root)
    build(spark, capsys)
    plays = fixture("plays_2025_w01.json")
    norfolk = [p for p in plays if p["gameId"] == NORFOLK_GAME]
    norfolk.sort(key=lambda p: (p["driveNumber"], p["playNumber"]))
    dropped = norfolk[0]["id"]
    norfolk = norfolk[1:]
    norfolk[-1]["yardsGained"] = 99
    write_landing(path, "plays", 2025, 1, ts("2025-09-03T03:30:05"), norfolk)  # only this game re-landed
    load(spark, root)
    code, s = build(spark, capsys)
    p = s["tables"]["silver.plays"]
    assert code == 0 and p["games"] == 1 and p["deleted"] == 1 and p["inserted"] == 0
    assert p["updated"] == 29  # every remaining play moved up one in play_sequence
    rows = spark.table("sa_catalog.silver.plays").where(f"game_id = {NORFOLK_GAME}").collect()
    assert len(rows) == 29 and dropped not in {r.play_id for r in rows}
    assert max(r.play_sequence for r in rows) == 29
    assert spark.table("sa_catalog.silver.plays").where(f"game_id = {RUTGERS_GAME}").count() == 30


def test_rejects_and_quality_gate(env, capsys):
    spark, root, path = env
    games = fixture("games_2025_w01.json")
    bad_game = dict(games[0], id=1, awayId=games[0]["homeId"])  # home == away
    plays = fixture("plays_2025_w01.json")
    orphan = dict(plays[0], id="x1", gameId=999)  # game not in silver.games
    stranger = dict(plays[1], id="x2", offense="Nobody U")  # name matches neither team
    bad_down = dict(plays[2], id="x3", down=7, playNumber=999)
    write_landing(path, "games", 2025, 1, ts("2025-09-01T03:30:00"), games + [bad_game])
    write_landing(path, "plays", 2025, 1, ts("2025-09-01T03:30:05"), plays + [orphan, stranger, bad_down])
    load(spark, root)
    code, s = build(spark, capsys)
    assert code == build_silver.EXIT_QUALITY
    assert s["status"] == "quality_failed"
    assert set(s["over_threshold"]) == {
        "silver.games:home_equals_away", "silver.plays:unknown_game", "silver.plays:unresolved_team",
        "silver.plays:down_out_of_range",
    }
    rej = {(r.table_name, r.reason, r.record_key) for r in spark.table("sa_catalog.silver._rejects").collect()}
    assert rej == {
        ("silver.games", "home_equals_away", "1"),
        ("silver.plays", "unknown_game", "x1"),
        ("silver.plays", "unresolved_team", "x2"),
        ("silver.plays", "down_out_of_range", "x3"),
    }
    # Data was still written; rejected rows stayed out.
    assert spark.table("sa_catalog.silver.games").count() == 4
    assert spark.table("sa_catalog.silver.plays").count() == 60
    raw = spark.table("sa_catalog.silver._rejects").where("record_key = 'x2'").collect()[0].raw
    assert json.loads(raw)["offense"] == "Nobody U"
    # Rejects are not appended again on a rerun.
    before = all_snapshots(spark, SILVER)
    assert build(spark, capsys)[0] == 0
    assert all_snapshots(spark, SILVER) == before


def test_teams_aliases_and_conference_by_season(env, capsys, monkeypatch):
    spark, root, path = env
    monkeypatch.setattr(teams_mod, "load_go_aliases", lambda: {"scarlet knights": "Rutgers", "bobcats": "Ohio"})
    t2025 = fixture("teams_2025.json")
    t2024 = [dict(t, conference="Old Conf") for t in fixture("teams_2025.json")]
    write_landing(path, "teams", 2024, 0, ts("2024-09-01T03:29:00"), t2024)
    write_landing(path, "teams", 2025, 0, ts("2025-08-01T03:29:00"), [dict(t, conference="Stale") for t in t2025])
    write_landing(path, "teams", 2025, 0, ts("2025-09-01T03:29:00"), t2025)  # latest 2025 landing wins
    load(spark, root)
    build(spark, capsys)
    t = spark.table("sa_catalog.silver.teams").where("team_id = '164'").collect()[0]
    assert t.conference_by_season == {2024: "Old Conf", 2025: "Big Ten"}
    rutgers = next(x for x in t2025 if x["id"] == 164)
    expected = {a.lower() for a in rutgers["alternateNames"] + [rutgers["abbreviation"], rutgers["school"]]}
    assert set(t.aliases) == expected | {"scarlet knights"}
    ohio = spark.table("sa_catalog.silver.teams").where("team_id = '195'").collect()[0]
    assert "bobcats" in ohio.aliases and "scarlet knights" not in ohio.aliases


def test_committed_aliases_resource_loads():
    aliases = teams_mod.load_go_aliases()
    assert len(aliases) > 50 and aliases["bama"] == "Alabama"
    assert all(k == k.lower() for k in aliases)


def test_play_type_enum(spark):
    raws = {
        "Rushing Touchdown": "rush", "Pass Interception Return": "pass", "Blocked Punt Touchdown": "punt",
        "Extra Point Good": "extra_point", "Two Point Rush": "two_point", "2pt Conversion": "two_point",
        "Fumble Recovery (Opponent)": "fumble_recovery", "End of Regulation": "administrative",
        "Uncategorized": "other", "Something New": "other",
    }
    df = spark.createDataFrame([(k,) for k in raws], "play_type_raw STRING")
    got = dict(df.selectExpr("play_type_raw", f"{play_type_sql()} AS t").collect())
    assert got == raws
