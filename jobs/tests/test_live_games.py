import json
from datetime import datetime

from drain_support import fixture_lines
from pyspark.sql import functions as F

from sa_lakehouse import live_games


def _bronze(spark, values):
    rows = [
        (v if isinstance(v, str) else json.dumps(v), 0, i, datetime(2026, 11, 13, 4, 0))
        for i, v in enumerate(values)
    ]
    return spark.createDataFrame(rows, "value_json string, _kafka_partition int, _kafka_offset bigint, _ingested_at timestamp")


def _msg(game_id=1, fetched_at="2026-11-13T01:00:00Z", status="in_progress", clock="07:51", start="2026-11-13T01:00:00.000Z", **payload):
    p = {
        "id": game_id, "startDate": start, "status": status, "period": 2, "clock": clock,
        "neutralSite": False, "conferenceGame": True,
        "homeTeam": {"id": 2006, "conference": "Mid-American", "points": 14},
        "awayTeam": {"id": 2711, "conference": "Mid-American", "points": 21},
    }
    p.update(payload)
    return {"game_id": game_id, "fetched_at": fetched_at, "payload": p}


def test_fixture_maps_to_silver_games(spark):
    updates, rejects = live_games.parse_live_games(_bronze(spark, fixture_lines("live_games.jsonl")))
    assert updates.columns == live_games.GAMES_COLUMNS
    assert rejects.count() == 0
    rows = {r.game_id: r for r in updates.collect()}
    assert len(rows) == 7  # one per game, latest fetched_at wins

    final = rows[401866458]
    assert (final.status, final.home_team_id, final.away_team_id) == ("final", "2006", "2711")
    assert (final.home_points, final.away_points, final.season) == (14, 42, 2026)
    assert final.home_conference == "Mid-American" and final.conference_game is True and final.neutral_site is False
    assert final.week is None and final.season_type is None and final.venue_id is None and final.attendance is None
    assert final._source == "kafka:cfb.live_games"
    assert len(final._record_hash) == 64

    live = rows[401862824]
    assert live.status == "in_progress" and live.period == 4 and live.clock_seconds == 13 * 60 + 9
    assert live._fetched_at == datetime(2026, 11, 13, 4, 2, 58)

    assert rows[401858293].status == "scheduled" and rows[401858293].clock_seconds is None


def test_status_map_clock_and_season(spark):
    msgs = [
        _msg(1, status="completed"),
        _msg(2, status="postponed"),
        _msg(3, status="cancelled"),
        _msg(4, status="canceled", clock="0:05"),
        _msg(5, status="scheduled", clock=None, start="2027-01-09T00:30:00.000Z"),
        _msg(6, clock="bogus", start="2026-08-30T16:00:00.000Z"),
    ]
    rows = {r.game_id: r for r in live_games.parse_live_games(_bronze(spark, msgs))[0].collect()}
    assert [rows[i].status for i in range(1, 7)] == ["final", "postponed", "canceled", "canceled", "scheduled", "in_progress"]
    assert rows[1].clock_seconds == 471 and rows[4].clock_seconds == 5 and rows[6].clock_seconds is None
    assert rows[5].season == 2026  # January game belongs to the previous season
    assert rows[6].season == 2026


def test_unknown_status_and_bad_rows_are_rejected(spark):
    msgs = [_msg(1, status="delayed"), "not json", _msg(3, homeTeam={"id": None}), _msg(4)]
    updates, rejects = live_games.parse_live_games(_bronze(spark, msgs))
    assert [r.game_id for r in updates.collect()] == [4]
    got = {r.record_key: r for r in rejects.collect()}
    assert got["1"].reason == "unknown_status" and got["1"].table_name == "silver.games"
    assert got["kafka:0:1"].reason == "unparseable"
    assert got["3"].reason == "missing_required"
    assert all(r._source == "kafka:cfb.live_games" for r in got.values())
    assert json.loads(got["1"].raw)["payload"]["status"] == "delayed"


def test_dedupe_keeps_latest_fetched_at(spark):
    msgs = [
        _msg(1, fetched_at="2026-11-13T01:02:00Z", clock="10:00"),
        _msg(1, fetched_at="2026-11-13T01:03:00Z", clock="09:00"),
        _msg(1, fetched_at="2026-11-13T01:01:00Z", clock="11:00"),  # arrives last, older
    ]
    (row,) = live_games.parse_live_games(_bronze(spark, msgs))[0].collect()
    assert row.clock_seconds == 540


def test_record_hash_ignores_metadata_and_tracks_business_columns(spark):
    a = _msg(1, fetched_at="2026-11-13T01:02:00Z")
    b = _msg(1, fetched_at="2026-11-13T01:05:00Z")
    c = _msg(1, fetched_at="2026-11-13T01:05:00Z", clock="01:00")
    h = [live_games.parse_live_games(_bronze(spark, [m]))[0].select("_record_hash").first()[0] for m in (a, b, c)]
    assert h[0] == h[1] != h[2]
    # Canonical JSON: sorted keys, nulls kept.
    df = spark.range(1).select(F.lit(None).cast("int").alias("b"), F.lit(1).alias("a"))
    import hashlib

    expected = hashlib.sha256(b'{"a":1,"b":null}').hexdigest()
    assert df.select(live_games.record_hash(["b", "a"])).first()[0] == expected
