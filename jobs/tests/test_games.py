import pytest
from helpers import snapshots, ts

from sa_lakehouse.games import merge_games
from sa_lakehouse.tables import ensure_tables

T = "sa_catalog.silver.games"

BASE = dict(
    game_id=1, season=2025, week=1, season_type="regular", start_time_utc=ts("2025-08-30T16:00:00"),
    status="scheduled", period=None, clock_seconds=None, home_team_id="10", away_team_id="20",
    home_conference="SEC", away_conference="ACC", home_points=None, away_points=None, neutral_site=False,
    conference_game=False, venue_id=99, attendance=None, _source="cfbd:games", _fetched_at=ts("2025-08-29T03:30:00"),
    _ingested_at=None, _record_hash=None,
)


def rows(spark, *updates):
    schema = spark.table(T).schema
    return spark.createDataFrame([tuple({**BASE, **u}[c] for c in schema.names) for u in updates], schema)


def get(spark, game_id=1):
    return spark.table(T).where(f"game_id = {game_id}").collect()[0]


@pytest.fixture
def games(clean_catalog):
    ensure_tables(clean_catalog, ["silver.games"])
    return clean_catalog


def test_insert_then_unchanged_adds_no_snapshot(games):
    spark = games
    r = merge_games(spark, rows(spark, {}), "batch")
    assert (r["inserted"], r["updated"], r["touched_seasons"]) == (1, 0, [2025])
    first = get(spark)
    assert first._record_hash and first._ingested_at
    n = snapshots(spark, "silver.games")
    # Same content fetched later: hash equal -> skipped, no MERGE.
    r = merge_games(spark, rows(spark, {"_fetched_at": ts("2025-08-30T03:30:00")}), "batch")
    assert (r["inserted"], r["updated"]) == (0, 0)
    assert snapshots(spark, "silver.games") == n
    assert get(spark)._fetched_at == first._fetched_at


def test_newer_wins_and_dedupe_keeps_latest(games):
    spark = games
    merge_games(spark, rows(spark, {}), "batch")
    older = {"_fetched_at": ts("2025-08-28T00:00:00"), "attendance": 1}
    assert merge_games(spark, rows(spark, older), "batch")["updated"] == 0
    batch = rows(
        spark,
        {"_fetched_at": ts("2025-08-30T00:00:00"), "attendance": 5},
        {"_fetched_at": ts("2025-08-31T00:00:00"), "attendance": 7},
    )
    assert merge_games(spark, batch, "batch")["updated"] == 1
    assert get(spark).attendance == 7


def test_final_is_sticky(games):
    spark = games
    final = {"status": "final", "home_points": 21, "away_points": 14}
    merge_games(spark, rows(spark, final), "batch")
    late = {"status": "in_progress", "home_points": 14, "_fetched_at": ts("2025-08-30T20:00:00"), "_source": "kafka:cfb.live_games"}
    assert merge_games(spark, rows(spark, late), "live")["updated"] == 0
    assert merge_games(spark, rows(spark, late), "batch")["updated"] == 0
    assert get(spark).status == "final"
    # A newer final correction still applies.
    fix = {**final, "home_points": 24, "_fetched_at": ts("2025-08-31T03:30:00")}
    assert merge_games(spark, rows(spark, fix), "batch")["updated"] == 1
    assert get(spark).home_points == 24


def test_live_mode_column_rules(games):
    spark = games
    merge_games(spark, rows(spark, {"away_conference": None, "neutral_site": None}), "batch")
    before = get(spark)
    live = {
        "week": None, "season_type": None, "venue_id": None, "attendance": None,
        "status": "in_progress", "period": 2, "clock_seconds": 300, "home_points": 7, "away_points": 3,
        "home_conference": "Big Ten", "away_conference": "Big 12", "neutral_site": True,
        "_source": "kafka:cfb.live_games", "_fetched_at": ts("2025-08-30T17:00:00"),
    }
    r = merge_games(spark, rows(spark, live), "live")
    assert r["updated"] == 1
    g = get(spark)
    assert (g.status, g.period, g.clock_seconds, g.home_points, g.away_points) == ("in_progress", 2, 300, 7, 3)
    assert (g.week, g.season_type, g.venue_id) == (1, "regular", 99)  # kept
    assert g.home_conference == "SEC"  # target had a value: kept
    assert (g.away_conference, g.neutral_site) == ("Big 12", True)  # target was null: filled
    assert g._source == "kafka:cfb.live_games" and g._record_hash != before._record_hash
    # Same live state again (newer fetch): merged row unchanged -> no snapshot.
    n = snapshots(spark, "silver.games")
    again = merge_games(spark, rows(spark, {**live, "_fetched_at": ts("2025-08-30T17:01:00")}), "live")
    assert again["updated"] == 0 and snapshots(spark, "silver.games") == n
    # The stored hash is the hash of the merged row: a batch row with the same content matches it.
    same = {k: g[k] for k in BASE if not k.startswith("_")}
    assert merge_games(spark, rows(spark, {**same, "_fetched_at": ts("2025-08-31T00:00:00")}), "batch")["updated"] == 0


def test_live_insert_takes_source(games):
    spark = games
    live = {"game_id": 2, "week": None, "season_type": None, "status": "in_progress", "_source": "kafka:cfb.live_games"}
    assert merge_games(spark, rows(spark, live), "live")["inserted"] == 1
    g = get(spark, 2)
    assert g.week is None and g.status == "in_progress"


def test_rejects_bad_mode(games):
    with pytest.raises(ValueError):
        merge_games(games, rows(games, {}), "stream")
