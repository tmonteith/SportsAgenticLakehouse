import os
from datetime import date, datetime, timezone

from helpers import fixture, snapshots, ts, write_landing

from sa_lakehouse.landing import LandingFile, derived_params, file_record, load_landing


def _landing(tmp_path):
    root = str(tmp_path / "landing" / "cfb")
    write_landing(root, "games", 2025, 1, ts("2025-09-01T03:30:00"), fixture("games_2025_w01.json"))
    write_landing(root, "plays", 2025, 1, ts("2025-09-01T03:30:05"), fixture("plays_2025_w01.json"))
    write_landing(root, "teams", 2025, 0, ts("2025-09-01T03:29:00"), fixture("teams_2025.json"))
    with open(os.path.join(root, "README.txt"), "w") as fh:
        fh.write("not a landing object")
    return root


def test_load_is_idempotent(clean_catalog, tmp_path):
    spark = clean_catalog
    root = _landing(tmp_path)
    s1 = load_landing(spark, "file://" + root)
    assert s1["loaded_files"] == 3 and s1["ignored"] == 1
    assert s1["by_endpoint"]["plays"] == {"files": 1, "records": 60}
    assert s1["meta_derived"] == 3 and s1["touched_seasons"] == [2025]

    rows = {r.endpoint: r for r in spark.table("sa_catalog.bronze.api_responses_raw").collect()}
    g = rows["games"]
    assert (g.season, g.week, g.season_type) == (2025, 1, "regular")
    assert g.params == "seasonType=regular&week=1&year=2025"
    assert g.fetched_at == datetime(2025, 9, 1, 3, 30)
    assert g._landing_file.endswith("games/season=2025/week=01/20250901T033000Z.json.gz")
    assert rows["teams"].params == "year=2025" and rows["teams"].week is None
    assert rows["plays"].season_type is None  # plays payload has no seasonType; object metadata has it on GCS
    state = spark.table("sa_catalog.bronze._load_state").collect()
    assert sorted(r.rows for r in state) == [4, 8, 60]

    before = {t: snapshots(spark, t) for t in ("bronze.api_responses_raw", "bronze._load_state")}
    s2 = load_landing(spark, "file://" + root)
    assert s2["loaded_files"] == 0 and s2["already_loaded"] == 3
    assert {t: snapshots(spark, t) for t in before} == before

    write_landing(root, "games", 2025, 1, ts("2025-09-02T03:30:00"), fixture("games_2025_w01.json"))
    s3 = load_landing(spark, "file://" + root)
    assert s3["loaded_files"] == 1
    assert spark.table("sa_catalog.bronze.api_responses_raw").count() == 4


def test_since_filters_by_fetch_date(clean_catalog, tmp_path):
    spark = clean_catalog
    root = _landing(tmp_path)
    s = load_landing(spark, "file://" + root, since=date(2025, 9, 2))
    assert s["skipped_since"] == 3 and s["loaded_files"] == 0
    assert snapshots(spark, "bronze.api_responses_raw") == 0


def test_repairs_state_after_a_partial_run(clean_catalog, tmp_path):
    spark = clean_catalog
    root = _landing(tmp_path)
    load_landing(spark, "file://" + root)
    spark.sql("DELETE FROM sa_catalog.bronze._load_state WHERE endpoint = 'plays'")
    s = load_landing(spark, "file://" + root)
    assert s["repaired"] == 1 and s["loaded_files"] == 0
    assert spark.table("sa_catalog.bronze.api_responses_raw").count() == 3


def test_object_metadata_wins_over_derivation():
    lf = LandingFile.parse("gs://p-sa-landing/cfb/plays/season=2025/week=01/20260105T033000Z.json.gz")
    meta = {
        "endpoint": "plays",
        "params": "seasonType=postseason&week=1&year=2025",
        "params_hash": "abc",
        "fetched_at": "2026-01-05T03:30:00Z",
        "http_status": "200",
    }
    r = file_record(lf, meta)
    assert (r["season"], r["week"], r["season_type"], r["http_status"]) == (2025, 1, "postseason", 200)
    assert r["fetched_at"] == datetime(2026, 1, 5, 3, 30, tzinfo=timezone.utc)
    d = file_record(lf, None)
    assert (d["season"], d["week"], d["meta_source"]) == (2025, 1, "derived")
    assert derived_params(2025, 1, "regular") == "seasonType=regular&week=1&year=2025"
    assert derived_params(2025, None, None) == "year=2025"


def test_gcs_metadata_listing_paginates_and_maps_paths():
    from sa_lakehouse.landing import TOKEN_URL, gcs_object_metadata

    calls = []
    pages = {
        None: {"items": [{"name": "cfb/games/season=2025/week=01/20250901T033000Z.json.gz",
                          "metadata": {"endpoint": "games", "params": "seasonType=postseason&week=1&year=2025"}}],
               "nextPageToken": "p2"},
        "p2": {"items": [{"name": "cfb/teams/season=2025/week=00/20250901T032900Z.json.gz"}]},
    }

    def fetch(url, headers, timeout=30.0):
        calls.append(url)
        if url == TOKEN_URL:
            assert headers == {"Metadata-Flavor": "Google"}
            return {"access_token": "t"}
        assert headers == {"Authorization": "Bearer t"}
        assert "prefix=cfb%2F" in url and "fields=items%28name%2Cmetadata%29%2CnextPageToken" in url
        token = url.split("pageToken=")[1] if "pageToken=" in url else None
        return pages[token]

    games = "gs://b/cfb/games/season=2025/week=01/20250901T033000Z.json.gz"
    teams = "gs://b/cfb/teams/season=2025/week=00/20250901T032900Z.json.gz"
    got = gcs_object_metadata(None, "gs://b/cfb", [games, teams, "gs://b/cfb/missing.json.gz"], fetch=fetch)
    assert got == {games: {"endpoint": "games", "params": "seasonType=postseason&week=1&year=2025"}, teams: {}}
    assert len(calls) == 3


def test_gcs_metadata_falls_back_without_token_or_gcs():
    from sa_lakehouse.landing import gcs_object_metadata

    def no_server(url, headers, timeout=30.0):
        raise OSError("no metadata server")

    assert gcs_object_metadata(None, "gs://b/cfb", ["gs://b/cfb/x"], fetch=no_server) == {}
    assert gcs_object_metadata(None, "file:///tmp/cfb", ["file:///tmp/cfb/x"], fetch=no_server) == {}


def test_postseason_metadata_wins_over_path():
    lf = LandingFile.parse("gs://b/cfb/plays/season=2025/week=01/20260102T033000Z.json.gz")
    meta = {"endpoint": "plays", "params": "seasonType=postseason&week=1&year=2025", "params_hash": "h",
            "fetched_at": "2026-01-02T03:30:00Z", "http_status": "200"}
    r = file_record(lf, meta)
    assert (r["season_type"], r["week"], r["params_hash"], r["http_status"], r["meta_source"]) == ("postseason", 1, "h", 200, "object")


def test_reload_clears_and_loads_everything_again(clean_catalog, tmp_path):
    spark = clean_catalog
    root = _landing(tmp_path)
    load_landing(spark, "file://" + root)
    s = load_landing(spark, "file://" + root, reload=True)
    assert s["loaded_files"] == 3 and s["already_loaded"] == 0
    assert s["cleared_rows"] == {"sa_catalog.bronze._load_state": 3, "sa_catalog.bronze.api_responses_raw": 3}
    assert spark.table("sa_catalog.bronze.api_responses_raw").count() == 3
    assert spark.table("sa_catalog.bronze._load_state").count() == 3
