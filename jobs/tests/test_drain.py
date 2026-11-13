import json
from datetime import datetime

import pytest
from drain_support import empty_kafka_df, fixture_lines, kafka_df, merge_games

import drain
from sa_lakehouse.config import settings_from_env, table
from sa_lakehouse.tables import ensure_tables

LIVE = "cfb.live_games"
AGENT = "agent.events"


@pytest.fixture
def tables(clean_catalog):
    ensure_tables(clean_catalog, drain.DRAIN_TABLES)
    return clean_catalog


def _live(spark, values, start_offset=0, batch_id=0):
    df = kafka_df(spark, LIVE, values, key_field="game_id", start_offset=start_offset)
    return drain.process_live_games_batch(spark, df, batch_id, merge_games=merge_games)


def _refresh(spark):
    # Streaming foreachBatch runs in a cloned session with its own Iceberg table cache.
    for n in drain.DRAIN_TABLES:
        spark.sql(f"REFRESH TABLE {table(n)}")


def _snapshots(spark):
    _refresh(spark)
    return {n: spark.sql(f"SELECT count(*) FROM {table(n)}.snapshots").first()[0] for n in drain.DRAIN_TABLES}


def _games(spark):
    return {r.game_id: r for r in spark.table(table("silver.games")).collect()}


def test_to_bronze_shape(spark):
    df = kafka_df(spark, LIVE, [{"game_id": 7}, "x"], key_field="game_id", start_offset=40)
    rows = drain.to_bronze(df).collect()
    assert drain.to_bronze(df).columns == drain.BRONZE_COLUMNS
    assert [(r.kafka_key, r.value_json, r._kafka_partition, r._kafka_offset) for r in rows] == [
        ("7", '{"game_id":7}', 0, 40),
        (None, "x", 0, 41),
    ]
    assert all(r._ingested_at is not None and r._kafka_timestamp is not None for r in rows)


def test_live_replay_writes_bronze_and_silver(tables):
    spark = tables
    lines = fixture_lines("live_games.jsonl")
    result = _live(spark, lines)
    assert result == {"batch_id": 0, "kafka_records": 14, "bronze_rows": 14, "silver_rows": 7, "rejects": 0}

    bronze = spark.table(table("bronze.live_game_events")).orderBy("_kafka_offset").collect()
    assert [r._kafka_offset for r in bronze] == list(range(14))
    assert json.loads(bronze[0].value_json)["game_id"] == int(bronze[0].kafka_key)

    games = _games(spark)
    assert len(games) == 7
    assert games[401866458].status == "final" and games[401866458].home_points == 14
    assert games[401862824].clock_seconds == 789 and games[401862824].period == 4


def test_replayed_batch_does_not_duplicate_bronze(tables):
    spark = tables
    lines = fixture_lines("live_games.jsonl")
    _live(spark, lines[:10])
    # The job died after the bronze commit: the next run replays offsets 0-9 plus new ones.
    result = _live(spark, lines, batch_id=0)
    assert result["kafka_records"] == 14 and result["bronze_rows"] == 4
    assert spark.table(table("bronze.live_game_events")).count() == 14
    assert drain.committed_offsets(spark, table("bronze.live_game_events")) == {0: 13}


def test_final_never_regresses_and_older_updates_lose(tables):
    spark = tables
    _live(spark, fixture_lines("live_games.jsonl"))
    regress = {
        "game_id": 401866458, "fetched_at": "2026-11-13T05:00:00Z",
        "payload": {"startDate": "2026-11-11T00:00:00.000Z", "status": "in_progress", "period": 4, "clock": "01:00",
                    "homeTeam": {"id": 2006, "points": 99}, "awayTeam": {"id": 2711, "points": 0}},
    }
    stale = {
        "game_id": 401862824, "fetched_at": "2026-11-13T01:30:00Z",
        "payload": {"startDate": "2026-11-13T01:00:00.000Z", "status": "in_progress", "period": 1, "clock": "02:00",
                    "homeTeam": {"id": 1, "points": 0}, "awayTeam": {"id": 2, "points": 0}},
    }
    _live(spark, [regress, stale], start_offset=14)
    games = _games(spark)
    assert games[401866458].status == "final" and games[401866458].home_points == 14
    assert games[401862824].period == 4


def test_unknown_status_goes_to_rejects(tables):
    spark = tables
    msg = json.loads(fixture_lines("live_games.jsonl")[0])
    msg["payload"]["status"] = "delayed"
    result = _live(spark, [msg])
    assert result["rejects"] == 1 and result["silver_rows"] == 0
    (rej,) = spark.table(table("silver._rejects")).collect()
    assert (rej.table_name, rej.reason, rej.record_key) == ("silver.games", "unknown_status", "401866458")
    assert rej._fetched_at == datetime(2026, 11, 13, 0, 45)
    assert spark.table(table("silver.games")).count() == 0


def test_empty_batches_write_no_snapshot(tables):
    spark = tables
    _live(spark, fixture_lines("live_games.jsonl"))
    drain.process_agent_events_batch(spark, kafka_df(spark, AGENT, fixture_lines("agent_events.jsonl")), 0)
    before = _snapshots(spark)
    assert drain.process_live_games_batch(spark, empty_kafka_df(spark), 1, merge_games=merge_games)["empty"]
    assert drain.process_agent_events_batch(spark, empty_kafka_df(spark), 1)["empty"]
    assert _snapshots(spark) == before


def test_parse_args():
    args = drain.parse_args([])
    assert args.stream_names == ["live_game_events", "agent_events"] and not args.parallel
    assert drain.parse_args(["--streams", "agent_events", "--parallel"]).stream_names == ["agent_events"]
    with pytest.raises(SystemExit):
        drain.parse_args(["--streams", "nope"])


def test_drain_conf_and_checkpoints():
    settings = settings_from_env({"GCP_PROJECT": "proj-x", "KAFKA_BOOTSTRAP": "k:9092"})
    conf = drain.drain_conf(settings)
    assert conf["spark.ui.enabled"] == "false"
    assert conf["spark.hadoop.fs.gs.auth.type"] == "APPLICATION_DEFAULT"
    assert conf["spark.sql.catalog.sa_catalog.type"] == "rest"
    assert settings.checkpoint_dir("live_game_events") == "gs://proj-x-sa-warehouse/_checkpoints/live_game_events/"
    assert set(drain.STREAMS) == {"live_game_events", "agent_events"}
    assert {s.topic for s in drain.STREAMS.values()} == {LIVE, AGENT}


def test_main_fails_without_kafka(monkeypatch, capsys):
    monkeypatch.setenv("GCP_PROJECT", "proj-x")
    monkeypatch.delenv("KAFKA_BOOTSTRAP", raising=False)
    assert drain.main([]) == 1
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["event"] == "drain_summary" and summary["status"] == "failed"
    assert "KAFKA_BOOTSTRAP" in summary["error"]


def test_available_now_stream_wiring(tables, tmp_path):
    """The real foreachBatch/availableNow/checkpoint path, with a parquet file source shaped
    like the Kafka source (scripts/test.sh has no Kafka jar or broker)."""
    from functools import partial

    from drain_support import KAFKA_SCHEMA

    spark = tables
    src, ckpt = tmp_path / "src", str(tmp_path / "ckpt")
    lines = fixture_lines("live_games.jsonl")
    spec = drain.STREAMS["live_game_events"]
    process = partial(drain.process_live_games_batch, merge_games=merge_games)

    def run():
        stats = {}
        source = spark.readStream.schema(KAFKA_SCHEMA).option("recursiveFileLookup", "true").parquet(str(src))
        q = drain.start_stream(source, spec, ckpt, stats, process=process)
        q.awaitTermination()
        assert q.exception() is None
        return stats["live_game_events"]

    kafka_df(spark, LIVE, lines[:10], key_field="game_id").write.parquet(str(src / "part1"))
    first = run()
    assert first["batches"] == 1 and first["bronze_rows"] == 10 and first["silver_rows"] == 7

    before = _snapshots(spark)
    assert run()["batches"] == 0  # nothing new: no micro-batch, no snapshot
    assert _snapshots(spark) == before

    kafka_df(spark, LIVE, lines[10:], key_field="game_id", start_offset=10).write.parquet(str(src / "part2"))
    third = run()
    assert third["bronze_rows"] == 4
    _refresh(spark)
    assert spark.table(table("bronze.live_game_events")).count() == 14
    assert spark.table(table("silver.games")).count() == 7
