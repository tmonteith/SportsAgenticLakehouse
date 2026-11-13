from datetime import datetime

import pytest
from drain_support import empty_kafka_df, fixture_lines, kafka_df

import drain
from sa_lakehouse import agent_events
from sa_lakehouse.config import table
from sa_lakehouse.tables import ensure_tables

TOPIC = "agent.events"
R = "dddddddd-0000-4000-8000-00000000000d"


@pytest.fixture
def tables(clean_catalog):
    ensure_tables(clean_catalog, drain.DRAIN_TABLES)
    return clean_catalog


def _process(spark, values, start_offset=0, tools=None):
    df = kafka_df(spark, TOPIC, values, key_field="request_id", start_offset=start_offset)
    return drain.process_agent_events_batch(spark, df, 0, tools=tools)


def _rows(spark):
    return {r.request_id: r for r in spark.table(table("silver.agent_requests")).collect()}


def _ev(n, event_type, time, **kw):
    e = {
        "event_id": f"f0000000-0000-4000-8000-{n:012d}", "event_type": event_type, "request_id": R,
        "session_id": "33333333-3333-4333-8333-333333333333", "user_key": "ef" * 32, "event_time": time,
    }
    e.update(kw)
    return e


def test_known_tools_env_override():
    assert agent_events.known_tools({}) == agent_events.DEFAULT_KNOWN_TOOLS
    assert agent_events.known_tools({"KNOWN_TOOLS": " "}) == agent_events.DEFAULT_KNOWN_TOOLS
    assert agent_events.known_tools({"KNOWN_TOOLS": "a, b,,"}) == frozenset({"a", "b"})


def test_fixture_replay(tables):
    spark = tables
    result = _process(spark, fixture_lines("agent_events.jsonl"), tools=agent_events.DEFAULT_KNOWN_TOOLS)
    assert result["kafka_records"] == 9 and result["bronze_rows"] == 9 and result["silver_rows"] == 3
    rows = _rows(spark)

    a = rows["aaaaaaaa-0000-4000-8000-00000000000a"]
    assert a.status == "completed" and a.requested_at == datetime(2026, 11, 12, 19, 55, 15, 223000)
    assert a.completed_at == datetime(2026, 11, 12, 19, 55, 17, 847000)
    assert (a.latency_ms, a.model, a.prompt_chars, a.client, a.app_version) == (2624, "gemini-test-model", 34, "web", "test123")
    assert a.error_code is None and a.input_tokens is None
    assert [(c.tool, c.latency_ms, c.status, c.known_tool) for c in a.tool_calls] == [
        ("get_cfb_scores", 236, "ok", True),
        ("get_weather", 120, "ok", False),  # unknown tools are kept and flagged
    ]
    assert a._last_event_at == a.completed_at and a._kafka_partition == 0 and a._kafka_offset == 3

    b = rows["bbbbbbbb-0000-4000-8000-00000000000b"]
    assert (b.status, b.error_code, b.latency_ms) == ("failed", "mcp_error", 5941)
    assert [(c.tool, c.status) for c in b.tool_calls] == [("get_team_schedule", "error")]

    c = rows["cccccccc-0000-4000-8000-00000000000c"]
    assert c.status == "in_flight" and c.completed_at is None and c.latency_ms is None
    assert c._last_event_at == datetime(2026, 11, 12, 20, 30, 1)


def test_out_of_order_across_runs(tables):
    spark = tables
    tool = _ev(2, "tool_called", "2026-11-12T10:00:02.000Z", tool="get_cfb_scores", latency_ms=300, status="ok")
    done = _ev(
        3, "request_completed", "2026-11-12T10:00:05.000Z", latency_ms=5000, status="completed", error_code=None,
        model="m1", input_tokens=None, output_tokens=None, prompt_chars=20, client="web", app_version="v1",
    )
    _process(spark, [tool, done], start_offset=0)
    (r,) = _rows(spark).values()
    assert r.status == "completed" and r.requested_at == datetime(2026, 11, 12, 10, 0, 2)  # earliest so far

    # The start event arrives in a later run, without app_version, and the tool call is redelivered.
    start = _ev(1, "request_started", "2026-11-12T10:00:00.000Z", prompt_chars=20, client="web", app_version=None)
    _process(spark, [start, tool], start_offset=2)
    (r,) = _rows(spark).values()
    assert r.requested_at == datetime(2026, 11, 12, 10, 0, 0)
    assert (r.status, r.completed_at, r.model, r.latency_ms) == ("completed", datetime(2026, 11, 12, 10, 0, 5), "m1", 5000)
    assert r.app_version == "v1"  # never overwritten with null
    assert len(r.tool_calls) == 1
    assert r._last_event_at == datetime(2026, 11, 12, 10, 0, 5) and r._kafka_offset == 3

    # A second tool call shows up last; calls stay sorted by started_at.
    late = _ev(4, "tool_called", "2026-11-12T10:00:01.000Z", tool="brand_new_tool", latency_ms=50, status="ok")
    _process(spark, [late], start_offset=4)
    (r,) = _rows(spark).values()
    assert [(c.tool, c.known_tool) for c in r.tool_calls] == [("brand_new_tool", False), ("get_cfb_scores", True)]
    assert spark.table(table("silver.agent_requests")).count() == 1


def test_completion_after_in_flight(tables):
    spark = tables
    _process(spark, [_ev(1, "request_started", "2026-11-12T10:00:00.000Z", prompt_chars=5)])
    assert _rows(spark)[R].status == "in_flight"
    _process(
        spark,
        [_ev(2, "request_failed", "2026-11-12T10:00:09.000Z", latency_ms=9000, status="failed", error_code="llm_error")],
        start_offset=1,
    )
    r = _rows(spark)[R]
    assert (r.status, r.error_code, r.prompt_chars, r.requested_at) == ("failed", "llm_error", 5, datetime(2026, 11, 12, 10))


def test_duplicate_events_in_one_batch_count_once(tables):
    spark = tables
    tool = _ev(2, "tool_called", "2026-11-12T10:00:02.000Z", tool="get_cfb_scores", latency_ms=300, status="ok")
    _process(spark, [tool, tool, _ev(1, "request_started", "2026-11-12T10:00:00.000Z")])
    assert len(_rows(spark)[R].tool_calls) == 1


def test_bad_events_go_to_rejects(tables):
    spark = tables
    bad = ["{broken", {"event_type": "request_started", "event_time": "2026-11-12T10:00:00Z"}, _ev(9, "mystery", "2026-11-12T10:00:00Z")]
    result = _process(spark, bad)
    assert result["rejects"] == 3 and result["silver_rows"] == 0
    reasons = sorted(r.reason for r in spark.table(table("silver._rejects")).collect())
    assert reasons == ["missing_required", "unknown_event_type", "unparseable"]
    assert spark.table(table("silver.agent_requests")).count() == 0


def test_empty_batch_is_a_no_op(tables):
    spark = tables
    assert drain.process_agent_events_batch(spark, empty_kafka_df(spark), 7) == {"batch_id": 7, "empty": True}
