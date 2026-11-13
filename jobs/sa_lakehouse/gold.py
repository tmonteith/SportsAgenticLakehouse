"""Gold tables (contract H7): silver -> team_efficiency_weekly, game_summaries, agent_usage_daily.

Definitions:
- scrimmage play: ``play_type IN ('rush', 'pass', 'sack')``;
- success: down 1 and gain >= 50% of distance, down 2 and >= 70%, down 3/4 and >= 100%, or an
  offensive touchdown (``is_scoring``, raw type contains "Touchdown" and is not an interception
  or fumble return);
- EPA per play = EPA sum / scrimmage plays with EPA; success rate = successes / scrimmage plays;
  explosiveness = EPA sum over successful scrimmage plays / successes; drives = distinct
  ``drive_id`` with a scrimmage play; points per drive = final points / drives.

team_efficiency_weekly covers final games with a known week and season type, keyed by
(team, season, week, season_type) so a postseason week 1 never collides with regular week 1;
several games in one key aggregate. ``def_*`` columns are the opponents' offense in those games.

Every table is replaced per season (per day for usage) through :func:`sync_rows`, which writes
only rows whose values changed (doubles compared at 9 decimals), so rebuilding unchanged silver
adds no snapshot. Gold never carries ``user_key`` or ``session_id``.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import DEFAULT_CATALOG, table
from .games import GAMES
from .hashing import get_table_property, sync_rows
from .plays import PLAYS
from .teams import TEAMS

TEAM_WEEKLY = "gold.team_efficiency_weekly"
GAME_SUMMARIES = "gold.game_summaries"
USAGE = "gold.agent_usage_daily"
AGENT_REQUESTS = "silver.agent_requests"
GOLD_TABLES = (TEAM_WEEKLY, GAME_SUMMARIES, USAGE)
# Highest silver _ingested_at (microseconds) the last default build consumed; on game_summaries.
WATERMARK = "sa.build_gold.silver_watermark"
ROUND_DOUBLES = 9

SUCCESS_SQL = """coalesce(
  (down = 1 AND yards_gained >= 0.5 * distance)
  OR (down = 2 AND yards_gained >= 0.7 * distance)
  OR (down IN (3, 4) AND yards_gained >= distance)
  OR (is_scoring AND play_type_raw LIKE '%Touchdown%'
      AND play_type_raw NOT LIKE '%Interception%' AND play_type_raw NOT LIKE '%Fumble%'),
  false)"""

STATS = ("plays", "epa_plays", "epa_sum", "successes", "success_epa_sum", "drives", "points")


def _in(values) -> str:
    return ", ".join(str(int(v)) for v in values)


def _ratio(num: str, den: str) -> str:
    return f"CASE WHEN {den} > 0 THEN CAST({num} AS DOUBLE) / {den} END"


def offense_by_game(spark, seasons, catalog: str = DEFAULT_CATALOG) -> DataFrame:
    """Per (game_id, team_id = offense): scrimmage counts, EPA sums, drives, yards, turnovers."""
    return spark.sql(
        f"""
        SELECT game_id, offense_team_id AS team_id,
          count_if(scrimmage) AS plays,
          count_if(scrimmage AND source_epa IS NOT NULL) AS epa_plays,
          coalesce(sum(CASE WHEN scrimmage THEN source_epa END), 0D) AS epa_sum,
          count_if(scrimmage AND success) AS successes,
          coalesce(sum(CASE WHEN scrimmage AND success THEN source_epa END), 0D) AS success_epa_sum,
          count(DISTINCT CASE WHEN scrimmage THEN drive_id END) AS drives,
          coalesce(sum(CASE WHEN scrimmage THEN yards_gained END), 0) AS yards,
          count_if(is_turnover) AS turnovers
        FROM (
          SELECT *, play_type IN ('rush', 'pass', 'sack') AS scrimmage, {SUCCESS_SQL} AS success
          FROM {table(PLAYS, catalog)} WHERE season IN ({_in(seasons)})
        )
        GROUP BY game_id, offense_team_id
        """
    )


def team_efficiency_weekly(spark, seasons, catalog: str = DEFAULT_CATALOG) -> DataFrame:
    offense_by_game(spark, seasons, catalog).createOrReplaceTempView("__gold_offense")
    sums = ",\n".join(
        f"coalesce(sum({side}.{s}), 0) AS {prefix}_{s}"
        for prefix, side in (("off", "o"), ("def", "d"))
        for s in STATS
        if s != "points"
    )
    side_cols = []
    for p in ("off", "def"):
        side_cols += [
            f"CAST({p}_plays AS INT) AS {p}_plays",
            f"CAST({p}_epa_plays AS INT) AS {p}_epa_plays",
            f"CAST({p}_epa_sum AS DOUBLE) AS {p}_epa_sum",
            f"CAST({p}_successes AS INT) AS {p}_successes",
            f"CAST({p}_success_epa_sum AS DOUBLE) AS {p}_success_epa_sum",
            f"CAST({p}_drives AS INT) AS {p}_drives",
            f"CAST({p}_points AS INT) AS {p}_points",
            f"{_ratio(f'{p}_epa_sum', f'{p}_epa_plays')} AS {p}_epa_per_play",
            f"{_ratio(f'{p}_successes', f'{p}_plays')} AS {p}_success_rate",
            f"{_ratio(f'{p}_success_epa_sum', f'{p}_successes')} AS {p}_explosiveness",
            f"{_ratio(f'{p}_points', f'{p}_drives')} AS {p}_points_per_drive",
        ]
    games = table(GAMES, catalog)
    final = f"status = 'final' AND week IS NOT NULL AND season_type IS NOT NULL AND season IN ({_in(seasons)})"
    return spark.sql(
        f"""
        WITH tg AS (
          SELECT game_id, season, week, season_type, home_team_id AS team_id, away_team_id AS opp_id,
                 home_points AS pts, away_points AS opp_pts
          FROM {games} WHERE {final}
          UNION ALL
          SELECT game_id, season, week, season_type, away_team_id, home_team_id, away_points, home_points
          FROM {games} WHERE {final}
        ),
        agg AS (
          SELECT tg.team_id, tg.season, tg.week, tg.season_type, count(*) AS games,
                 {sums},
                 coalesce(sum(tg.pts), 0) AS off_points, coalesce(sum(tg.opp_pts), 0) AS def_points
          FROM tg
          LEFT JOIN __gold_offense o ON o.game_id = tg.game_id AND o.team_id = tg.team_id
          LEFT JOIN __gold_offense d ON d.game_id = tg.game_id AND d.team_id = tg.opp_id
          GROUP BY tg.team_id, tg.season, tg.week, tg.season_type
        )
        SELECT team_id, season, week, season_type, CAST(games AS INT) AS games,
               {", ".join(side_cols)},
               current_timestamp() AS _built_at
        FROM agg
        """
    )


def game_summaries(spark, seasons, catalog: str = DEFAULT_CATALOG) -> DataFrame:
    offense_by_game(spark, seasons, catalog).createOrReplaceTempView("__gold_offense")
    side = []
    for s, alias in (("home", "h"), ("away", "a")):
        side += [
            f"CAST({alias}.plays AS INT) AS {s}_plays",
            f"CAST({alias}.yards AS INT) AS {s}_yards",
            f"{_ratio(f'{alias}.epa_sum', f'{alias}.epa_plays')} AS {s}_epa_per_play",
            f"{_ratio(f'{alias}.successes', f'{alias}.plays')} AS {s}_success_rate",
            f"CAST({alias}.turnovers AS INT) AS {s}_turnovers",
        ]
    return spark.sql(
        f"""
        SELECT g.game_id, g.season, g.week, g.season_type, g.start_time_utc, g.status,
               g.home_team_id, th.display_name AS home_team_name,
               g.away_team_id, ta.display_name AS away_team_name,
               g.home_points, g.away_points,
               CASE WHEN g.status = 'final' AND g.home_points > g.away_points THEN g.home_team_id
                    WHEN g.status = 'final' AND g.away_points > g.home_points THEN g.away_team_id END AS winner_team_id,
               {", ".join(side)},
               current_timestamp() AS _built_at
        FROM {table(GAMES, catalog)} g
        LEFT JOIN __gold_offense h ON h.game_id = g.game_id AND h.team_id = g.home_team_id
        LEFT JOIN __gold_offense a ON a.game_id = g.game_id AND a.team_id = g.away_team_id
        LEFT JOIN {table(TEAMS, catalog)} th ON th.team_id = g.home_team_id
        LEFT JOIN {table(TEAMS, catalog)} ta ON ta.team_id = g.away_team_id
        WHERE g.season IN ({_in(seasons)})
        """
    )


def agent_usage_daily(spark, days, catalog: str = DEFAULT_CATALOG) -> DataFrame:
    """Per UTC day and tool; plus ``tool = '_all'`` per day. Reads no identifiers."""
    lo, hi = min(days), max(days) + timedelta(days=1)
    in_days = ", ".join(f"DATE'{d.isoformat()}'" for d in days)
    p50 = "CAST(round(percentile({c}, 0.5)) AS INT)"
    p95 = "CAST(round(percentile({c}, 0.95)) AS INT)"
    return spark.sql(
        f"""
        WITH r AS (
          SELECT request_id, status, latency_ms, tool_calls, to_date(requested_at) AS day
          FROM {table(AGENT_REQUESTS, catalog)}
          WHERE requested_at >= TIMESTAMP'{lo.isoformat()} 00:00:00'
            AND requested_at < TIMESTAMP'{hi.isoformat()} 00:00:00'
            AND to_date(requested_at) IN ({in_days})
        ),
        c AS (
          SELECT r.day, r.request_id, tc.tool, tc.status, tc.latency_ms, tc.known_tool
          FROM r LATERAL VIEW explode(r.tool_calls) x AS tc
          WHERE tc.tool IS NOT NULL
        )
        SELECT day, tool, CAST(count(DISTINCT request_id) AS INT) AS requests, CAST(count(*) AS INT) AS calls,
               CAST(count_if(status <> 'ok') AS INT) AS errors,
               {p50.format(c="latency_ms")} AS p50_latency_ms, {p95.format(c="latency_ms")} AS p95_latency_ms,
               coalesce(bool_or(NOT known_tool), false) AS unknown_tool,
               current_timestamp() AS _built_at
        FROM c GROUP BY day, tool
        UNION ALL
        SELECT day, '_all', CAST(count(*) AS INT),
               CAST(sum(CASE WHEN tool_calls IS NULL THEN 0 ELSE size(tool_calls) END) AS INT),
               CAST(count_if(status = 'failed') AS INT),
               {p50.format(c="latency_ms")}, {p95.format(c="latency_ms")}, false, current_timestamp()
        FROM r GROUP BY day
        """
    )


def build_seasons(spark, seasons, catalog: str = DEFAULT_CATALOG) -> dict:
    """Replace the given seasons in team_efficiency_weekly and game_summaries."""
    if not seasons:
        return {}
    slice_filter = f"season IN ({_in(seasons)})"
    return {
        TEAM_WEEKLY: sync_rows(
            spark, table(TEAM_WEEKLY, catalog), team_efficiency_weekly(spark, seasons, catalog),
            ["team_id", "season", "week", "season_type"], slice_filter=slice_filter, round_doubles=ROUND_DOUBLES,
        ),
        GAME_SUMMARIES: sync_rows(
            spark, table(GAME_SUMMARIES, catalog), game_summaries(spark, seasons, catalog),
            ["game_id"], slice_filter=slice_filter, prune=["season"], round_doubles=ROUND_DOUBLES,
        ),
    }


def build_days(spark, days, catalog: str = DEFAULT_CATALOG) -> dict:
    """Replace the given UTC days in agent_usage_daily."""
    if not days:
        return {}
    slice_filter = "day IN (" + ", ".join(f"DATE'{d.isoformat()}'" for d in days) + ")"
    return {
        USAGE: sync_rows(
            spark, table(USAGE, catalog), agent_usage_daily(spark, days, catalog), ["day", "tool"],
            slice_filter=slice_filter, round_doubles=ROUND_DOUBLES,
        )
    }


def default_days(today: date | None = None, n: int = 3) -> list[date]:
    today = today or datetime.now(timezone.utc).date()
    return [today - timedelta(days=i) for i in range(n - 1, -1, -1)]


def touched_seasons(spark, catalog: str = DEFAULT_CATALOG) -> tuple[list[int], int | None]:
    """Seasons whose silver rows changed since the last default build, and the new watermark.

    A season is touched when a silver.games or silver.plays row in it was written after the
    watermark, or when it has a game of a team whose silver.teams row was written after it.
    Without a watermark (first build) every season in silver.games is touched.
    """
    games, plays, teams = (spark.table(table(t, catalog)) for t in (GAMES, PLAYS, TEAMS))
    high = None
    for df in (games, plays, teams):
        v = df.agg(F.max(F.expr("unix_micros(_ingested_at)"))).collect()[0][0]
        if v is not None:
            high = v if high is None else max(high, v)
    wm = get_table_property(spark, table(GAME_SUMMARIES, catalog), WATERMARK)
    if wm is None:
        seasons = {r[0] for r in games.select("season").distinct().collect()}
        return sorted(seasons), high
    after = F.col("_ingested_at") > F.expr(f"timestamp_micros({int(wm)})")
    seasons = {r[0] for r in games.where(after).select("season").distinct().collect()}
    seasons |= {r[0] for r in plays.where(after).select("season").distinct().collect()}
    changed_teams = [r[0] for r in teams.where(after).select("team_id").collect()]
    if changed_teams:
        involved = F.col("home_team_id").isin(changed_teams) | F.col("away_team_id").isin(changed_teams)
        seasons |= {r[0] for r in games.where(involved).select("season").distinct().collect()}
    return sorted(seasons), high
