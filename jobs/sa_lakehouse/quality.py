"""Quality checks for silver (plan WS4a, contract H9).

A check is a reason name and a SQL predicate that is true when a row FAILS. Each row gets the
first failing reason; failing rows go to silver._rejects and stay out of silver. A reason that
fails on more than ``THRESHOLD`` of a table's input rows fails the job (after writing).
"""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

THRESHOLD = 0.01

Check = tuple[str, str]  # (reason, SQL predicate true when the row fails)

GAME_CHECKS: list[Check] = [
    ("missing_key", "game_id IS NULL OR season IS NULL"),
    ("unresolved_team", "home_team_id IS NULL OR away_team_id IS NULL"),
    ("home_equals_away", "home_team_id = away_team_id"),
    ("negative_points", "home_points < 0 OR away_points < 0"),
    ("final_without_points", "status = 'final' AND (home_points IS NULL OR away_points IS NULL)"),
]

PLAY_CHECKS: list[Check] = [
    ("missing_key", "play_id IS NULL OR game_id IS NULL OR season IS NULL"),
    ("unknown_game", "__game_known IS NULL"),
    ("unresolved_team", "offense_team_id IS NULL OR defense_team_id IS NULL"),
    ("down_out_of_range", "down IS NOT NULL AND (down < 1 OR down > 4)"),
    ("yards_to_goal_out_of_range", "yards_to_goal IS NOT NULL AND (yards_to_goal < 0 OR yards_to_goal > 100)"),
    ("duplicate_play_sequence", "__seq_count > 1"),
]

TEAM_CHECKS: list[Check] = [
    ("missing_key", "team_id IS NULL"),
]


def apply_checks(df: DataFrame, checks: Sequence[Check], reason_col: str = "__reason") -> DataFrame:
    """Add ``reason_col``: the first failing check's reason, or null when the row passes."""
    expr = "CASE " + " ".join(f"WHEN coalesce({pred}, false) THEN '{name}'" for name, pred in checks) + " END"
    return df.withColumn(reason_col, F.expr(expr))


def evaluate(input_rows: int, failed: dict[str, int], threshold: float = THRESHOLD) -> dict:
    """Per-reason counts and rates, plus the reasons over the threshold."""
    checks = {
        reason: {"failed": n, "rate": round(n / input_rows, 6) if input_rows else 0.0}
        for reason, n in sorted(failed.items())
    }
    over = [r for r, c in checks.items() if input_rows and c["failed"] / input_rows > threshold]
    return {"input_rows": input_rows, "checks": checks, "over_threshold": over}
