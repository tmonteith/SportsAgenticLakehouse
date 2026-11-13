"""gold_after_live: on game days, rebuild gold.game_summaries for the current season.

23:30 America/Denver on Thursdays, Fridays and Saturdays, August through January: one
Serverless batch runs build_gold.py --tables gold.game_summaries --seasons <season>. The
stream_drain DAG has already merged the live finals into silver.games. The season is the
run's Denver date's year; January and February belong to the previous season.
"""

from __future__ import annotations

from airflow.sdk import DAG

from lakehouse.batches import batch_task
from lakehouse.common import dag_kwargs

with DAG(
    dag_id="gold_after_live",
    description="Rebuild gold.game_summaries for the current season after the day's games",
    doc_md=__doc__,
    **dag_kwargs("30 23 * 1,8-12 4-6"),
):
    batch_task(
        "gold_game_summaries_batch",
        "build_gold",
        ["--tables", "gold.game_summaries", "--seasons", "{{ sa_cfb_season(dag_run) }}"],
        id_prefix="gold-after-live",
    )
