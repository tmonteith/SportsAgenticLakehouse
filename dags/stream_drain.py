"""stream_drain: every 15 minutes, drain Kafka (cfb.live_games, agent.events) into bronze and silver.

A KubernetesPodOperator in namespace data runs the spark-drain image with the pod spec of the
infra chart k8s/data/drain/ (trigger availableNow, so the pod exits when the topics are drained).
The image is the full reference in Airflow Variable spark_drain_image (env
AIRFLOW_VAR_SPARK_DRAIN_IMAGE); without it the task fails and says so.
"""

from __future__ import annotations

from airflow.sdk import DAG

from lakehouse.common import dag_kwargs
from lakehouse.drain import drain_task

with DAG(
    dag_id="stream_drain",
    description="Drain cfb.live_games and agent.events into bronze and silver (local-mode Spark pod)",
    doc_md=__doc__,
    **dag_kwargs("*/15 * * * *"),
):
    drain_task("drain")
