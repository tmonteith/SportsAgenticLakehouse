"""cfb_nightly: wait for the extractor's completion marker, then one Serverless batch runs nightly.py.

03:45 America/Denver, 15 minutes after the extractor-nightly CronJob starts (03:30). The sensor
waits up to 3 hours for gs://<proj>-sa-landing/_markers/extractor-nightly/<Denver date>.json,
then nightly.py runs load_landing -> build_silver -> build_gold from gs://<proj>-sa-jobs/jobs/current/.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.sdk import DAG

from lakehouse.batches import batch_task
from lakehouse.common import dag_kwargs, gcp_project

MARKER = "_markers/extractor-nightly/{{ sa_denver_ds(dag_run) }}.json"

with DAG(
    dag_id="cfb_nightly",
    description="Extractor marker, then load_landing -> build_silver -> build_gold in one Serverless batch",
    doc_md=__doc__,
    dagrun_timeout=timedelta(hours=4),  # replaces the plan's SLA (SLAs were removed in Airflow 3)
    **dag_kwargs("45 3 * * *"),
):
    wait = GCSObjectExistenceSensor(
        task_id="wait_for_extractor_marker",
        bucket=f"{gcp_project()}-sa-landing",
        object=MARKER,
        mode="reschedule",
        poke_interval=300,
        timeout=3 * 60 * 60,
        deferrable=False,  # the triggerer is disabled
        retries=2,  # transient API errors; a timeout fails at once
        retry_delay=timedelta(minutes=5),
    )
    wait >> batch_task("nightly_batch", "nightly")
