"""Settings and template macros shared by the lakehouse DAGs (contract I6).

Airflow puts the DAG folder (git-sync sub-path ``dags``) on ``sys.path``, so the DAG files
import this package as ``lakehouse``. Credentials are Application Default Credentials
(``GOOGLE_APPLICATION_CREDENTIALS``, set in every Airflow container); ``GCP_PROJECT`` comes
from the environment too.

Macros take ``dag_run`` rather than ``data_interval_end``: a manually triggered run may have
no logical date (so no data interval), but always has ``run_after``. For scheduled runs
(CronTriggerTimetable) ``run_after`` equals ``data_interval_end``, the time the run fired.
"""

from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime

import pendulum
from airflow.sdk import CronTriggerTimetable, Variable

TIMEZONE = "America/Denver"
REGION = "us-central1"
TAGS = ["lakehouse"]
START_DATE = pendulum.datetime(2026, 11, 1, tz=TIMEZONE)
DRAIN_IMAGE_VARIABLE = "spark_drain_image"

_BATCH_ID = re.compile(r"^[a-z0-9][a-z0-9-]{2,61}[a-z0-9]$")


def gcp_project() -> str:
    project = os.environ.get("GCP_PROJECT", "").strip()
    if not project:
        raise RuntimeError("GCP_PROJECT is not set in the Airflow environment")
    return project


def schedule(cron: str) -> CronTriggerTimetable:
    """A cron schedule in America/Denver; each run's data interval is the instant it fires."""
    return CronTriggerTimetable(cron, timezone=TIMEZONE)


def _run_time(dag_run) -> pendulum.DateTime:
    when = getattr(dag_run, "data_interval_end", None) or dag_run.run_after
    return pendulum.instance(when).in_timezone(TIMEZONE)


def denver_ds(dag_run) -> str:
    """The run's date in America/Denver, YYYY-MM-DD (the extractor marker's date)."""
    return _run_time(dag_run).strftime("%Y-%m-%d")


def season_for(day: datetime) -> int:
    """CFB season of a date: January and February belong to the previous season (as the extractor's)."""
    return day.year - 1 if day.month < 3 else day.year


def cfb_season(dag_run) -> int:
    return season_for(_run_time(dag_run))


def batch_id(job: str, dag_run, ti) -> str:
    """Dataproc batch ID, unique per run and try: sa-<job>-<run time UTC>-<run hash>-<try>.

    Batch IDs must be 4-63 characters of [a-z0-9-]; a retry gets a new ID, so it submits a
    new batch instead of attaching to the failed one.
    """
    stamp = pendulum.instance(dag_run.run_after).in_timezone("UTC").strftime("%Y%m%dt%H%M%S")
    run_hash = hashlib.sha1(dag_run.run_id.encode()).hexdigest()[:6]
    value = f"sa-{job.replace('_', '-')}-{stamp}-{run_hash}-{ti.try_number}".lower()
    if not _BATCH_ID.match(value):
        raise ValueError(f"invalid Dataproc batch id {value!r}")
    return value


def drain_image() -> str:
    """The drain image reference from Airflow Variable spark_drain_image (env AIRFLOW_VAR_SPARK_DRAIN_IMAGE)."""
    image = (Variable.get(DRAIN_IMAGE_VARIABLE, default=None) or "").strip()
    if not image or "/" not in image or not re.search(r"[:@]", image.rsplit("/", 1)[-1]) or " " in image:
        raise ValueError(
            f"Airflow Variable {DRAIN_IMAGE_VARIABLE} is not set to a full image reference "
            f"(got {image!r}). Set env AIRFLOW_VAR_SPARK_DRAIN_IMAGE on the Airflow pods to "
            "ord.ocir.io/<tenancy-namespace>/sportsagentic/spark-drain:<tag>."
        )
    return image


MACROS = {
    "sa_denver_ds": denver_ds,
    "sa_cfb_season": cfb_season,
    "sa_batch_id": batch_id,
    "sa_drain_image": drain_image,
}


def dag_kwargs(cron: str, **overrides) -> dict:
    """Arguments every lakehouse DAG shares."""
    kwargs = dict(
        schedule=schedule(cron),
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        tags=TAGS,
        user_defined_macros=MACROS,
    )
    kwargs.update(overrides)
    return kwargs
