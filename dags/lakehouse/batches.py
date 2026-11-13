"""Serverless for Apache Spark batch tasks (contracts I3, I4).

``batch_spec.json`` is generated from ``jobs/sa_lakehouse/batch_spec.py`` by
``scripts/gen-batch-spec.py`` (the same batch ``scripts/submit-batch.sh`` submits). The DAGs
fill its placeholders and always submit the code published at ``gs://<proj>-sa-jobs/jobs/current/``.
"""

from __future__ import annotations

import json
import os
from datetime import timedelta
from functools import lru_cache
from typing import Sequence

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

from lakehouse.common import REGION, gcp_project

SPEC_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "batch_spec.json")
JOBS_REF = "current"
PROJECT, JOBS_REF_TOKEN, JOB, ARGS = "${GCP_PROJECT}", "${JOBS_REF}", "${JOB}", "${ARGS}"

# Batch tasks: 2 retries with exponential backoff from 5 minutes (each retry is a new batch, new ID).
BATCH_RETRIES = 2
BATCH_RETRY_DELAY = timedelta(minutes=5)
BATCH_BACKOFF = 2.0  # retry_exponential_backoff is a multiplier in Airflow 3.2 (0 = off)
BATCH_MAX_RETRY_DELAY = timedelta(minutes=30)
BATCH_TIMEOUT = timedelta(hours=1)  # per try; a normal batch takes a few minutes
POLL_SECONDS = 30


@lru_cache(maxsize=1)
def load_spec() -> dict:
    with open(SPEC_PATH) as fh:
        return json.load(fh)


def _fill(value, project: str, jobs_ref: str, job: str, args: list[str]):
    if value == ARGS:
        return list(args)
    if isinstance(value, dict):
        return {k: _fill(v, project, jobs_ref, job, args) for k, v in value.items()}
    if isinstance(value, list):
        return [_fill(v, project, jobs_ref, job, args) for v in value]
    if isinstance(value, str):
        return value.replace(PROJECT, project).replace(JOBS_REF_TOKEN, jobs_ref).replace(JOB, job)
    return value


def batch(job: str, args: Sequence[str] = (), project: str | None = None, jobs_ref: str = JOBS_REF) -> dict:
    """The Dataproc Batch dict for ``gs://<proj>-sa-jobs/jobs/<jobs_ref>/<job>.py <args>``."""
    filled = _fill(load_spec(), project or gcp_project(), jobs_ref, job, list(args))
    if "${" in json.dumps(filled):
        raise ValueError("batch_spec.json has a placeholder the DAGs do not fill")
    return filled


def batch_task(task_id: str, job: str, args: Sequence[str] = (), id_prefix: str | None = None, **kwargs):
    """A non-deferrable DataprocCreateBatchOperator for one job.

    The batch ID is rendered per try from ``id_prefix`` (default: the job name). ``args`` may
    hold Jinja templates (``batch`` is a templated field).
    """
    prefix = (id_prefix or job).replace("_", "-")
    params = dict(
        task_id=task_id,
        project_id=gcp_project(),
        region=REGION,
        batch=batch(job, args),
        batch_id="{{ sa_batch_id('%s', dag_run, ti) }}" % prefix,
        deferrable=False,  # the triggerer is disabled
        asynchronous=False,
        polling_interval_seconds=POLL_SECONDS,
        retries=BATCH_RETRIES,
        retry_delay=BATCH_RETRY_DELAY,
        retry_exponential_backoff=BATCH_BACKOFF,
        max_retry_delay=BATCH_MAX_RETRY_DELAY,
        execution_timeout=BATCH_TIMEOUT,
    )
    params.update(kwargs)
    return DataprocCreateBatchOperator(**params)
