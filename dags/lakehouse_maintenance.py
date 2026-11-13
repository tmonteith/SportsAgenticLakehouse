"""lakehouse_maintenance: Sundays 05:00 America/Denver, one Serverless batch runs maintain.py.

maintain.py with its defaults: every lakehouse table is compacted, old snapshots are expired
and orphan files removed.
"""

from __future__ import annotations

from airflow.sdk import DAG

from lakehouse.batches import batch_task
from lakehouse.common import dag_kwargs

with DAG(
    dag_id="lakehouse_maintenance",
    description="Weekly Iceberg maintenance (maintain.py) in one Serverless batch",
    doc_md=__doc__,
    **dag_kwargs("0 5 * * 0"),
):
    batch_task("maintain_batch", "maintain")
