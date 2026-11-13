"""The one Serverless for Apache Spark batch spec (contracts H1, H2, I4).

:func:`build_batch` returns a Dataproc ``Batch`` as the dict ``DataprocCreateBatchOperator(batch=...)``
accepts (proto field names). ``scripts/submit-batch.sh`` turns it into ``gcloud`` flags with
:func:`gcloud_args`; ``scripts/gen-batch-spec.py`` writes it, with placeholders, to
``dags/lakehouse/batch_spec.json`` for the Airflow DAGs (Airflow cannot import this package).

Command line (used by submit-batch.sh; prints one gcloud argument per line, the main file first)::

    GCP_PROJECT=<proj> python3 -m sa_lakehouse.batch_spec --job build_gold --jobs-ref <sha> [--staging-bucket gs://...]

``SA_MAX_EXECUTORS`` (default 2) caps dynamic allocation; ``ICEBERG_CATALOG`` names the catalog.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Sequence

from .config import Settings, catalog_conf, settings_from_env

RUNTIME_VERSION = "2.3"  # Spark 3.5, Scala 2.13, Python 3.11
SUBNET = "sa-dataproc"
ICEBERG_VERSION = "1.10.2"
SCALA = "2.13"
# Driver + 2 executors (4 vCPUs each) = 12 DCUs. Without a cap, dynamic allocation scales to
# the project's 32-vCPU CPUS_ALL_REGIONS quota and the next batch cannot start until those
# workers are released.
DEFAULT_MAX_EXECUTORS = "2"
JOBS = ("create_tables", "load_landing", "build_silver", "build_gold", "maintain", "nightly")

# Placeholders in dags/lakehouse/batch_spec.json, filled by the DAGs.
PROJECT_PLACEHOLDER = "${GCP_PROJECT}"
JOBS_REF_PLACEHOLDER = "${JOBS_REF}"
JOB_PLACEHOLDER = "${JOB}"
ARGS_PLACEHOLDER = "${ARGS}"


def jobs_base(settings: Settings, jobs_ref: str) -> str:
    """``gs://<proj>-sa-jobs/jobs/<ref>``: a git short SHA, or ``current`` (contract I3)."""
    return f"gs://{settings.jobs_bucket}/jobs/{jobs_ref}"


def jar_uris(settings: Settings) -> list[str]:
    jars = f"gs://{settings.jobs_bucket}/jars"
    return [
        f"{jars}/iceberg-spark-runtime-3.5_{SCALA}-{ICEBERG_VERSION}.jar",
        f"{jars}/iceberg-gcp-bundle-{ICEBERG_VERSION}.jar",
    ]


def properties(settings: Settings, max_executors: str = DEFAULT_MAX_EXECUTORS) -> dict[str, str]:
    """Catalog properties (H2), the driver's env (GCP_PROJECT, ICEBERG_CATALOG) and the executor cap."""
    props = dict(catalog_conf(settings))
    props["spark.dataproc.driverEnv.GCP_PROJECT"] = settings.gcp_project
    props["spark.dataproc.driverEnv.ICEBERG_CATALOG"] = settings.catalog
    props["spark.dynamicAllocation.maxExecutors"] = str(max_executors)
    return props


def service_account(settings: Settings) -> str:
    return f"sa-spark-runner@{settings.gcp_project}.iam.gserviceaccount.com"


def build_batch(
    settings: Settings,
    job: str,
    jobs_ref: str,
    args: Sequence[str] | str = (),
    max_executors: str = DEFAULT_MAX_EXECUTORS,
    staging_bucket: str | None = None,
) -> dict:
    """The Dataproc Batch for one job. ``args`` is a list, or ARGS_PLACEHOLDER for the template."""
    base = jobs_base(settings, jobs_ref)
    execution = {"service_account": service_account(settings), "subnetwork_uri": SUBNET}
    if staging_bucket:  # a bucket name; the API rejects a gs:// URI here
        execution["staging_bucket"] = staging_bucket.removeprefix("gs://").rstrip("/")
    return {
        "pyspark_batch": {
            "main_python_file_uri": f"{base}/{job}.py",
            "args": args if isinstance(args, str) else list(args),
            "python_file_uris": [f"{base}/sa_lakehouse.zip"],
            "jar_file_uris": jar_uris(settings),
        },
        "runtime_config": {"version": RUNTIME_VERSION, "properties": properties(settings, max_executors)},
        "environment_config": {"execution_config": execution},
        "labels": {"app": "sportsagentic", "job": job},
    }


def template() -> dict:
    """The batch with placeholders for the project, jobs ref, job and args (dags/lakehouse/batch_spec.json)."""
    settings = Settings(gcp_project=PROJECT_PLACEHOLDER)
    return build_batch(settings, JOB_PLACEHOLDER, JOBS_REF_PLACEHOLDER, ARGS_PLACEHOLDER)


def gcloud_args(batch: dict) -> list[str]:
    """``gcloud dataproc batches submit pyspark`` arguments for a batch: the main file, then the flags.

    The caller adds --project, --region, --batch and the job args. Properties use gcloud's
    ``^#^`` list delimiter, because values contain ':' and '/'.
    """
    py = batch["pyspark_batch"]
    props = batch["runtime_config"]["properties"]
    execution = batch["environment_config"]["execution_config"]
    bad = [k for k, v in props.items() if "#" in k + v]
    if bad:
        raise ValueError(f"'#' in properties: {bad}")
    out = [
        py["main_python_file_uri"],
        "--version", batch["runtime_config"]["version"],
        "--py-files", ",".join(py["python_file_uris"]),
        "--jars", ",".join(py["jar_file_uris"]),
        "--subnet", execution["subnetwork_uri"],
        "--service-account", execution["service_account"],
        "--labels", ",".join(f"{k}={v}" for k, v in batch["labels"].items()),
        "--properties", "^#^" + "#".join(f"{k}={v}" for k, v in props.items()),
    ]
    if execution.get("staging_bucket"):
        out += ["--staging-bucket", execution["staging_bucket"]]
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Print the gcloud arguments of one batch, one per line.")
    ap.add_argument("--job", required=True, choices=JOBS)
    ap.add_argument("--jobs-ref", required=True, help="published code version: a git short SHA or 'current'")
    ap.add_argument("--staging-bucket", help="optional Dataproc staging bucket")
    args = ap.parse_args(argv)
    batch = build_batch(
        settings_from_env(),
        args.job,
        args.jobs_ref,
        max_executors=os.environ.get("SA_MAX_EXECUTORS", "").strip() or DEFAULT_MAX_EXECUTORS,
        staging_bucket=args.staging_bucket,
    )
    for arg in gcloud_args(batch):
        if "\n" in arg:
            raise ValueError(f"newline in argument: {arg!r}")
        print(arg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
