"""The one batch spec (contract I4): builder, gcloud flags, and the DAGs' generated JSON copy."""

import json
import os
import subprocess
import sys

import pytest

from sa_lakehouse import batch_spec
from sa_lakehouse.config import Settings, catalog_conf

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GENERATED = os.path.join(ROOT, "dags", "lakehouse", "batch_spec.json")
GEN_SCRIPT = os.path.join(ROOT, "scripts", "gen-batch-spec.py")
PROJ = "sports-agent-511915"


def fill(value, project, jobs_ref, job, args):
    """What the DAGs do with batch_spec.json (dags/lakehouse/batches.py)."""
    if value == batch_spec.ARGS_PLACEHOLDER:
        return list(args)
    if isinstance(value, dict):
        return {k: fill(v, project, jobs_ref, job, args) for k, v in value.items()}
    if isinstance(value, list):
        return [fill(v, project, jobs_ref, job, args) for v in value]
    return (value.replace(batch_spec.PROJECT_PLACEHOLDER, project)
            .replace(batch_spec.JOBS_REF_PLACEHOLDER, jobs_ref).replace(batch_spec.JOB_PLACEHOLDER, job))


def test_build_batch_carries_the_contract():
    s = Settings(gcp_project=PROJ)
    b = batch_spec.build_batch(s, "build_gold", "current", ["--tables", "gold.game_summaries"])
    base = f"gs://{PROJ}-sa-jobs/jobs/current"
    assert b["pyspark_batch"] == {
        "main_python_file_uri": f"{base}/build_gold.py",
        "args": ["--tables", "gold.game_summaries"],
        "python_file_uris": [f"{base}/sa_lakehouse.zip"],
        "jar_file_uris": [
            f"gs://{PROJ}-sa-jobs/jars/iceberg-spark-runtime-3.5_2.13-1.10.2.jar",
            f"gs://{PROJ}-sa-jobs/jars/iceberg-gcp-bundle-1.10.2.jar",
        ],
    }
    assert b["runtime_config"]["version"] == "2.3"
    props = b["runtime_config"]["properties"]
    assert {k: props[k] for k in catalog_conf(s)} == catalog_conf(s)
    assert props["spark.dataproc.driverEnv.GCP_PROJECT"] == PROJ
    assert props["spark.dataproc.driverEnv.ICEBERG_CATALOG"] == "sa_catalog"
    assert props["spark.dynamicAllocation.maxExecutors"] == "2"
    assert "X-Iceberg-Access-Delegation" not in json.dumps(props)
    assert b["environment_config"] == {"execution_config": {
        "service_account": f"sa-spark-runner@{PROJ}.iam.gserviceaccount.com", "subnetwork_uri": "sa-dataproc"}}
    assert b["labels"] == {"app": "sportsagentic", "job": "build_gold"}


def test_gcloud_args_match_the_previous_submit_script():
    s = Settings(gcp_project=PROJ)
    args = batch_spec.gcloud_args(batch_spec.build_batch(s, "maintain", "abc1234", max_executors="4",
                                                         staging_bucket="gs://stage/"))
    assert args[0] == f"gs://{PROJ}-sa-jobs/jobs/abc1234/maintain.py"
    flags = dict(zip(args[1::2], args[2::2]))
    assert flags["--version"] == "2.3"
    assert flags["--py-files"] == f"gs://{PROJ}-sa-jobs/jobs/abc1234/sa_lakehouse.zip"
    assert flags["--jars"].count(",") == 1
    assert flags["--subnet"] == "sa-dataproc"
    assert flags["--labels"] == "app=sportsagentic,job=maintain"
    assert flags["--staging-bucket"] == "stage"
    assert flags["--properties"].startswith("^#^spark.sql.extensions=")
    assert flags["--properties"].endswith("#spark.dynamicAllocation.maxExecutors=4")


def test_cli_prints_one_argument_per_line(monkeypatch, capsys):
    monkeypatch.setenv("GCP_PROJECT", PROJ)
    monkeypatch.delenv("SA_MAX_EXECUTORS", raising=False)
    assert batch_spec.main(["--job", "nightly", "--jobs-ref", "current"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == f"gs://{PROJ}-sa-jobs/jobs/current/nightly.py"
    assert "--staging-bucket" not in lines and lines[lines.index("--labels") + 1] == "app=sportsagentic,job=nightly"
    with pytest.raises(SystemExit):
        batch_spec.main(["--job", "nope", "--jobs-ref", "current"])


def test_template_filled_equals_the_built_batch():
    with open(GENERATED) as fh:
        spec = json.load(fh)
    got = fill(spec, PROJ, "current", "nightly", [])
    assert got == batch_spec.build_batch(Settings(gcp_project=PROJ), "nightly", "current", [])
    assert "${" not in json.dumps(got)


def test_committed_json_matches_generated():
    """Fails while dags/lakehouse/batch_spec.json is stale: run scripts/gen-batch-spec.py."""
    with open(GENERATED) as fh:
        committed = fh.read()
    assert committed == json.dumps(batch_spec.template(), indent=2) + "\n"
    r = subprocess.run([sys.executable, GEN_SCRIPT, "--check"], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
