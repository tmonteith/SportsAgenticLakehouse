"""DAG tests (contract I6). Run by scripts/test-dags.sh inside apache/airflow:3.2.2.

Standard library unittest only: the image has no pytest and the script installs nothing.
The DAG folder is loaded the way the DAG processor and task runner load it (BundleDagBag,
which puts the folder on sys.path so the DAG files import ``lakehouse``).
"""

from __future__ import annotations

import json
import os
import re
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace as NS
from unittest import mock

import pendulum
from airflow.dag_processing.dagbag import BundleDagBag
from airflow.serialization.encoders import coerce_to_core_timetable
from airflow.timetables.base import TimeRestriction
from kubernetes.client import ApiClient

ROOT = Path(__file__).resolve().parents[2]
DAGS = ROOT / "dags"
PROJ = os.environ["GCP_PROJECT"]
DRAIN_IMAGE = os.environ["AIRFLOW_VAR_SPARK_DRAIN_IMAGE"]
DENVER = "America/Denver"
BATCH_ID = re.compile(r"^[a-z0-9-]{4,63}$")

EXPECTED = {
    "cfb_nightly": "45 3 * * *",
    "stream_drain": "*/15 * * * *",
    "gold_after_live": "30 23 * 1,8-12 4-6",
    "lakehouse_maintenance": "0 5 * * 0",
}
BATCH_TASKS = {
    "cfb_nightly": "nightly_batch",
    "gold_after_live": "gold_game_summaries_batch",
    "lakehouse_maintenance": "maintain_batch",
}

_BAG = None


def bag() -> BundleDagBag:
    global _BAG
    if _BAG is None:
        _BAG = BundleDagBag(dag_folder=str(DAGS), bundle_path=DAGS, include_examples=False)
    return _BAG


def utc(*args) -> pendulum.DateTime:
    return pendulum.datetime(*args, tz="UTC")


def ctx(run_after, try_number=1, data_interval_end="same", run_id="scheduled__2026-11-14T10:45:00+00:00"):
    end = run_after if data_interval_end == "same" else data_interval_end
    return {"dag_run": NS(run_after=run_after, data_interval_end=end, run_id=run_id), "ti": NS(try_number=try_number)}


def render(dag, task, value, context):
    return task.render_template(value, context, jinja_env=dag.get_template_env())


def next_runs(dag, after, n=3):
    tt = coerce_to_core_timetable(dag.timetable)
    out, last = [], None
    for _ in range(n):
        info = tt.next_dagrun_info(
            last_automated_data_interval=last,
            restriction=TimeRestriction(earliest=after, latest=None, catchup=False),
        )
        out.append(info.run_after)
        last, after = info.data_interval, info.run_after.add(seconds=1)
    return out


class DagBagTest(unittest.TestCase):
    def test_no_import_errors_and_four_dags(self):
        self.assertEqual(bag().import_errors, {})
        self.assertEqual(sorted(bag().dags), sorted(EXPECTED))

    def test_helper_package_is_not_parsed_as_dags(self):
        files = {Path(s.file).name for s in bag().dagbag_stats}
        self.assertEqual(files, {f"{d}.py" for d in EXPECTED})

    def test_common_dag_settings(self):
        for dag_id, cron in EXPECTED.items():
            with self.subTest(dag=dag_id):
                dag = bag().dags[dag_id]
                tt = coerce_to_core_timetable(dag.timetable)
                self.assertEqual(type(tt).__name__, "CronTriggerTimetable")
                self.assertEqual(tt.summary, cron)
                self.assertEqual(tt._timezone.name, DENVER)
                self.assertEqual(str(dag.timezone), DENVER)
                self.assertFalse(dag.catchup)
                self.assertEqual(dag.max_active_runs, 1)
                self.assertIn("lakehouse", dag.tags)
                for task in dag.tasks:
                    self.assertFalse(getattr(task, "deferrable", False), task.task_id)

    def test_cfb_nightly_runs_at_0345_denver_with_a_4h_timeout(self):
        dag = bag().dags["cfb_nightly"]
        self.assertEqual(dag.dagrun_timeout, timedelta(hours=4))
        # 03:45 MST = 10:45 UTC; after the DST change in March, 03:45 MDT = 09:45 UTC.
        self.assertEqual(next_runs(dag, utc(2026, 11, 13, 12), 2), [utc(2026, 11, 14, 10, 45), utc(2026, 11, 15, 10, 45)])
        self.assertEqual(next_runs(dag, utc(2027, 3, 20), 1), [utc(2027, 3, 20, 9, 45)])

    def test_gold_after_live_fires_thu_fri_sat_aug_to_jan(self):
        dag = bag().dags["gold_after_live"]
        runs = next_runs(dag, utc(2026, 11, 16), 4)  # Monday 2026-11-16
        local = [r.in_timezone(DENVER) for r in runs]
        self.assertEqual([d.strftime("%a %H:%M") for d in local], ["Thu 23:30", "Fri 23:30", "Sat 23:30", "Thu 23:30"])
        # Nothing from February to July: after the last January Saturday comes Thursday 2027-08-05.
        after_jan = next_runs(dag, utc(2027, 1, 31, 12), 1)[0].in_timezone(DENVER)
        self.assertEqual((after_jan.year, after_jan.month, after_jan.day), (2027, 8, 5))

    def test_maintenance_runs_sunday_0500_denver(self):
        run = next_runs(bag().dags["lakehouse_maintenance"], utc(2026, 11, 13), 1)[0].in_timezone(DENVER)
        self.assertEqual(run.strftime("%a %H:%M"), "Sun 05:00")


class BatchTaskTest(unittest.TestCase):
    def test_batch_tasks_retry_twice_with_backoff(self):
        for dag_id, task_id in BATCH_TASKS.items():
            with self.subTest(dag=dag_id):
                t = bag().dags[dag_id].get_task(task_id)
                self.assertEqual(type(t).__name__, "DataprocCreateBatchOperator")
                self.assertEqual(t.retries, 2)
                self.assertGreater(t.retry_exponential_backoff, 1)
                self.assertEqual(t.region, "us-central1")
                self.assertEqual(t.project_id, PROJ)
                self.assertFalse(t.deferrable)
                self.assertFalse(t.asynchronous)

    def check_batch(self, batch, job, args):
        base = f"gs://{PROJ}-sa-jobs/jobs/current"
        py = batch["pyspark_batch"]
        self.assertEqual(py["main_python_file_uri"], f"{base}/{job}.py")
        self.assertEqual(py["args"], args)
        self.assertEqual(py["python_file_uris"], [f"{base}/sa_lakehouse.zip"])
        self.assertEqual(py["jar_file_uris"], [
            f"gs://{PROJ}-sa-jobs/jars/iceberg-spark-runtime-3.5_2.13-1.10.2.jar",
            f"gs://{PROJ}-sa-jobs/jars/iceberg-gcp-bundle-1.10.2.jar",
        ])
        self.assertEqual(batch["runtime_config"]["version"], "2.3")
        props = batch["runtime_config"]["properties"]
        self.assertEqual(props["spark.sql.catalog.sa_catalog.warehouse"], f"bl://projects/{PROJ}/catalogs/sa_catalog")
        self.assertEqual(props["spark.sql.catalog.sa_catalog.header.x-goog-user-project"], PROJ)
        self.assertEqual(props["spark.dataproc.driverEnv.GCP_PROJECT"], PROJ)
        self.assertEqual(props["spark.dataproc.driverEnv.ICEBERG_CATALOG"], "sa_catalog")
        self.assertEqual(props["spark.dynamicAllocation.maxExecutors"], "2")
        self.assertEqual(batch["environment_config"]["execution_config"], {
            "service_account": f"sa-spark-runner@{PROJ}.iam.gserviceaccount.com", "subnetwork_uri": "sa-dataproc"})
        self.assertEqual(batch["labels"], {"app": "sportsagentic", "job": job})
        self.assertNotIn("${", json.dumps(batch))
        self.assertNotIn("{{", json.dumps(batch))

    def rendered(self, dag_id, context):
        dag = bag().dags[dag_id]
        t = dag.get_task(BATCH_TASKS[dag_id])
        return render(dag, t, t.batch, context), render(dag, t, t.batch_id, context)

    def test_nightly_batch(self):
        batch, bid = self.rendered("cfb_nightly", ctx(utc(2026, 11, 14, 10, 45)))
        self.check_batch(batch, "nightly", [])
        self.assertTrue(bid.startswith("sa-nightly-20261114t104500-"), bid)
        self.assertTrue(bid.endswith("-1"))
        self.assertRegex(bid, BATCH_ID)

    def test_maintenance_batch(self):
        batch, bid = self.rendered("lakehouse_maintenance", ctx(utc(2026, 11, 15, 12)))
        self.check_batch(batch, "maintain", [])
        self.assertTrue(bid.startswith("sa-maintain-20261115t120000-"), bid)

    def test_gold_after_live_batch_and_season(self):
        cases = [
            (utc(2026, 11, 14, 6, 30), "2026"),  # Fri 2026-11-13 23:30 MST
            (utc(2027, 1, 2, 6, 30), "2026"),    # Fri 2027-01-01 23:30 MST: January is last season
            (utc(2026, 8, 30, 5, 30), "2026"),   # Sat 2026-08-29 23:30 MDT
            # Denver date decides, not UTC: Feb 28 23:30 MST is Mar 1 06:30 UTC (manual run).
            (utc(2027, 3, 1, 6, 30), "2026"),
            (utc(2027, 3, 2, 6, 30), "2027"),
        ]
        for when, season in cases:
            with self.subTest(when=when):
                batch, bid = self.rendered("gold_after_live", ctx(when))
                self.check_batch(batch, "build_gold", ["--tables", "gold.game_summaries", "--seasons", season])
                self.assertTrue(bid.startswith("sa-gold-after-live-"), bid)
                self.assertRegex(bid, BATCH_ID)

    def test_batch_id_is_unique_per_try_and_run(self):
        when = utc(2026, 11, 14, 10, 45)
        ids = {
            self.rendered("cfb_nightly", ctx(when, try_number=n, run_id=run_id))[1]
            for n in (1, 2, 3)
            for run_id in ("scheduled__2026-11-14T10:45:00+00:00", "manual__2026-11-14T10:45:00+00:00")
        }
        self.assertEqual(len(ids), 6)
        for bid in ids:
            self.assertRegex(bid, BATCH_ID)
            self.assertEqual(bid, bid.lower())


class NightlySensorTest(unittest.TestCase):
    def setUp(self):
        self.dag = bag().dags["cfb_nightly"]
        self.sensor = self.dag.get_task("wait_for_extractor_marker")

    def obj(self, context):
        return render(self.dag, self.sensor, self.sensor.object, context)

    def test_sensor_settings(self):
        s = self.sensor
        self.assertEqual(type(s).__name__, "GCSObjectExistenceSensor")
        self.assertEqual(s.bucket, f"{PROJ}-sa-landing")
        self.assertEqual(s.mode, "reschedule")
        self.assertEqual(s.poke_interval, 300)
        self.assertEqual(s.timeout, 3 * 60 * 60)
        self.assertEqual(self.dag.get_task("nightly_batch").upstream_task_ids, {"wait_for_extractor_marker"})

    def test_marker_is_the_denver_date_of_the_0345_run(self):
        # The run that fires at 03:45 Denver on 2026-11-14 waits for that day's marker.
        self.assertEqual(self.obj(ctx(utc(2026, 11, 14, 10, 45))), "_markers/extractor-nightly/2026-11-14.json")
        self.assertEqual(self.obj(ctx(utc(2026, 7, 4, 9, 45))), "_markers/extractor-nightly/2026-07-04.json")

    def test_manual_run_without_logical_date_uses_run_after_in_denver(self):
        # 20:00 Denver on 2026-11-14 is 03:00 UTC on 2026-11-15.
        context = ctx(utc(2026, 11, 15, 3, 0), data_interval_end=None, run_id="manual__x")
        self.assertEqual(self.obj(context), "_markers/extractor-nightly/2026-11-14.json")


class DrainTaskTest(unittest.TestCase):
    def setUp(self):
        self.dag = bag().dags["stream_drain"]
        self.task = self.dag.get_task("drain")

    def pod(self):
        task = self.task
        image = render(self.dag, task, task.image, ctx(utc(2026, 11, 14)))
        # No cluster here: the hook only reports whether it runs in-cluster (a pod label).
        fake_hook = mock.PropertyMock(return_value=NS(is_in_cluster=True))
        with mock.patch.object(task, "image", image), mock.patch.object(type(task), "hook", new_callable=lambda: fake_hook):
            pod = task.build_pod_request_obj()
        return ApiClient().sanitize_for_serialization(pod)

    def test_operator_settings(self):
        t = self.task
        self.assertEqual(type(t).__name__, "KubernetesPodOperator")
        self.assertEqual(t.namespace, "data")
        self.assertTrue(t.in_cluster)
        self.assertTrue(t.get_logs)
        self.assertEqual(str(t.on_finish_action.value), "delete_succeeded_pod")
        self.assertEqual(t.retries, 1)
        self.assertFalse(t.deferrable)

    def test_pod_spec_matches_the_drain_chart(self):
        pod = self.pod()
        spec = pod["spec"]
        self.assertEqual(pod["metadata"]["namespace"], "data")
        self.assertEqual(spec["serviceAccountName"], "spark-drain")
        self.assertIs(spec["automountServiceAccountToken"], False)
        self.assertEqual(spec["restartPolicy"], "Never")
        self.assertEqual(spec["imagePullSecrets"], [{"name": "ocir-pull"}])
        self.assertEqual(spec["securityContext"], {
            "runAsNonRoot": True, "runAsUser": 185, "runAsGroup": 185, "fsGroup": 185,
            "seccompProfile": {"type": "RuntimeDefault"}})
        self.assertEqual(spec["activeDeadlineSeconds"], 3600)
        self.assertEqual(spec["volumes"], [
            {"name": "gcp-key", "secret": {"secretName": "gcp-key-spark-drain", "defaultMode": 0o440}},
            {"name": "tmp", "emptyDir": {"sizeLimit": "2Gi"}},
        ])
        (c,) = spec["containers"]
        self.assertEqual(c["name"], "drain")
        self.assertEqual(c["image"], DRAIN_IMAGE)
        self.assertEqual(c["imagePullPolicy"], "IfNotPresent")
        # No command or args (empty lists mean the same): the image's entrypoint runs drain.py with its defaults.
        self.assertFalse(c.get("command"))
        self.assertFalse(c.get("args"))
        self.assertEqual({e["name"]: e["value"] for e in c["env"]}, {
            "GCP_PROJECT": PROJ,
            "ICEBERG_CATALOG": "sa_catalog",
            "KAFKA_BOOTSTRAP": "sa-kafka-kafka-bootstrap.data:9092",
            "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/gcp/key.json",
            "SPARK_DRIVER_MEMORY": "1400m",
        })
        self.assertEqual(c["resources"], {"requests": {"cpu": "250m", "memory": "2Gi"},
                                          "limits": {"cpu": "1000m", "memory": "2Gi"}})
        self.assertEqual(c["securityContext"], {
            "allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}})
        self.assertEqual(c["volumeMounts"], [
            {"name": "gcp-key", "mountPath": "/var/secrets/gcp", "readOnly": True},
            {"name": "tmp", "mountPath": "/tmp"},
        ])
        self.assertEqual(pod["metadata"]["labels"]["app.kubernetes.io/name"], "spark-drain")

    def test_missing_image_variable_fails_with_a_clear_message(self):
        for value in (None, "", "spark-drain", "abc1234"):
            with self.subTest(value=value), mock.patch.dict(os.environ):
                os.environ.pop("AIRFLOW_VAR_SPARK_DRAIN_IMAGE", None)
                if value is not None:
                    os.environ["AIRFLOW_VAR_SPARK_DRAIN_IMAGE"] = value
                with self.assertRaisesRegex(Exception, "spark_drain_image.*AIRFLOW_VAR_SPARK_DRAIN_IMAGE"):
                    render(self.dag, self.task, self.task.image, ctx(utc(2026, 11, 14)))


if __name__ == "__main__":
    unittest.main()
