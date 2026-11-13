"""The streaming drain pod (contract H8): the same pod spec as the infra chart k8s/data/drain/
(``drain.podSpec`` with its default values), run by a KubernetesPodOperator in namespace ``data``.

Keep in step with SportsAgenticInfra k8s/data/drain/values.yaml and templates/_helpers.tpl.
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from lakehouse.common import gcp_project

NAMESPACE = "data"
SERVICE_ACCOUNT = "spark-drain"
KEY_SECRET = "gcp-key-spark-drain"
KEY_DIR = "/var/secrets/gcp"
PULL_SECRET = "ocir-pull"
CONTAINER = "drain"
DRIVER_MEMORY = "1400m"
TMP_SIZE_LIMIT = "2Gi"
ACTIVE_DEADLINE_SECONDS = 3600
SPARK_UID = 185  # the apache/spark image's "spark" user and group

LABELS = {
    "app.kubernetes.io/name": "spark-drain",
    "app.kubernetes.io/component": "spark-drain",
    "app.kubernetes.io/part-of": "sportsagentic",
    "sportsagentic.dev/run": "airflow",
}


def env_vars() -> list[k8s.V1EnvVar]:
    return [
        k8s.V1EnvVar(name="GCP_PROJECT", value=gcp_project()),
        k8s.V1EnvVar(name="ICEBERG_CATALOG", value=os.environ.get("ICEBERG_CATALOG", "").strip() or "sa_catalog"),
        k8s.V1EnvVar(
            name="KAFKA_BOOTSTRAP",
            value=os.environ.get("KAFKA_BOOTSTRAP", "").strip() or "sa-kafka-kafka-bootstrap.data:9092",
        ),
        k8s.V1EnvVar(name="GOOGLE_APPLICATION_CREDENTIALS", value=f"{KEY_DIR}/key.json"),
        k8s.V1EnvVar(name="SPARK_DRIVER_MEMORY", value=DRIVER_MEMORY),
    ]


def resources() -> k8s.V1ResourceRequirements:
    return k8s.V1ResourceRequirements(
        requests={"cpu": "250m", "memory": "2Gi"},
        limits={"cpu": "1000m", "memory": "2Gi"},
    )


def pod_security_context() -> k8s.V1PodSecurityContext:
    return k8s.V1PodSecurityContext(
        run_as_non_root=True,
        run_as_user=SPARK_UID,
        run_as_group=SPARK_UID,
        fs_group=SPARK_UID,
        seccomp_profile=k8s.V1SeccompProfile(type="RuntimeDefault"),
    )


def container_security_context() -> k8s.V1SecurityContext:
    return k8s.V1SecurityContext(
        allow_privilege_escalation=False,
        read_only_root_filesystem=True,
        capabilities=k8s.V1Capabilities(drop=["ALL"]),
    )


def volumes() -> list[k8s.V1Volume]:
    return [
        k8s.V1Volume(
            name="gcp-key",
            secret=k8s.V1SecretVolumeSource(secret_name=KEY_SECRET, default_mode=0o440),
        ),
        k8s.V1Volume(name="tmp", empty_dir=k8s.V1EmptyDirVolumeSource(size_limit=TMP_SIZE_LIMIT)),
    ]


def volume_mounts() -> list[k8s.V1VolumeMount]:
    return [
        k8s.V1VolumeMount(name="gcp-key", mount_path=KEY_DIR, read_only=True),
        k8s.V1VolumeMount(name="tmp", mount_path="/tmp"),
    ]


def drain_task(task_id: str = "drain", **kwargs) -> KubernetesPodOperator:
    """One drain run: both streams (drain.py defaults), then the pod exits."""
    params = dict(
        task_id=task_id,
        name="spark-drain",
        namespace=NAMESPACE,
        in_cluster=True,
        image="{{ sa_drain_image() }}",
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[k8s.V1LocalObjectReference(name=PULL_SECRET)],
        service_account_name=SERVICE_ACCOUNT,
        automount_service_account_token=False,
        base_container_name=CONTAINER,
        env_vars=env_vars(),
        container_resources=resources(),
        security_context=pod_security_context(),
        container_security_context=container_security_context(),
        volumes=volumes(),
        volume_mounts=volume_mounts(),
        labels=LABELS,
        active_deadline_seconds=ACTIVE_DEADLINE_SECONDS,
        # The first pull of the Spark image on a node can take minutes.
        startup_timeout_seconds=600,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        deferrable=False,  # the triggerer is disabled
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(seconds=ACTIVE_DEADLINE_SECONDS + 600),
    )
    params.update(kwargs)
    return KubernetesPodOperator(**params)
