"""Spark and Iceberg catalog configuration shared by every lakehouse job.

The catalog is the BigLake (Lakehouse runtime) Iceberg REST catalog ``sa_catalog`` in
end-user credential mode: every caller reads and writes storage with its own Application
Default Credentials (``GOOGLE_APPLICATION_CREDENTIALS`` on OKE, the batch service account
on Serverless for Apache Spark). No vended credentials, so ``X-Iceberg-Access-Delegation``
is never set.

Jobs call :func:`table` for every table name and never hard-code the catalog.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

REST_URI = "https://biglake.googleapis.com/iceberg/v1/restcatalog"
DEFAULT_CATALOG = "sa_catalog"
ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


@dataclass(frozen=True)
class Settings:
    """Environment the jobs run in (shared contract env vars)."""

    gcp_project: str
    catalog: str = DEFAULT_CATALOG
    kafka_bootstrap: str | None = None

    @property
    def landing_bucket(self) -> str:
        return f"{self.gcp_project}-sa-landing"

    @property
    def warehouse_bucket(self) -> str:
        return f"{self.gcp_project}-sa-warehouse"

    @property
    def gold_bucket(self) -> str:
        return f"{self.gcp_project}-sa-gold"

    @property
    def jobs_bucket(self) -> str:
        return f"{self.gcp_project}-sa-jobs"

    def checkpoint_dir(self, stream: str) -> str:
        return f"gs://{self.warehouse_bucket}/_checkpoints/{stream}/"


def settings_from_env(env: Mapping[str, str] | None = None) -> Settings:
    """Read GCP_PROJECT, ICEBERG_CATALOG and KAFKA_BOOTSTRAP. GCP_PROJECT is required."""
    env = os.environ if env is None else env
    project = env.get("GCP_PROJECT", "").strip()
    if not project:
        raise RuntimeError("GCP_PROJECT is not set")
    return Settings(
        gcp_project=project,
        catalog=env.get("ICEBERG_CATALOG", "").strip() or DEFAULT_CATALOG,
        kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", "").strip() or None,
    )


def catalog_conf(settings: Settings) -> dict[str, str]:
    """Spark properties for the BigLake Iceberg REST catalog (contract H2)."""
    c = f"spark.sql.catalog.{settings.catalog}"
    return {
        "spark.sql.extensions": ICEBERG_EXTENSIONS,
        c: "org.apache.iceberg.spark.SparkCatalog",
        f"{c}.type": "rest",
        f"{c}.uri": REST_URI,
        f"{c}.warehouse": f"bl://projects/{settings.gcp_project}/catalogs/{settings.catalog}",
        f"{c}.io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
        f"{c}.header.x-goog-user-project": settings.gcp_project,
        f"{c}.rest.auth.type": "org.apache.iceberg.gcp.auth.GoogleAuthManager",
    }


def local_catalog_conf(warehouse_path: str, catalog: str = DEFAULT_CATALOG) -> dict[str, str]:
    """A local Hadoop catalog with the same name, for tests and dry runs."""
    c = f"spark.sql.catalog.{catalog}"
    return {
        "spark.sql.extensions": ICEBERG_EXTENSIONS,
        c: "org.apache.iceberg.spark.SparkCatalog",
        f"{c}.type": "hadoop",
        f"{c}.warehouse": warehouse_path,
    }


def build_spark(app_name: str, conf: Mapping[str, str], master: str | None = None):
    """Create (or get) a SparkSession with the given properties.

    On Serverless the batch already has a session; properties passed with
    ``--properties`` win, and setting them again here is harmless.
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def table(name: str, catalog: str = DEFAULT_CATALOG) -> str:
    """Fully qualified Spark name: ``table("silver.games") -> "sa_catalog.silver.games"``."""
    if name.count(".") != 1:
        raise ValueError(f"expected <namespace>.<table>, got {name!r}")
    return f"{catalog}.{name}"
