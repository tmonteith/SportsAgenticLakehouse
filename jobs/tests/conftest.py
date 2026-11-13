"""Shared fixtures: one local Spark session with a local Iceberg catalog named sa_catalog.

Run through scripts/test.sh, which provides Java, PySpark and the Iceberg runtime jar.
The catalog is a Hadoop catalog in a temporary directory, so tests never touch GCP.
"""

from __future__ import annotations

import os
import shutil
import tempfile

import pytest

from sa_lakehouse.config import build_spark, local_catalog_conf
from sa_lakehouse.tables import NAMESPACES, ensure_local_namespaces


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="sa-warehouse-")
    conf = local_catalog_conf(f"file://{warehouse}")
    conf.update(
        {
            "spark.ui.enabled": "false",
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",
            "spark.driver.memory": os.environ.get("TEST_DRIVER_MEMORY", "1g"),
        }
    )
    jars = os.environ.get("SA_TEST_JARS")
    if jars:
        conf["spark.jars"] = jars
    session = build_spark("sa-lakehouse-tests", conf, master="local[2]")
    ensure_local_namespaces(session)
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def clean_catalog(spark):
    """Drop every table before a test that needs an empty catalog."""
    for ns in NAMESPACES:
        for row in spark.sql(f"SHOW TABLES IN sa_catalog.{ns}").collect():
            spark.sql(f"DROP TABLE IF EXISTS sa_catalog.{ns}.{row.tableName} PURGE")
    yield spark
