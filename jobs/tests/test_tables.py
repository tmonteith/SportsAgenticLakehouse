from sa_lakehouse.config import catalog_conf, settings_from_env, table
from sa_lakehouse.tables import TABLES, ensure_tables

import pytest

EXPECTED = {
    "bronze.api_responses_raw", "bronze._load_state", "bronze.live_game_events", "bronze.agent_events",
    "silver.games", "silver.plays", "silver.agent_requests", "silver.teams", "silver._rejects",
    "gold.team_efficiency_weekly", "gold.game_summaries", "gold.agent_usage_daily",
}


def test_every_contract_table_is_defined():
    assert set(TABLES) == EXPECTED


def test_ensure_tables_is_idempotent(clean_catalog):
    spark = clean_catalog
    assert set(ensure_tables(spark)) == EXPECTED
    assert ensure_tables(spark) == []


def test_silver_games_shape(clean_catalog):
    spark = clean_catalog
    ensure_tables(spark, ["silver.games"])
    cols = {f.name: f for f in spark.table(table("silver.games")).schema.fields}
    assert not cols["game_id"].nullable and not cols["_fetched_at"].nullable
    assert {"_source", "_ingested_at", "_record_hash"} <= set(cols)
    props = dict(spark.sql("SHOW TBLPROPERTIES sa_catalog.silver.games").collect())
    assert props["write.merge.mode"] == "merge-on-read"
    assert props["format-version"] == "2"


def test_tool_calls_struct(clean_catalog):
    spark = clean_catalog
    ensure_tables(spark, ["silver.agent_requests"])
    elem = spark.table(table("silver.agent_requests")).schema["tool_calls"].dataType.elementType
    assert [f.name for f in elem.fields] == ["tool", "started_at", "latency_ms", "status", "known_tool"]


def test_catalog_conf_matches_contract():
    conf = catalog_conf(settings_from_env({"GCP_PROJECT": "sports-agent-511915"}))
    c = "spark.sql.catalog.sa_catalog"
    assert conf[f"{c}.warehouse"] == "bl://projects/sports-agent-511915/catalogs/sa_catalog"
    assert conf[f"{c}.rest.auth.type"] == "org.apache.iceberg.gcp.auth.GoogleAuthManager"
    assert not any("Access-Delegation" in k for k in conf)


def test_settings_require_project():
    with pytest.raises(RuntimeError):
        settings_from_env({})


# BigQuery reserves these column-name prefixes (case-insensitive), and the BigLake catalog
# rejects a CreateTable whose schema uses one with a bare 400 INVALID_ARGUMENT.
BQ_RESERVED_PREFIXES = (
    "_TABLE_", "_FILE_", "_PARTITION", "_ROW_TIMESTAMP", "__ROOT__", "_COLIDENTIFIER",
    "_CHANGE_SEQUENCE_NUMBER", "_CHANGE_TYPE", "_CHANGE_TIMESTAMP",
)


def test_no_bigquery_reserved_column_names(clean_catalog):
    spark = clean_catalog
    ensure_tables(spark)

    def names(dtype, prefix=""):
        from pyspark.sql.types import ArrayType, MapType, StructType

        if isinstance(dtype, StructType):
            for f in dtype.fields:
                yield prefix + f.name, f.name
                yield from names(f.dataType, prefix + f.name + ".")
        elif isinstance(dtype, ArrayType):
            yield from names(dtype.elementType, prefix)
        elif isinstance(dtype, MapType):
            yield from names(dtype.valueType, prefix)

    bad = [
        f"{t}.{path}"
        for t in TABLES
        for path, leaf in names(spark.table(table(t)).schema)
        if leaf.upper().startswith(BQ_RESERVED_PREFIXES)
    ]
    assert bad == []
