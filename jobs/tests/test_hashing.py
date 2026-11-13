import hashlib
import json
from datetime import datetime, timezone

from pyspark.sql import types as T

from sa_lakehouse.hashing import record_hash_col, record_hash_sql, with_record_hash


def py_hash(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


SCHEMA = T.StructType(
    [
        T.StructField("b_id", T.LongType()),
        T.StructField("a_name", T.StringType()),
        T.StructField("ts", T.TimestampType()),
        T.StructField("flag", T.BooleanType()),
        T.StructField("conf", T.MapType(T.IntegerType(), T.StringType())),
        T.StructField("tags", T.ArrayType(T.StringType())),
        T.StructField(
            "calls",
            T.ArrayType(T.StructType([T.StructField("z", T.IntegerType()), T.StructField("a", T.StringType())])),
        ),
        T.StructField("_source", T.StringType()),
        T.StructField("_record_hash", T.StringType()),
    ]
)


def test_hash_is_canonical_json_sha256(spark):
    ts = datetime(2025, 8, 23, 16, 0, tzinfo=timezone.utc)
    row = (7, None, ts, True, {2025: "SEC", 2024: "Pac-12"}, ["b", "a"], [(1, "x")], "cfbd:games", None)
    df = with_record_hash(spark.createDataFrame([row], SCHEMA))
    got = df.collect()[0]["_record_hash"]
    expected = py_hash(
        {
            "b_id": 7,
            "a_name": None,
            "ts": "2025-08-23T16:00:00.000000Z",
            "flag": True,
            "conf": {"2024": "Pac-12", "2025": "SEC"},
            "tags": ["b", "a"],
            "calls": [{"a": "x", "z": 1}],
        }
    )
    assert got == expected


def test_metadata_columns_do_not_change_the_hash(spark):
    rows = [(1, "a", None, None, None, None, None, "x", None), (1, "a", None, None, None, None, None, "y", "zzz")]
    hashes = {r[0] for r in spark.createDataFrame(rows, SCHEMA).select(record_hash_col(SCHEMA)).collect()}
    assert len(hashes) == 1


def test_map_insertion_order_does_not_matter(spark):
    df = spark.sql("SELECT map(2025, 'SEC', 2024, 'B') AS m1, map(2024, 'B', 2025, 'SEC') AS m2")
    t = T.MapType(T.IntegerType(), T.StringType())
    out = df.selectExpr(
        record_hash_sql({"m": ("m1", t)}) + " AS h1", record_hash_sql({"m": ("m2", t)}) + " AS h2"
    ).collect()[0]
    assert out.h1 == out.h2
