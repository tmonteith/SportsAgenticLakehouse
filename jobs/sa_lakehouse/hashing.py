"""Row hashing (contract H4) and hash-guarded writes.

``_record_hash`` is the lowercase hex SHA-256 of a row's business columns (every column whose
name does not start with ``_``) serialized as canonical JSON: keys sorted at every level, no
whitespace, timestamps as RFC 3339 UTC (``2025-08-23T16:00:00.000000Z``), nulls kept. Maps are
serialized with their entries sorted by key; array order is significant.

Everything here builds Spark SQL expressions, so the same hash is available on a DataFrame
(:func:`with_record_hash`), inside a MERGE condition or SET clause (:func:`record_hash_sql`),
and in the drain (WS4b imports this module).

:func:`sync_rows` is the one writer pattern the batch jobs share: it makes a slice of a target
table equal to a new set of rows while writing only rows whose business columns changed, so
rebuilding unchanged input adds no Iceberg snapshot.
"""

from __future__ import annotations

import itertools
from typing import Iterable, Mapping, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"
JSON_OPTIONS = {
    "ignoreNullFields": "false",
    "timestampFormat": TIMESTAMP_FORMAT,
    "timestampNTZFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
    "dateFormat": "yyyy-MM-dd",
    "timeZone": "UTC",
}
HASH_COLUMN = "_record_hash"

_lambda_ids = itertools.count()


def quote(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def sql_string(value: str) -> str:
    """A Spark SQL string literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def business_fields(schema: T.StructType) -> list[T.StructField]:
    """Columns that make up a row's identity and content: every column not starting with ``_``."""
    return [f for f in schema.fields if not f.name.startswith("_")]


def _needs_canonical(dtype: T.DataType) -> bool:
    if isinstance(dtype, (T.StructType, T.MapType)):
        return True
    if isinstance(dtype, T.ArrayType):
        return _needs_canonical(dtype.elementType)
    return False


def canonical_sql(expr: str, dtype: T.DataType, round_doubles: int | None = None) -> str:
    """Rewrite ``expr`` so its JSON form is canonical: struct fields sorted by name (recursively),
    map entries sorted by key. ``round_doubles`` rounds DOUBLE/FLOAT values (comparison use only)."""
    if isinstance(dtype, (T.DoubleType, T.FloatType)) and round_doubles is not None:
        return f"round({expr}, {round_doubles})"
    if isinstance(dtype, T.StructType):
        parts = ", ".join(
            f"'{f.name}', {canonical_sql(f'{expr}.{quote(f.name)}', f.dataType, round_doubles)}"
            for f in sorted(dtype.fields, key=lambda f: f.name)
        )
        return f"CASE WHEN {expr} IS NULL THEN NULL ELSE named_struct({parts}) END"
    if isinstance(dtype, T.ArrayType):
        inner = dtype.elementType
        if not _needs_canonical(inner) and not (round_doubles is not None and _has_double(inner)):
            return expr
        x = f"x{next(_lambda_ids)}"
        return f"transform({expr}, {x} -> {canonical_sql(x, inner, round_doubles)})"
    if isinstance(dtype, T.MapType):
        k, v, l, r = (f"{p}{next(_lambda_ids)}" for p in "kvlr")
        value = canonical_sql(v, dtype.valueType, round_doubles)
        mapped = expr if value == v else f"transform_values({expr}, ({k}, {v}) -> {value})"
        cmp = f"CASE WHEN {l}.key < {r}.key THEN -1 WHEN {l}.key > {r}.key THEN 1 ELSE 0 END"
        return f"map_from_entries(array_sort(map_entries({mapped}), ({l}, {r}) -> {cmp}))"
    return expr


def _has_double(dtype: T.DataType) -> bool:
    if isinstance(dtype, (T.DoubleType, T.FloatType)):
        return True
    if isinstance(dtype, T.ArrayType):
        return _has_double(dtype.elementType)
    if isinstance(dtype, T.MapType):
        return _has_double(dtype.valueType)
    if isinstance(dtype, T.StructType):
        return any(_has_double(f.dataType) for f in dtype.fields)
    return False


def canonical_json_sql(values: Mapping[str, tuple[str, T.DataType]], round_doubles: int | None = None) -> str:
    """SQL for the canonical JSON of ``{name: (sql_expr, type)}`` (keys sorted, nulls kept)."""
    parts = ", ".join(
        f"'{name}', {canonical_sql(expr, dtype, round_doubles)}" for name, (expr, dtype) in sorted(values.items())
    )
    opts = ", ".join(f"{sql_string(k)}, {sql_string(v)}" for k, v in sorted(JSON_OPTIONS.items()))
    return f"to_json(named_struct({parts}), map({opts}))"


def record_hash_sql(values: Mapping[str, tuple[str, T.DataType]], round_doubles: int | None = None) -> str:
    """SQL expression for the H4 record hash of ``{name: (sql_expr, type)}``."""
    return f"sha2({canonical_json_sql(values, round_doubles)}, 256)"


def column_values(schema: T.StructType, qualifier: str | None = None) -> dict[str, tuple[str, T.DataType]]:
    """``{name: (qualified column reference, type)}`` for the business columns of ``schema``."""
    prefix = f"{qualifier}." if qualifier else ""
    return {f.name: (prefix + quote(f.name), f.dataType) for f in business_fields(schema)}


def record_hash_col(schema: T.StructType, round_doubles: int | None = None) -> Column:
    """Column computing the record hash over the business columns of ``schema``."""
    return F.expr(record_hash_sql(column_values(schema), round_doubles))


def with_record_hash(df: DataFrame, column: str = HASH_COLUMN) -> DataFrame:
    """Set ``column`` (default ``_record_hash``) from the DataFrame's business columns."""
    return df.withColumn(column, record_hash_col(df.schema))


# ---- hash-guarded writes ------------------------------------------------------------------

_view_ids = itertools.count()


def sync_rows(
    spark,
    target: str,
    new_df: DataFrame,
    keys: Sequence[str],
    *,
    slice_filter: str | None = None,
    prune: Iterable[str] = (),
    round_doubles: int | None = None,
) -> dict[str, int]:
    """Make ``target`` agree with ``new_df`` in one MERGE, writing only changed rows.

    - Rows of ``new_df`` whose key is new are inserted; rows whose business columns changed are
      updated (compared through ``_record_hash`` when the target has it, else through a hash
      computed on the fly, with doubles rounded to ``round_doubles`` places).
    - With ``slice_filter`` (a SQL predicate over target columns), target rows inside the slice
      whose key is missing from ``new_df`` are deleted: the slice is replaced. Without it the
      write is an upsert.
    - ``prune`` columns are added to the MERGE ON clause so Iceberg can prune partitions and
      files (they must be stable attributes of a key, e.g. ``season``).
    - When nothing differs, no MERGE runs, so no snapshot is created.

    ``new_df`` must carry every target column. Returns ``{"inserted", "updated", "deleted"}``.
    """
    keys = list(keys)
    prune = [p for p in prune if p not in keys]
    tgt = spark.table(target)
    cols = tgt.columns
    has_hash = HASH_COLUMN in cols
    compare = (
        (lambda df: F.col(HASH_COLUMN))
        if has_hash
        else (lambda df: record_hash_col(df.schema, round_doubles))
    )

    new = new_df.select(*cols)
    new_h = new.withColumn("__h", compare(new))
    if slice_filter:
        old = tgt.where(slice_filter)
    else:
        old = tgt.join(new.select(*keys).distinct(), keys, "left_semi")
    old_h = old.select(*keys, *prune, compare(old).alias("__h"))

    old_keys = old_h.select(*keys).withColumn("__exists", F.lit(True))
    upserts = (
        new_h.join(old_h.select(*keys, "__h"), keys + ["__h"], "left_anti")
        .join(old_keys, keys, "left")
        .withColumn("__op", F.when(F.col("__exists"), F.lit("u")).otherwise(F.lit("i")))
        .drop("__h", "__exists")
    )
    src = upserts
    if slice_filter:
        deletes = old_h.select(*keys, *prune).join(new.select(*keys), keys, "left_anti").withColumn("__op", F.lit("d"))
        src = upserts.unionByName(deletes, allowMissingColumns=True)
    src = src.cache()
    try:
        counts = {r["__op"]: r["count"] for r in src.groupBy("__op").count().collect()}
        result = {"inserted": counts.get("i", 0), "updated": counts.get("u", 0), "deleted": counts.get("d", 0)}
        if not any(result.values()):
            return result
        view = f"__sync_src_{next(_view_ids)}"
        src.createOrReplaceTempView(view)
        on = " AND ".join(f"t.{quote(c)} = s.{quote(c)}" for c in keys + prune)
        changed = f" AND NOT (s.{HASH_COLUMN} <=> t.{HASH_COLUMN})" if has_hash else ""
        sets = ", ".join(f"t.{quote(c)} = s.{quote(c)}" for c in cols)
        names = ", ".join(quote(c) for c in cols)
        values = ", ".join(f"s.{quote(c)}" for c in cols)
        spark.sql(
            f"""
            MERGE INTO {target} t
            USING {view} s
            ON {on}
            WHEN MATCHED AND s.__op = 'd' THEN DELETE
            WHEN MATCHED AND s.__op IN ('i', 'u'){changed} THEN UPDATE SET {sets}
            WHEN NOT MATCHED AND s.__op IN ('i', 'u') THEN INSERT ({names}) VALUES ({values})
            """
        )
        spark.catalog.dropTempView(view)
        return result
    finally:
        src.unpersist()


# ---- write bookkeeping ---------------------------------------------------------------------
# Watermarks live in Iceberg table properties: changing a property writes new table metadata
# but no snapshot, and the property is only changed when a job actually consumed new input.


def get_table_property(spark, target: str, key: str) -> str | None:
    props = {r[0]: r[1] for r in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()}
    return props.get(key)


def set_table_property(spark, target: str, key: str, value: str) -> None:
    spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('{key}' = '{value}')")
