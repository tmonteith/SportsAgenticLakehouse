"""silver.teams from ``/teams`` plus the Go alias map (contracts E and H6).

When new ``/teams`` responses arrive, teams are rebuilt from the latest landed response of
every season in bronze (``/teams`` is one small response per season), and :func:`sync_rows`
upserts only teams whose business columns changed.

- ``display_name`` = ``school`` from the team's most recent season;
- ``aliases`` = distinct lowercased ``alternateNames`` + ``abbreviation`` + ``school`` across the
  landed seasons, plus Go alias keys whose school equals the team's ``school``; sorted;
- ``conference_by_season`` = season -> ``conference`` (seasons without a conference omitted).
"""

from __future__ import annotations

import json
import os
import zipfile

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import DEFAULT_CATALOG, table
from .hashing import sync_rows, with_record_hash
from .landing import RAW, explode_payload
from .quality import TEAM_CHECKS, apply_checks
from .rejects import reject_rows

TEAMS = "silver.teams"
SOURCE = "cfbd:teams"
ALIASES_RESOURCE = "resources/team_aliases.json"

TEAM_PAYLOAD = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("school", T.StringType()),
        T.StructField("abbreviation", T.StringType()),
        T.StructField("alternateNames", T.ArrayType(T.StringType())),
        T.StructField("conference", T.StringType()),
    ]
)


def load_go_aliases() -> dict[str, str]:
    """alias -> school from ``resources/team_aliases.json``, beside the package on disk or inside
    the ``sa_lakehouse.zip`` the batch is submitted with."""
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(os.path.dirname(here), ALIASES_RESOURCE)
    if os.path.exists(path):
        with open(path) as fh:
            return json.load(fh)["aliases"]
    archive = getattr(globals().get("__loader__"), "archive", None)
    if archive:
        with zipfile.ZipFile(archive) as zf:
            return json.loads(zf.read(ALIASES_RESOURCE))["aliases"]
    raise FileNotFoundError(f"{ALIASES_RESOURCE} not found next to {here}")


def latest_response_per_season(bronze: DataFrame) -> DataFrame:
    w = Window.partitionBy("season")
    newest = F.max(F.struct("fetched_at", "_file_name")).over(w)
    return bronze.withColumn("__newest", newest).where(F.struct("fetched_at", "_file_name") == F.col("__newest")).drop("__newest")


def map_teams(spark, records: DataFrame, go_aliases: dict[str, str]) -> DataFrame:
    """Records of the latest response per season -> silver.teams columns + ``__raw``."""
    r = F.col("r")
    per_season = records.select(
        r["id"].cast("string").alias("team_id"),
        F.col("season"),
        r["school"].alias("school"),
        r["conference"].alias("conference"),
        F.concat(
            F.coalesce(r["alternateNames"], F.array().cast("array<string>")),
            F.array(r["abbreviation"], r["school"]),
        ).alias("names"),
        F.col("fetched_at"),
        F.to_json(r).alias("__raw"),
    ).dropDuplicates(["team_id", "season"])

    go = spark.createDataFrame(
        [(school, alias) for alias, school in sorted(go_aliases.items())] or [("", "")],
        "go_school STRING, go_alias STRING",
    ).groupBy("go_school").agg(F.collect_list("go_alias").alias("go_aliases"))
    per_season = per_season.join(go, per_season["school"] == go["go_school"], "left").withColumn(
        "names", F.concat("names", F.coalesce("go_aliases", F.array().cast("array<string>")))
    )

    latest = Window.partitionBy("team_id").orderBy(F.col("season").desc())
    return (
        per_season.withColumn("__latest_school", F.first("school").over(latest))
        .withColumn("__latest_raw", F.first("__raw").over(latest))
        .groupBy("team_id")
        .agg(
            F.first("__latest_school").alias("display_name"),
            F.array_sort(
                F.array_distinct(F.expr("filter(transform(flatten(collect_list(names)), x -> lower(x)), x -> x IS NOT NULL AND x <> '')"))
            ).alias("aliases"),
            F.map_from_entries(
                F.array_sort(F.collect_list(F.when(F.col("conference").isNotNull(), F.struct("season", "conference"))))
            ).alias("conference_by_season"),
            F.max("fetched_at").alias("_fetched_at"),
            F.first("__latest_raw").alias("__raw"),
        )
        .select(
            "team_id",
            "display_name",
            "aliases",
            "conference_by_season",
            F.lit(SOURCE).alias("_source"),
            "_fetched_at",
            F.current_timestamp().alias("_ingested_at"),
            F.lit(None).cast("string").alias("_record_hash"),
            "__raw",
        )
    )


def build_teams(spark, bronze_new: DataFrame, catalog: str = DEFAULT_CATALOG) -> tuple[dict, DataFrame]:
    """Rebuild silver.teams from every landed season (called when new ``/teams`` rows arrived)."""
    _, bad = explode_payload(bronze_new, TEAM_PAYLOAD, table_name=TEAMS, source=SOURCE)
    all_teams = spark.table(table(RAW, catalog)).where(F.col("endpoint") == "teams")
    records, _ = explode_payload(latest_response_per_season(all_teams), TEAM_PAYLOAD, table_name=TEAMS, source=SOURCE)
    mapped = apply_checks(map_teams(spark, records, load_go_aliases()), TEAM_CHECKS).cache()
    try:
        input_rows = mapped.count()
        cols = spark.table(table(TEAMS, catalog)).columns
        good = with_record_hash(mapped.where("__reason IS NULL").select(*cols))
        result = {"input_rows": input_rows}
        result.update(sync_rows(spark, table(TEAMS, catalog), good, ["team_id"]))
        rejects = reject_rows(
            mapped.where("__reason IS NOT NULL"),
            table_name=TEAMS,
            reason=F.col("__reason"),
            record_key=F.col("team_id"),
            raw=F.col("__raw"),
            source=SOURCE,
        ).unionByName(bad).localCheckpoint()
        result["touched_seasons"] = []
        return result, rejects
    finally:
        mapped.unpersist()
