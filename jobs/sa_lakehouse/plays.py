"""silver.plays from ``/plays`` (contract H6).

Per build, the plays of every game present in the new bronze rows are replaced: for each game
the latest landed response wins, plays are numbered (``play_sequence``), team names are
resolved to ``team_id`` through the game, quality checks run, and :func:`sync_rows` makes the
game's rows in silver equal the new set (insert, update changed, delete missing) in one MERGE.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import DEFAULT_CATALOG, table
from .games import GAME_PAYLOAD, GAMES
from .hashing import sync_rows, with_record_hash
from .landing import RAW, explode_payload
from .quality import PLAY_CHECKS, apply_checks
from .rejects import reject_rows

PLAYS = "silver.plays"
SOURCE = "cfbd:plays"

PLAY_PAYLOAD = T.StructType(
    [
        T.StructField("id", T.StringType()),
        T.StructField("gameId", T.LongType()),
        T.StructField("driveId", T.StringType()),
        T.StructField("driveNumber", T.IntegerType()),
        T.StructField("playNumber", T.IntegerType()),
        T.StructField("offense", T.StringType()),
        T.StructField("defense", T.StringType()),
        T.StructField("home", T.StringType()),
        T.StructField("away", T.StringType()),
        T.StructField("period", T.IntegerType()),
        T.StructField(
            "clock", T.StructType([T.StructField("minutes", T.IntegerType()), T.StructField("seconds", T.IntegerType())])
        ),
        T.StructField("down", T.IntegerType()),
        T.StructField("distance", T.IntegerType()),
        T.StructField("yardsToGoal", T.IntegerType()),
        T.StructField("yardsGained", T.IntegerType()),
        T.StructField("scoring", T.BooleanType()),
        T.StructField("playType", T.StringType()),
        T.StructField("playText", T.StringType()),
        T.StructField("ppa", T.DoubleType()),
    ]
)

# H6 play_type enum: exact raw values first, then the prefix/substring rules, then "other".
PLAY_TYPES: dict[str, tuple[str, ...]] = {
    "rush": ("Rush", "Rushing Touchdown"),
    "pass": (
        "Pass Reception", "Pass Incompletion", "Pass Completion", "Passing Touchdown", "Interception",
        "Pass Interception Return", "Interception Return Touchdown",
    ),
    "sack": ("Sack",),
    "penalty": ("Penalty",),
    "punt": ("Punt", "Blocked Punt", "Blocked Punt Touchdown", "Punt Return", "Punt Return Touchdown"),
    "kickoff": ("Kickoff", "Kickoff Return (Offense)", "Kickoff Return Touchdown"),
    "field_goal": (
        "Field Goal Good", "Field Goal Missed", "Blocked Field Goal", "Blocked Field Goal Touchdown",
        "Missed Field Goal Return", "Missed Field Goal Return Touchdown",
    ),
    "fumble_recovery": ("Fumble Recovery (Own)", "Fumble Recovery (Opponent)", "Fumble Return Touchdown"),
    "safety": ("Safety",),
    "timeout": ("Timeout",),
    "administrative": ("End Period", "End of Half", "End of Game", "End of Regulation"),
}
TURNOVERS = (
    "Interception", "Pass Interception Return", "Interception Return Touchdown",
    "Fumble Recovery (Opponent)", "Fumble Return Touchdown",
)


def _sql_list(values) -> str:
    return ", ".join("'" + v.replace("'", "\\'") + "'" for v in values)


def play_type_sql(raw: str = "play_type_raw") -> str:
    exact = " ".join(f"WHEN {raw} IN ({_sql_list(v)}) THEN '{k}'" for k, v in PLAY_TYPES.items())
    return (
        f"CASE WHEN {raw} IS NULL THEN NULL {exact}"
        f" WHEN {raw} LIKE 'Extra Point%' THEN 'extra_point'"
        f" WHEN lower({raw}) LIKE '%2pt%' OR lower({raw}) LIKE '%two point%' THEN 'two_point'"
        " ELSE 'other' END"
    )


def plays_payload(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    return explode_payload(bronze, PLAY_PAYLOAD, table_name=PLAYS, source=SOURCE)


def latest_response_per_game(records: DataFrame) -> DataFrame:
    """Keep, per game, only the records of its latest landed response."""
    w = Window.partitionBy(F.col("r.gameId"))
    newest = F.max(F.struct("fetched_at", "_landing_file")).over(w)
    return records.withColumn("__newest", newest).where(F.struct("fetched_at", "_landing_file") == F.col("__newest")).drop("__newest")


def game_team_names(spark, seasons: list[int], catalog: str = DEFAULT_CATALOG) -> DataFrame:
    """(game_id, home_name, away_name) from the latest landed ``/games`` record of each game."""
    bronze = spark.table(table(RAW, catalog)).where(F.col("endpoint") == "games")
    if seasons:
        bronze = bronze.where(F.col("season").isin(seasons))
    records, _ = explode_payload(bronze, GAME_PAYLOAD, table_name=GAMES, source="cfbd:games")
    w = Window.partitionBy(F.col("r.id")).orderBy(F.col("fetched_at").desc(), F.col("_landing_file").desc())
    return (
        records.withColumn("__rn", F.row_number().over(w))
        .where("__rn = 1")
        .select(F.col("r.id").alias("game_id"), F.col("r.homeTeam").alias("home_name"), F.col("r.awayTeam").alias("away_name"))
    )


def map_plays(records: DataFrame, games: DataFrame, names: DataFrame) -> DataFrame:
    """Records (latest response per game) -> silver.plays columns + ``__raw`` and check helpers."""
    r = F.col("r")
    seq = Window.partitionBy(r["gameId"]).orderBy(r["driveNumber"], r["playNumber"], r["id"])
    df = (
        records.withColumn("play_sequence", F.row_number().over(seq))
        .withColumn("__dup", F.row_number().over(Window.partitionBy(r["gameId"], r["id"]).orderBy("play_sequence")))
        .where("__dup = 1")
        .join(games, F.col("r.gameId") == games["__gid"], "left")
        .join(names, F.col("r.gameId") == names["__nid"], "left")
    )
    home_name = F.coalesce(F.col("home_name"), r["home"])
    away_name = F.coalesce(F.col("away_name"), r["away"])

    def team_id(name_col):
        return (
            F.when(name_col == home_name, F.col("home_team_id"))
            .when(name_col == away_name, F.col("away_team_id"))
        )

    raw_type = r["playType"]
    df = df.select(
        r["id"].alias("play_id"),
        r["gameId"].alias("game_id"),
        r["driveId"].alias("drive_id"),
        F.col("season"),
        F.col("week"),
        F.col("play_sequence"),
        r["period"].alias("period"),
        (r["clock"]["minutes"] * 60 + r["clock"]["seconds"]).alias("clock_seconds"),
        team_id(r["offense"]).alias("offense_team_id"),
        team_id(r["defense"]).alias("defense_team_id"),
        # CFBD uses down 0 for plays without a down (period ends, some kicks): stored as null.
        F.when(r["down"] == 0, F.lit(None)).otherwise(r["down"]).alias("down"),
        r["distance"].alias("distance"),
        r["yardsToGoal"].alias("yards_to_goal"),
        F.expr(play_type_sql("r.playType")).alias("play_type"),
        raw_type.alias("play_type_raw"),
        r["yardsGained"].alias("yards_gained"),
        r["scoring"].alias("is_scoring"),
        F.coalesce(raw_type.isin(*TURNOVERS), F.lit(False)).alias("is_turnover"),
        r["ppa"].alias("source_epa"),
        r["playText"].alias("play_text"),
        F.lit(SOURCE).alias("_source"),
        F.col("fetched_at").alias("_fetched_at"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(None).cast("string").alias("_record_hash"),
        F.col("__gid").alias("__game_known"),
        F.to_json(r).alias("__raw"),
    )
    return df.withColumn("__seq_count", F.count("*").over(Window.partitionBy("game_id", "play_sequence")))


def build_plays(spark, bronze: DataFrame, catalog: str = DEFAULT_CATALOG) -> tuple[dict, DataFrame]:
    """Bronze ``/plays`` rows -> silver.plays. Returns (summary, rejects)."""
    records, bad = plays_payload(bronze)
    records = latest_response_per_game(records).cache()
    try:
        keys = records.select(F.col("r.gameId").alias("game_id"), "season").distinct().collect()
        game_ids = sorted({k.game_id for k in keys if k.game_id is not None})
        seasons = sorted({k.season for k in keys if k.season is not None})
        games = (
            spark.table(table(GAMES, catalog))
            .where(F.col("game_id").isin(game_ids))
            .select(F.col("game_id").alias("__gid"), "home_team_id", "away_team_id")
        )
        names = game_team_names(spark, seasons, catalog).withColumnRenamed("game_id", "__nid")
        mapped = apply_checks(map_plays(records, games, names), PLAY_CHECKS).cache()
        input_rows = mapped.count()
        cols = spark.table(table(PLAYS, catalog)).columns
        good = with_record_hash(mapped.where("__reason IS NULL").select(*cols))
        rejects = reject_rows(
            mapped.where("__reason IS NOT NULL"),
            table_name=PLAYS,
            reason=F.col("__reason"),
            record_key=F.col("play_id"),
            raw=F.col("__raw"),
            source=SOURCE,
        ).unionByName(bad)
        result = {"input_rows": input_rows, "games": len(game_ids), "inserted": 0, "updated": 0, "deleted": 0}
        if game_ids and seasons:
            slice_filter = (
                f"season IN ({', '.join(map(str, seasons))}) AND game_id IN ({', '.join(map(str, game_ids))})"
            )
            result.update(
                sync_rows(
                    spark, table(PLAYS, catalog), good, ["play_id"], slice_filter=slice_filter, prune=["season", "game_id"]
                )
            )
        changed = result["inserted"] or result["updated"] or result["deleted"]
        result["touched_seasons"] = seasons if changed else []
        rejects = rejects.localCheckpoint()  # materialize before the caches go away
        mapped.unpersist()
        return result, rejects
    finally:
        records.unpersist()
