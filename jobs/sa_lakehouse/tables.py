"""Iceberg table definitions: the only place DDL lives (contract H4 and plan Shared contracts).

Every job calls :func:`ensure_tables` for the tables it touches. Creation is idempotent
(``CREATE TABLE IF NOT EXISTS``); an existing table is never altered here. Schema changes
go through the lead.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .config import DEFAULT_CATALOG, table

MOR = "merge-on-read"
COW = "copy-on-write"

SILVER_META = """
  _source STRING,
  _fetched_at TIMESTAMP NOT NULL,
  _ingested_at TIMESTAMP,
  _record_hash STRING"""


@dataclass(frozen=True)
class TableDef:
    name: str  # <namespace>.<table>
    columns: str  # Spark SQL column list, without the surrounding parentheses
    partition: str | None = None  # Spark PARTITIONED BY expression list
    write_mode: str | None = None  # MOR / COW for row-level operations; None = append only
    write_order: str | None = None  # ALTER TABLE ... WRITE ORDERED BY <this>
    properties: dict[str, str] = field(default_factory=dict)

    def tblproperties(self) -> dict[str, str]:
        props = {"format-version": "2", "write.format.default": "parquet"}
        if self.write_mode:
            for op in ("delete", "update", "merge"):
                props[f"write.{op}.mode"] = self.write_mode
        props.update(self.properties)
        return props

    def create_sql(self, catalog: str = DEFAULT_CATALOG) -> str:
        props = ", ".join(f"'{k}'='{v}'" for k, v in sorted(self.tblproperties().items()))
        parts = [f"CREATE TABLE IF NOT EXISTS {table(self.name, catalog)} ({self.columns}\n) USING iceberg"]
        if self.partition:
            parts.append(f"PARTITIONED BY ({self.partition})")
        parts.append(f"TBLPROPERTIES ({props})")
        return "\n".join(parts)


TABLES: dict[str, TableDef] = {
    t.name: t
    for t in [
        # ---- bronze ------------------------------------------------------------------------
        TableDef(
            "bronze.api_responses_raw",
            """
  endpoint STRING NOT NULL,
  params STRING NOT NULL,
  params_hash STRING NOT NULL,
  fetched_at TIMESTAMP NOT NULL,
  http_status INT,
  season INT,
  week INT,
  season_type STRING,
  response_json STRING NOT NULL,
  _file_name STRING NOT NULL,
  _ingested_at TIMESTAMP NOT NULL""",
            partition="days(fetched_at)",
        ),
        TableDef(
            "bronze._load_state",
            """
  _file_name STRING NOT NULL,
  endpoint STRING,
  params_hash STRING,
  fetched_at TIMESTAMP,
  rows INT,
  _ingested_at TIMESTAMP NOT NULL""",
        ),
        TableDef(
            "bronze.live_game_events",
            """
  kafka_key STRING,
  value_json STRING NOT NULL,
  _kafka_partition INT NOT NULL,
  _kafka_offset BIGINT NOT NULL,
  _kafka_timestamp TIMESTAMP,
  _ingested_at TIMESTAMP NOT NULL""",
            partition="days(_ingested_at)",
        ),
        TableDef(
            "bronze.agent_events",
            """
  kafka_key STRING,
  value_json STRING NOT NULL,
  _kafka_partition INT NOT NULL,
  _kafka_offset BIGINT NOT NULL,
  _kafka_timestamp TIMESTAMP,
  _ingested_at TIMESTAMP NOT NULL""",
            partition="days(_ingested_at)",
        ),
        # ---- silver ------------------------------------------------------------------------
        TableDef(
            "silver.games",
            """
  game_id BIGINT NOT NULL,
  season INT NOT NULL,
  week INT,
  season_type STRING,
  start_time_utc TIMESTAMP,
  status STRING,
  period INT,
  clock_seconds INT,
  home_team_id STRING NOT NULL,
  away_team_id STRING NOT NULL,
  home_conference STRING,
  away_conference STRING,
  home_points INT,
  away_points INT,
  neutral_site BOOLEAN,
  conference_game BOOLEAN,
  venue_id BIGINT,
  attendance INT,"""
            + SILVER_META,
            partition="season",
            write_mode=MOR,
            write_order="week, game_id",
        ),
        TableDef(
            "silver.plays",
            """
  play_id STRING NOT NULL,
  game_id BIGINT NOT NULL,
  drive_id STRING,
  season INT NOT NULL,
  week INT,
  play_sequence INT,
  period INT,
  clock_seconds INT,
  offense_team_id STRING NOT NULL,
  defense_team_id STRING NOT NULL,
  down INT,
  distance INT,
  yards_to_goal INT,
  play_type STRING,
  play_type_raw STRING,
  yards_gained INT,
  is_scoring BOOLEAN,
  is_turnover BOOLEAN,
  source_epa DOUBLE,
  play_text STRING,"""
            + SILVER_META,
            partition="season",
            write_mode=COW,
            write_order="game_id, play_sequence",
        ),
        TableDef(
            "silver.agent_requests",
            """
  request_id STRING NOT NULL,
  session_id STRING,
  user_key STRING,
  requested_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  status STRING,
  error_code STRING,
  latency_ms INT,
  model STRING,
  input_tokens INT,
  output_tokens INT,
  prompt_chars INT,
  tool_calls ARRAY<STRUCT<tool: STRING, started_at: TIMESTAMP, latency_ms: INT, status: STRING, known_tool: BOOLEAN>>,
  client STRING,
  app_version STRING,
  _last_event_at TIMESTAMP NOT NULL,
  _kafka_partition INT,
  _kafka_offset BIGINT,
  _ingested_at TIMESTAMP""",
            partition="days(requested_at)",
            write_mode=MOR,
        ),
        TableDef(
            "silver.teams",
            """
  team_id STRING NOT NULL,
  display_name STRING,
  aliases ARRAY<STRING>,
  conference_by_season MAP<INT, STRING>,"""
            + SILVER_META,
            write_mode=COW,
        ),
        TableDef(
            "silver._rejects",
            """
  rejected_at TIMESTAMP NOT NULL,
  table_name STRING NOT NULL,
  reason STRING NOT NULL,
  record_key STRING,
  raw STRING,
  _source STRING,
  _fetched_at TIMESTAMP""",
            partition="days(rejected_at)",
        ),
        # ---- gold --------------------------------------------------------------------------
        TableDef(
            "gold.team_efficiency_weekly",
            """
  team_id STRING NOT NULL,
  season INT NOT NULL,
  week INT NOT NULL,
  season_type STRING NOT NULL,
  games INT,
  off_plays INT,
  off_epa_plays INT,
  off_epa_sum DOUBLE,
  off_successes INT,
  off_success_epa_sum DOUBLE,
  off_drives INT,
  off_points INT,
  off_epa_per_play DOUBLE,
  off_success_rate DOUBLE,
  off_explosiveness DOUBLE,
  off_points_per_drive DOUBLE,
  def_plays INT,
  def_epa_plays INT,
  def_epa_sum DOUBLE,
  def_successes INT,
  def_success_epa_sum DOUBLE,
  def_drives INT,
  def_points INT,
  def_epa_per_play DOUBLE,
  def_success_rate DOUBLE,
  def_explosiveness DOUBLE,
  def_points_per_drive DOUBLE,
  _built_at TIMESTAMP NOT NULL""",
            partition="season",
            write_mode=COW,
        ),
        TableDef(
            "gold.game_summaries",
            """
  game_id BIGINT NOT NULL,
  season INT NOT NULL,
  week INT,
  season_type STRING,
  start_time_utc TIMESTAMP,
  status STRING,
  home_team_id STRING,
  home_team_name STRING,
  away_team_id STRING,
  away_team_name STRING,
  home_points INT,
  away_points INT,
  winner_team_id STRING,
  home_plays INT,
  away_plays INT,
  home_yards INT,
  away_yards INT,
  home_epa_per_play DOUBLE,
  away_epa_per_play DOUBLE,
  home_success_rate DOUBLE,
  away_success_rate DOUBLE,
  home_turnovers INT,
  away_turnovers INT,
  _built_at TIMESTAMP NOT NULL""",
            partition="season",
            write_mode=COW,
        ),
        TableDef(
            "gold.agent_usage_daily",
            """
  day DATE NOT NULL,
  tool STRING NOT NULL,
  requests INT,
  calls INT,
  errors INT,
  p50_latency_ms INT,
  p95_latency_ms INT,
  unknown_tool BOOLEAN,
  _built_at TIMESTAMP NOT NULL""",
            write_mode=COW,
        ),
    ]
}

NAMESPACES = ("bronze", "silver", "gold")


def ensure_tables(spark, names=None, catalog: str = DEFAULT_CATALOG) -> list[str]:
    """Create the given tables (default: all) if missing. Returns the names it created.

    Namespaces are created by OpenTofu in the real catalog; locally they are created here.
    """
    names = list(TABLES) if names is None else list(names)
    unknown = [n for n in names if n not in TABLES]
    if unknown:
        raise KeyError(f"unknown tables: {unknown}")
    created = []
    for name in names:
        tdef = TABLES[name]
        if spark.catalog.tableExists(table(name, catalog)):
            continue
        spark.sql(tdef.create_sql(catalog))
        if tdef.write_order:
            spark.sql(f"ALTER TABLE {table(name, catalog)} WRITE ORDERED BY {tdef.write_order}")
        created.append(name)
    return created


def ensure_local_namespaces(spark, catalog: str = DEFAULT_CATALOG) -> None:
    """Tests only: the real namespaces belong to OpenTofu."""
    for ns in NAMESPACES:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns}")
