"""Landing -> bronze.api_responses_raw (plan WS4a, contract H4, G8).

Landing objects are ``<root>/<endpoint>/season=<YYYY>/week=<WW>/<YYYYMMDDTHHMMSSZ>.json.gz``,
where ``<root>`` is ``gs://<proj>-sa-landing/cfb``. Each object is one gzipped CFBD response body;
``endpoint``, ``params``, ``params_hash``, ``fetched_at`` and ``http_status`` are GCS custom
metadata on the object (written by the extractor).

One run: list the landing root through the Hadoop FileSystem (GCS connector on Serverless,
``file://`` in tests), drop files already in ``bronze._load_state``, read each new file whole
(Spark decompresses ``.gz``), and append one bronze row per file plus one state row per file.

Object metadata is read with one GCS JSON API list call (standard library, token from the
metadata server on Serverless). Where it is unavailable (local files, no metadata server) the same values are derived from the object name and the payload:
``fetched_at`` from the file name, ``season``/``week`` from the path, ``seasonType`` from the
first record when the payload carries it, ``http_status`` null. The summary counts both cases.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from urllib.parse import parse_qsl, quote, urlencode, urlparse
from urllib.request import Request, urlopen

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import DEFAULT_CATALOG, table
from .tables import ensure_tables

RAW = "bronze.api_responses_raw"
STATE = "bronze._load_state"

OBJECT_RE = re.compile(
    r"(?P<rel>(?P<endpoint>[A-Za-z0-9_]+)/season=(?P<season>\d{4})/week=(?P<week>\d{2})/"
    r"(?P<ts>\d{8}T\d{6}Z)\.json\.gz)$"
)
# The same pattern without named groups, for Spark (Java regex).
REL_PATTERN = re.sub(r"\(\?P<\w+>", "(", OBJECT_RE.pattern)
FILE_TS_FORMAT = "%Y%m%dT%H%M%SZ"


@dataclass(frozen=True)
class LandingFile:
    path: str  # full URI as the FileSystem lists it; stored as _landing_file
    rel: str  # <endpoint>/season=YYYY/week=WW/<ts>.json.gz
    endpoint: str
    season: int
    week: int
    fetched_at: datetime  # UTC, from the object name

    @staticmethod
    def parse(path: str) -> "LandingFile | None":
        m = OBJECT_RE.search(path)
        if not m:
            return None
        ts = datetime.strptime(m["ts"], FILE_TS_FORMAT).replace(tzinfo=timezone.utc)
        return LandingFile(path, m["rel"], m["endpoint"], int(m["season"]), int(m["week"]), ts)


def default_landing_root(settings) -> str:
    return f"gs://{settings.landing_bucket}/cfb"


# ---- listing and object metadata ----------------------------------------------------------


def _filesystem(spark, root: str):
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(root)
    return path, path.getFileSystem(spark._jsc.hadoopConfiguration())


def list_landing(spark, root: str) -> tuple[list[LandingFile], int]:
    """Every landing object under ``root`` (recursive). Returns (files, ignored non-matching count)."""
    path, fs = _filesystem(spark, root)
    if not fs.exists(path):
        return [], 0
    files, ignored = [], 0
    it = fs.listFiles(path, True)
    while it.hasNext():
        name = it.next().getPath().toString()
        lf = LandingFile.parse(name)
        if lf is None:
            ignored += 1
        else:
            files.append(lf)
    files.sort(key=lambda f: (f.fetched_at, f.rel))
    return files, ignored


GCS_LIST_URL = "https://storage.googleapis.com/storage/v1/b/{bucket}/o"
TOKEN_URL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"


def _http_json(url: str, headers: dict[str, str], timeout: float = 30.0) -> dict:
    with urlopen(Request(url, headers=headers), timeout=timeout) as resp:  # noqa: S310 - fixed https/metadata URLs
        return json.loads(resp.read().decode("utf-8"))


def metadata_server_token(fetch=_http_json) -> str | None:
    """Access token of the batch service account from the metadata server (Serverless), or None."""
    try:
        return fetch(TOKEN_URL, {"Metadata-Flavor": "Google"}, 5.0).get("access_token")
    except Exception:  # noqa: BLE001 - not on GCP: derive instead
        return None


def list_gcs_metadata(bucket: str, prefix: str, token: str, fetch=_http_json) -> dict[str, dict[str, str]]:
    """``{gs://bucket/name: custom metadata}`` for every object under ``prefix``.

    One JSON API list (``fields=items(name,metadata),nextPageToken``), the same call the
    extractor makes. The GCS connector cannot be used for this: it base64-decodes custom
    metadata values (its own convention) and fails on the extractor's plain strings.
    """
    out: dict[str, dict[str, str]] = {}
    page = None
    while True:
        q = {"prefix": prefix, "fields": "items(name,metadata),nextPageToken", "maxResults": "1000"}
        if page:
            q["pageToken"] = page
        body = fetch(GCS_LIST_URL.format(bucket=quote(bucket, safe="")) + "?" + urlencode(q), {"Authorization": f"Bearer {token}"})
        for item in body.get("items", []):
            out[f"gs://{bucket}/{item['name']}"] = dict(item.get("metadata") or {})
        page = body.get("nextPageToken")
        if not page:
            return out


def gcs_object_metadata(spark, root: str, paths: list[str], fetch=_http_json) -> dict[str, dict[str, str]]:
    """Custom metadata of the given landing objects; {} when not on GCS or the API is unreachable."""
    parsed = urlparse(root)
    if parsed.scheme != "gs" or not paths:
        return {}
    token = metadata_server_token(fetch)
    if not token:
        return {}
    prefix = parsed.path.lstrip("/")
    prefix = prefix + "/" if prefix and not prefix.endswith("/") else prefix
    try:
        listed = list_gcs_metadata(parsed.netloc, prefix, token, fetch)
    except Exception as exc:  # noqa: BLE001 - derive instead, and say so
        print(json.dumps({"event": "landing_metadata_unavailable", "error": type(exc).__name__}), flush=True)
        return {}
    return {p: listed[p] for p in paths if p in listed}


def _parse_rfc3339(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _int_or_none(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def file_record(lf: LandingFile, meta: dict[str, str] | None) -> dict:
    """Driver-side values for one file: object metadata when present, else derived (seasonType
    and params are then completed from the payload in Spark)."""
    meta = meta or {}
    params = meta.get("params")
    if params is not None and meta.get("endpoint"):
        q = dict(parse_qsl(params, keep_blank_values=True))
        return {
            "rel": lf.rel,
            "_landing_file": lf.path,
            "endpoint": meta["endpoint"],
            "params": params,
            "params_hash": meta.get("params_hash") or hashlib.sha256(params.encode()).hexdigest(),
            "fetched_at": _parse_rfc3339(meta.get("fetched_at")) or lf.fetched_at,
            "http_status": _int_or_none(meta.get("http_status")),
            "season": _int_or_none(q.get("year")),
            "week": _int_or_none(q.get("week")),
            "season_type": q.get("seasonType"),
            "meta_source": "object",
        }
    return {
        "rel": lf.rel,
        "_landing_file": lf.path,
        "endpoint": lf.endpoint,
        "params": None,
        "params_hash": None,
        "fetched_at": lf.fetched_at,
        "http_status": None,
        "season": lf.season,
        "week": lf.week if lf.week > 0 else None,
        "season_type": None,
        "meta_source": "derived",
    }


def derived_params(season: int, week: int | None, season_type: str | None) -> str:
    """The extractor's canonical query string (keys sorted, URL-encoded)."""
    q = {"year": str(season)}
    if week is not None:
        q["week"] = str(week)
    if season_type:
        q["seasonType"] = season_type
    return urlencode(sorted(q.items()))


FILE_SCHEMA = T.StructType(
    [
        T.StructField("rel", T.StringType(), False),
        T.StructField("_landing_file", T.StringType(), False),
        T.StructField("endpoint", T.StringType(), False),
        T.StructField("params", T.StringType()),
        T.StructField("params_hash", T.StringType()),
        T.StructField("fetched_at", T.TimestampType(), False),
        T.StructField("http_status", T.IntegerType()),
        T.StructField("season", T.IntegerType()),
        T.StructField("week", T.IntegerType()),
        T.StructField("season_type", T.StringType()),
        T.StructField("meta_source", T.StringType()),
    ]
)


# ---- load ----------------------------------------------------------------------------------


def read_contents(spark, paths: list[str]) -> DataFrame:
    """(rel, response_json) for each file, read whole and decompressed."""
    df = spark.read.option("wholetext", "true").text(paths)
    rel = F.regexp_extract(F.expr("url_decode(_metadata.file_path)"), REL_PATTERN, 1)
    return df.select(rel.alias("rel"), F.col("value").alias("response_json"))


def _bronze_rows(files_df: DataFrame, contents: DataFrame, ingested_at: datetime) -> DataFrame:
    df = files_df.join(contents, "rel", "inner")
    first_season_type = F.get_json_object("response_json", "$[0].seasonType")
    season_type = F.coalesce(F.col("season_type"), F.when(F.col("week").isNotNull(), first_season_type))
    # Same canonical form as derived_params(): keys sorted (seasonType, week, year).
    derived = F.concat_ws(
        "&",
        F.when(season_type.isNotNull(), F.concat(F.lit("seasonType="), season_type)),
        F.when(F.col("week").isNotNull(), F.concat(F.lit("week="), F.col("week").cast("string"))),
        F.concat(F.lit("year="), F.col("season").cast("string")),
    )
    params = F.coalesce(F.col("params"), derived)
    return df.select(
        F.col("endpoint"),
        params.alias("params"),
        F.coalesce(F.col("params_hash"), F.sha2(params, 256)).alias("params_hash"),
        F.col("fetched_at"),
        F.col("http_status"),
        F.col("season"),
        F.col("week"),
        season_type.alias("season_type"),
        F.col("response_json"),
        F.col("_landing_file"),
        F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
    )


def _record_count(col: str = "response_json"):
    n = F.expr(f"json_array_length({col})")
    return F.coalesce(n, F.when(F.col(col).isNotNull() & (F.length(F.trim(F.col(col))) > 0), F.lit(1)), F.lit(0))


def load_landing(
    spark,
    root: str,
    since: date | None = None,
    catalog: str = DEFAULT_CATALOG,
    now: datetime | None = None,
    reload: bool = False,
) -> dict:
    """Load new landing files into bronze. Returns the summary dict.

    ``reload`` first deletes every row of ``bronze.api_responses_raw`` and ``bronze._load_state``
    (in this run, before listing), so every landing file is loaded again. Use it when rows were
    loaded with wrong metadata. Downstream jobs pick the new rows up by their newer ``_ingested_at``.
    """
    ensure_tables(spark, [RAW, STATE], catalog)
    raw_t, state_t = table(RAW, catalog), table(STATE, catalog)
    cleared = {}
    if reload:
        for t in (state_t, raw_t):
            n = spark.table(t).count()
            if n:
                spark.sql(f"DELETE FROM {t}")
            cleared[t] = n
    ingested_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)

    files, ignored = list_landing(spark, root)
    listed = len(files)
    if since is not None:
        files = [f for f in files if f.fetched_at.date() >= since]
    skipped_since = listed - len(files)

    summary = {
        "landing_root": root,
        "reload": reload,
        "cleared_rows": cleared,
        "since": since.isoformat() if since else None,
        "listed": listed,
        "ignored": ignored,
        "skipped_since": skipped_since,
        "already_loaded": 0,
        "repaired": 0,
        "loaded_files": 0,
        "loaded_records": 0,
        "meta_from_object": 0,
        "meta_derived": 0,
        "by_endpoint": {},
        "touched_seasons": [],
    }
    if not files:
        return summary

    known = {r[0] for r in spark.table(state_t).select("_landing_file").collect()}
    new = [f for f in files if f.path not in known]
    summary["already_loaded"] = len(files) - len(new)
    if not new:
        return summary

    # Repair: a file in bronze but not in the state table (a run died between the two appends)
    # gets its state row and is not loaded again.
    names = [f.path for f in new]
    lo, hi = min(f.fetched_at for f in new), max(f.fetched_at for f in new)
    in_bronze = (
        spark.table(raw_t)
        .where(F.col("fetched_at").between(F.lit(lo), F.lit(hi)))
        .where(F.col("_landing_file").isin(names))
        .select("_landing_file", "endpoint", "params_hash", "fetched_at", _record_count().cast("int").alias("rows"))
        .dropDuplicates(["_landing_file"])
    )
    repaired = in_bronze.collect()
    if repaired:
        spark.createDataFrame(
            [(r["_landing_file"], r["endpoint"], r["params_hash"], r["fetched_at"], r["rows"], ingested_at) for r in repaired],
            spark.table(state_t).schema,
        ).writeTo(state_t).append()
        done = {r["_landing_file"] for r in repaired}
        new = [f for f in new if f.path not in done]
        summary["repaired"] = len(repaired)
        if not new:
            return summary

    meta = gcs_object_metadata(spark, root, [f.path for f in new])
    records = [file_record(f, meta.get(f.path)) for f in new]
    files_df = spark.createDataFrame([tuple(r[f.name] for f in FILE_SCHEMA.fields) for r in records], FILE_SCHEMA)

    contents = read_contents(spark, [f.path for f in new])
    bronze = _bronze_rows(files_df, contents, ingested_at).cache()
    try:
        bronze.writeTo(raw_t).append()
        state = files_df.join(
            bronze.select("_landing_file", "params_hash", _record_count().alias("rows")), "_landing_file", "left"
        ).select(
            "_landing_file",
            "endpoint",
            F.coalesce(bronze["params_hash"], files_df["params_hash"]).alias("params_hash"),
            "fetched_at",
            F.coalesce(F.col("rows"), F.lit(0)).cast("int").alias("rows"),
            F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
        )
        state.writeTo(state_t).append()

        stats = bronze.groupBy("endpoint").agg(
            F.count("*").alias("files"), F.sum(_record_count()).alias("records"), F.collect_set("season").alias("seasons")
        ).collect()
    finally:
        bronze.unpersist()

    seasons = set()
    for s in stats:
        summary["by_endpoint"][s["endpoint"]] = {"files": s["files"], "records": int(s["records"] or 0)}
        summary["loaded_files"] += s["files"]
        summary["loaded_records"] += int(s["records"] or 0)
        seasons.update(x for x in s["seasons"] if x is not None)
    summary["touched_seasons"] = sorted(seasons)
    summary["meta_from_object"] = sum(1 for r in records if r["meta_source"] == "object")
    summary["meta_derived"] = len(records) - summary["meta_from_object"]
    return summary


# ---- bronze payloads -----------------------------------------------------------------------

RAW_REJECT_CHARS = 65536


def explode_payload(bronze: DataFrame, record_schema: T.StructType, *, table_name: str, source: str):
    """Explode bronze ``response_json`` arrays into one row per record (struct column ``r``,
    plus the bronze columns except ``response_json``). Returns (records, rejects for files that
    are not a JSON array).

    ``response_json`` is the whole response body (a week of plays is tens of MB). Carrying it
    onto every exploded record copies it thousands of times per file and runs executors out of
    heap, so it is dropped in the same projection as the explode.
    """
    from .rejects import reject_rows

    parsed = bronze.withColumn("__arr", F.from_json("response_json", T.ArrayType(record_schema)))
    keep = [c for c in bronze.columns if c != "response_json"]
    records = parsed.where("__arr IS NOT NULL").select(*keep, F.explode("__arr").alias("r"))
    bad = reject_rows(
        parsed.where("__arr IS NULL"),
        table_name=table_name,
        reason="invalid_payload",
        record_key=F.col("_landing_file"),
        raw=F.substring("response_json", 1, RAW_REJECT_CHARS),
        source=source,
        fetched_at="fetched_at",
    )
    return records, bad
