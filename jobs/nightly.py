"""Nightly pipeline in one batch: load_landing -> build_silver -> build_gold (contract I5).

Usage: nightly.py [--landing-root URI]

Runs the three entry scripts' ``main`` in one SparkSession, with their defaults: new landing
files only, silver from new bronze rows, gold for the touched seasons and the last 3 UTC days.
Each step prints its own ``*_summary`` line; then one ``nightly_summary`` line lists every
step's exit code and duration.

A step that raises or exits non-zero stops the pipeline, except a build_silver quality failure
(exit 3): silver and its rejects are already written, so build_gold still runs and the batch
then exits 3. Every step is idempotent, so a retry of the whole batch is safe.
Exit codes: 0 ok, 3 silver quality check failed, 1 a step failed, 2 bad arguments.

On Serverless, ``sa_lakehouse.zip`` (``--py-files``) also carries the entry scripts, so they
import as modules here (scripts/publish-jobs.sh).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback

import build_gold
import build_silver
import load_landing
from sa_lakehouse.config import build_spark, catalog_conf, settings_from_env

STEPS = (("load_landing", load_landing), ("build_silver", build_silver), ("build_gold", build_gold))
EXIT_FAILED = 1


def _call(module, argv, spark) -> tuple[int, str | None]:
    try:
        return module.main(argv, spark=spark), None
    except SystemExit as exc:  # argparse errors and explicit exits
        if exc.code is None:
            return 0, None
        return (exc.code if isinstance(exc.code, int) else EXIT_FAILED), f"SystemExit({exc.code!r})"
    except Exception as exc:  # noqa: BLE001 - reported in the summary, then the batch fails
        traceback.print_exc()
        return EXIT_FAILED, f"{type(exc).__name__}: {exc}"


def run(spark, landing_root: str | None = None) -> tuple[dict, int]:
    argv = {
        "load_landing": ["--landing-root", landing_root] if landing_root else [],
        "build_silver": [],
        "build_gold": [],
    }
    steps, code, stopped = [], 0, False
    for name, module in STEPS:
        if stopped:
            steps.append({"step": name, "status": "skipped"})
            continue
        started = time.monotonic()
        rc, error = _call(module, argv[name], spark)
        step = {"step": name, "exit_code": rc, "seconds": round(time.monotonic() - started, 1)}
        if rc == 0:
            step["status"] = "ok"
        elif name == "build_silver" and rc == build_silver.EXIT_QUALITY:
            step["status"] = "quality_failed"
            code = rc
        else:
            step["status"] = "failed"
            code, stopped = rc, True
        if error:
            step["error"] = error
        steps.append(step)
    status = "ok" if code == 0 else ("quality_failed" if code == build_silver.EXIT_QUALITY else "failed")
    return {"status": status, "exit_code": code, "steps": steps}, code


def main(argv=None, spark=None) -> int:
    ap = argparse.ArgumentParser(description="Run load_landing, build_silver and build_gold in one session.")
    ap.add_argument("--landing-root", help="landing root URI for load_landing (default gs://<proj>-sa-landing/cfb)")
    args = ap.parse_args(argv)
    settings = settings_from_env()
    if spark is None:
        spark = build_spark("sa-nightly", catalog_conf(settings))
    started = time.monotonic()
    summary, code = run(spark, args.landing_root)
    summary["seconds"] = round(time.monotonic() - started, 1)
    print(json.dumps({"event": "nightly_summary", **summary}, sort_keys=True, default=str), flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
