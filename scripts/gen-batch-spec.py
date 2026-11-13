#!/usr/bin/env python3
"""Write dags/lakehouse/batch_spec.json from sa_lakehouse.batch_spec (contract I4).

Airflow cannot import sa_lakehouse, so the DAGs read this generated copy of the batch
submit-batch.sh uses, with placeholders they fill: ${GCP_PROJECT}, ${JOBS_REF} (the DAGs use
"current"), ${JOB} and ${ARGS}. Rerun after changing batch_spec.py or config.py and commit
the result; jobs/tests/test_batch_spec.py fails while the committed file is stale.

Usage: scripts/gen-batch-spec.py [--check]
  --check  exit 1 if the committed file differs from the generated one; write nothing
"""

from __future__ import annotations

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "dags", "lakehouse", "batch_spec.json")
sys.path.insert(0, os.path.join(ROOT, "jobs"))

from sa_lakehouse.batch_spec import template  # noqa: E402


def render() -> str:
    return json.dumps(template(), indent=2) + "\n"


def main(argv: list[str]) -> int:
    if argv not in ([], ["--check"]):
        print(__doc__, file=sys.stderr)
        return 2
    text = render()
    if argv == ["--check"]:
        try:
            with open(OUT) as fh:
                current = fh.read()
        except FileNotFoundError:
            current = None
        if current != text:
            print(f"{os.path.relpath(OUT, ROOT)} is stale; run scripts/gen-batch-spec.py", file=sys.stderr)
            return 1
        return 0
    with open(OUT, "w") as fh:
        fh.write(text)
    print(f"wrote {os.path.relpath(OUT, ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
