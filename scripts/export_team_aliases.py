#!/usr/bin/env python3
"""Export the Go resolver's team alias map to jobs/resources/team_aliases.json (contract H6).

Parses ``var teamAliases = map[string]string{ ... }`` in the SportsAgentic repo's
``pkg/resolver/team.go`` (read-only) and writes
``{"source_commit": "<sha>", "aliases": {"<alias>": "<school>"}}`` with keys sorted.
Rerun and commit the result whenever the Go map changes.

Usage: scripts/export_team_aliases.py [--repo PATH] [--out PATH] [--check]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_REPO = os.path.join(os.path.dirname(ROOT), "SportsAgentic")
DEFAULT_OUT = os.path.join(ROOT, "jobs", "resources", "team_aliases.json")
GO_FILE = os.path.join("pkg", "resolver", "team.go")

MAP_START = re.compile(r"^\s*var\s+teamAliases\s*=\s*map\[string\]string\s*\{\s*$")
ENTRY = re.compile(r'^\s*"((?:[^"\\]|\\.)*)"\s*:\s*"((?:[^"\\]|\\.)*)"\s*,?\s*(?://.*)?$')
SKIP = re.compile(r"^\s*(?://.*)?$")


def parse_aliases(source: str) -> dict[str, str]:
    """alias -> school from the Go source. Fails on any line it does not understand."""
    lines = source.splitlines()
    try:
        start = next(i for i, line in enumerate(lines) if MAP_START.match(line))
    except StopIteration:
        raise ValueError("teamAliases map not found") from None
    aliases: dict[str, str] = {}
    for n, line in enumerate(lines[start + 1 :], start + 2):
        if line.strip() == "}":
            return dict(sorted(aliases.items()))
        m = ENTRY.match(line)
        if m:
            alias, school = (json.loads(f'"{g}"') for g in m.groups())
            if alias in aliases and aliases[alias] != school:
                raise ValueError(f"line {n}: alias {alias!r} mapped twice")
            aliases[alias] = school
        elif not SKIP.match(line):
            raise ValueError(f"line {n}: cannot parse {line.strip()!r}")
    raise ValueError("teamAliases map is not closed")


def source_commit(repo: str) -> str:
    sha = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(
        ["git", "-C", repo, "status", "--porcelain", "--", GO_FILE], check=True, capture_output=True, text=True
    ).stdout.strip()
    return sha + ("-dirty" if dirty else "")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--repo", default=DEFAULT_REPO, help="SportsAgentic repo (default: sibling checkout)")
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--check", action="store_true", help="exit 1 if the committed file's aliases are stale")
    args = ap.parse_args(argv)

    with open(os.path.join(args.repo, GO_FILE)) as fh:
        aliases = parse_aliases(fh.read())
    doc = {"source_commit": source_commit(args.repo), "aliases": aliases}

    if args.check:
        with open(args.out) as fh:
            current = json.load(fh)
        if current.get("aliases") != aliases:
            print(f"{args.out} is stale; rerun scripts/export_team_aliases.py", file=sys.stderr)
            return 1
        print(f"{args.out} is up to date ({len(aliases)} aliases)")
        return 0

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as fh:
        json.dump(doc, fh, indent=2, sort_keys=True)
        fh.write("\n")
    print(f"wrote {len(aliases)} aliases from {doc['source_commit']} to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
