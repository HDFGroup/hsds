#!/usr/bin/env python3
"""Resolve the release version from the repo-local single source of truth.

SSOT: the `version` field of [project] in pyproject.toml.

HSDS keeps a second copy of the version in hsds/basenode.py (HSDS_VERSION),
which is served to clients via /about and in domain JSON. A release where the
two disagree would ship a service that misreports its own version, so a
mismatch fails the run here - before the tag is turned into a release.
"""
import os
import re
import sys
import tomllib
from pathlib import Path

PYPROJECT = Path("pyproject.toml")
BASENODE = Path("hsds/basenode.py")

# PEP 440 pre-release / dev-release markers.
PRERELEASE_RE = re.compile(r"(a|b|rc|alpha|beta|dev)\d*$", re.IGNORECASE)


def set_output(name, value):
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            f.write(f"{name}={value}\n")
    print(f"{name}={value}")


def fail(msg):
    print(f"::error::{msg}", file=sys.stderr)
    raise SystemExit(1)


def read_pyproject_version():
    if not PYPROJECT.is_file():
        fail(f"{PYPROJECT} not found")
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    project = data.get("project", {})
    if "version" not in project:
        fail(
            f"{PYPROJECT} [project] has no static `version`. If the project has "
            "moved to a dynamic version, point this script at the new SSOT."
        )
    return str(project["version"]).strip()


def check_service_version(version):
    """Fail if hsds/basenode.py HSDS_VERSION disagrees with the SSOT."""
    if not BASENODE.is_file():
        fail(f"{BASENODE} not found - HSDS_VERSION can no longer be checked")
    match = re.search(
        r"^HSDS_VERSION\s*=\s*[\"']([^\"']+)[\"']",
        BASENODE.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if not match:
        fail(
            f"{BASENODE} does not define HSDS_VERSION. It is the version the "
            "service reports over /about; if it moved, update this script."
        )
    if match.group(1) != version:
        fail(
            f"version mismatch: {PYPROJECT} says {version}, "
            f"{BASENODE} HSDS_VERSION says {match.group(1)}. "
            "Both must be bumped together."
        )


def main():
    version = read_pyproject_version()
    check_service_version(version)

    tag = f"v{version}"
    if os.environ.get("EVENT_NAME") == "push":
        ref = os.environ.get("GITHUB_REF_NAME", "")
        if ref != tag:
            fail(
                f"tag {ref!r} does not match the version in {PYPROJECT} "
                f"({version}, expected tag {tag!r}). Bump the SSOT and re-tag."
            )

    set_output("version", version)
    set_output("tag", tag)
    set_output("prerelease", "true" if PRERELEASE_RE.search(version) else "false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
