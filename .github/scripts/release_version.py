#!/usr/bin/env python3
"""Resolve the release version from the repo-local single source of truth.

SSOT: the `version` field of [project] in pyproject.toml.

HSDS keeps a second copy of the version in hsds/basenode.py (HSDS_VERSION),
which is served to clients via /about and in domain JSON. A release where the
two disagree would ship a service that misreports its own version, so a
mismatch fails the run here - before the tag is created.
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

# PEP 440 version syntax, from the spec's Appendix B (the same pattern
# `packaging.version` uses). `packaging` is not installed in the job that runs
# this script, so the pattern is inlined. Always applied with fullmatch().
PEP440_RE = re.compile(
    r"""
    v?
    (?:
        (?:(?P<epoch>[0-9]+)!)?                           # epoch
        (?P<release>[0-9]+(?:\.[0-9]+)*)                  # release segment
        (?P<pre>                                          # pre-release
            [-_\.]?
            (?P<pre_l>(a|b|c|rc|alpha|beta|pre|preview))
            [-_\.]?
            (?P<pre_n>[0-9]+)?
        )?
        (?P<post>                                         # post release
            (?:-(?P<post_n1>[0-9]+))
            |
            (?:
                [-_\.]?
                (?P<post_l>post|rev|r)
                [-_\.]?
                (?P<post_n2>[0-9]+)?
            )
        )?
        (?P<dev>                                          # dev release
            [-_\.]?
            (?P<dev_l>dev)
            [-_\.]?
            (?P<dev_n>[0-9]+)?
        )?
    )
    (?:\+(?P<local>[a-z0-9]+(?:[-_\.][a-z0-9]+)*))?       # local version
    """,
    re.VERBOSE | re.IGNORECASE,
)


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
    return str(project["version"])


def validate_version(version):
    """Fail unless the version is a well-formed PEP 440 version with no whitespace."""
    if any(c.isspace() for c in version) or not PEP440_RE.fullmatch(version):
        fail(
            f"{PYPROJECT} [project] version {version!r} is not a valid PEP 440 "
            "version (whitespace and newlines are not allowed)"
        )


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
    validate_version(version)
    check_service_version(version)

    tag = f"v{version}"
    set_output("version", version)
    set_output("tag", tag)
    set_output("prerelease", "true" if PRERELEASE_RE.search(version) else "false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
