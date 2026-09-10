v1.0.1 --- 9/10/26

# 🔺 HSDS Changelog

All notable changes to this project are recorded here. This file describes the
differences between this release and the previous HSDS release; the copy attached to
each tag is that release's own record.

For releases up to and including v1.0.0, the notes are attached to the release itself
at <https://github.com/HDFGroup/hsds/releases>.

# 🔗 Quick Links

* [HSDS documentation](https://github.com/HDFGroup/hsds/tree/master/docs)
* [Official HSDS releases](https://github.com/HDFGroup/hsds/releases)
* [HSDS on PyPI](https://pypi.org/project/hsds/)
* [Getting help, questions, or comments](https://forum.hdfgroup.org/c/hsds)

## 📖 Contents

* [Executive Summary](CHANGELOG.md#-executive-summary-hsds-version-101)
* [Breaking Changes](CHANGELOG.md#%EF%B8%8F-breaking-changes)
* [Deprecations](CHANGELOG.md#-deprecations)
* [New Features & Improvements](CHANGELOG.md#-new-features--improvements)
* [Bug Fixes](CHANGELOG.md#-bug-fixes)
* [Platforms Tested](CHANGELOG.md#%EF%B8%8F-platforms-tested)
* [Known Problems](CHANGELOG.md#-known-problems)

# 🔆 Executive Summary: HSDS Version 1.0.1

## Significant Advancements:

- Fixed issue introduced in 1.0.0 where headless Kubernetes deployments could not serve requests.
  Every Kubernetes manifest HSDS ships runs without a head node, and on those deployments every node had
  been stuck in `WAITING` for the lifetime of the process, answering 503 to every
  request.

## Enhanced Features:

- A new Kubernetes CI job that runs the full integration suite against a real k3s cluster,
  preventing silent breakage of Kubernetes functionality.

## Bug Fixes:

- A domain deleted while a bucket scan was queued is no longer partly re-created by that
  scan after garbage collection has swept it

- A `PUT` to the POSIX backend no longer returns 500 when the key it just wrote is
  removed concurrently

## Dependencies:

- aiohttp upgraded from 3.9.4 to 3.14.3

## Acknowledgements:

We would like to thank the HSDS community members who contributed to this release.

## Changelog and release information:

The full list of changes in this release is in
[CHANGELOG.md](https://github.com/HDFGroup/hsds/blob/master/CHANGELOG.md).

# ⚠️ Breaking Changes

None.

# 🪦 Deprecations

None.

# 🚀 New Features & Improvements

## Deployment

### New `hsds-node-state` console script for container health probes

   An `exec` probe command, meant to run inside the sn or dn container.
   It reads `node_state` from
   `http://localhost:<port>/info` and exits 0 only when the node is `READY`. The
   port comes from `NODE_TYPE`, so both containers share one command.

   `tests/k8s/manifests.yaml` uses it for both liveness and readiness; the
   manifests under `admin/kubernetes/` do not yet (see Known Problems).

## Testing

### The Kubernetes CI job runs the full test suite and checks partitioning across a rescale

   `tests/k8s/run.sh` brings HSDS up headless on a k3d cluster, waits for `READY`, and
   now runs the whole integration suite over a port-forward.

   `tests/k8s/rescale_check.py` writes objects, rescales the deployment, and
   verifies that every object is still reachable.

## Dependencies

### aiohttp upgraded from 3.9.4 to 3.14.3

   HSDS pins aiohttp exactly (`aiohttp == 3.14.3`), so an environment holding aiohttp to
   a 3.9.x release cannot install this version of HSDS.

# 🪲 Bug Fixes

## Kubernetes

### Headless deployments now reach READY instead of returning 503 for the life of the process

   The readiness gate in `updateReadyState()` requires `cluster_state` to be `READY`,
   but `cluster_state` was only ever written from a head node's `/register` response.
   `update_dn_info()` routes Kubernetes deployments to `k8s_update_dn_info()`, which
   never touched it, so on a deployment with no head node it stayed at its initial
   `WAITING` and the node never served.

   Every manifest under `admin/kubernetes/` runs headless, so this affected all three
   documented Kubernetes topologies. `k8s_update_dn_info()` now derives `cluster_state`
   from the dn roster it already computes.

   Consequently, a rescale will now result in the service returning 503 for roughly one health check
   interval while the roster reconverges.

## Data Node

### A deleted domain is no longer re-created by a queued bucket scan

   `bucketScan` and `bucketGC` run as independent asyncio tasks in each data node. Deleting a
   root left it in `root_scan_ids`, so a scan still on the queue could run after
   `bucketGC` had swept the prefix and write `db/<root>/.info.json` and `.summary.json`
   back into a domain that no longer exists. `delete_metadata_obj()` now drops the root
   from `root_scan_ids`, and `scanRoot()` returns without writing if the root has been
   deleted since the scan was queued.

## Storage

### A concurrent delete no longer turns a successful POSIX write into a 500

   `FileClient.put_object()` stat'd the file it had just written in order to build the
   response, so if a key was removed between the write and the stat, then it would raise
   a 404 and eventually surface a 500, despite the data having reached
   the filesystem and `close()` having returned. The missing key is now a warning and
   the response is synthesized from what was written, the way `s3Client` already
   handled it.

## Testing

### hs.log no longer loses everything written after the first node exits

   The subprocess reader thread in `hsds_app.py` used a bytes sentinel on a text-mode
   pipe, so it never stopped at EOF. The drain loop then never broke and never closed
   the buffered logfile, and `hs.log` lost every line written after the first node
   exited. The sentinel is fixed, the log is closed from a `finally` block, and the
   `p.communicate()` that could race the reader thread for `p.stdout` is gone.

# ☑️ Platforms Tested

Every push and pull request runs the suite against Python 3.11, 3.12 and 3.13 on
`ubuntu-22.04`, `ubuntu-latest` and `windows-latest`, with the POSIX backend, plus the
h5pyd integration suite and a headless k3s cluster on Ubuntu. `.github/workflows/` holds
the current matrix, and the badges in [README.md](README.md) show its status.

# ⛔ Known Problems

- A JSON-encoded write to a dataset whose own type is a top-level `H5T_ARRAY` is not
  handled. Binary reads and writes of such datasets work.

- The manifests under `admin/kubernetes/` point their liveness probes at `/info`, which
  bypasses the `node_state` gate and so returns 200 from a node that is answering 503 to
  every real request, and they define no readiness probes. Use `hsds-node-state` for
  both, as `tests/k8s/manifests.yaml` does.

Please report new problems at <https://github.com/HDFGroup/hsds/issues>.
