v1.0.2 --- unreleased

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

* [Executive Summary](CHANGELOG.md#-executive-summary-hsds-version-102)
* [Breaking Changes](CHANGELOG.md#%EF%B8%8F-breaking-changes)
* [Deprecations](CHANGELOG.md#-deprecations)
* [New Features & Improvements](CHANGELOG.md#-new-features--improvements)
* [Bug Fixes](CHANGELOG.md#-bug-fixes)
* [Platforms Tested](CHANGELOG.md#%EF%B8%8F-platforms-tested)
* [Known Problems](CHANGELOG.md#-known-problems)

# 🔆 Executive Summary: HSDS Version 1.0.2

## Bug Fixes:

- `PUT` to an object's attributes collection now returns 400 when the request contains
  no attributes, instead of reporting success without writing anything

- A read that fails on the data node returns an error instead of a 200 with an
  empty or partial body

- A range read that runs past the end of an object keeps the bytes it read
  instead of returning zeros

- Chunks and contiguous datasets are limited to the data node chunk cache size,
  and compact datasets to 64 KiB, so a large chunk can no longer exhaust a data
  node's memory

- The head node refuses to start when `TARGET_SN_COUNT` or `TARGET_DN_COUNT` is
  unset or 0, instead of leaving the cluster stuck in `WAITING`

## Acknowledgements:

We would like to thank the HSDS community members who contributed to this release.

## Changelog and release information:

The full list of changes in this release is in
[CHANGELOG.md](https://github.com/HDFGroup/hsds/blob/master/CHANGELOG.md).

# ⚠️ Breaking Changes

### Chunks larger than `chunk_mem_cache_size` are refused at creation

   HSDS 1.0.0 and 1.0.1 stored client chunk shapes with no size limit, and stored
   contiguous and compact datasets as a single chunk the size of the whole dataset.
   To read or write any part of a chunk, a data node loads the whole chunk, and its
   memory use peaks at 2-3x the chunk's size while it does, so a large chunk could
   exhaust a data node's memory. Writes to chunks over roughly 128M elements failed
   outright.

   Creating a dataset whose chunk, or whose whole contiguous dataset, is larger than
   `chunk_mem_cache_size` (128 MB by default) now returns 400. A chunked layout should
   be used for larger contiguous data. Compact datasets are limited to
   `max_compact_dset_size` (64 KiB, as in HDF5). Writes to existing datasets with
   chunks over the limit now fail, but their data is still readable. In order to
   handle large contiguous datasets or large chunks, raise for `chunk_mem_cache_size`
   and `dn_ram`.

# 🪦 Deprecations

None.

# 🚀 New Features & Improvements

None.

# 🪲 Bug Fixes

## Service Node

### `PUT /{collection}/{id}/attributes` no longer returns 200 without writing anything

   A body without an `attributes` map, such as the single-attribute form
   `{"name": ..., "type": ..., "value": ...}` that belongs on
   `/attributes/{name}`, produced an empty set of attributes that was forwarded to the
   data node and answered with 200. It now returns 400, as does an `obj_ids` entry
   with no `attributes` key, which was previously skipped while the other objects'
   attributes were written.

### Failed reads no longer return 200

   `GET` and `POST /datasets/{id}/value` sent their 200 status before fetching any
   data, so a data node failure produced an empty or truncated body that looked
   successful. Errors now return their real status, and a failure partway through a
   streamed response drops the connection.

## Storage

### Short range reads no longer come back as zeros

   A range read that returned fewer bytes than requested discarded the bytes it had
   read and returned zeros in their place. It now keeps them and zero-fills only the
   missing remainder. This affected `H5D_CONTIGUOUS_REF` datasets whose last chunk
   extends past the end of their file.

## Head Node

### Missing target node counts are reported at startup

   With `target_sn_count` or `target_dn_count` at its default of 0, the head node
   admitted no nodes and every request got 503. It now exits at startup with an
   error naming the relevant settings.

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
