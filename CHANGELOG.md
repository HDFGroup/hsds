v1.0.3 --- 11/5/26

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

* [Executive Summary](CHANGELOG.md#-executive-summary-hsds-version-103)
* [Breaking Changes](CHANGELOG.md#%EF%B8%8F-breaking-changes)
* [Deprecations](CHANGELOG.md#-deprecations)
* [New Features & Improvements](CHANGELOG.md#-new-features--improvements)
* [Bug Fixes](CHANGELOG.md#-bug-fixes)
* [Platforms Tested](CHANGELOG.md#%EF%B8%8F-platforms-tested)
* [Known Problems](CHANGELOG.md#-known-problems)

# 🔆 Executive Summary: HSDS Version 1.0.3

## Security:

This release fixes several security vulnerabilities, including one that let any user
with an account run commands on a data node. Every deployment should upgrade. The
details are in these security advisories:

- [GHSA-5c9x-6w3j-4pfh](https://github.com/HDFGroup/hsds/security/advisories/GHSA-5c9x-6w3j-4pfh):
  command execution on data nodes through dataset chunk initializers (critical)
- [GHSA-w5jh-vhvj-vppc](https://github.com/HDFGroup/hsds/security/advisories/GHSA-w5jh-vhvj-vppc):
  users can read domains they have no access to (high)
- [GHSA-fc4g-2g65-rqh9](https://github.com/HDFGroup/hsds/security/advisories/GHSA-fc4g-2g65-rqh9):
  unauthenticated internal services reachable from the network in some deployments (high)
- [GHSA-c687-6xwp-992v](https://github.com/HDFGroup/hsds/security/advisories/GHSA-c687-6xwp-992v):
  denial of service through decompression, metadata caching and bearer tokens (medium)

Some of the fixes change behavior that existing deployments may rely on. Read
[Breaking Changes](CHANGELOG.md#%EF%B8%8F-breaking-changes) before upgrading, in
particular if you link HDF5 files from buckets other than the server's default bucket.

## Bug Fixes:

- `POST /` can report the paths it couldn't resolve instead of failing the whole
  request, with the new `ignore_unresolved` parameter (#487)

- External links whose file is a relative path, such as the bare `other.h5` that h5py
  writes, are resolved from the linking domain's folder instead of returning 500 (#488)

- Malformed requests that returned 500 or wrong data now return 400

- POSIX storage objects are written atomically, so a crash mid-write no longer leaves
  a truncated object

- Stopping the `hsds` launcher with SIGTERM stops its nodes as well

- An S3 read starting past the end of a linked file returns zeros, as POSIX storage
  does, instead of 500

## Dependencies:

- aiohttp 3.14.4, which requires yarl 1.25.1 or later

## Acknowledgements:

We would like to thank the HSDS community members who contributed to this release.

## Changelog and release information:

The full list of changes in this release is in
[CHANGELOG.md](https://github.com/HDFGroup/hsds/blob/master/CHANGELOG.md).

# ⚠️ Breaking Changes

### Reference layouts and chunk initializers may only read from allowed buckets

   A dataset's `file_uri`, the paths in its chunk table, and a `chunklocator`
   initializer's `--bucket` may now name only the dataset's own bucket, the server's
   default bucket, or a bucket listed in the new `reference_buckets` setting. Reading an
   existing dataset that points anywhere else returns 403 with a message naming the
   bucket and the setting. Setting `reference_buckets: ["*"]` restores the old
   behavior, which lets any user read any object the server's credentials can reach.

   If you link HDF5 files from other buckets, for example with `hsload --link`, run
   `tools/find_reference_buckets.py` against your server as an admin before upgrading.
   It lists every bucket your existing datasets read from and prints the
   `reference_buckets` line to add.

   Reference targets also may not name HSDS's own storage objects (anything under
   `db/`, or a `.domain.json`).

### Chunk initializers are limited to known programs and options

   A dataset's `initializer` must be `chunklocator` (with `--h5path`, `--filepath` and
   `--bucket`) or `arange` (with `--start` and `--step`), each option given as
   `--name=value`. Dataset creation returns 400 for anything else, and data nodes
   refuse to run other initializers stored by earlier versions.

### Docker Compose no longer publishes data node and head node ports

   The Compose files under `admin/docker/` now `expose` the data node and head node
   ports instead of publishing them on the host, so only containers on the Compose
   network can reach them. The service node port is published as before.

### Folder listings only include what the caller can read

   `GET /domains` requires read access to the folder being listed, and returns only
   the domains in it the caller can read. Attribute queries on `GET /domains` are
   filtered the same way.

### Requests that now return 400

   - `GET /` or `POST /` with a `parent_id` that isn't a group in the request's domain
   - A domain path containing `.` or `..` segments (repeated slashes are now collapsed,
     so `/home//user/x.h5` is `/home/user/x.h5`)
   - A `select` value not enclosed in brackets, such as `select=0:3`, which previously
     read the whole dataset
   - An h5path that follows more than 16 soft or external links, HDF5's default limit

### Other changes

   - `/about` returns `dn_urls` and `dn_ids` only to admin users.
   - `runall.sh --no-docker` and `--no-docker-tcp` exit if `admin/config/passwd.txt` is
     missing, instead of using `passwd.default` and its default admin password.
   - Bearer tokens without a `kid` header are rejected with 401.
   - HSDS refuses to start if its socket directory is owned by another user.

# 🪦 Deprecations

None.

# 🚀 New Features & Improvements

### `ignore_unresolved` for `POST /`

   `POST /` resolves a list of h5paths, and the first one that can't be resolved (a
   dangling soft link, an external link to a missing domain, or a link the request
   didn't ask to follow) fails the whole request. With `ignore_unresolved=1`, such paths
   are left out of `h5paths` and listed in an `errors` map with the status they would
   have returned. The default is unchanged.

### New settings

   - `reference_buckets`: buckets reference layouts and chunk initializers may read from,
     beyond the dataset's own bucket and the default bucket. See Breaking Changes.
   - `domain_cache_expire`: seconds a service node caches a domain's JSON, including its
     ACLs (default 10). ACL changes take effect within this time on every service node,
     and immediately on the one that made the change.

   List settings such as `reference_buckets` and `top_level_domains` can be given as
   comma-separated environment variables, for example
   `REFERENCE_BUCKETS=s3://bucket1,s3://bucket2`. Previously each character became a
   separate entry.

### `tools/find_reference_buckets.py`

   Lists the buckets that a server's reference datasets and chunk initializers read
   from, for setting `reference_buckets` before upgrading.

# 🪲 Bug Fixes

## Service Node

### External links with a relative file are followed

   An external link whose file is a relative path returned 500 when followed by path,
   because the bucket name was prefixed directly onto the filename. A relative file is
   now resolved from the folder of the domain holding the link, as HDF5 does from the
   linking file's directory, so `b.h5`, `./b.h5`, `sub/c.h5` and `../b.h5` all work.

### External link targets resolve in the right domain

   Paths after one that went through an external link were resolved in the external
   domain, so an absolute soft link could return an object from the wrong domain. An
   external link with a relative target path returned 500, and now resolves from the
   target domain's root. A target that isn't a valid domain returns 404 instead of 500.

### Malformed requests return 400 instead of 500

   A Basic `Authorization` header without a colon, a dataset whose `maxdims` is smaller
   than its shape, point selections with negative or wrongly shaped coordinates, query
   expressions nested past the parser's limit, and link cycles all returned 500. They
   now return 400. A selection missing its closing bracket, such as `[[0,1`, returned
   wrong data and is now 400.

### A data node's 403 reaches the client

   A 403 from a data node during a read was retried and then reported as 500, losing
   its message. It is now returned as 403 with the data node's reason.

### Bearer tokens no longer stall the service node

   With OpenID configured, every new token made the service node fetch the provider's
   signing keys with blocking requests and no timeout. Keys are now cached for an hour,
   refetched at most once a minute, and fetched with a 2 s timeout. Every token error
   returns 401.

## Data Node

### Metadata cache stays within its configured size

   The cache counted every object's JSON as 1 KB, so objects with large attributes could
   grow data node memory far past `metadata_mem_cache_size`. Entries are now counted at
   their serialized size.

### Chunk decompression is bounded

   A compressed chunk may decompress to at most its uncompressed chunk size. Larger
   output is an error instead of being allocated.

## Storage

### POSIX writes are atomic

   Objects are written to a temporary file and renamed into place, so readers see
   either the old object or the new one, and a crash mid-write no longer leaves a
   truncated object.

### S3 reads past the end of a linked file

   A reference read whose range started past the end of its file returned 500 on S3,
   while POSIX storage returned zeros. S3 now matches POSIX.

### `hsds-chunklocator` reads S3 without credentials

   The `chunklocator` initializer ignored `aws_s3_no_sign_request`, so it failed on
   public buckets when the server had no AWS credentials.

## Head Node

### A restarted node no longer takes the cluster down

   When a node re-registered under a new id at the same address, as a restarted Compose
   container does, the head node left the old id in its node list and every later
   registration failed, so the whole cluster returned 503 until the head node was
   restarted.

## Launcher

### SIGTERM stops the nodes

   The `hsds` launcher stopped its node processes only on Ctrl-C. SIGTERM, as sent by
   service managers and container runtimes, ended the launcher alone and left the nodes
   running. It now stops them the same way.

### Data nodes listen only on localhost

   `hsds` and `runall.sh --no-docker-tcp` started data nodes listening on every network
   interface. Data nodes are local child processes and now listen only on localhost, and
   in socket mode only their owner can connect to their sockets. The service node
   listens on all interfaces as before, or only on `--host` when one is given.

# ☑️ Platforms Tested

Every push and pull request runs the suite against Python 3.11, 3.12 and 3.13 on
`ubuntu-22.04`, `ubuntu-latest` and `windows-latest`, with the POSIX backend, plus the
h5pyd integration suite and a headless k3s cluster on Ubuntu. The socket job also reads
linked HDF5 files from the public `hdf5.sample` S3 bucket. `.github/workflows/` holds
the current matrix, and the badges in [README.md](README.md) show its status.

# ⛔ Known Problems

- A JSON-encoded write to a dataset whose own type is a top-level `H5T_ARRAY` is not
  handled. Binary reads and writes of such datasets work.

- The manifests under `admin/kubernetes/` point their liveness probes at `/info`, which
  bypasses the `node_state` gate and so returns 200 from a node that is answering 503 to
  every real request, and they define no readiness probes. Use `hsds-node-state` for
  both, as `tests/k8s/manifests.yaml` does.

Please report new problems at <https://github.com/HDFGroup/hsds/issues>.
