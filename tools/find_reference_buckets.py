##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of HSDS (HDF5 Scalable Data Service), Libraries and      #
# Utilities.  The full HSDS copyright notice, including                      #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################
"""List the buckets that reference datasets and chunk initializers read from.

HSDS lets a reference layout (H5D_CONTIGUOUS_REF, H5D_CHUNKED_REF,
H5D_CHUNKED_REF_INDIRECT) or a chunklocator initializer read from its own
dataset's bucket and the server's default bucket. Any other bucket must be
listed in the reference_buckets setting. Run this against a server, as an
admin, before upgrading, and add the buckets it reports as "needs listing".

    python find_reference_buckets.py --endpoint http://localhost:5101 \\
        --user admin --password <admin password> [--folder /home]

The endpoint, user and password default to HSDS_ENDPOINT, ADMIN_USERNAME and
ADMIN_PASSWORD. http+unix:// endpoints need the requests-unixsocket package.
"""
import argparse
import os
import sys
from collections import defaultdict

import requests

REF_LAYOUTS = ("H5D_CONTIGUOUS_REF", "H5D_CHUNKED_REF", "H5D_CHUNKED_REF_INDIRECT")


def bucketOf(uri):
    """Return the bucket name in a storage uri like s3://bucket/key or bucket/key."""
    path = uri.split("://", 1)[1] if "://" in uri else uri
    parts = [p for p in path.split("/") if p]
    if parts and parts[0].endswith(".blob.core.windows.net"):
        parts = parts[1:]  # azure: account host, then container
    return parts[0] if parts else ""


class Scanner:
    def __init__(self, endpoint, user, password, default_bucket=None):
        if endpoint.startswith("http+unix://"):
            import requests_unixsocket
            self.session = requests_unixsocket.Session()
        else:
            self.session = requests.Session()
        self.session.auth = (user, password)
        self.endpoint = endpoint.rstrip("/")
        self.default_bucket = default_bucket
        # bucket -> list of (domain, dataset id, how it's referenced)
        self.found = defaultdict(list)
        self.errors = []

    def get(self, path, domain=None, **params):
        if domain is not None:
            params["domain"] = domain
        rsp = self.session.get(self.endpoint + path, params=params, timeout=120)
        rsp.raise_for_status()
        return rsp.json()

    def walk(self, folder):
        """Scan every domain under folder, recursively."""
        try:
            listing = self.get("/domains", domain=folder.rstrip("/") + "/")
        except requests.RequestException as e:
            self.errors.append(f"{folder}: unable to list ({e})")
            return
        for item in listing.get("domains", []):
            name = item["name"]
            if item.get("class") == "folder":
                self.walk(name)
            else:
                self.scanDomain(name)

    def scanDomain(self, domain):
        try:
            root_id = self.get("/", domain=domain)["root"]
            domain_bucket = self.get("/groups/" + root_id, domain=domain).get("bucket")
            dset_ids = self.get("/datasets", domain=domain).get("datasets", [])
        except (requests.RequestException, KeyError) as e:
            self.errors.append(f"{domain}: unable to read ({e})")
            return
        for dset_id in dset_ids:
            try:
                dset = self.get("/datasets/" + dset_id, domain=domain)
                self.scanDataset(domain, domain_bucket, dset)
            except (requests.RequestException, KeyError, TypeError) as e:
                self.errors.append(f"{domain} {dset_id}: unable to read ({e})")

    def scanDataset(self, domain, domain_bucket, dset):
        dset_id = dset["id"]
        cprops = dset.get("creationProperties") or {}
        layout = cprops.get("layout") or dset.get("layout") or {}
        if layout.get("class") in REF_LAYOUTS:
            if layout.get("file_uri"):
                self.add(bucketOf(layout["file_uri"]), domain_bucket, domain, dset_id,
                         layout["class"])
            chunk_table = layout.get("chunk_table")
            if chunk_table:
                self.scanChunkTable(domain, domain_bucket, dset_id, chunk_table)
        initializer = cprops.get("initializer")
        if isinstance(initializer, list) and initializer and initializer[0] == "chunklocator":
            options = dict(a[2:].split("=", 1) for a in initializer[1:]
                           if isinstance(a, str) and "=" in a)
            if options.get("filepath"):
                bucket = options.get("bucket") or domain_bucket
                self.add(bucketOf(bucket + "/x"), domain_bucket, domain, dset_id,
                         "chunklocator initializer")

    def scanChunkTable(self, domain, domain_bucket, dset_id, table_id):
        """Collect buckets from a chunk table's per-chunk paths (its third field)."""
        table = self.get("/datasets/" + table_id, domain=domain)
        fields = (table.get("type") or {}).get("fields") or []
        if len(fields) < 3:
            return  # offset and size only; the layout's file_uri names the file
        value = self.get("/datasets/" + table_id + "/value", domain=domain).get("value", [])
        stack = [value]
        while stack:
            item = stack.pop()
            if isinstance(item, list) and len(item) == len(fields) and isinstance(item[2], str):
                if item[2]:
                    self.add(bucketOf(item[2]), domain_bucket, domain, dset_id, "chunk table path")
            elif isinstance(item, list):
                stack.extend(item)

    def add(self, bucket, domain_bucket, domain, dset_id, how):
        if bucket:
            allowed = bucket in (domain_bucket, self.default_bucket)
            self.found[(bucket, allowed)].append((domain, dset_id, how))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--endpoint", default=os.environ.get("HSDS_ENDPOINT"))
    parser.add_argument("--user", default=os.environ.get("ADMIN_USERNAME"))
    parser.add_argument("--password", default=os.environ.get("ADMIN_PASSWORD"))
    parser.add_argument("--folder", default="/", help="folder to scan (default: all)")
    parser.add_argument("--default-bucket", default=os.environ.get("BUCKET_NAME"),
                        help="the server's default bucket (default: $BUCKET_NAME)")
    args = parser.parse_args()
    if not (args.endpoint and args.user and args.password):
        sys.exit("endpoint, user and password are required (see --help)")

    scanner = Scanner(args.endpoint, args.user, args.password, args.default_bucket)
    scanner.walk(args.folder)

    if not scanner.found:
        print("No reference datasets or chunk initializers found.")
    needs = sorted(b for (b, allowed) in scanner.found if not allowed)
    for (bucket, allowed), uses in sorted(scanner.found.items()):
        status = "allowed by default" if allowed else "needs listing"
        print(f"{bucket}: {len(uses)} dataset(s), {status}")
        for domain, dset_id, how in uses[:3]:
            print(f"    {domain} {dset_id} ({how})")
        if len(uses) > 3:
            print(f"    ... and {len(uses) - 3} more")
    if needs:
        print()
        print("Add these to reference_buckets before upgrading:")
        print("    reference_buckets: [" + ", ".join(f'"{b}"' for b in needs) + "]")
    for error in scanner.errors:
        print("warning:", error, file=sys.stderr)
    return 1 if scanner.errors else 0


if __name__ == "__main__":
    sys.exit(main())
