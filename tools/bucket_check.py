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
#
#
import sys
import os
import asyncio
import time

if "CONFIG_DIR" not in os.environ:
    os.environ["CONFIG_DIR"] = "../admin/config/"

from hsds import config
from hsds.util.timeUtil import unixTimeToUTC
from hsds.util.storUtil import getStorKeys, getStorJSONObj, releaseStorageClient
from hsds.async_lib import scanRoot
from hsds import hsds_logger as log


async def bucketCheck(app, base_folder):
    """ Verify that contents of bucket are self-consistent
    """

    now = int(time.time())
    log.info("bucket check {}".format(unixTimeToUTC(now)))

    bucket = app["bucket_name"]

    if base_folder.startswith('/'):
        # slash is not part of the storage key
        prefix = base_folder[1:]
    else:
        prefix = base_folder

    keys = await getStorKeys(app, prefix=prefix, suffix='domain.json')

    root_count = 0
    group_count = 0
    dataset_count = 0
    datatype_count = 0
    chunk_count = 0
    total_chunk_bytes = 0
    total_metadata_bytes = 0

    if not keys:
        print("no storage keys were found!")
        return

    log.info(f"got {len(keys)} keys")
    print("name, num_groups, num_datasets, num_datatypes, num chunks, metadata bytes, chunk bytes")
    for key in keys:
        log.info(f"got key: {key}")
        domain_json = await getStorJSONObj(app, key, bucket=bucket)
        #print("domain_json:", domain_json)
        if "root" not in domain_json:
            log.info(f"skipping folder object: {key}")
            continue
        root_id = domain_json["root"]
        scan = await scanRoot(app, root_id, bucket=bucket)
        log.debug(f"got scan_results: {scan}")
        num_groups = scan["num_groups"]
        datasets = scan["datasets"]
        num_datasets = len(datasets)
        num_datatypes = scan["num_datatypes"]
        num_chunks = scan["num_chunks"]
        chunk_bytes = scan["allocated_bytes"]
        metadata_bytes = scan["metadata_bytes"]

        print(f"{key},{num_groups},{num_datasets},{num_datatypes},{num_chunks},{metadata_bytes},{chunk_bytes}")

        # TBD - get service scan results from .info.json and compare to ones just calculated
        root_count += 1
        group_count += num_groups
        dataset_count += num_datasets
        datatype_count += num_datatypes
        chunk_count += num_chunks
        total_chunk_bytes += chunk_bytes
        total_metadata_bytes += metadata_bytes

    await releaseStorageClient(app)

    print("")
    print("Totals")
    print("="*40)
    print(f"folders: {len(keys) - root_count}")
    print(f"domains: {root_count}")
    print(f"groups: {group_count}")
    print(f"datasets: {dataset_count}")
    print(f"chunk count {chunk_count}")
    print(f"metadata bytes: {total_metadata_bytes}")
    print(f"chunk bytes: {total_chunk_bytes}")
    print("")

#
# Main
#

if __name__ == '__main__':

    base_folder = "/home"
    if len(sys.argv) > 1:
        last_arg = sys.argv[-1]
        if last_arg in ("-h","--help"):
            print("Usage: python bucket_check.py <base_domain>")
            sys.exit(0)
        if not last_arg.startswith("-"):
            base_folder = last_arg

    print("base_folder:", base_folder)

    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()
    app = {}
    app["bucket_name"] = config.get("bucket_name")
    app["s3objs"] = {}
    app["domains"] = {}  # domain to root map
    app["roots"] = {}    # root obj to domain map
    app["deleted_ids"] = set()
    app["bytes_in_bucket"] = 0
    app["loop"] = loop
    app["filter_map"] = {}
    loop.run_until_complete(bucketCheck(app, base_folder))
    loop.close()

    print("done!")


