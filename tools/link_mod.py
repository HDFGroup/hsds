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
import asyncio
import sys
import os
from aiobotocore.session import get_session
from aiohttp.client_exceptions import ClientError
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError


if "CONFIG_DIR" not in os.environ:
    os.environ["CONFIG_DIR"] = "../admin/config/"

from hsds.util.lruCache import LruCache
from hsds.util.idUtil import isValidUuid, isSchema2Id, getS3Key
from hsds.util.storUtil import (
    releaseStorageClient,
    getStorKeys,
    getStorJSONObj,
    putStorJSONObj,
)
from hsds import config
from hsds import hsds_logger as log


# This is a utility to scan keys and replace links to chunks in HDF5 files with a different path
# Example: the HDF5 file that was linked has been moved to a different bucket
# Note: only works with schema v2 domains!

#
# Print usage and exit
#
def printUsage():
    print("       python link_mod.py [rootid] [prefix_old] [prefix_new] [-update]")
    sys.exit()


async def checkDataset(app, dset_key):
    log.info(f"checkDataset for key: {dset_key}")
    dset_json = await getStorJSONObj(app, dset_key)
    dset_id = dset_json["id"]
    prefix_old = app["prefix_old"]
    prefix_new = app["prefix_new"]
    do_update = app["do_update"]
    indirect_dataset_keys = app["indirect_dataset_keys"]
    app["dataset_count"] += 1
    log.info(f"checkDataset for id: {dset_id}")
    if "layout" not in dset_json:
        log.info("no layout found")
        return
    layout_json = dset_json["layout"]
    if "class" not in layout_json:
        log.warn(f"no class found in layout for id: {dset_id}")
        return
    layout_class = layout_json["class"]
    log.info(f"got layout_class: {layout_class}")
    if layout_class in ("H5D_CONTIGUOUS_REF", "H5D_CHUNKED_REF"):
        if "file_uri" not in layout_json:
            log.warn(
                f"Expected to find key 'file_uri' in layout_json for id: {dset_id}"
            )
            return
        file_uri = layout_json["file_uri"]
        if file_uri.startswith(prefix_old):
            prefix_len = len(prefix_old)
            new_file_uri = prefix_new + file_uri[prefix_len:]
            log.info(f"replacing uri: {file_uri} with {new_file_uri}")
            app["matched_dset_uri"] += 1
            if do_update:
                # update the dataset json
                layout_json["file_uri"] = new_file_uri
                dset_json["layout"] = layout_json
                # write back to storage
                try:
                    await putStorJSONObj(app, dset_key, dset_json)
                    log.info(f"dataset {dset_id} updated")
                except Exception as e:
                    log.error(f"get exception writing dataset json: {e}")
    elif layout_class == "H5D_CHUNKED_REF_INDIRECT":
        # add to list to be scanned later
        indirect_dataset_keys += dset_key[: -len(".dataset.json")]
    else:
        log.info(f"skipping check for layout_class: {layout_class}")


async def getKeysCallback(app, s3keys):
    log.info(f"getKeysCallback, {len(s3keys)} items")

    if not isinstance(s3keys, list):
        log.error("expected list result for objDeleteCallback")
        raise ValueError("unexpected callback format")

    if "root_prefix" not in app or not app["root_prefix"]:
        log.error("Unexpected getKeysCallback")
        raise ValueError("Invalid getKeysCallback")

    prefix = app["root_prefix"]
    prefix_len = len(prefix)
    for s3key in s3keys:
        if not s3key.startswith(prefix):
            log.error(f"Unexpected key {s3key} for prefix: {prefix}")
            raise ValueError("invalid s3key for getKeysCallback")
        if not s3key.endswith(".dataset.json"):
            log.info(f"got unexpected key {s3key}, ignoring")
            continue
        dset_key = prefix + s3key[prefix_len:]
        log.info(f"getKeys - :{dset_key}")
        await checkDataset(app, dset_key)

    log.info("getKeysCallback complete")


async def run_scan(app, rootid, update=False):

    root_key = getS3Key(rootid)

    if not root_key.endswith("/.group.json"):
        raise ValueError("unexpected root key")
    root_prefix = root_key[: -(len(".group.json"))]
    app["root_prefix"] = root_prefix

    try:
        await getStorKeys(
            app,
            prefix=root_prefix,
            suffix=".dataset.json",
            include_stats=False,
            callback=getKeysCallback,
        )
    except ClientError as ce:
        log.error(f"removeKeys - getS3Keys faiiled: {ce}")
    except HTTPNotFound:
        log.warn(
            f"getStorKeys - HTTPNotFound error for getStorKeys with prefix: {root_prefix}"
        )
    except HTTPInternalServerError:
        log.error(
            f"getStorKeys - HTTPInternalServerError for getStorKeys with prefix: {root_prefix}"
        )
    except Exception as e:
        log.error(
            f"getStorKeys - Unexpected Exception for getStorKeys with prefix: {root_prefix}: {e}"
        )

    # update all chunks for datasets with H5D_CHUNKED_REF_INDIRECT layout
    indirect_dataset_keys = app["indirect_dataset_keys"]
    for prefix in indirect_dataset_keys:
        log.info(f"got inidirect prefix: {prefix}")
        # TBD...

    await releaseStorageClient(app)


def main():

    do_update = False

    if len(sys.argv) < 4:
        printUsage()

    rootid = sys.argv[1]
    prefix_old = sys.argv[2]
    prefix_new = sys.argv[3]
    if len(sys.argv) > 4 and sys.argv[4] == "-update":
        do_update = True

    if not isValidUuid(rootid):
        print("Invalid root id!")
        sys.exit(1)

    if not isSchema2Id(rootid):
        print("This tool can only be used with Schema v2 ids")
        sys.exit(1)

    if prefix_old == prefix_new:
        print("prefix_old and prefix_new or the same")
        sys.exit(1)

    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()

    app = {}
    app["bucket_name"] = config.get("bucket_name")
    app["prefix_old"] = prefix_old
    app["prefix_new"] = prefix_new
    app["do_update"] = do_update
    app["dataset_count"] = 0
    app["matched_dset_uri"] = 0
    app["indirect_dataset_keys"] = []
    app["loop"] = loop
    session = get_session()
    app["session"] = session
    app["filter_map"] = {}

    # need the metadata cache since we will be calling into some SN methods
    metadata_mem_cache_size = int(config.get("metadata_mem_cache_size"))
    app["meta_cache"] = LruCache(mem_target=metadata_mem_cache_size, name="MetaCache")

    loop.run_until_complete(run_scan(app, rootid=rootid, update=do_update))

    loop.close()

    print("datsets scanned:", app["dataset_count"])
    print(
        "datasets with matching uri ('H5D_CONTIGUOUS_REF', 'H5D_CHUNKED_REF' layouts):",
        app["matched_dset_uri"],
    )

    print("done!")


main()
