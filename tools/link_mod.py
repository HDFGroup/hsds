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


def getFileUri(dset_json):
    # older format used "layout" key to store file_uri.
    # newer ones use "creationProperties"
    # return whichever one has a file_uri or None if neither do

    if "creationProperties" in dset_json:
        cpl = dset_json["creationProperties"]
        if "layout" in cpl:
            cpl_layout = cpl["layout"]
            if "file_uri" in cpl_layout:
                return cpl_layout["file_uri"]
    # not found under cpl
    if "layout" in dset_json:
        layout = dset_json["layout"]
        if "file_uri" in layout:
            return layout["file_uri"]

    # no found in either
    return None


def setFileUri(dset_json, file_uri):
    if "creationProperties" in dset_json:
        cpl = dset_json["creationProperties"]
        if "layout" in cpl:
            cpl_layout = cpl["layout"]
            if "file_uri" in cpl_layout:
                cpl_layout["file_uri"] = file_uri
                log.info(f"updated creationProperties layout with {file_uri}")
                return
    # not found under cpl
    if "layout" in dset_json:
        layout = dset_json["layout"]
        if "file_uri" in layout:
            layout["file_uri"] = file_uri
            log.info(f"updated dset layout with {file_uri}")
            return

    # no found in either
    log.warning("expected to find file_uri for update")


async def checkDataset(app, dset_key):
    log.info(f"checkDataset for key: {dset_key}")
    dset_json = await getStorJSONObj(app, dset_key)
    log.debug(f"get dset_json: {dset_json}")
    dset_id = dset_json["id"]
    prefix_old = app["prefix_old"]
    prefix_new = app["prefix_new"]
    do_update = app["do_update"]
    app["dataset_count"] += 1
    log.info(f"checkDataset for id: {dset_id}")
    file_uri = getFileUri(dset_json)

    if not file_uri:
        log.debug(f"no file_uri for {dset_key}")
        return

    log.debug(f"got file_uri: {file_uri}")

    if file_uri.startswith(prefix_old):
        prefix_len = len(prefix_old)
        new_file_uri = prefix_new + file_uri[prefix_len:]
        log.info(f"replacing uri: {file_uri} with {new_file_uri}")
        app["matched_dset_uri"] += 1
        if do_update:
            setFileUri(dset_json, new_file_uri)

            # write back to storage
            try:
                await putStorJSONObj(app, dset_key, dset_json)
                log.info(f"dataset {dset_id} updated")
            except Exception as e:
                log.error(f"get exception writing dataset json: {e}")


async def getKeysCallback(app, s3keys):
    log.info(f"getKeysCallback, {len(s3keys)} items")

    if not isinstance(s3keys, list):
        log.error("expected list result for objDeleteCallback")
        raise ValueError("unexpected callback format")

    if "root_prefix" not in app or not app["root_prefix"]:
        log.error("Unexpected getKeysCallback")
        raise ValueError("Invalid getKeysCallback")

    prefix = app["root_prefix"]
    for s3key in s3keys:
        if not s3key.startswith(prefix):
            log.error(f"Unexpected key {s3key} for prefix: {prefix}")
            raise ValueError("invalid s3key for getKeysCallback")
        dset_key = s3key + ".dataset.json"
        await checkDataset(app, dset_key)

    log.info("getKeysCallback complete")


async def run_scan(app, rootid, update=False):

    root_key = getS3Key(rootid)

    if not root_key.endswith("/.group.json"):
        raise ValueError("unexpected root key")
    root_prefix = root_key[: -(len(".group.json"))]
    root_prefix += "d/"
    log.info(f"getting s3 keys with prefix: {root_prefix}")
    app["root_prefix"] = root_prefix

    try:
        await getStorKeys(
            app,
            prefix=root_prefix,
            deliminator="/",
            include_stats=False,
            callback=getKeysCallback,
        )
    except ClientError as ce:
        log.error(f"removeKeys - getS3Keys faiiled: {ce}")
    except HTTPNotFound:
        msg = f"getStorKeys - HTTPNotFound error for getStorKeys with prefix: {root_prefix}"
        log.warn(msg)
    except HTTPInternalServerError:
        msg = f"getStorKeys - HTTPInternalServerError for getStorKeys with prefix: {root_prefix}"
        log.error(msg)
    except Exception as e:
        msg = "getStorKeys - Unexpected Exception for getStorKeys with prefix: "
        msg += f"{root_prefix}: {e}"
        log.error(msg)

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

    # setup log config
    log_level = "WARN"  # ERROR, WARN, INFO, or DEBUG
    log.setLogConfig(log_level)

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
