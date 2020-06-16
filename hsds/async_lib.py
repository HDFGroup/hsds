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

import time
import hashlib
import numpy as np
from aiohttp.client_exceptions import ClientError
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError, HTTPForbidden
from .util.idUtil import isValidUuid, isSchema2Id, getS3Key, isS3ObjKey, getObjId, isValidChunkId, getCollectionForId
from .util.chunkUtil import getDatasetId, getChunkIds
from .util.hdf5dtype import getItemSize
from .util.arrayUtil import getShapeDims, getNumElements
from .util.dsetUtil import getHyperslabSelection
from .chunk_sn import getChunkInfoMap

from .util.storUtil import getStorKeys, putStorJSONObj, getStorJSONObj, deleteStorObj
from . import hsds_logger as log
from . import config


# List all keys under given root and optionally update info.json
# Note: only works with schema v2 domains!

async def getDatasetJson(app, dsetid, bucket=None):
    # try to read the dataset json from s3
    s3_key = getS3Key(dsetid)
    try:
        dset_json = await getStorJSONObj(app, s3_key, bucket=bucket)
    except HTTPNotFound:
        log.warn(f"HTTPpNotFound error for {s3_key} bucket:{bucket}")
        return None
    except HTTPForbidden:
        log.warn(f"HTTPForbidden error for {s3_key} bucket:{bucket}")
        return None
    except HTTPInternalServerError:
        log.warn(f"HTTPInternalServerError error for {s3_key} bucket:{bucket}")
        return None
    return dset_json

async def updateDatasetInfo(app, dset_id, dataset_info, bucket=None):
    # get dataset metadata and deteermine number logical)_bytes, linked_bytes, and num_linked_chunks

    dset_json = await getDatasetJson(app, dset_id, bucket=bucket)
    if dset_json:
        log.debug(f"getDsetJson: {dset_json}")
    if "shape" not in dset_json:
        return   # null dataspace
    shape_json = dset_json["shape"]
    if "type" not in dset_json:
        log.warn("expected to find type in dataet_json")
        return
    type_json = dset_json["type"]
    item_size = getItemSize(type_json)

    log.debug(f"item size: {item_size}")

    dims = getShapeDims(shape_json)  # throws 400 for HS_NULL dsets

    if item_size == 'H5T_VARIABLE':
        # arbitrary lgoical size for vaariable, so just set to allocated size
        logical_bytes = dataset_info['allocated_bytes']  
    else:
        num_elements = getNumElements(dims)
        logical_bytes = num_elements * item_size
    dataset_info["logical_bytes"] = logical_bytes 
    log.debug(f"dims: {dims}")
    rank = len(dims)
    log.debug(f"rank: {rank}")
    #layout = getChunkLayout(dset_json)
    #log.debug(f"layout: {layout}")

    if "layout" in dset_json:
        layout = dset_json["layout"]
        layout_class = layout["class"]
        if layout_class != 'H5D_CHUNKED':
            log.debug(f"get chunk info for layout_class: {layout_class}")
            selection = getHyperslabSelection(dims)
            log.debug(f"got selection: {selection}")
            chunk_ids = getChunkIds(dset_id, selection, layout['dims'])
            log.debug(f"chunk_ids: {chunk_ids}")

            if "dn_urls" in app:
                # getChunkInfoMap cannot be used from the tools scripts
                chunk_map = await getChunkInfoMap(app, dset_id, dset_json, chunk_ids, bucket=bucket)
                log.debug(f"chunkinfo_map: {chunk_map}")
                for chunk_id in chunk_map:
                    chunk_link = chunk_map[chunk_id]
                    if "s3size" in chunk_link:
                        s3size = chunk_link["s3size"]
                        dataset_info["linked_bytes"] += s3size
                        dataset_info["num_linked_chunks"] += 1
            else:
                # run from tools script, just set num_linked_chunks since we
                # can't get the chunk_map
                dataset_info["num_linked_chunks"] = len(chunk_ids)
    else:
        log.warn(f"updateDatasetInfo - no layout for dataset: {dset_id}")

def scanRootCallback(app, s3keys):
    log.debug(f"scanRoot - callback, {len(s3keys)} items")
    if isinstance(s3keys, list):
        log.error("got list result for s3keys callback")
        raise ValueError("unexpected callback format")

    results = app["scanRoot_results"]
    checksums = results["checksums"]
    if results:
        log.debug(f"previous scanRoot_results: {results}")
    for s3key in s3keys.keys():

        if not isS3ObjKey(s3key):
            log.info(f"not s3obj key, ignoring: {s3key}")
            continue
        objid = getObjId(s3key)
        etag = None
        obj_size = None
        lastModified = None
        item = s3keys[s3key]
        if "ETag" in item:
            etag = item["ETag"]
            checksums[objid] = etag
        if "Size" in item:
            obj_size = item["Size"]
        if "LastModified" in item:
            lastModified = item["LastModified"]
        log.debug(f"scanRoot - got key {objid}: {etag} {obj_size} {lastModified}")

        if lastModified > results["lastModified"]:
            log.debug(f"scanRoot: changing lastModified from: {results['lastModified']} to {lastModified}")
            results["lastModified"] = lastModified
        is_chunk = False
        if isValidChunkId(objid):
            is_chunk = True
            results["num_chunks"] += 1
            results["allocated_bytes"] += obj_size
        else:
            results["metadata_bytes"] += obj_size


        if is_chunk or getCollectionForId(objid) == "datasets":
            if is_chunk:
                dsetid = getDatasetId(objid)
            else:
                dsetid = objid
            datasets = results["datasets"]
            if dsetid not in datasets:
                dataset_info = {}
                dataset_info["lastModified"] = 0
                dataset_info["num_chunks"] = 0
                dataset_info["allocated_bytes"] = 0
                dataset_info["logical_bytes"] = 0
                dataset_info["linked_bytes"] = 0
                dataset_info["num_linked_chunks"] = 0
                dataset_info["logical_bytes"] = 0
                datasets[dsetid] = dataset_info
            dataset_info = datasets[dsetid]
            if lastModified > dataset_info["lastModified"]:
                dataset_info["lastModified"] = lastModified
                if is_chunk:
                    dataset_info["num_chunks"] += 1
                    dataset_info["allocated_bytes"] += obj_size
        elif getCollectionForId(objid) == "groups":
            results["num_groups"] += 1
        elif getCollectionForId(objid) == "datatypes":
            results["num_datatypes"] += 1
        else:
            log.error(f"scanRoot - Unexpected collection type for id: {objid}")


async def scanRoot(app, rootid, update=False, bucket=None):

    # iterate through all s3 keys under the given root.
    # Return dict with stats for the root.
    #
    # Note: not re-entrant!  Only one scanRoot an be run at a time per app.
    log.info(f"scanRoot for rootid: {rootid} bucket: {bucket}")

    if not isValidUuid(rootid):
        raise ValueError("Invalid root id")

    if not isSchema2Id(rootid):
        log.warn(f"no tabulation for schema v1 id: {rootid} returning null results")
        return {}

    if not bucket:
        bucket = config.get("bucket_name")
    if not bucket:
        raise ValueError(f"no bucket defined for scan of {rootid}")

    root_key = getS3Key(rootid)

    if not root_key.endswith("/.group.json"):
        raise ValueError("unexpected root key")
    root_prefix = root_key[:-(len(".group.json"))]

    log.debug(f"scanRoot - using prefix: {root_prefix}")

    results = {}
    results["lastModified"] = 0
    results["num_groups"] = 0
    results["num_datatypes"] = 0
    results["datasets"] = {}  # since we need per dataset info
    results["num_chunks"] = 0
    results["allocated_bytes"] = 0
    results["metadata_bytes"] = 0
    results["num_linked_chunks"] = 0
    results["linked_bytes"] = 0
    results["logical_bytes"] = 0
    results["checksums"] = {}  # map of objid to checksums
    results["bucket"] = bucket
    results["scan_start"] = time.time()

    app["scanRoot_results"] = results

    await getStorKeys(app, prefix=root_prefix, include_stats=True, bucket=bucket, callback=scanRootCallback)
    num_objects = results["num_groups"] + results["num_datatypes"] + len(results["datasets"]) + results["num_chunks"]
    log.info(f"scanRoot - got {num_objects} keys for rootid: {rootid}")


    dataset_results = results["datasets"]
    for dsetid in dataset_results:
        dataset_info = dataset_results[dsetid]
        log.info(f"got dataset: {dsetid}: {dataset_info}")
        await updateDatasetInfo(app, dsetid, dataset_info, bucket=bucket)
        if dataset_info["logical_bytes"] != "variable":
            results["logical_bytes"] += dataset_info["logical_bytes"]
            results["linked_bytes"] += dataset_info["linked_bytes"]
            results["num_linked_chunks"] += dataset_info["num_linked_chunks"]

    log.info(f"scanRoot - scan complete for rootid: {rootid}")

    # compute overall checksum
    checksums = results["checksums"]
    
    if len(checksums) != num_objects:
        log.warn(f"skipping domain checksum calculation - {len(checksums)} found but {num_objects} hdf objects")
    else:
        # create a numpy array to store checksums
        log.debug(f"creating numpy checksum array for {num_objects} checksums")
        checksum_arr = np.zeros((num_objects,), dtype='S16')
        objids = list(checksums.keys())
        objids.sort()
        for i in range(num_objects):
            objid = objids[i]
            checksum_arr[i] = checksums[objid]
        log.debug("numpy array created")
        hash_object = hashlib.md5(checksum_arr.tobytes())
        md5_sum = hash_object.hexdigest()
        log.debug(f"got domain_checksum: {md5_sum}")
        results["md5_sum"] = md5_sum
    # free up memory used by the checksums
    del results["checksums"]

    results["scan_complete"] = time.time()

    if update:
        # write .info object back to S3
        info_key = root_prefix + ".info.json"
        log.info(f"scanRoot - updating info key: {info_key}")
        await putStorJSONObj(app, info_key, results, bucket=bucket)
    return results

async def objDeleteCallback(app, s3keys):
    log.info(f"objDeleteCallback, {len(s3keys)} items")

    if not isinstance(s3keys, list):
        log.error("expected list result for objDeleteCallback")
        raise ValueError("unexpected callback format")


    if "objDelete_prefix" not in app or not app["objDelete_prefix"]:
        log.error("Unexpected objDeleteCallback")
        raise ValueError("Invalid objDeleteCallback")

    prefix = app["objDelete_prefix"]
    prefix_len = len(prefix)
    for s3key in s3keys:
        if not s3key.startswith(prefix):
            log.error(f"Unexpected key {s3key} for prefix: {prefix}")
            raise ValueError("invalid s3key for objDeleteCallback")
        full_key = prefix + s3key[prefix_len:]
        log.info(f"removeKeys - objDeleteCallback deleting key: {full_key}")
        await deleteStorObj(app, full_key)


    log.info("objDeleteCallback complete")

async def removeKeys(app, objid):
    # iterate through all s3 keys under the given root or dataset id and delete them
    #
    # Note: not re-entrant!  Only one scanRoot an be run at a time per app.
    log.debug(f"removeKeys: {objid}")
    if not isSchema2Id(objid):
        log.warn("ignoring non-schema2 id")
        raise KeyError("Invalid key")
    s3key = getS3Key(objid)
    log.debug(f"removeKeys - got s3key: {s3key}")
    expected_suffixes = (".dataset.json", ".group.json")
    s3prefix = None

    for suffix in expected_suffixes:
        if s3key.endswith(suffix):
                s3prefix = s3key[:-len(suffix)]
    if not s3prefix:
        log.error("removeKeys - unexpected s3key for delete_set")
        raise KeyError("unexpected key suffix")
    log.info(f"removeKeys - delete for {objid} searching for s3prefix: {s3prefix}")
    if app["objDelete_prefix"]:
        log.error("removeKeys - objDelete_prefix is already set - improper use of non-reentrant call?")
        # just continue and reset
    app["objDelete_prefix"] = s3prefix
    try:
        await getStorKeys(app, prefix=s3prefix, include_stats=False, callback=objDeleteCallback)
    except ClientError as ce:
        log.error(f"removeKeys - getS3Keys faiiled: {ce}")
    except HTTPNotFound:
        log.warn(f"removeKeys - HTTPNotFound error for getStorKeys with prefix: {s3prefix}")
    except HTTPInternalServerError:
        log.error(f"removeKeys - HTTPInternalServerError for getStorKeys with prefix: {s3prefix}")
    except Exception as e:
        log.error(f"removeKeys - Unexpected Exception for getStorKeys with prefix: {s3prefix}: {e}")

    # reset the prefix
    app["objDelete_prefix"] = None
