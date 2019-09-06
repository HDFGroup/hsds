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
from aiohttp.client_exceptions import ClientError
from util.idUtil import isValidUuid, isSchema2Id, getS3Key, isS3ObjKey, getObjId, isValidChunkId, getCollectionForId
from util.chunkUtil import getDatasetId
from util.storUtil import getStorKeys, putStorJSONObj, deleteStorObj
import hsds_logger as log
import config

 
# List all keys under given root and optionally update info.json
# Note: only works with schema v2 domains!  
  
def scanRootCallback(app, s3keys):
    log.debug(f"scanRootCallback, {len(s3keys)} items")
    if isinstance(s3keys, list):
        log.error("got list result for s3keys callback")
        raise ValueError("unexpected callback format")
        
    results = app["scanRoot_results"]
    if results:
        log.debug(f"previous scanRoot_results:".format(results))
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
        if "Size" in item:
            obj_size = item["Size"]
        if "LastModified" in item:
            lastModified = item["LastModified"]
        log.debug(f"{objid}: {etag} {obj_size} {lastModified}")

        if lastModified > results["lastModified"]:
            log.debug(f"changing lastModified from: {results['lastModified']} to {lastModified}")
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
            log.error(f"Unexpected collection type for id: {objid}")
       

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
    results["scan_start"] = time.time()

    app["scanRoot_results"] = results
     
    await getStorKeys(app, prefix=root_prefix, include_stats=True, bucket=bucket, callback=scanRootCallback)

    log.info(f"scan complete for rootid: {rootid}")
    results["scan_complete"] = time.time()

    if update:
        # write .info object back to S3
        info_key = root_prefix + ".info.json"
        log.info(f"updating info key: {info_key}")
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
        log.info(f"objDeleteCallback got key: {full_key}")
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
    log.debug(f"got s3key: {s3key}")
    expected_suffixes = (".dataset.json", ".group.json")
    s3prefix = None
    
    for suffix in expected_suffixes:
        if s3key.endswith(suffix):
                s3prefix = s3key[:-len(suffix)]
    if not s3prefix:
        log.error("unexpected s3key for delete_set")
        raise KeyError("unexpected key suffix")
    log.info(f"delete for {objid} searching for s3prefix: {s3prefix}")
    if app["objDelete_prefix"]:
        log.error("objDelete_prefix is already set - impropere use of non-reentrant call?")
        # just continue and reset
    app["objDelete_prefix"] = s3prefix
    try:
        await getStorKeys(app, prefix=s3prefix, include_stats=False, callback=objDeleteCallback)
    except ClientError as ce:
        log.error(f"getS3Keys faiiled: {ce}")
    # reset the prefix
    app["objDelete_prefix"] = None
