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

from util.idUtil import isValidUuid, isSchema2Id, getS3Key, isS3ObjKey, getObjId, isValidChunkId, getCollectionForId
from util.chunkUtil import getDatasetId
from util.s3Util import getS3Keys, putS3JSONObj
import hsds_logger as log

 
# This is a utility to scan keys for a given domain and report totals.
# Note: only works with schema v2 domains!  
  
def getS3KeysCallback(app, s3keys):
    log.debug(f"getS3KeysCallback, {len(s3keys)} items")
    if isinstance(s3keys, list):
        log.error("got list result for s3keys callback")
        raise ValueError("unexpected callback format")
        
    root_prefix = app["s3scan_prefix"] 
    results = app["results"]
    for s3key in s3keys.keys():
        full_key = root_prefix + s3key

        if not isS3ObjKey(full_key):
            log.warn("not s3obj key, ignoring")
            continue
        objid = getObjId(full_key)
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

        results["allocated_bytes"] += obj_size
        if lastModified > results["lastModified"]:
            results["lastModified"] = lastModified
        is_chunk = False
        if isValidChunkId(objid):
            is_chunk = True
            results["num_chunks"] += 1
        
  
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
       

async def scanRoot(app, rootid, update=False):

    # iterate through all s3 keys under the given root.
    # Return dict with stats for the root.
    #
    # Note: not re-entrant!  Only one scanRoot an be run at a time per app.
    log.info(f"scanRoot for rootid: {rootid}")

    if not isValidUuid(rootid):
        raise ValueError("Invalid root id")

    if not isSchema2Id(rootid):
        log.warn(f"no tabulation for schema v1 id: {rootid} returning null results")
        return {}

    root_key = getS3Key(rootid)

    if not root_key.endswith("/.group.json"):
        raise ValueError("unexpected root key")
    root_prefix = root_key[:-(len(".group.json"))]
    
    log.debug(f"using prefix: {root_prefix}")
    app["s3scan_prefix"] = root_prefix

    results = {}
    results["lastModified"] = 0
    results["num_groups"] = 0
    results["num_datatypes"] = 0
    results["datasets"] = {}  # since we need per dataset info
    results["num_chunks"] = 0
    results["allocated_bytes"] = 0

    app["results"] = results
     
    await getS3Keys(app, prefix=root_prefix, include_stats=True, callback=getS3KeysCallback)

    if update:
        # write .info object back to S3
        info_key = root_prefix + ".info.json"
        log.info(f"updating info key: {info_key}")
        await putS3JSONObj(app, info_key, results) 


    return results
