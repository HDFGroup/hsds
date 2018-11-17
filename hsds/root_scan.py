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
from datetime import datetime
from aiobotocore import get_session
from util.idUtil import isValidUuid,isSchema2Id, getS3Key, isS3ObjKey, getObjId, isValidChunkId, getCollectionForId
from util.chunkUtil import getDatasetId
from util.s3Util import getS3Keys, releaseClient
import hsds_logger as log
import config

 
# This is a utility to scan keys for a given domain and report totals.
# Note: only works with schema v2 domains!
    

#
# Print usage and exit
#
def printUsage():
     
    print("       python root_scan.py [rootid]")
    sys.exit(); 
  
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
       

async def scanRoot(app, rootid):

    log.info(f"scanRoot for rootid: {rootid}")
    root_key = getS3Key(rootid)

    if not root_key.endswith("/.group.json"):
        raise ValueError("unexpected root key")
    root_prefix = root_key[:-(len(".group.json"))]
    
    log.debug("using prefix: {root_prefix}")
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
    await releaseClient(app)
    
               
def main():
     
    if len(sys.argv) == 1 or len(sys.argv) > 1 and (sys.argv[1] == "-h" or sys.argv[1] == "--help"):
        printUsage()


    rootid = sys.argv[1]

    if not isValidUuid(rootid):
        print("Invalid root id!")
        sys.exit(1)

    if not isSchema2Id(rootid):
        print("This tool can only be used with Schema v2 ids")
        sys.exit(1)
         
    
    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()
    
    app = {}
    app["bucket_name"] = config.get("bucket_name")
    app["loop"] = loop
    session = get_session(loop=loop)
    app["session"] = session
    loop.run_until_complete(scanRoot(app, rootid=rootid))
  
    loop.close()

    results = app["results"]
    datasets = results["datasets"]
    dt = datetime.fromtimestamp(results["lastModified"])
    print(f"lastModified: {results['lastModified']} ({dt})")
    print(f"size: {results['allocated_bytes']}")
    print(f"num chunks: {results['num_chunks']}")
    print(f"num_groups: {results['num_groups']}")
    print(f"num_datatypes: {results['num_datatypes']}")
    print(f"num_datasets: {len(datasets)}")
    for dsetid in datasets:
        dataset_info = datasets[dsetid]
        print(f"   {dsetid}: {dataset_info['lastModified']}, {dataset_info['num_chunks']}, {dataset_info['allocated_bytes']}")

    print("done!")

main()
