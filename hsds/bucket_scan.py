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
from aiobotocore import get_session
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError
from util.s3Util import releaseClient, getS3Keys, getS3JSONObj
from util.idUtil import getObjId
from async_lib import scanRoot
import config
import hsds_logger as log

 
# List all root keys and create/update info.json
# Note: only works with schema v2 domains!

 
  
async def getS3RootKeysCallback(app, s3keys):
    log.info(f"getS3RootKeysCallback, {len(s3keys)} items")
    if not isinstance(s3keys, list):
        log.error("expected list result for s3keys callback")
        raise ValueError("unexpected callback format")
    results = app["bucket_scan"]
        
    for s3key in s3keys:
        log.info(f"got key: {s3key}")
        if not s3key.startswith("db/") or s3key[-1] != '/':
            log.error(f"unexpected key for getS3RootKeysCallback: {s3key}")
            continue
        root_id = getObjId(s3key + ".group.json")
        log.info(f"root_id: {root_id}")
        results["root_count"] += 1
        
        info_key = s3key + ".info.json"

        if app["scanRootKeys_update"]:
            log.info("updating...")
            await scanRoot(app, root_id, update=True)

        info_obj = None
        try:
            info_obj = await getS3JSONObj(app, info_key)
        except HTTPNotFound:
            pass  # info.json not created yet
        except HTTPInternalServerError as ie:
            log.warn(f"error getting s3obj: {ie}")
            continue

        if info_obj:
            log.info(f"got obj: {info_obj}")
            results["info_count"] += 1
            results["group_count"] += info_obj["num_groups"]
            results["dataset_count"] += len(info_obj["datasets"])
            results["datatype_count"] += info_obj["num_datatypes"]
            results["chunk_count"] += info_obj["num_chunks"]
            results["allocated_bytes"] += info_obj["allocated_bytes"]
    
        

async def scanRootKeys(app, update=False):

    # iterate through all s3 root keys in the bucket.
    #
    # Note: not re-entrant!  Only one scanRoot an be run at a time per app.
    log.info("scanRootKeys")
    app["scanRootKeys_update"] = update

    await getS3Keys(app, prefix="db/", deliminator='/', include_stats=False, callback=getS3RootKeysCallback)




#
# Print usage and exit
#
def printUsage():  
    print("       python bucket_scan.py [--update]")
    sys.exit(); 
 
 
async def run_scan(app, update=False):
    scan_results = {}
    scan_results["root_count"] = 0
    scan_results["info_count"] = 0
    scan_results["updated_count"] = 0
    scan_results["group_count"] = 0
    scan_results["dataset_count"] = 0
    scan_results["datatype_count"] = 0
    scan_results["chunk_count"] = 0
    scan_results["allocated_bytes"] = 0
    app["bucket_scan"] = scan_results
    results = await scanRootKeys(app, update=update)
    await releaseClient(app)
    return results
    
               
def main():
     
    if len(sys.argv) > 1 and (sys.argv[1] == "-h" or sys.argv[1] == "--help"):
        printUsage()


    if len(sys.argv) > 1 and sys.argv[1] == "--update":
        do_update = True
    else:
        do_update = False

         
    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()
    
    app = {}
    app["bucket_name"] = config.get("bucket_name")
    app["loop"] = loop
    session = get_session(loop=loop)
    app["session"] = session
    loop.run_until_complete(run_scan(app, update=do_update))
  
    loop.close()

    results = app["bucket_scan"]
    print("root_count:", results["root_count"])
    print("info_count:", results["info_count"])
    print("group_count", results["group_count"])
    print("dataset_count:", results["dataset_count"])
    print("datatype_count", results["datatype_count"])
    print("chunk_count:"), results["chunk_count"]
    print('allocated_bytes:', results["allocated_bytes"])
    print("updated_count:", results["updated_count"])

    print("done!")

main()
