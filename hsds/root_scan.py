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
from util.idUtil import isValidUuid,isSchema2Id 
from util.s3Util import releaseClient
from async_lib import scanRoot
import config

 
# This is a utility to scan keys for a given domain and report totals.
# Note: only works with schema v2 domains!
    

#
# Print usage and exit
#
def printUsage():  
    print("       python root_scan.py [rootid] [-update]")
    sys.exit(); 
 
 
async def run_scan(app, rootid, update=False):
    results = await scanRoot(app, rootid, update=update)
    await releaseClient(app)
    return results
    
               
def main():
     
    if len(sys.argv) == 1 or len(sys.argv) > 1 and (sys.argv[1] == "-h" or sys.argv[1] == "--help"):
        printUsage()


    rootid = sys.argv[1]

    if len(sys.argv) > 2 and sys.argv[2] == "-update":
        do_update = True
    else:
        do_update = False

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
    loop.run_until_complete(run_scan(app, rootid=rootid, update=do_update))
  
    loop.close()

    results = app["results"]
    datasets = results["datasets"]
    lastModified = datetime.fromtimestamp(results["lastModified"])
    total_size  = results["metadata_bytes"] + results["allocated_bytes"]
    print(f"lastModified: {lastModified}")
    print(f"size: {total_size}")
    print(f"num chunks: {results['num_chunks']}")
    print(f"num_groups: {results['num_groups']}")
    print(f"num_datatypes: {results['num_datatypes']}")
    print(f"num_datasets: {len(datasets)}")
    for dsetid in datasets:
        dataset_info = datasets[dsetid]
        print(f"   {dsetid}: {dataset_info['lastModified']}, {dataset_info['num_chunks']}, {dataset_info['allocated_bytes']}")

    scan_start = datetime.fromtimestamp(results["scan_start"])
    print(f"scan_start: {scan_start}")
    scan_complete = datetime.fromtimestamp(results["scan_complete"])
    print(f"scan_complete: {scan_complete}")


    
    print("done!")

main()
