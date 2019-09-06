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
import json
from aiobotocore import get_session
from aiohttp.client_exceptions import ClientOSError

sys.path.append('../../hsds')
sys.path.append('../../hsds/util')
from util.storUtil import getStorJSONObj, isStorObj, releaseStorageClient
from util.idUtil import getS3Key, isValidUuid
import config
 
# This is a utility to dump a JSON obj (group, dataset, ctype) given the
# the objects UUID
    
#
# Print usage and exit
#
def printUsage():
    print("usage: python get_s3json [--bucket_name=<bucket>] [--aws_s3_gateway=<s3_endpoint>] objid ")
    print("  objid: s3 JSON obj to fetch")
    print("  Example: python get_s3json --aws_s3_gateway=http://192.168.99.100:9000 --bucket_name=minio.hsdsdev t-cf2fc310-996f-11e6-8ef6-0242ac110005")
    sys.exit(); 
    
async def printS3Obj(app, obj_id):
    try:
        s3_key = getS3Key(obj_id)
        obj_exists = await isStorObj(app, s3_key)
        if not obj_exists:
            print(f"key: {s3_key} not found")
            return
        json_obj = await getStorJSONObj(app, s3_key)
        print(f"s3key {s3_key}:")
        print(json.dumps(json_obj, sort_keys=True, indent=4))
    except ValueError as ve:
        print(f"Got ValueError exception: {ve}")
    except ClientOSError as coe:
        print(f"Got error: {coe}") 
    await releaseStorageClient(app)
    
               
def main():
    if len(sys.argv) == 1 or sys.argv[1] == "-h" or sys.argv[1] == "--help":
        printUsage()
        sys.exit(1)
 
    obj_id = sys.argv[-1]
    if not isValidUuid(obj_id):
        print("Invalid obj id")

    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()
    session = get_session(loop=loop)

    app = {}
    app["session"] = session
    app['bucket_name'] = config.get("bucket_name")

    loop.run_until_complete(printS3Obj(app, obj_id))
    
    loop.close()
    

main()
