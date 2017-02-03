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
from util.s3Util import getS3Client, deleteS3Obj
import config

 
# This is a utility to list all objects in the bucket
    

#
# Print usage and exit
#
def printUsage():
     
    print(" python delete_bucket.py")
    print("Removes all objects in the bucket!")
    sys.exit(); 

async def fetch_all(pages):
    responses = []
    while True:
        n = await pages.next_page()
        if n is None:
            break
        responses.append(n)
    return responses

async def deleteAll(app):
    print("getting list of objects")
    keys = await listObjects(app)
    print("got: {} objects".format(len(keys)))
    if len(keys) == 0:
        print("bucket is empty!")
        return
    # verify we really want to do this!
    response = input("Enter 'Y' to continue:")
    if response != 'Y':
        print("cancel")
        return

    for key in keys:
        await deleteS3Obj(app, key)
        
    print("delete!")

async def listObjects(app):
    keys = []
    s3_client = app['s3']
    paginator = s3_client.get_paginator('list_objects')
    bucket_name = app["bucket_name"]
    print("bucket:", bucket_name)
    pages = paginator.paginate(MaxKeys=1000, Bucket=bucket_name)
    responses = await fetch_all(pages)
    print("got {} responses".format(len(responses)))
    
    for response in responses:
        #print(response)
        if 'Contents' in response:
            contents = response['Contents']
            for item in contents:
                key_names = item['Key']
                keys.append(key_names)
                print(key_names)
    return keys
                 
     
               
def main():
     
    if len(sys.argv) > 1 and (sys.argv[1] == "-h" or sys.argv[1] == "--help"):
        printUsage()
        sys.exit(1)
     
    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()
    #loop.run_until_complete(init(loop))   
    session = get_session(loop=loop)

    s3client = getS3Client(session)

    app = {}
    app['s3'] = s3client
    app['bucket_name'] = config.get("bucket_name")

    loop.run_until_complete(deleteAll(app))
    
    loop.close()
    s3client.close()

    print("done!")

         
            
    

main()

    
	
