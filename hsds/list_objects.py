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
from util.s3Util import getS3Client
import config

 
# This is a utility to list all objects in the bucket
    

#
# Print usage and exit
#
def printUsage():
     
    print("       python list_objects.py [prefix] [deliminator]")
    sys.exit(); 

async def fetch_all(pages):
    responses = []
    while True:
        n = await pages.next_page()
        if n is None:
            break
        responses.append(n)
    return responses
    
async def listObjects(app, prefix='', deliminator=''):
    s3_client = app['s3']
    paginator = s3_client.get_paginator('list_objects')
    bucket_name = app["bucket_name"]
    print("bucket:", bucket_name)
    pages = paginator.paginate(MaxKeys=1000, Bucket=bucket_name, Prefix=prefix, Delimiter=deliminator)
    responses = await fetch_all(pages)
    print("got {} responses".format(len(responses)))
    
    for response in responses:
        print(response)
        if 'Contents' in response:
            contents = response['Contents']
            for item in contents:
                key_names = item['Key']
                print(key_names)
     
               
def main():
     
    if len(sys.argv) > 1 and (sys.argv[1] == "-h" or sys.argv[1] == "--help"):
        printUsage()
        sys.exit(1)

    prefix = ''
    deliminator = '' 
    if len(sys.argv) > 1:
        prefix = sys.argv[1]

    if len(sys.argv) > 2:
        deliminator = sys.argv[2]

     
    
    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()
    #loop.run_until_complete(init(loop))   
    session = get_session(loop=loop)

    s3client = getS3Client(session)

    app = {}
    app['s3'] = s3client
    app['bucket_name'] = config.get("bucket_name")

    loop.run_until_complete(listObjects(app, prefix=prefix, deliminator=deliminator))
    
    loop.close()
    s3client.close()

    print("done!")

         
            
    

main()

    
	
