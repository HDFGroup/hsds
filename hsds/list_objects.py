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
from util.s3Util import getS3Keys, releaseClient
import config

 
# This is a utility to list all objects in the bucket
    

#
# Print usage and exit
#
def printUsage():
     
    print("       python list_objects.py [prefix] [deliminator]")
    sys.exit(); 
  
async def listObjects(app, prefix='', deliminator=''):
    s3keys = await getS3Keys(app, prefix=prefix, deliminator=deliminator)
    
    print("got {} responses".format(len(s3keys)))
    for s3key in s3keys:
        print(s3key)
   
               
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
    
    app = {}
    app["bucket_name"] = config.get("bucket_name")
    app["loop"] = loop
    session = get_session(loop=loop)
    app["session"] = session
    loop.run_until_complete(listObjects(app, prefix=prefix, deliminator=deliminator))
    releaseClient(app)
    loop.close()

    print("done!")

main()

    
	
