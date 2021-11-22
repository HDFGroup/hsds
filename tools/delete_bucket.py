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
import os
from aiobotocore.session import get_session

if "CONFIG_DIR" not in os.environ:
    os.environ["CONFIG_DIR"] = "../admin/config/"
from hsds import config
from hsds.util.storUtil import releaseStorageClient, deleteStorObj, getStorKeys



# This is a utility to delete all objects in the bucket


#
# Print usage and exit
#
def printUsage():

    print("python delete_bucket.py")
    print("Removes all objects in the bucket!")
    sys.exit(0)


async def deleteAll(app):
    print(f"getting list of objects for bucket: {app['bucket_name']}")
    keys =  await getStorKeys(app)
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
        await deleteStorObj(app, key)

    print("deleted!")
    log.info("closing storage connections")
    await releaseStorageClient(app)


def main():

    if len(sys.argv) > 1 and (sys.argv[1] == "-h" or sys.argv[1] == "--help"):
        printUsage()
        sys.exit(1)

    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()
    #loop.run_until_complete(init(loop))
    session = get_session()

    app = {}
    app['bucket_name'] = config.get("bucket_name")
    app["session"] = session
    app["loop"] = loop
    app["filter_map"] = {}

    loop.run_until_complete(deleteAll(app))
    #releaseClient(app)

    loop.close()

    print("done!")

main()



