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

if "CONFIG_DIR" not in os.environ:
    os.environ["CONFIG_DIR"] = "../admin/config/"

from aiobotocore.session import get_session
from hsds.util.storUtil import getStorKeys, releaseStorageClient
from hsds import config


# This is a utility to list all objects in the bucket


#
# Print usage and exit
#
def printUsage():
    usage = "      python list_objects.py [--prefix <prefix> ] "
    usage += "[--deliminator deliminator] [--showstats]"

    print(usage)
    sys.exit()


def getS3KeysCallback(app, s3keys):
    print("getS3KeysCallback, {} items".format(len(s3keys)))
    if isinstance(s3keys, list):
        for s3key in s3keys:
            print(s3key)
    else:
        for s3key in s3keys.keys():
            etag = None
            obj_size = None
            lastModified = None
            item = s3keys[s3key]
            print("item:", item)
            if "ETag" in item:
                etag = item["ETag"]
            if "Size" in item:
                obj_size = item["Size"]
            if "LastModified" in item:
                lastModified = item["LastModified"]
            print("{}: {} {} {}".format(s3key, etag, obj_size, lastModified))


async def listObjects(app, prefix="", deliminator="", suffix="", showstats=False):
    await getStorKeys(
        app,
        prefix=prefix,
        deliminator=deliminator,
        suffix=suffix,
        include_stats=showstats,
        callback=getS3KeysCallback,
    )
    await releaseStorageClient(app)


def main():

    if len(sys.argv) > 1 and (sys.argv[1] == "-h" or sys.argv[1] == "--help"):
        printUsage()

    prefix = ""
    deliminator = ""
    suffix = ""
    showstats = False

    argn = 1
    while argn < len(sys.argv):
        arg = sys.argv[argn]
        val = None
        if len(sys.argv) > argn + 1:
            val = sys.argv[argn + 1]
        if arg == "--prefix":
            prefix = val
            argn += 2
        elif arg == "--deliminator":
            deliminator = val
            argn += 2
        elif arg == "--suffix":
            suffix = val
            argn += 2
        elif arg == "--showstats":
            showstats = True
            argn += 1
        else:
            printUsage()

    print("prefix:", prefix)
    print("deliminator:", deliminator)
    print("suffix:", suffix)
    print("showstats:", showstats)

    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()

    app = {}
    app["bucket_name"] = config.get("bucket_name")
    app["loop"] = loop
    session = get_session()
    app["session"] = session
    app["filter_map"] = {}
    loop.run_until_complete(
        listObjects(
            app,
            prefix=prefix,
            deliminator=deliminator,
            suffix=suffix,
            showstats=showstats,
        )
    )

    loop.close()

    print("done!")


main()
