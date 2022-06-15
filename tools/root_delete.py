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

from hsds.util.idUtil import isValidUuid, isSchema2Id
from hsds.util.storUtil import releaseStorageClient

from hsds.async_lib import removeKeys
from hsds import config

# This is a utility to remove all keys for a given rootid
# Note: only works with schema v2 domains!


#
# Print usage and exit
#
def printUsage():
    print("       python root_delete.py [rootid]")
    sys.exit(0)


async def run_delete(app, rootid):
    results = await removeKeys(app, rootid)
    await releaseStorageClient(app)
    return results


def main():

    if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
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
    app["objDelete_prefix"] = None
    session = get_session()
    app["session"] = session
    app["filter_map"] = {}
    loop.run_until_complete(run_delete(app, rootid))

    loop.close()

    print("done!")


main()
