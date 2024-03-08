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

#
# Return data from given bucket and object key
#

import asyncio
import sys
import os
from aiobotocore.session import get_session

if "CONFIG_DIR" not in os.environ:
    os.environ["CONFIG_DIR"] = "../admin/config/"

from hsds.util.storUtil import releaseStorageClient, getStorBytes
from hsds import config


async def get_bytes(app, key, offset=0, length=64, bucket=None):
    data = await getStorBytes(app, key, offset=offset, length=length, bucket=bucket)
    n = 0
    while n < length:
        hex = "".join(f"{n:02X}" for n in data[n:n + 64])
        print(f"{n:06d}-{n + 64:06d}: {hex}")
        n += 64

    await releaseStorageClient(app)


def main():

    if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
        print("usage: <bucket/key> [offset] [length]")
        sys.exit(1)

    s3path = sys.argv[1]

    if len(sys.argv) > 2:
        offset = int(sys.argv[2])
    else:
        offset = 0

    if len(sys.argv) > 3:
        length = int(sys.argv[3])
    else:
        length = 64

    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()

    if s3path.startswith("s3://"):
        # strip off the protocol chars
        s3path = s3path[len("s3://"):]

    if s3path[0] == "/":
        bucket = None  # use default bucket
        key = s3path[1:]
    else:
        n = s3path.find("/")
        if n == -1:
            print("storage path is invalid")
            sys.exit(1)
        bucket = s3path[:n]
        key = s3path[n + 1:]

    app = {}
    app["bucket_name"] = config.get("bucket_name")
    app["loop"] = loop
    session = get_session()
    app["session"] = session

    loop.run_until_complete(get_bytes(app, key, offset=offset, length=length, bucket=bucket))

    loop.close()


main()
