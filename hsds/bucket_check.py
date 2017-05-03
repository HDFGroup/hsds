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
# Head node of hsds cluster
# 
import asyncio
import time

from aiobotocore import get_session

import config
from util.timeUtil import unixTimeToUTC
from util.s3Util import releaseClient
from asyncnode_lib import listKeys, markObj
import hsds_logger as log
 

async def bucketCheck(app):
    """ Verify that contents of bucket are self-consistent
    """
 
    now = int(time.time())
    log.info("bucket check {}".format(unixTimeToUTC(now)))
    # do initial listKeys
    await listKeys(app)
     
    domains = app["domains"]
    # check each domain
    for domain in domains:
        await markObj(app, domain)

    s3keys = app["s3keys"]
    unlink_count = 0
    for s3key in s3keys:
        obj = s3keys[s3key]
        if not obj["used"]:
            print("Key: {} not linked".format(s3key))
            unlink_count += 1

    print("total storage: {}".format(app["bytes_in_bucket"]))
    print("Num domains: {}".format(len(app["domains"])))
    print("Num groups: {}".format(len(app["groups"])))
    print("Num datatypes: {}".format(len(app["datatypes"])))
    print("Num datasets: {}".format(len(app["datasets"])))
    print("Num chunks: {}".format(app["chunk_count"]))
    print("Unlinked objects: {}".format(unlink_count))
 

#
# Main
#

if __name__ == '__main__':    
    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()
    app = {}
    app["bucket_name"] = config.get("bucket_name")
    app["loop"] = loop
    session = get_session(loop=loop)
    app["session"] = session
    loop.run_until_complete(bucketCheck(app))
    releaseClient(app)
    loop.close()

    print("done!")

     
