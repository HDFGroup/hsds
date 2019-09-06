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
from aiohttp.web_exceptions import HTTPInternalServerError


import config
from util.timeUtil import unixTimeToUTC
from util.s3Util import releaseClient
from util.idUtil import isValidUuid, isValidChunkId
from asyncnode_lib import listKeys, getS3Obj, clearUsedFlags, markObjs
import hsds_logger as log
 

async def bucketCheck(app):
    """ Verify that contents of bucket are self-consistent
    """
 
    now = int(time.time())
    log.info("bucket check {}".format(unixTimeToUTC(now)))
     
    # do initial listKeys
    await listKeys(app)

    # clear used flags
    clearUsedFlags(app)

    # mark objs
    await markObjs(app)
     
    unlinked_count = 0
    s3objs = app["s3objs"]
    for objid in s3objs:
        if isValidUuid(objid) and not isValidChunkId(objid):
            try:
                s3obj = await getS3Obj(app, objid)
                if s3obj.used is False:
                    unlinked_count += 1
            except HTTPInternalServerError as hpe:
                log.warn("got error retreiving {}: {}".format(objid, hpe.code))
                
    domains = app["domains"]
    for domain in domains:
        print("domain:", domain)
    roots = app["roots"]
    for root in roots:
        print("root:", root)    

    top_level_domains = []
    for domain in domains:
        if domain[0] != '/':
            log.error("unexpected domain: {}".format(domain))
            continue
        if domain[1:].find('/') == -1:
            top_level_domains.append(domain)

    print("top-level-domains:")
    for domain in top_level_domains:
        print(domain)
    print("="*80)
 
    print("total storage: {}".format(app["bytes_in_bucket"]))
    print("Num objects: {}".format(len(app["s3objs"])))
    print("Num domains: {}".format(len(app["domains"])))
    print("Num root groups: {}".format(len(app["roots"])))
    print("Unlinked objects: {}".format(unlinked_count))
     

#
# Main
#

if __name__ == '__main__':    
    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()
    app = {}
    app["bucket_name"] = config.get("bucket_name")
    app["anonymous_ttl"] = config.get("anonymous_ttl")
    app["s3objs"] = {}
    app["domains"] = {}  # domain to root map
    app["roots"] = {}    # root obj to domain map
    app["deleted_ids"] = set()
    app["bytes_in_bucket"] = 0
    app["loop"] = loop
    session = get_session(loop=loop)
    app["session"] = session
    loop.run_until_complete(bucketCheck(app))
    releaseClient(app)
    loop.close()

    print("done!")

     
