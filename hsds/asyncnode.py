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
from datetime import datetime

from aiohttp.web import run_app, json_response
#from aiohttp.web_exceptions import HTTPBadRequest, HTTPNotFound, HTTPConflict, HTTPInternalServerError, HTTPServiceUnavailable
from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp.client_exceptions import ClientError
import config
from basenode import baseInit, healthCheck
#from util.chunkUtil import getDatasetId
from util.idUtil import isValidUuid, getObjId, isSchema2Id, getRootObjId
from util.s3Util import getS3Keys
from async_lib import scanRoot
import hsds_logger as log
#from async_lib import scanRoot
  
 
async def GET_AsyncInfo(request):
    """HTTP Method to retun async node state to caller"""
    log.request(request)
    app = request.apps
    answer = {}
    answer["bucket_stats"] = app["bucket_stats"]
    resp = json_response(answer)
    log.response(request, resp=resp)
    return resp

async def PUT_Objects(request):
    """HTTP method to notify creation/update of objid"""
    log.request(request)
    app = request.app
    pending = app["pending"]
    log.info("PUT_Objects")

    if not request.has_body:
        msg = "PUT objects with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    body = await request.json()
    log.debug("Got PUT Objects body: {}".format(body))
    if "objs" not in body:
        msg = "expected to find objs key in body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    objs = body["objs"]
    for objid in objs:
        log.debug("PUT_Objects, objid: {}".format(objid))
        if not isValidUuid(objid):
            log.warn(f"Invalid id: {objid}, ignoring")
            continue

        if not isSchema2Id(objid):
            log.info(f"PUT_Objects ignoring v1 id: {objid}")
            continue
        rootid = getRootObjId(objid)
        log.debug(f"adding root: {rootid} to pending queue for objid: {objid}")
        pending.add(rootid) 

    resp_json = {  } 
    resp = json_response(resp_json, status=201)
    log.response(request, resp=resp)
    return resp

async def PUT_Object(request):
    """HTTP method to notify creation/update of objid"""
    log.request(request)
    app = request.app
    pending = app["pending"]
    objid = request.match_info.get('id')
    if not objid:
        log.error("PUT_Object with no id")
        raise HTTPBadRequest()

    log.info(f"PUT_Object/{objid}")
 
    if not isValidUuid(objid):
        log.warn(f"Invalid id: {objid}, ignoring")
        raise HTTPBadRequest()

    if isSchema2Id(objid):
        rootid = getRootObjId(objid)
        log.debug(f"adding root: {rootid} to pending queue for objid: {objid}")
        pending.add(rootid) 

    resp_json = {  } 
    resp = json_response(resp_json, status=201)
    log.response(request, resp=resp)
    return resp


async def PUT_Domain(request):
    """HTTP method to get object s3 state """
    log.request(request)
    
    app = request.app
    pending = app["pending"]
    params = request.rel_url.query
    if "domain" not in params:
        msg = "No domain provided"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    domain = params["domain"]

    if not domain.startswith("/"):
        msg = "Domain expected to start with /"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if len(domain) < 2:
        msg = "Invalid domain"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if "root" in params:
        rootid = params["root"]
       
        if not isValidUuid(rootid):
            log.warn(f"Invalid id: {rootid}")
            raise HTTPBadRequest()
        log.debug(f"new rootid: {rootid} for domain: {domain}")

        if isSchema2Id(rootid):
            log.info(f"Adding root: {rootid} to pending for PUT domain: {domain}")
            pending.add(rootid)

    resp_json = {}
    resp = json_response(resp_json, status=201)
    log.response(request, resp=resp)
    return resp

async def DELETE_Domain(request):
    log.request(request)

    #app = request.app
    params = request.rel_url.query
    if "domain" not in params:
        msg = "No domain provided"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    domain = params["domain"]

    if not domain.startswith("/"):
        msg = "Domain expected to start with /"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if len(domain) < 2:
        msg = "Invalid domain"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if "root" in params:
        rootid = params["root"]
        log.debug(f"delete rootid: {rootid} for domain: {domain}")
        if not isValidUuid(rootid):
            log.warn(f"Invalid id: {rootid}")
            raise HTTPBadRequest()

        if isSchema2Id(rootid):
            # TBD: schedule root collection for deletion
            pass

    resp_json = {}
    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp

async def DELETE_Object(request):
    log.request(request)

    app = request.app
    pending = app["pending"]

    objid = request.match_info.get('id')
    if not isValidUuid(objid):
        log.warn(f"Invalid id: {objid}")
        raise HTTPBadRequest()

    if isSchema2Id(objid):
        # get rootid for this id
        rootid = getRootObjId(objid)
        log.info(f"Adding root: {rootid} to pending for obj deletion of: {objid}")
        pending.add(rootid)

    resp_json = {}
    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp

async def bucketScanCallback(app, s3keys):
    log.info(f"getS3RootKeysCallback, {len(s3keys)} items")
    if not isinstance(s3keys, list):
        log.error("expected list result for s3keys callback")
        raise ValueError("unexpected callback format")

    pending = app["pending"]
        
    for s3key in s3keys:
        log.info(f"got key: {s3key}")
        if not s3key.startswith("db/") or s3key[-1] != '/':
            log.error(f"unexpected key for getS3RootKeysCallback: {s3key}")
            continue
        rootid = getObjId(s3key + ".group.json")
        log.info(f"root_id: {rootid}")

        # if there are many items in the pending set, wait for it to drain a bit
        while len(pending) > 100:
            log.info(f"bucketScan: waiting for pending to drain: {len(pending)}")
            await asyncio.sleep(1)
        log.debug(f"bucket scan - adding key {rootid} to pending")
        pending.add(rootid)

    log.info("getS3RootKeysCallback complete")

 
async def processPending(app):
    """ Process rootids in pending set """      
    pending = app["pending"]
    pending_count = len(pending)
    #conn = app["conn"]
    if pending_count == 0:
        return 0 # nothing to do
    log.info("processPendingSet start - {} items".format(pending_count))    
     
    # TBD - this could starve other work if items are getting added to the pending
    # queue continually.  Copy items off pending queue synchronously and then process?
    while len(pending) > 0:
        log.debug("pending len: {}".format(len(pending)))
        rootid = pending.pop()  # remove from the front

        log.debug("pop from pending set: obj: {}".format(rootid))
        if not isValidUuid(rootid):
            log.error(f"Invalid root id: {rootid}")
            continue

        if not isSchema2Id(rootid):
            log.info(f"ignoring v1 id: {rootid}")
            continue

        await scanRoot(app, rootid, update=True)

      

async def pendingCheck(app):
    """ Periodic method to check pending updates 
    """
    log.info("pendingCheck start")

    async_sleep_time = config.get("async_sleep_time")
    log.info("async_sleep_time: {}".format(async_sleep_time))
     
    # update/initialize root object before starting node updates
 
    while True:  
        if app["node_state"] != "READY":
            log.info("pendingCheck waiting for Node state to be READY")
            await asyncio.sleep(1)
            continue  # wait for READY state
            
        try:
            await processPending(app)
        except Exception as e:
            log.warn("bucketCheck - got exception from processPendingQueue: {}".format(e))

        
        await asyncio.sleep(async_sleep_time)   

    # shouldn't ever get here     
    log.error("bucketCheck terminating unexpectedly")


async def bucketScan(app):
    """ Scan all v2 keys in the bucket 
    """
 
    log.info("bucketScan start")
    last_scan = app["last_bucket_scan"]

    async_sleep_time = config.get("async_sleep_time")
    log.info("async_sleep_time: {}".format(async_sleep_time))
     
    # update/initialize root object before starting node updates
 
    while True:  
        if app["node_state"] != "READY":
            log.info("bucketScan waiting for Node state to be READY")
            await asyncio.sleep(1)
            continue  # wait for READY state

        now = time.time()
        date = datetime.fromtimestamp(now)
        # run the scan if the last scan was more than an hour ago and
        # the local hour is 0 (i.e. after midnight)
        if int(now - last_scan) > 60*60 and date.hour == 0:
            log.info(f"starting bucket scan: {date}")
            try:
                await getS3Keys(app, prefix="db/", deliminator='/', include_stats=False, callback=bucketScanCallback)
            except ClientError as ce:
                log.error(f"getS3Keys faiiled: {ce}")
            now = time.time*()
            log.info(f"bucketScan complete {datetime.fromtimestamp(now)}")
            app["last_bucket_scan"] = int(time.time())

        await asyncio.sleep(async_sleep_time)   

    # shouldn't ever get here     
    log.error("bucketScan terminating unexpectedly")



async def init(loop):
    """Intitialize application and return app object"""
    
    app = baseInit(loop, 'an')
    app.router.add_route('GET', '/async_info', GET_AsyncInfo)
    app.router.add_route('PUT', '/objects', PUT_Objects)
    app.router.add_route('PUT', '/object/{id}', PUT_Object)
    app.router.add_route('DELETE', '/object/{id}', DELETE_Object)
    app.router.add_route('PUT', '/domain', PUT_Domain)
    app.router.add_route('DELETE', '/domain', DELETE_Domain)
    # set of rootids to scans
    app["pending"] = set() 
    app["bucket_stats"] = {}
    app["last_bucket_scan"] = 0
    app["anonymous_ttl"] = int(config.get("anonymous_ttl"))
    log.info("anonymous_ttl: {}".format(app["anonymous_ttl"]))
    app["updated_domains"] = set()
     
    return app

#
# Main
#

if __name__ == '__main__':
    log.info("AsyncNode initializing")
    
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init(loop))   
   
    # run background tasks
    asyncio.ensure_future(pendingCheck(app), loop=loop) 
    asyncio.ensure_future(bucketScan(app), loop=loop) 
    asyncio.ensure_future(healthCheck(app), loop=loop)

    async_port = config.get("an_port")
    log.info("Starting service on port: {}".format(async_port))
    run_app(app, port=int(async_port))
