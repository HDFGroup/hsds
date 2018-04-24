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
from os.path import isfile, join
import time

from aiohttp.web import run_app
from aiohttp.errors import HttpBadRequest, HttpProcessingError
import sqlite3

import config
from basenode import baseInit, healthCheck
#from util.timeUtil import unixTimeToUTC
#from util.s3Util import putS3Bytes, isS3Obj, deleteS3Obj, getS3Keys
#from util.s3Util import getS3JSONObj
from util.idUtil import isValidChunkId, isValidUuid #, getCollectionForId  #isS3ObjKey, getObjId
from util.httpUtil import jsonResponse, StreamResponse
from util.domainUtil import  isValidDomain
#from asyncnode_lib import listKeys, markObjs, deleteObj, getS3Obj, getRootProperty, clearUsedFlags, getDomainForObjId
#from util.dbutil import dbInitTable, insertDomainTable, batchInsertChunkTable, insertObjectTable, insertTLDTable
from util.dbutil import getRow, getTopLevelDomains
import hsds_logger as log


#
# lib methods
#
"""
async def getRootProperty(app, objid):
    # Get the root property if not already set 
    log.debug("getRootProperty {}".format(objid))
    
    if isValidDomain(objid):
        log.debug("got domain id: {}".format(objid))
    else:
        if not isValidUuid(objid) or isValidChunkId(objid):
            raise ValueError("unexpected key for root property: {}".format(objid))
    s3key = getS3Key(objid)
    obj_json = await getS3JSONObj(app, s3key)
    rootid = None
    if "root" not in obj_json:
        if isValidDomain(objid):
            log.info("No root for folder domain: {}".format(objid))
        else:
            log.error("no root for {}".format(objid))
    else:
        rootid = obj_json["root"]
        log.debug("got rootid {} for obj: {}".format(rootid, objid))
    return rootid
"""

    
#
# pending queue handler
#

     

async def rootDelete(app, rootid):
    """ get root obj for rootid """
    log.info("rootDelete {}".format(rootid))
     
    # await deleteObj(app, rootid, notify=False)
     
async def domainDelete(app, domain):
    """ Process domain deletion event """
    log.info("domainDelete: {}".format(domain))
     
    

async def domainCreate(app, domain):
    """ Process domain creation event """
    log.info("domainCreate: {}".format(domain))
     


async def objUpdate(app, objid):
    """ Process object update event """
    log.info("objUpdate: {}".format(objid))
    
    if not isValidChunkId(objid) and not isValidUuid(objid):
        log.error("Got unexpected objid: {}".format(objid))
        return

async def objDelete(app, objid):
    """ Process object update event """
    log.info("objUpdate: {}".format(objid))

    if not isValidChunkId(objid) and not isValidUuid(objid):
        log.error("Got unexpected objid: {}".format(objid))
        return

    
 
async def processPendingQueue(app):
    """ Process any pending queue events """      

    pending_queue = app["pending_queue"]
    pending_count = len(pending_queue)
    if pending_count == 0:
        return # nothing to do
    log.info("processPendingQueue start - {} items".format(pending_count))    
    domain_updates = set()  # set of ids for which we'll need to update domain content
    dataset_updates = set()
    # TBD - this could starve other work if items are getting added to the pending
    # queue continually.  Copy items off pending queue synchronously and then process?
    while len(pending_queue) > 0:
        log.debug("pending_queue len: {}".format(len(pending_queue)))
        item = pending_queue.pop(0)  # remove from the front
        objid = item["objid"]
        action = item["action"]
        log.debug("pop from pending queue: obj: {} action: {}".format(objid, action))
            
        if isValidDomain(objid):
            if action == "DELETE":
                await domainDelete(app, objid)
            elif action == "PUT":
                await domainCreate(app, objid)
                domain_updates.add(objid)
            else:
                log.error("Unexpected action: {}".format(action))
        elif isValidChunkId(objid) or isValidUuid(objid):
            if action == "PUT":
                await objUpdate(app, objid)
            elif action == "DELETE":
                await objDelete(app, objid)
            else:
                log.error("Unexpected action: {}".format(action))
                continue
            domain_updates.add(objid)
            if isValidChunkId(objid):
                dataset_updates.add(objid)

    log.info("processPendingQueue stop")
    log.info("domains to be updated: {}".format(len(domain_updates)))
    log.info("datasets to be updated: {}".format(len(dataset_updates)))
                 
 

async def bucketCheck(app):
    """ Periodic method for GC and pending queue updates 
    """
 
    app["last_bucket_check"] = int(time.time())

    async_sleep_time = config.get("async_sleep_time")
    log.info("async_sleep_time: {}".format(async_sleep_time))
     
    # update/initialize root object before starting node updates
 
    while True:  
        if app["node_state"] != "READY":
            log.info("bucketCheck waiting for Node state to be READY")
            await asyncio.sleep(1)
            continue  # wait for READY state
        
        pending_queue = app["pending_queue"]
        if len(pending_queue) > 0:
            
            try:
                await processPendingQueue(app)
            except Exception as e:
                log.warn("bucketCheck - got exception from processPendingQueue: {}".format(e))

        
        await asyncio.sleep(async_sleep_time)   

    # shouldn't ever get here     
    log.error("bucketCheck terminating unexpectedly")
     
def updateBucketStats(app):  
    """ Collect some high level stats for use by the info request """
    bucket_stats = app["bucket_stats"]
    bucket_stats["object_count"] = 42
    """
    s3objs = app["s3objs"]
    domains = app["domains"]
    roots = app["roots"]
    deleted_ids = app["deleted_ids"]
    pending_queue = app["pending_queue"]
    
    bucket_stats["object_count"] = len(s3objs)  
    bucket_stats["domain_count"] = len(domains)
    bucket_stats["root_count"] = len(roots)
    bucket_stats["storage_size"] = app["bytes_in_bucket"]
    bucket_stats["pending_count"] = len(pending_queue)    
    bucket_stats["deleted_count"] = len(deleted_ids)
    """
        

async def GET_AsyncInfo(request):
    """HTTP Method to retun async node state to caller"""
    log.request(request)
    app = request.app
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
    updateBucketStats(app)
    answer = {}
    answer["bucket_stats"] = app["bucket_stats"]
    resp = await jsonResponse(request, answer) 
    log.response(request, resp=resp)
    return resp

async def PUT_Objects(request):
    """HTTP method to notify creation/update of objid"""
    log.request(request)
    app = request.app
    log.info("PUT_Objects")
    conn = app["conn"]
    if not conn:
        msg = "db not initizalized"
        log.warn(msg)
        raise HttpProcessingError(code=501, message=msg)

    if not request.has_body:
        msg = "PUT objects with no body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    body = await request.json()
    if "objs" not in body:
        msg = "expected to find objs key in body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    objs = body["objs"]
    for obj in objs:
        if "id" not in obj:
            log.error("Expected id in PUT_Objects request")
            continue
        if "lastModified" not in obj:
            log.error("Expected lastModified in PUT_Objects request for id: {}".format(obj["id"]))
            continue
        if "size" not in obj:
            log.error("Expected size in PUT_Objects request for id: {}".format(obj["id"]))
            continue
        
        objid = obj["id"]
        lastModified = obj["lastModified"]
        etag = ''
        if "root"  in obj:
            rootid = obj["root"]
        else:
            rootid = ''
        if isValidDomain(objid):
            try:
                insertRow(conn, "DomainTable", objid, lastModified=lastModified, objSize=objSize, rootid=rootid)
            except KeyError:
                log.warn("got KeyError inserting domain: {}".format(id))
                continue
            # is this a top-level domain?
            index = id[1:-1].find('/')  # look for interior slash - isValidDomain implies len > 2
            if index == -1:
                log.info("Top-level-domain name received: {}".format(objid))
                try:
                    insertTLDTable(conn, objid)
                except KeyError:
                    log.warn("got KeyError inserting TLD: {}".format(objid))
                    continue
        elif isValidUuid(objid):
            if not rootid:
                log.error("no rootid provided for obj: {}".format(objid))
                continue
            collection = getCollectionForId(id)
            if collection == "groups":
                table = "GroupTable"
            elif collection == "datatypes":
                table = "TypeTable"
            elif collection == "datasets":
                table = "DatasetTable"
            else:
                log.error("Unexpected collection: {}".format(collection))
                continue
            try:
                insertRow(conn, table, objid, etag=etag, lastModified=lastModified, objSize=objSize, rootid=rootid)
            except KeyError:
                log.error("got KeyError inserting object: {}".format(id))
                continue
            # if this is a new root group, add to root table
            if collection == "groups" and objid == rootid:
                try:
                    insertRow(conn, "RootTable", rootid, etag=etag, lastModified=lastModified, objSize=objsize, groupCount=1)            
                except KeyError:
                    log.error("got KeyError inserting root: {}".format(rootid))
                    continue
            else:
                # update size and lastModified in root table
                try:
                    rootEntry = getRow(conn, rootid, table="RootTable")
                except KeyError:
                    log.error("Unable to find {} in RootTable".format(rootid))
                    continue
                if "size" not in rootEntry:
                    log.error("Expected to find size in RootTable for root: {}".format(rootid))
                    continue
                domain_size = rootEntry["size"] + objsize

                updateRowColumn(conn, "RootTable", "size", rootid, domain_size)
                # update lastModified timestamp
                updateLastModified(conn, "RootTable", "lastModified", rootid, lastModified)
        else:
            msg = "PUT_Objects Invalid id: {}".format(objid)
            log.warn(msg)
            raise HttpBadRequest(message=msg)

    pending_queue = app["pending_queue"]
    for objid in objids:
        item = {"objid": objid, "action": "PUT"}
        log.info("adding item: {} to pending queue".format(item))
        pending_queue.append(item)

    resp_json = {  } 
    resp = await jsonResponse(request, resp_json, status=201)
    log.response(request, resp=resp)
    return resp

async def DELETE_Objects(request):
    """HTTP method to notify deletion of objid"""
    log.request(request)
    app = request.app
    log.info("DELETE_Objects")

    if not request.has_body:
        msg = "DELETE objects with no body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    body = await request.json()
    if "objids" not in body:
        msg = "expected to find objids key in body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    objids = body["objids"]
    for objid in objids:
        if not isValidDomain(objid) and not isValidUuid(objid):
            msg = "DELETE_Objects Invalid id: {}".format(objid)
            log.warn(msg)
            raise HttpBadRequest(message=msg)

    pending_queue = app["pending_queue"]
    for objid in objids:
        item = {"objid": objid, "action": "DELETE"}
        log.info("adding item: {} to pending queue".format(item))
        pending_queue.append(item)

    resp_json = {  } 
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

async def GET_Object(request):
    """HTTP method to get object s3 state """
    log.request(request)
    app = request.app
    log.info("GET_Object")
    conn = app["conn"]
    if not conn:
        msg = "db not initizalized"
        log.warn(msg)
        raise HttpProcessingError(code=501, message=msg)
    

    objid = request.match_info.get('id')
    if "Root" in request.GET:
        rootid = request.GET["Root"]
    else:
        rootid = ''
    resp_json = getRow(conn, objid, rootid=rootid)
    if not resp_json:
        msg = "objid: {} not found".format(objid)
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)

    log.info("GET_Object response: {}".format(resp_json))
    
    resp = await jsonResponse(request, resp_json, status=200)
    log.response(request, resp=resp)
    return resp

async def GET_Domain(request):
    """HTTP method to get object s3 state """
    log.request(request)
    
    app = request.app
    if "domain" not in request.GET:
        msg = "No domain provided"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    domain = request.GET["domain"]

    log.info("GET_Domain: {}".format(domain))

    if not domain.startswith("/"):
        msg = "Domain expected to start with /"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    conn = app["conn"]
    if not conn:
        msg = "db not initizalized"
        log.warn(msg)
        raise HttpProcessingError(code=501, message=msg)

    if domain == '/':
        # get top level domains
        domains = getTopLevelDomains(conn)
        print("got domains:", domains)
        resp_json = {"domains": domains}
    else:
        resp_json = getRow(conn, domain)
        if not resp_json:
            msg = "domain: {} not found".format(domain)
            log.warn(msg)
            raise HttpProcessingError(code=404, message=msg)

        log.info("GET_Domain response: {}".format(resp_json))
    
    resp = await jsonResponse(request, resp_json, status=200)
    log.response(request, resp=resp)
    return resp

async def GET_Root(request):
    """HTTP method to get root object state """
    log.request(request)
    log.info("GET_Root")
    
    rootid = request.match_info.get('id')

    app = request.app
    conn = app["conn"]
    if not conn:
        msg = "db not initizalized"
        log.warn(msg)
        raise HttpProcessingError(code=501, message=msg)
    resp_json = getRow(conn, rootid, table="RootTable")
    if not resp_json:
        msg = "object not found"
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)
    log.info("GET_Root response: {}".format(resp_json))
    
    resp = await jsonResponse(request, resp_json, status=200)
    log.response(request, resp=resp)
    return resp


async def init(loop):
    """Intitialize application and return app object"""
    
    app = baseInit(loop, 'an')
    app.router.add_route('GET', '/async_info', GET_AsyncInfo)
    app.router.add_route('PUT', '/objects', PUT_Objects)
    app.router.add_route('DELETE', '/objects', DELETE_Objects)
    app.router.add_route('GET', '/objects/{id}', GET_Object)
    app.router.add_route('GET', '/domains', GET_Domain)
    app.router.add_route('GET', '/root/{id}', GET_Root)
    app["bucket_stats"] = {}
    # object and domain updates will be posted here to be worked on offline
    app["pending_queue"] = [] 
     
    app["bytes_in_bucket"] = 0
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

    # connect to db
    db_path = join(config.get("db_dir"), config.get("db_file"))
    log.info("db_file: {}".format(db_path))
    if isfile(db_path):
        # found db file, connect to it
        log.info("connecting to sqlite db")
        conn = sqlite3.connect(db_path)
        app["conn"] = conn
    else:
        log.info("no dbfile found")
        app['conn'] = None
        
        
    # run background tasks
    asyncio.ensure_future(bucketCheck(app), loop=loop) 
    asyncio.ensure_future(healthCheck(app), loop=loop)

    async_port = config.get("an_port")
    log.info("Starting service on port: {}".format(async_port))
    run_app(app, port=int(async_port))
