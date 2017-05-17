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

from aiohttp.web import run_app
from aiohttp.errors import HttpProcessingError, HttpBadRequest

import config
from basenode import baseInit, healthCheck
from util.timeUtil import unixTimeToUTC
from util.s3Util import putS3Bytes, isS3Obj
from util.idUtil import getCollectionForId, isValidChunkId, isValidUuid, getClassForObjId
from util.domainUtil import isValidDomain
from util.httpUtil import jsonResponse, StreamResponse
from util.chunkUtil import getDatasetId 
from asyncnode_lib import listKeys, markObjs, deleteObj, getS3Obj, getRootProperty, clearUsedFlags, getDomainForObjId
import hsds_logger as log
 
FORCE_CONTENT_LIST_CREATION = True
 
async def updateDatasetContents(app, domain, dsetid):
    """ Create a object listing all the chunks for given dataset
    """
    log.info("updateDatasetContents: {}".format(dsetid))
    s3objs = app["s3objs"]
    if dsetid not in s3objs:
        log.warn("updateDatasetContents - {} not found in s3objs".format(dsetid))
        return
    dsetS3Obj = s3objs[dsetid]
    if dsetS3Obj.collection != "datasets":
        log.error("udpateDatasetContents - {} is not a dataset".format(dsetid))
        return
     
    if len(dsetS3Obj.chunks) == 0:
        log.info("no chunks for dataset")
        return
    # TBD: Replace with domainUtil func
    col_s3key = domain[1:] + "/." + dsetid + ".chunks.txt"  
    if await isS3Obj(app, col_s3key):
        # contents already exist, return
        # TBD: Add an option to force re-creation of index?
        if not FORCE_CONTENT_LIST_CREATION:
            return
         
    chunk_ids = list(dsetS3Obj.chunks)
    chunk_ids.sort()
    text_data = b""
    for chunkid in chunk_ids:
        if chunkid not in s3objs:
            log.warn("updateDatasetContents: chunk {} for dset: {} not found in s3objs".format(chunkid, dsetid))
            continue
        log.info("getting chunk_obj for {}".format(chunkid))
        chunk_obj = s3objs[chunkid]
        # chunk_obj should have keys: ETag, Size, and LastModified 
        if chunk_obj.etag is None:
            log.warn("chunk_obj for {} not initialized".format(chunkid))
            continue
        line = "{} {} {} {}\n".format(chunkid[39:], chunk_obj.etag, chunk_obj.lastModified, chunk_obj.size)
        log.info("chunk contents: {}".format(line))
        line = line.encode('utf8')
        text_data += line
    log.info("write chunk collection key: {}, count: {}".format(col_s3key, len(chunk_ids)))
    try:
        await putS3Bytes(app, col_s3key, text_data)
    except HttpProcessingError:
        log.error("S3 Error writing chunk collection key: {}".format(col_s3key))
  

async def updateDomainContent(app, domain, objs_updated=None):
    """ Create/update context files listing objids and size for objects in the domain.
    """
    log.info("updateDomainContent: {}".format(domain))
    if objs_updated is not None:
        log.info("objs_updated: {}".format(objs_updated))

    s3objs = app["s3objs"]
    roots = app["roots"]
    if domain not in s3objs:
        log.warn("updateDomainContents - {} not found in s3objs".format(domain))
        return
    domainS3Obj = s3objs[domain]

    if domainS3Obj.root is None:
        log.info("updateDomainContents - folder domain: {}, skipping".format(domain))
        return
  
    rootid = domainS3Obj.root
    if rootid not in roots:
        log.warn("expected to find root: {} in roots set".format(rootid))
        return
    if rootid not in s3objs:
        log.warn("expected to find root: {} in s3objs collection".format(rootid))
        return
    rootS3Obj = s3objs[rootid]
    for collection in ("groups", "datatypes", "datasets"):
        # if objs_updated is passed in, check that at least one of the relevant object types
        # is included, otherwise skip
        if objs_updated is not None:
            collection_included = False
            for objid in objs_updated:
                if isValidChunkId(objid) and collection == "datasets":
                    # if a chunk is updated, recalculate the datasets list
                    collection_included = True
                    break
                elif getCollectionForId(objid) == collection:
                    collection_included = True
                    break
            if not collection_included:
                log.info("no updated for collection: {}".format(collection))
                continue  # go on to next collection
        domain_col = rootS3Obj.get_collection(collection)
        log.info("domain_{} count: {}".format(collection, len(domain_col)))
        log.info("domain_{} items: {}".format(collection, domain_col))
        col_s3key = domain[1:] + "/." + collection + ".txt"  
        if await isS3Obj(app, col_s3key):
            # Domain collection already exist
            # TBD: add option to force re-creation?
            if not FORCE_CONTENT_LIST_CREATION:
                continue
        if len(domain_col) > 0:
            log.info("updating domain collection: {} for domain: {}".format(domain_col, domain))
            col_ids = list(domain_col)
            col_ids.sort()
            text_data = b""
            for obj_id in col_ids:
                if obj_id not in s3objs:
                    log.warn("updateDomainContent - expected to find {} in s3objs".format(obj_id))
                    continue
                col_obj = s3objs[obj_id]
                if col_obj.etag is None or col_obj.lastModified is None or col_obj.size is None:
                    log.warn("updateDomainContent - s3 properties not set for {}".format(obj_id))
                    continue

                line = "{} {} {} {}\n".format(obj_id, col_obj.etag, col_obj.lastModified, col_obj.size)
                line = line.encode('utf8')
                text_data += line
                if col_obj.collection == "datasets":
                    # create chunk listing
                    if objs_updated is None:
                        await updateDatasetContents(app, domain, obj_id)
                    else:
                        # if objs_updated is passed in, only update if at least one chunk in
                        # the dataset has been updated
                        for updated_id in objs_updated:
                            if isValidChunkId(updated_id) and getDatasetId(updated_id) == obj_id:
                                await updateDatasetContents(app, domain, obj_id)
                                break

            log.info("write collection key: {}, count: {}".format(col_s3key, len(col_ids)))
            try:
                await putS3Bytes(app, col_s3key, text_data)
            except HttpProcessingError:
                log.error("S3 Error writing {}.json key: {}".format(collection, col_s3key))

    log.info("updateDomainContent: {} Done".format(domain))



def sweepObj(app, objid):
    """ return True if this object should be deleted """
    log.info("sweepObj {}".format(objid))
    s3objs = app["s3objs"]
    if objid not in s3objs:
        log.error("sweepObj {} - expected to find in s3objs".format(objid))
        return False
    s3obj = s3objs[objid]
    if s3obj.used is True:
        log.info("sweepObj {} - obj in use".format(objid))
        return False
    if s3obj.lastModified is None:
        log.warn("sweepObj({}) - lastModified not set".format(objid))
        return False
    now = time.time()
    if now - s3obj.lastModified < app["anonymous_ttl"]:
        log.info("obj: {} isn't old enough to delete yet".format(objid))
        return False
 
    return True

async def sweepObjs(app):
    """ Iterate through the object tree and delete any unlinked objects """
    s3objs = app["s3objs"]
    deleted_ids = app["deleted_ids"]

    log.info("sweepObjs")

    delete_set = set()
    # sweep all non-domain objects
    for objid in s3objs:
        if isValidDomain(objid):
            continue # skip domain objects
        if isValidChunkId(objid):
            continue  # chunks get sweeped with their dataset
        if not isValidUuid(objid):
            log.warn("sweepObjs: unexpected id: {}".format(objid))
            continue
        if sweepObj(app, objid):  
            delete_set.add(objid)

    # add any chunks that are a member of to-be deleted dataset
    for objid in delete_set:
        if getClassForObjId(objid) != "datasets":
            continue
        s3obj = s3objs[objid]
        chunks = s3obj.chunks
        for chunkid in chunks:
            deleted_ids.add(chunkid)

    delete_count = 0
    log.info("sweepObjs - {} objects to be deleted".format(len(delete_set)))
    # delete objects in the delete set
    for objid in delete_set:
        log.info("delete: {}".format(objid))
        success = await deleteObj(app, objid)
        if success:
            delete_count += 1
        else:
            log.warn("failed to delete: {}".format(objid))    

    # iteratate through 
    log.info("SweepObjs done - delete count: {}".format(delete_count))

#
# pending queue handler
#

     

async def rootDelete(app, rootid):
    """ get root obj for rootid """
    log.info("rootDelte {}".format(rootid))
    s3objs = app["s3objs"]
    roots = app["roots"]
    if rootid not in roots:
        log.warn("expected to find: {} in roots set".format(rootid))
        return
    if rootid not in s3objs:
        log.warn("expected to find: {} in s3objs".format(rootid))
        return
    rootS3Obj = s3objs[rootid]

    root_groups = rootS3Obj["groups"]
    for grpid in root_groups:
        await deleteObj(app, grpid, notify=False)
    # delete all types of domain
    root_datatypes = rootS3Obj["datatypes"]
    for datatypeid in root_datatypes:
        await deleteObj(app, datatypeid, notify=False)
    # delete all datasets of domain
    root_datasets = rootS3Obj["datasets"]
    for dsetid in root_datasets:
        if dsetid not in s3objs:
            log.warn("Expected to find {} in s3objs".format(dsetid))
            continue
        datasetS3Obj = s3objs[dsetid]
        # for each dataset, delete all its chunks
        dataset_chunks = datasetS3Obj["chunks"]
        for chunkid in dataset_chunks:
            await deleteObj(app, chunkid, notify=False)
        await deleteObj(app, dsetid, notify=False)
    # finally didn't the root group
    await deleteObj(app, rootid, notify=False)

async def domainDelete(app, domain):
    """ Process domain deletion event """
    log.info("domainDelete: {}".format(domain))
     
    domains = app["domains"]
    if domain not in domains:
        log.warn("Expected to find domain: {} in collection".format(domain))
        return
    s3objs = app["s3objs"]
    if domain not in s3objs:
        log.warn("Expected to find domain {} in s3objs".format(domain))
        return
    domainS3Obj = s3objs[domain]
    if domainS3Obj.root:
        await rootDelete(app, domainS3Obj.root)
    await deleteObj(app, domain, notify=False)
    

async def domainCreate(app, domain):
    """ Process domain creation event """
    log.info("domainCreate: {}".format(domain))
    
    try:
        s3obj = await getS3Obj(app, domain)
    except HttpProcessingError as hpe:
        log.warn("domainCreate - getS3Obj({}) error: {}".format(domain, hpe.code))
        return

    if s3obj.root is None:
        # fetch the root property
        try:
            await getRootProperty(app, s3obj)  # will set root prop
        except HttpProcessingError as hpe:
            log.warn("domainCreate - getRootProperty({}) error: {}".format(domain, hpe.code))
    if s3obj.root:
        log.info("domainCreate for {}, root found: {}".format(domain, s3obj.root))
        updated_domains = app["updated_domains"]
        updated_domains.add(domain)  # flag to update domain contents
    else:
        log.info("domainCreate for {}, no root (folder)".format(domain)) 

    domains = app["domains"]
    domains.add(domain)
    log.info("domainCreate - added domain s3obj: {}".format(domain))



async def objUpdate(app, objid):
    """ Process object update event """
    log.info("objUpdate: {}".format(objid))
    
    if not isValidChunkId(objid) and not isValidUuid(objid):
        log.error("Got unexpected objid: {}".format(objid))
        return

    s3objs = app["s3objs"]
        
    try:
        s3obj = await getS3Obj(app, objid)
    except HttpProcessingError as hpe:
        log.warn("objUpdate - getS3Obj({}) Error: {}".format(objid, hpe.code))
        return

    if objid not in s3objs:
        log.error("objUpdate: Expecting {} in s3objs".format(objid))
        return

    # add chunks to the dataset chunks set if the dataset is present
    if isValidChunkId(objid):
        dsetid = getDatasetId(objid)
        if dsetid in s3objs:
            dsetObj = s3objs[dsetid]
            log.info("adding {} to {} chunks".format(objid, dsetid))
            dsetObj.chunks.add(objid)

    elif s3obj.root is None:
        # fetch the root property
        try:
            await getRootProperty(app, s3obj)  # will set root prop
        except HttpProcessingError as hpe:
            log.warn("domainCreate - getRootProperty({}) error: {}".format(objid, hpe.code))

        if s3obj.isRoot:
            roots = app["roots"]
            roots.add(objid)
        elif s3obj.root and s3obj.root in s3objs:
            # add to the appropriate root collection
            s3root = s3objs[s3obj.root]
            collection = getCollectionForId(objid)
            s3root_col = s3root.get_collection(collection)
            log.info("adding {} to root collection: {}".format(objid, s3obj.root))
            s3root_col.add(objid)


    domain = getDomainForObjId(app, objid)
    if domain:
        # Flag that the content files should be updated
        log.info("adding {} to updated_domains".format(domain))
        updated_domains = app["updated_domains"]
        updated_domains.add(domain)  
     
   

async def objDelete(app, objid):
    """ Process object delete event """
    log.info("objectDelete: {}".format(objid))

    if not isValidUuid(objid):
        log.error("Got unexpected objid: {}".format(objid))
        return

    domain = getDomainForObjId(app, objid)
          
    # don't notify the DN node, since this event came from the DN node orginally
    # and it would be nice to avoid an infinite regress of notifications
    success = await deleteObj(app, objid, notify=False)
    if success and domain and domain.root != objid:
        # Flag that the content files should be updated
        # Note: for root deletions, we'll just let the DN node cleanup the content objects
        log.info("adding {} to updated_domains".format(domain))
        updated_domains = app["updated_domains"]
        updated_domains.add(domain) 



async def gcsweep(app):
    """ Do garbage collection run across all objects in the bucket 
    """

    now = int(time.time())
    log.info("gcweep {}".format(unixTimeToUTC(now)))
    # list all keys from bucket, save stats to s3objs
    await listKeys(app)

    # clear used flags
    clearUsedFlags(app)

    # mark used objects
    await markObjs(app)

    # clear out any unused objects
    await sweepObjs(app)

 
async def processPendingQueue(app):
    """ Process any pending queue events """      

    pending_queue = app["pending_queue"]
    log.info("processPendingQueue start")    
    domain_updates = set()  # set of ids for which we'll need to update domain content
    dataset_updates = set()
    while len(pending_queue) > 0:
        log.info("pending_queue len: {}".format(len(pending_queue)))
        item = pending_queue.pop(0)  # remove from the front
        objid = item["objid"]
        action = item["action"]
        log.info("pop from pending queue: obj: {} action: {}".format(objid, action))
            
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
    log.info("datasets to be updates: {}".format(len(dataset_updates)))
    # TBD: update domain.txt and chunks.txt files

                 

async def bucketCheck(app):
    """ Periodic method for GC and pending queue updates 
    """
 
    app["last_bucket_check"] = int(time.time())
    app["last_gcsweep"] = 0
    async_sleep_time = config.get("async_sleep_time")
    log.info("async_sleep_time: {}".format(async_sleep_time))
    gc_freq = config.get("gc_freq")
    log.info("gc_freq: {}".format(gc_freq))
    first_run = True

    # update/initialize root object before starting node updates
 
    while True:  
        if app["node_state"] != "READY":
            log.info("bucketCheck waiting for Node state to be READY")
            await asyncio.sleep(1)
            continue  # wait for READY state

        now = int(time.time())

        if now - app["last_gcsweep"] > gc_freq:
            app["last_gcsweep"] = now
            log.info("running gcsweep")    
            try:
                await gcsweep(app)
            except Exception as e:
                log.warn("bucketCheck - got exception from gcsweep: {}".format(e))
        
       
        try:
            await processPendingQueue(app)
        except Exception as e:
            log.warn("bucketCheck - got exception from processPendingQueue: {}".format(e))

        # set of domains that will need the contents files updated
        updated_domains = app["updated_domains"]
        if first_run:
            first_run = False
            # update all content files on first iteration of bucketCheck
            domains = app["domains"]
            for domain in domains:
                updated_domains.add(domain)
        while len(updated_domains) > 0:
            domain = updated_domains.pop()
            await updateDomainContent(app, domain)
            

        app["last_bucket_check"] = int(time.time())
        await asyncio.sleep(async_sleep_time)   

    # shouldn't ever get here     
    log.error("bucketCheck terminating unexpectedly")
     
def updateBucketStats(app):  
    """ Collect some high level stats for use by the info request """
    bucket_stats = app["bucket_stats"]
    s3objs = app["s3objs"]
    domains = app["domains"]
    roots = app["roots"]
    pending_queue = app["pending_queue"]

    
    bucket_stats["object_count"] = len(s3objs)  
    bucket_stats["domain_count"] = len(domains)
    bucket_stats["root_count"] = len(roots)
    bucket_stats["storage_size"] = app["bytes_in_bucket"]
    bucket_stats["pending_count"] = len(pending_queue)    
        

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
    if not request.has_body:
        msg = "PUT objects with no body"
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
            msg = "PUT_Objects Invalid id: {}".format(objid)
            log.warn(msg)
            raise HttpBadRequest(message=msg)

    pending_queue = app["pending_queue"]
    for objid in objids:
        if isValidDomain(objid):
            # ignore domain events
            continue
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
        msg = "PUT objects with no body"
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
        if isValidDomain(objid):
            # ignore domain events
            continue
        item = {"objid": objid, "action": "DELETE"}
        log.info("adding item: {} to pending queue".format(item))
        pending_queue.append(item)

    resp_json = {  } 
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp


async def init(loop):
    """Intitialize application and return app object"""
    
    app = baseInit(loop, 'an')
    app.router.add_route('GET', '/async_info', GET_AsyncInfo)
    app.router.add_route('PUT', '/objects', PUT_Objects)
    app.router.add_route('DELETE', '/objects', DELETE_Objects)
    app["bucket_stats"] = {}
    # object and domain updates will be posted here to be worked on offline
    app["pending_queue"] = [] 
    app["s3objs"] = {}
    app["domains"] = set()
    app["roots"] = set()
    app["deleted_ids"] = set()
    app["bytes_in_bucket"] = 0
    app["anonymous_ttl"] = config.get("anonymous_ttl")
    app["s3_sync_interval"] = config.get("s3_sync_interval")
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
    asyncio.ensure_future(bucketCheck(app), loop=loop)
    asyncio.ensure_future(healthCheck(app), loop=loop)
    async_port = config.get("an_port")
    log.info("Starting service on port: {}".format(async_port))
    run_app(app, port=int(async_port))
