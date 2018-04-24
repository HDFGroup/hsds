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
# data node of hsds cluster
# 
import asyncio
import time 
from aiohttp.errors import HttpProcessingError   
from util.idUtil import validateInPartition, getS3Key, isValidUuid, isValidChunkId
from util.s3Util import getS3JSONObj, putS3JSONObj, putS3Bytes, isS3Obj, deleteS3Obj
from util.domainUtil import isValidDomain
from util.attrUtil import getRequestCollectionName
from util.httpUtil import http_put, http_delete
from util.chunkUtil import getDatasetId
from util.arrayUtil import arrayToBytes
from basenode import getAsyncNodeUrl
import config
import hsds_logger as log


def get_obj_id(request, body=None):
    """ Get object id from request 
        Raise HTTPException on errors.
    """

    obj_id = None
    collection = None
    app = request.app
    if body and "id" in body:
        obj_id = body["id"]
    else:
        collection = getRequestCollectionName(request) # returns datasets|groups|datatypes
        obj_id = request.match_info.get('id')

    if not obj_id:
        msg = "Missing object id"
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")

    if not isValidUuid(obj_id, obj_class=collection):
        msg = "Invalid obj id: {}".format(obj_id)
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")

    try:
        validateInPartition(app, obj_id)
    except KeyError as ke:
        msg = "Domain not in partition"
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg) 

    return obj_id   

async def check_metadata_obj(app, obj_id):
    """Raise Http 404 or 410 if not found (or recently removed)
    """
    if not isValidDomain(obj_id) and not isValidUuid(obj_id):
        msg = "Invalid obj id: {}".format(obj_id)
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")

    try:
        validateInPartition(app, obj_id)
    except KeyError as ke:
        msg = "Domain not in partition"
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg) 

    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        msg = "{} has been deleted".format(obj_id)
        log.warn(msg)
        raise HttpProcessingError(code=410, message="Object has been deleted") 
    
    meta_cache = app['meta_cache'] 
    obj_json = None 
    if obj_id in meta_cache:
        log.debug("check_metadata_obj, {} found in meta cache".format(obj_id))
    else:   
        s3_key = getS3Key(obj_id)
        log.debug("check_metadata_obj({})".format(s3_key))
        # does key exist?
        found = await isS3Obj(app, s3_key)
        if not found:
            raise HttpProcessingError(code=404, message="Object not found")
 
    return obj_json

async def get_metadata_obj(app, obj_id):
    """ Get object from metadata cache (if present).
        Otherwise fetch from S3 and add to cache
    """
    log.info("get_metadata_obj: {}".format(obj_id))
    if not isValidDomain(obj_id) and not isValidUuid(obj_id):
        msg = "Invalid obj id: {}".format(obj_id)
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")

    try:
        validateInPartition(app, obj_id)
    except KeyError as ke:
        msg = "Domain not in partition"
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg) 

    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        msg = "{} has been deleted".format(obj_id)
        log.warn(msg)
        raise HttpProcessingError(code=410, message="Object has been deleted") 
    
    meta_cache = app['meta_cache'] 
    obj_json = None 
    if obj_id in meta_cache:
        log.debug("{} found in meta cache".format(obj_id))
        obj_json = meta_cache[obj_id]
    else:   
        # TBD: put a flag here that S3 read in progress so that we don'task
        # double transfer the same object
        s3_key = getS3Key(obj_id)
        log.debug("getS3JSONObj({})".format(s3_key))
        # read S3 object as JSON
        obj_json = await getS3JSONObj(app, s3_key)
         
        meta_cache[obj_id] = obj_json  # add to cache
    return obj_json

def save_metadata_obj(app, obj_id, obj_json, notify=True):
    """ Persist the given object """
    log.info("save_metadata_obj {} notify={}".format(obj_id, notify))
    if not isValidDomain(obj_id) and not isValidUuid(obj_id):
        msg = "Invalid obj id: {}".format(obj_id)
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    if not isinstance(obj_json, dict):
        log.error("Passed non-dict obj to save_metadata_obj")
        raise HttpProcessingError(code=500, message="Unexpected Error") 

    try:
        validateInPartition(app, obj_id)
    except KeyError as ke:
        msg = "Domain not in partition"
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg) 

    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        if isValidUuid(obj_id):
            # domain objects may be re-created, but shouldn't see repeats of 
            # deleted uuids
            log.warn("{} has been deleted".format(obj_id))
            raise HttpProcessingError(code=500, message="Unexpected Error") 
        elif obj_id in deleted_ids:
            deleted_ids.remove(obj_id)  # un-gone the domain id
    
    # update meta cache
    meta_cache = app['meta_cache'] 
    log.debug("save: {} to cache".format(obj_id))
    meta_cache[obj_id] = obj_json
    meta_cache.setDirty(obj_id)
    
    # flag to write to S3
    now = int(time.time())
    dirty_ids = app["dirty_ids"]
    dirty_ids[obj_id] = now

    # set flag if AN should be notified on S3 write
    if notify:
        notify_obj = {"id": obj_id, "lastModified": now}
        if "root" in obj_json:
            notify_obj["root"] = obj_json["root"]
        if "size" in obj_json:
            notify_obj["size"] = obj_json["size"]
        notify_objs = app["notify_objs"]
        notify_objs.add(notify_obj)


async def delete_metadata_obj(app, obj_id, notify=True):
    """ Delete the given object """
    meta_cache = app['meta_cache'] 
    dirty_ids = app["dirty_ids"]
    log.info("delete_meta_data_obj: {} notify: {}".format(obj_id, notify))
    if not isValidDomain(obj_id) and not isValidUuid(obj_id):
        msg = "Invalid obj id: {}".format(obj_id)
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
        
    try:
        validateInPartition(app, obj_id)
    except KeyError as ke:
        msg = "Domain not in partition"
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg) 

    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        log.warn("{} has already been deleted".format(obj_id))
    else:
        deleted_ids.add(obj_id)
     
    if obj_id in meta_cache:
        del meta_cache[obj_id]
    if obj_id not in dirty_ids:
        now = time.time()
        dirty_ids[obj_id] = now
    if notify:
        notify_objs = app['notify_objs']
        notify_objss.add(notify_obj)

    

async def s3sync(app):
    """ Periodic method that writes dirty objects in the metadata cache to S3"""
    log.info("s3sync task start")
    sleep_secs = config.get("node_sleep_time")
    s3_sync_interval = config.get("s3_sync_interval")
    dirty_ids = app["dirty_ids"]
    deleted_ids = app["deleted_ids"]
    notify_objs = app["notify_objs"]
    meta_cache = app["meta_cache"] 
    chunk_cache = app["chunk_cache"] 
    deflate_map = app['deflate_map']
        

    while True:
        while app["node_state"] != "READY":
            log.info("s3sync - clusterstate is not ready, sleeping")
            await asyncio.sleep(sleep_secs)
        keys_to_update = []
        now = int(time.time())
        for obj_id in dirty_ids:
            if obj_id in deleted_ids or dirty_ids[obj_id] + s3_sync_interval < now:
                # time to write to S3
                keys_to_update.append(obj_id)

        if len(keys_to_update) == 0:
            log.info("s3sync task - nothing to update, sleeping")
            await asyncio.sleep(1)  # was sleep_secs
        else:
            # some objects need to be flushed to S3
            log.info("{} objects to be synced to S3".format(len(keys_to_update)))

            # first clear the dirty bit (before we hit the first await) to
            # avoid a race condition where the object gets marked as dirty again
            # (causing us to miss an update)
            for obj_id in keys_to_update:
                del dirty_ids[obj_id]
            
            retry_keys = []  # add any write failures back here
            success_keys = [] # keys we successfully wrote to S3
            for obj_id in keys_to_update:
                # write back to S3  
                s3_key = None
                log.info("s3sync for obj_id: {}".format(obj_id))
                s3_key = getS3Key(obj_id)  
                log.debug("s3sync for s3_key: {}".format(s3_key))
                if obj_id in deleted_ids:
                    # delete the s3 obj
                    try:
                        await deleteS3Obj(app, s3_key)
                        success_keys.append(obj_id)
                    except HttpProcessingError as hpe:
                        log.error("got S3 error deleting obj_id: {} to S3: {}".format(obj_id, str(hpe)))
                        retry_keys.append(obj_id)
                elif isValidChunkId(obj_id):
                    # chunk update
                    if obj_id not in chunk_cache:
                        log.error("expected to find obj_id: {} in data cache".format(obj_id))
                        retry_keys.append(obj_id)
                        continue
                    chunk_arr = chunk_cache[obj_id]
                    chunk_cache.clearDirty(obj_id)  # chunk may get evicted from cache now
                    #chunk_bytes = chunk_arr.tobytes()
                    chunk_bytes = arrayToBytes(chunk_arr)
                    dset_id = getDatasetId(obj_id)
                    deflate_level = None
                    if dset_id in deflate_map:
                        deflate_level = deflate_map[dset_id]
                        log.info("got deflate_level: {} for dset: {}".format(deflate_level, dset_id))

                    log.info("writing S3 object: {}, num_bytes: {}".format(s3_key, len(chunk_bytes)))
                    try:
                        await putS3Bytes(app, s3_key, chunk_bytes, deflate_level=deflate_level)
                        success_keys.append(obj_id)
                    except HttpProcessingError as hpe:
                        log.error("got S3 error writing obj_id: {} to S3: {}".format(obj_id, str(hpe)))
                        retry_keys.append(obj_id)
                        # re-add chunk to cache if it had gotten evicted
                        if obj_id not in chunk_cache:
                            chunk_cache[obj_id] = chunk_arr
                        chunk_cache.setDirty(obj_id)  # pin to cache        
                else:
                    # meta data update
                    if obj_id not in meta_cache:
                        log.error("expected to find obj_id: {} in meta cache".format(obj_id))
                        retry_keys.append(obj_id)
                        continue
                    obj_json = meta_cache[obj_id]
                    meta_cache.clearDirty(obj_id)
                    log.debug("writing s3_key: {}".format(s3_key))
                    try:
                        await putS3JSONObj(app, s3_key, obj_json) 
                        log.info("adding {} to success_keys".format(obj_id))
                        success_keys.append(obj_id)
                    except HttpProcessingError as hpe:
                        log.error("got S3 error writing obj_id: {} to S3: {}".format(obj_id, str(hpe)))
                        retry_keys.append(obj_id)
                        # re-add chunk to cache if it had gotten evicted
                        if obj_id not in meta_cache:
                            meta_cache[obj_id] = obj_json
                        meta_cache.setDirty(obj_id)  # pin to cache            
            
            # add any failed writes back to the dirty queue
            if len(retry_keys) > 0:
                log.warn("{} failed S3 writes, re-adding to dirty set".format(len(retry_keys)))
                # we'll put the timestamp down as now, so the rewrites won't be triggered immediately
                now = int(time.time())
                for obj_id in retry_keys:
                    dirty_ids[obj_id] = now

            # notify AN of key updates 
            an_url = getAsyncNodeUrl(app)
            log.info("processing success_keys: {}".format(success_keys))
            while len(success_keys) > 0:
                # package multiple updates or multiple deletes together as much
                # as possible
                action = None
                keys = []
                while len(success_keys) > 0:
                    key = success_keys.pop(0)
                    log.debug("pop success_key: {}".format(key))
                    if key in notify_ids:
                        notify_ids.remove(key)
                    else:
                        log.debug("notify not set for key: {}".format(key))
                        continue
                    if key in deleted_ids:
                        if action is None:
                            action = "DELETE"
                        elif action == "DELETE":
                            pass # keep going
                        else:
                            break
                    else:
                        if action is None:
                            action = "PUT"
                        elif action == "PUT":
                            pass # keep going
                        else:
                            break
                    keys.append(key)
                    log.debug("appended_keys: {}".format(keys))

                if len(keys) > 0:
                    body = { "objids": keys }
                    req = an_url + "/objects"
                    try:
                        if action == "PUT":
                            log.info("ASync PUT notify: {} body: {}".format(req, body))
                            await http_put(app, req, data=body)
                        else:
                            log.info("ASync DELETE notify: {} body: {}".format(req, body))
                            await http_delete(app, req, data=body)
                    except HttpProcessingError as hpe:
                        msg = "got error notifying async node: {}".format(hpe.code)
                        log.error(msg)
     