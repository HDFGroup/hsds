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
from util.idUtil import validateInPartition, getS3Key, isValidUuid
from util.s3Util import getS3JSONObj, putS3JSONObj, putS3Bytes, isS3Obj, deleteS3Obj
from util.domainUtil import isValidDomain
from util.attrUtil import getRequestCollectionName
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
        log.info("check_metadata_obj, {} found in meta cache".format(obj_id))
    else:   
        s3_key = getS3Key(obj_id)
        log.info("check_metadata_obj({})".format(s3_key))
        # does key exist?
        found = await isS3Obj(app, s3_key)
        if not found:
            raise HttpProcessingError(code=404, message="Object not foun")
 
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
        log.info("{} found in meta cache".format(obj_id))
        obj_json = meta_cache[obj_id]
    else:   
        # TBD: put a flag here that S3 read in progress so that we don'task
        # double transfer the same object
        s3_key = getS3Key(obj_id)
        log.info("getS3JSONObj({})".format(s3_key))
        # read S3 object as JSON
        obj_json = await getS3JSONObj(app, s3_key)
         
        meta_cache[obj_id] = obj_json  # add to cache
    return obj_json

def save_metadata_obj(app, obj_id, obj_json):
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
    log.info("save: {} to cache".format(obj_id))
    meta_cache[obj_id] = obj_json
    meta_cache.setDirty(obj_id)
    
    # flag to write to S3
    now = int(time.time())
    dirty_ids = app["dirty_ids"]
    dirty_ids[obj_id] = now


async def delete_metadata_obj(app, obj_id):
    meta_cache = app['meta_cache'] 
    dirty_ids = app["dirty_ids"]
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

    s3_key = getS3Key(obj_id)
      
    if obj_id in meta_cache:
        del meta_cache[obj_id]
    if obj_id in dirty_ids:
        del dirty_ids[obj_id]  # TBD - possible race condition?
         
    # remove from meta cache  
    await deleteS3Obj(app, s3_key)
    # TBD - anything special to do if this fails? 
    

async def s3sync(app):
    """ Periodic method that writes dirty objects in the metadata cache to S3"""
    log.info("s3sync task start")
    sleep_secs = config.get("node_sleep_time")
    s3_sync_interval = config.get("s3_sync_interval")
    dirty_ids = app["dirty_ids"]
    meta_cache = app['meta_cache'] 
    chunk_cache = app['chunk_cache'] 

    while True:
        keys_to_update = []
        now = int(time.time())
        for obj_id in dirty_ids:
            if dirty_ids[obj_id] + s3_sync_interval < now:
                # time to write to S3
                keys_to_update.append(obj_id)

        if len(keys_to_update) == 0:
            log.info("s3sync task - nothing to update, sleeping")
            await asyncio.sleep(sleep_secs)
        else:
            # some objects need to be flushed to S3
            log.info("{} objects to be syncd to S3".format(len(keys_to_update)))

            # first clear the dirty bit (before we hit the first await) to
            # avoid a race condition where the object gets marked as dirty again
            # (causing us to miss an update)
            for obj_id in keys_to_update:
                del dirty_ids[obj_id]
            
            retry_keys = []  # add any write failures back here
            for obj_id in keys_to_update:
                # write back to S3  
                s3_key = None
                is_chunk = False
                log.info("s3sync for obj_id: {}".format(obj_id))
                 
                s3_key = getS3Key(obj_id)  
                if obj_id[0] == 'c':
                    is_chunk = True
                log.info("s3sync for s3_key: {}".format(s3_key))
                if is_chunk:
                    # chunk update
                    if obj_id not in chunk_cache:
                        log.error("expected to find obj_id: {} in data cache".format(obj_id))
                        retry_keys.append(obj_id)
                        continue
                    chunk_arr = chunk_cache[obj_id]
                    chunk_cache.clearDirty(obj_id)  # chunk may get evicted from cache now
                    chunk_bytes = chunk_arr.tobytes()
                    try:
                        await putS3Bytes(app, s3_key, chunk_bytes)
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
                    log.info("writing s3_key: {}".format(s3_key))
                    try:
                        await putS3JSONObj(app, s3_key, obj_json) 
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




         
            

     
    

    
 
   