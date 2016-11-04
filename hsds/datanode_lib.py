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
from aiohttp import HttpProcessingError   
from util.idUtil import validateInPartition, getS3Key, isValidUuid
from util.s3Util import getS3JSONObj, putS3JSONObj, putS3Bytes
import config
import hsds_logger as log
    

async def get_metadata_obj(app, obj_id):
    """ Get object from metadata cache (if present).
        Otherwise fetch from S3 and add to cache
    """
    if not isValidUuid(obj_id):
        msg = "Invalid obj id: {}".format(obj_id)
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")

    validateInPartition(app, obj_id)

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
        s3_key = getS3Key(obj_id)
        log.info("getS3JSONObj({})".format(s3_key))
        # read S3 object as JSON
        obj_json = await getS3JSONObj(app, s3_key)
         
        meta_cache[obj_id] = obj_json  # add to cache
    return obj_json

async def save_metadata_obj(app, obj_json):
    if not isinstance(obj_json, dict):
        log.error("Passed non-dict obj to save_metadata_obj")
        raise HttpProcessingError(code=500, message="Unexpected Error") 
    if "id" not in obj_json:
        log.error("No id key found for json object")
        raise HttpProcessingError(code=500, message="Unexpected Error")
    obj_id = obj_json["id"]
    if not isValidUuid(obj_id):
        log.error("Invalid obj id: {}".format(obj_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")
    
    validateInPartition(app, obj_id)

    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        log.warn("{} has been deleted".format(obj_id))
        raise HttpProcessingError(code=500, message="Unexpected Error") 
    #s3_key = getS3Key(obj_id)

    # write back to S3    
    #await putS3JSONObj(app, s3_key, obj_json) 
    
    # update meta cache
    meta_cache = app['meta_cache'] 
    log.info("save: {} to cache".format(obj_id))
    meta_cache[obj_id] = obj_json
    
    # flag to write to S3
    now = int(time.time())
    dirty_ids = app["dirty_ids"]
    dirty_ids[obj_id] = now

async def s3sync(app):
    """ Periodic method that writes dirty objects in the metadata cache to S3"""
    log.info("s3sync task start")
    sleep_secs = config.get("node_sleep_time")
    s3_sync_interval = config.get("s3_sync_interval")
    dirty_ids = app["dirty_ids"]
    meta_cache = app['meta_cache'] 
    data_cache = app['data_cache'] 

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
                s3_key = getS3Key(obj_id)  
                log.info("s3sync for s3_key: {}".format(s3_key))
                if obj_id[0] == 'c':
                    # chunk update
                    if obj_id not in data_cache:
                        log.error("expected to find obj_id: {} in data cache".format(obj_id))
                        retry_keys.append(obj_id)
                        continue
                    chunk_arr = data_cache[obj_id]
                    chunk_bytes = chunk_arr.tobytes()
                    try:
                        await putS3Bytes(app, s3_key, chunk_bytes)
                    except HttpProcessingError as hpe:
                        log.error("got S3 error writing obj_id: {} to S3: {}".format(obj_id, str(hpe)))
                        retry_keys.append(obj_id)
                else:
                    # meta data update
                    if obj_id not in meta_cache:
                        log.error("expected to find obj_id: {} in meta cache".format(obj_id))
                        retry_keys.append(obj_id)
                        continue
                    obj_json = meta_cache[obj_id]
                    try:
                        await putS3JSONObj(app, s3_key, obj_json) 
                    except HttpProcessingError as hpe:
                        log.error("got S3 error writing obj_id: {} to S3: {}".format(obj_id, str(hpe)))
                        retry_keys.append(obj_id)
            
            # add any failed writes back to the dirty queue
            if len(retry_keys) > 0:
                log.warn("{} failed S3 writes, re-adding to dirty set".format(len(retry_keys)))
                # we'll put the timestamp down as now, so the rewrites won't be triggered immediately
                now = int(time.time())
                for obj_id in retry_keys:
                    dirty_ids[obj_id] = now




         
            

     
    

    
 
   