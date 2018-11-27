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
from aiohttp.web_exceptions import HTTPGone, HTTPInternalServerError, HTTPBadRequest
from aiohttp.client_exceptions import ClientError

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
        raise HTTPInternalServerError()

    if not isValidUuid(obj_id, obj_class=collection):
        msg = "Invalid obj id: {}".format(obj_id)
        log.error(msg)
        raise HTTPInternalServerError()

    try:
        validateInPartition(app, obj_id)
    except KeyError as ke:
        msg = "Domain not in partition"
        log.error(msg)
        raise HTTPInternalServerError() 

    return obj_id   

async def check_metadata_obj(app, obj_id):
    """ Return False is obj does not exist
    """
    if not isValidDomain(obj_id) and not isValidUuid(obj_id):
        msg = "Invalid obj id: {}".format(obj_id)
        log.error(msg)
        raise HTTPInternalServerError()

    try:
        validateInPartition(app, obj_id)
    except KeyError as ke:
        msg = "Domain not in partition"
        log.error(msg)
        raise HTTPInternalServerError() 

    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        msg = "{} has been deleted".format(obj_id)
        log.info(msg)
        return False
    
    meta_cache = app['meta_cache'] 
    if obj_id in meta_cache:
        found = True
    else:
        # Not in chache, check s3 obj exists   
        s3_key = getS3Key(obj_id)
        log.debug("check_metadata_obj({})".format(s3_key))
        # does key exist?
        found = await isS3Obj(app, s3_key)
    return found
    
 

async def get_metadata_obj(app, obj_id):
    """ Get object from metadata cache (if present).
        Otherwise fetch from S3 and add to cache
    """
    log.info("get_metadata_obj: {}".format(obj_id))
    if not isValidDomain(obj_id) and not isValidUuid(obj_id):
        msg = "Invalid obj id: {}".format(obj_id)
        log.error(msg)
        raise HTTPInternalServerError()

    try:
        validateInPartition(app, obj_id)
    except KeyError as ke:
        msg = "Domain not in partition"
        log.error(msg)
        raise HTTPInternalServerError() 

    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        msg = "{} has been deleted".format(obj_id)
        log.warn(msg)
        raise HTTPGone() 
    
    meta_cache = app['meta_cache'] 
    obj_json = None 
    if obj_id in meta_cache:
        log.debug("{} found in meta cache".format(obj_id))
        obj_json = meta_cache[obj_id]
    else:   
        # TBD: put a flag here that S3 read in progress so that we don't
        # double transfer the same object
        s3_key = getS3Key(obj_id)
        log.debug("getS3JSONObj({})".format(s3_key))
        # read S3 object as JSON
        obj_json = await getS3JSONObj(app, s3_key)
         
        meta_cache[obj_id] = obj_json  # add to cache
    return obj_json

async def save_metadata_obj(app, obj_id, obj_json, notify=False, flush=False):
    """ Persist the given object """
    log.info(f"save_metadata_obj {obj_id} notify={notify} flush={flush}")
    if not obj_id.startswith('/') and not isValidUuid(obj_id):
        msg = "Invalid obj id: {}".format(obj_id)
        log.error(msg)
        raise HTTPInternalServerError()
    if not isinstance(obj_json, dict):
        log.error("Passed non-dict obj to save_metadata_obj")
        raise HTTPInternalServerError() 

    try:
        validateInPartition(app, obj_id)
    except KeyError as ke:
        msg = "Domain not in partition"
        log.error(msg)
        raise HTTPInternalServerError() 

    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        if isValidUuid(obj_id):
            # domain objects may be re-created, but shouldn't see repeats of 
            # deleted uuids
            log.warn("{} has been deleted".format(obj_id))
            raise HTTPInternalServerError() 
        elif obj_id in deleted_ids:
            deleted_ids.remove(obj_id)  # un-gone the domain id
    
    # update meta cache
    meta_cache = app['meta_cache'] 
    log.debug("save: {} to cache".format(obj_id))
    meta_cache[obj_id] = obj_json
    meta_cache.setDirty(obj_id)
    now = int(time.time())
    
    if flush:
        # write to S3 immediately
        if isValidChunkId(obj_id):
            log.warn("flush not supported for save_metadata_obj with chunks")
            raise HTTPBadRequest()
        # write to S3, will raise HTTPInternalServerError if fails
        s3_key = getS3Key(obj_id)
        log.debug(f"writing {s3_key} to S3")
        await putS3JSONObj(app, s3_key, obj_json) 
                                             
    else:
        # flag to write to S3
        dirty_ids = app["dirty_ids"]
        dirty_ids[obj_id] = now

     
    # message AN immediately if notify flag is set
    # otherwise AN will be notified at next S3 sync
    if notify:
        an_url = getAsyncNodeUrl(app)

        if obj_id.startswith("/"):
            # domain update
            req = an_url + "/domain"
            params = {"domain": obj_id}
            if "root" in obj_json:
                params["root"] = obj_json["root"]
            if "owner" in obj_json:
                params["owner"] = obj_json["owner"]
            try:
                log.info("ASync PUT notify: {} params: {}".format(req, params))
                await http_put(app, req, params=params)
            except HTTPInternalServerError as hpe:
                log.error(f"got error notifying async node: {hpe}")
                log.error(msg)

        else:
            req = an_url + "/object/" + obj_id
            try:
                log.info("ASync PUT notify: {}".format(req))
                await http_put(app, req)
            except HTTPInternalServerError:
                log.error(f"got error notifying async node")
        


async def delete_metadata_obj(app, obj_id, notify=True, root_id=None):
    """ Delete the given object """
    meta_cache = app['meta_cache'] 
    dirty_ids = app["dirty_ids"]
    log.info("delete_meta_data_obj: {} notify: {}".format(obj_id, notify))
    if not isValidDomain(obj_id) and not isValidUuid(obj_id):
        msg = "Invalid obj id: {}".format(obj_id)
        log.error(msg)
        raise HTTPInternalServerError()
        
    try:
        validateInPartition(app, obj_id)
    except KeyError as ke:
        msg = "obj: {} not in partition".format(obj_id)
        log.error(msg)
        raise HTTPInternalServerError() 

    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        log.warn("{} has already been deleted".format(obj_id))
    else:
        deleted_ids.add(obj_id)
     
    if obj_id in meta_cache:
        log.debug(f"removing {obj_id} from meta_cache")
        del meta_cache[obj_id]
    
    if obj_id in dirty_ids:
        del dirty_ids[obj_id]

    # remove from S3 (if present)
    s3key = getS3Key(obj_id)

    if await isS3Obj(app, s3key):
        await deleteS3Obj(app, s3key)
    else:
        log.info(f"delete_metadata_obj - key {s3key} not found (never written)?")

    
    if notify:
        an_url = getAsyncNodeUrl(app)
        if obj_id.startswith("/"):
            # domain delete
            req = an_url + "/domain"
            params = {"domain": obj_id}
            
            try:
                log.info("ASync DELETE notify: {} params: {}".format(req, params))
                await http_delete(app, req, params=params)
            except ClientError as ce:
                log.error(f"got error notifying async node: {ce}")
            except HTTPInternalServerError as hse:
                log.error(f"got HTTPInternalServerError: {hse}")
        else:
            req = an_url + "/object/" + obj_id
            try:
                log.info(f"ASync DELETE notify: {req}")
                await http_delete(app, req)
            except ClientError as ce:
                log.error(f"got ClientError notifying async node: {ce}")
            except HTTPInternalServerError as ise:
                log.error(f"got HTTPInternalServerError notifying async node: {ise}")
    log.debug(f"delete_metadata_obj for {obj_id} done")

    

async def s3sync(app):
    """ Periodic method that writes dirty objects in the metadata cache to S3"""
    log.info("s3sync task start")
    sleep_secs = config.get("node_sleep_time")
    s3_sync_interval = config.get("s3_sync_interval")
    dirty_ids = app["dirty_ids"]
    #deleted_ids = app["deleted_ids"]
    meta_cache = app["meta_cache"] 
    chunk_cache = app["chunk_cache"] 
    deflate_map = app["deflate_map"]
    dset_root_map = app["dset_root_map"]
        

    while True:
        while app["node_state"] != "READY":
            log.info("s3sync - clusterstate is not ready, sleeping")
            await asyncio.sleep(sleep_secs)
        try:
            keys_to_update = []
            now = int(time.time())
            for obj_id in dirty_ids:
             
                if dirty_ids[obj_id] + s3_sync_interval < now:
                    # time to write to S3
                    keys_to_update.append(obj_id)

            if len(keys_to_update) == 0:
                await asyncio.sleep(1)  # was sleep_secs
            else:
                # some objects need to be flushed to S3
                log.info("{} objects to be synched to S3".format(len(keys_to_update)))

                # first clear the dirty bit (before we hit the first await) to
                # avoid a race condition where the object gets marked as dirty again
                # (causing us to miss an update)
                for obj_id in keys_to_update:
                    del dirty_ids[obj_id]
            
                retry_keys = []  # add any write failures back here
                notify_objs = []  # notifications to send to AN, also flags success write to S3
                for obj_id in keys_to_update:
                    # write back to S3  
                    s3_key = None
                    log.info("s3sync for obj_id: {}".format(obj_id))
                    s3_key = getS3Key(obj_id)  
                    log.debug("s3sync for s3_key: {}".format(s3_key))  
                 
                    if isValidChunkId(obj_id):
                        # chunk update
                        if obj_id not in chunk_cache:
                            log.error("expected to find obj_id: {} in chunk cache".format(obj_id))
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
                    
                        root_id = ""
                        if dset_id not in dset_root_map:
                            log.warn("expected to find {} in dset_root_map".format(dset_id))
                        else:
                            root_id = dset_root_map[dset_id]

                        log.info("writing chunk to S3: {}, num_bytes: {} root_id: {}".format(s3_key, len(chunk_bytes), root_id))
            
                        try:
                            await putS3Bytes(app, s3_key, chunk_bytes, deflate_level=deflate_level)
                            notify_objs.append(obj_id) # add to list of ids we'll tell AN about
                            # s3_rsp should have keys: "etag", "lastModified", and "size", add in obj_id    
                        except HTTPInternalServerError as hpe:
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
                            if isValidUuid(obj_id) or isValidChunkId(obj_id):
                                # notify AN for all non domain ids
                                notify_objs.append(obj_id) # add to list of ids we'll tell AN about

                        except HTTPInternalServerError as hpe:
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
                log.info("Notifying AN for S3 Updates")
                if len(notify_objs) > 0:
                 
                    body = { "objs": notify_objs }
                    req = an_url + "/objects"
                    try:
                        log.info("ASync PUT notify: {} body: {}".format(req, body))
                        await http_put(app, req, data=body)
                    except HTTPInternalServerError as hpe:
                        msg = "got error notifying async node: {}".format(hpe)
                        log.error(msg)
        except Exception as e:
            # catch all exception to keep the loop going
            log.error(f"Got Exception: {e}")
