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
    except KeyError:
        log.error("Domain not in partition")
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
    except KeyError:
        log.error("Domain not in partition")
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
    
 

async def get_metadata_obj(app, obj_id, bucket=None):
    """ Get object from metadata cache (if present).
        Otherwise fetch from S3 and add to cache
    """
    log.info("get_metadata_obj: {}".format(obj_id))
    if isValidDomain(obj_id):
        if obj_id[0] == '/':
            # bucket name should always be prefixed 
            # (so the obj_id is cannonical)
            msg = f"bucket not included in get_metadata_obj for domain: {obj_id}"
            log.error(msg)
            raise HTTPInternalServerError()
        if bucket:
            msg = f"bucket param should not be used with get_metadata_obj for domain: {obj_id}"
            log.error(msg)
            raise HTTPInternalServerError()
    elif not isValidUuid(obj_id):
        msg = f"Invalid obj id: {obj_id}"
        log.error(msg)
        raise HTTPInternalServerError()

    try:
        validateInPartition(app, obj_id)
    except KeyError:
        log.error("Domain not in partition")
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
        s3_key = getS3Key(obj_id)
        pending_s3_read = app["pending_s3_read"]
        if obj_id in pending_s3_read:
            # already a read in progress, wait for it to complete
            read_start_time = pending_s3_read[obj_id]
            log.info(f"s3 read request for {s3_key} was requested at: {read_start_time}")
            while time.time() - read_start_time < 2.0:
                log.debug("waiting for pending s3 read, sleeping")
                await asyncio.sleep(1)  # sleep for sub-second?
                if obj_id in meta_cache:
                    log.info(f"object {obj_id} has arrived!")
                    obj_json = meta_cache[obj_id]
                    break
            if not obj_json:
                log.warn(f"s3 read for object {s3_key} timed-out, initiaiting a new read")
        
        # invoke S3 read unless the object has just come in from pending read
        if not obj_json:
            log.debug("getS3JSONObj({})".format(s3_key))
            if obj_id not in pending_s3_read:
                pending_s3_read[obj_id] = time.time()
            # read S3 object as JSON
            obj_json = await getS3JSONObj(app, s3_key, bucket=bucket)
            if obj_id in pending_s3_read:
                # read complete - remove from pending map
                elapsed_time = time.time() - pending_s3_read[obj_id]
                log.info(f"s3 read for {s3_key} took {elapsed_time}")
                del pending_s3_read[obj_id] 
            meta_cache[obj_id] = obj_json  # add to cache
    return obj_json


async def save_metadata_obj(app, obj_id, obj_json, bucket=None, notify=False, flush=False):
    """ Persist the given object """
    log.info(f"save_metadata_obj {obj_id} bucket={bucket} notify={notify} flush={flush}")
    if notify and not flush:
        log.error("notify not valid when flush is false")
        raise HTTPInternalServerError()

    if not isValidDomain(obj_id) and not isValidUuid(obj_id):
        msg = "Invalid obj id: {}".format(obj_id)
        log.error(msg)
        raise HTTPInternalServerError()
    if not isinstance(obj_json, dict):
        log.error("Passed non-dict obj to save_metadata_obj")
        raise HTTPInternalServerError() 

    try:
        validateInPartition(app, obj_id)
    except KeyError:
        log.error("Domain not in partition")
        raise HTTPInternalServerError() 

    dirty_ids = app["dirty_ids"]
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
        try:
            await write_s3_obj(app, obj_id)
        except KeyError as ke:
            log.error(f"s3 sync got key error: {ke}")
            raise HTTPInternalServerError()
        except HTTPInternalServerError:
            log.warn(f" failed to write {obj_id}")
            raise  # re-throw  
        if obj_id in dirty_ids:
            log.warn(f"save_metadata_obj flush - object {obj_id} is still dirty")
    else:
        # flag to write to S3
        dirty_ids[obj_id] = (now, bucket)
  
     
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
        


async def delete_metadata_obj(app, obj_id, notify=True, root_id=None, bucket=None):
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
    except KeyError:
        log.error(f"obj: {obj_id} not in partition")
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

    if await isS3Obj(app, s3key, bucket=bucket):
        await deleteS3Obj(app, s3key, bucket=bucket)
    else:
        log.info(f"delete_metadata_obj - key {s3key} not found (never written)?")
    
    if notify:
        an_url = getAsyncNodeUrl(app)
        if isValidDomain(obj_id):
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



async def write_s3_obj(app, obj_id, bucket=None):
    """ writes the given object to s3 """
    s3key = getS3Key(obj_id)  
    log.info(f"write_s3_obj for obj_id: {obj_id} / s3_key: {s3key}  bucket: {bucket}")
    pending_s3_write = app["pending_s3_write"]
    pending_s3_write_tasks = app["pending_s3_write_tasks"]
    dirty_ids = app["dirty_ids"]
    chunk_cache = app['chunk_cache']
    meta_cache = app['meta_cache']
    deflate_map = app['deflate_map']
    notify_objs = app["an_notify_objs"]
    deleted_ids = app['deleted_ids']
    success = False

    if s3key in pending_s3_write:
        msg = f"write_s3_key - not expected for key {s3key} to be in pending_s3_write map"
        log.error(msg)
        raise KeyError(msg)

    if obj_id not in pending_s3_write_tasks:
        # don't allow reentrant write
        log.debug(f"write_s3_obj for {obj_id} not s3sync task")

    if obj_id in deleted_ids and isValidUuid(obj_id):
        # if this objid has been deleted (and its unique since this is not a domain id)
        # cancel any pending task and return
        log.warn(f"Canceling wrfite for {obj_id} since it has been deleted")
        if obj_id in pending_s3_write_tasks:
            log.info(f"removing pending s3 write task for {obj_id}")
            task = pending_s3_write_tasks[obj_id]
            task.cancel()
            del pending_s3_write_tasks[obj_id]
        return None
    
    now = time.time()

    last_update_time = now
    if obj_id in dirty_ids:
        last_update_time = dirty_ids[obj_id][0]  # timestamp is first element of two-tuple
    if last_update_time > now:
        msg = f"last_update time {last_update_time} is in the future for obj_id: {obj_id}"
        log.error(msg)
        raise ValueError(msg)
    
    pending_s3_write[s3key] = now 
    # do the following in the try block so we can always remove the pending_s3_write at the end
    
    try:
        if isValidChunkId(obj_id):
            if obj_id not in chunk_cache:
                log.error("expected to find obj_id: {} in chunk cache".format(obj_id))
                raise KeyError(f"{obj_id} not found in chunk cache")
            if not chunk_cache.isDirty(obj_id):
                log.error(f"expected chunk cache obj {obj_id} to be dirty")
                raise ValueError("bad dirty state for obj")
            chunk_arr = chunk_cache[obj_id]
            chunk_bytes = arrayToBytes(chunk_arr)
            dset_id = getDatasetId(obj_id)
            deflate_level = None
            if dset_id in deflate_map:
                deflate_level = deflate_map[dset_id]
                log.debug("got deflate_level: {} for dset: {}".format(deflate_level, dset_id))
     
            await putS3Bytes(app, s3key, chunk_bytes, deflate_level=deflate_level, bucket=bucket)
            success = True
        
            # if chunk has been evicted from cache something has gone wrong
            if obj_id not in chunk_cache:
                msg = f"expected to find {obj_id} in chunk_cache"
                log.error(msg)
            elif obj_id in dirty_ids and dirty_ids[obj_id][0] > last_update_time:
                log.info(f"write_s3_obj {obj_id} got updated while s3 write was in progress")
            else:
                # no new write, can clear dirty
                chunk_cache.clearDirty(obj_id)  # allow eviction from cache  
                log.debug("putS3Bytes Chunk cache utilization: {} per, dirty_count: {}".format(chunk_cache.cacheUtilizationPercent, chunk_cache.dirtyCount))
        else:
            # meta data update     
            # check for object in meta cache
            if obj_id not in meta_cache:
                log.error("expected to find obj_id: {} in meta cache".format(obj_id))
                raise KeyError(f"{obj_id} not found in meta cache")
            if not meta_cache.isDirty(obj_id):
                log.error(f"expected meta cache obj {obj_id} to be dirty")
                raise ValueError("bad dirty state for obj")
            obj_json = meta_cache[obj_id]
            
            await putS3JSONObj(app, s3key, obj_json, bucket=bucket)                     
            success = True 
            # should still be in meta_cache...
            if obj_id not in meta_cache:
                msg = f"expected to find {obj_id} in meta_cache"
                log.error(msg)
            elif obj_id in dirty_ids and dirty_ids[obj_id][0] > last_update_time:
                log.info(f"write_s3_obj {obj_id} got updated while s3 write was in progress")
            else:
                meta_cache.clearDirty(obj_id)  # allow eviction from cache 
    finally:
        # clear pending_s3_write item
        log.debug(f"write_s3_obj finally block, success={success}")
        if s3key not in pending_s3_write:
            msg = f"write s3 obj: Expected to find {s3key} in pending_s3_write map"
            log.error(msg)
        else:
            if pending_s3_write[s3key] != now:
                msg = f"pending_s3_write timestamp got updated unexpectedly for {s3key}"
                log.error(msg)
            del pending_s3_write[s3key]
        # clear task
        if obj_id not in pending_s3_write_tasks:
            log.debug(f"no pending s3 write task for {obj_id}")
        else:
            log.debug(f"removing pending s3 write task for {obj_id}")
            del pending_s3_write_tasks[obj_id]
        # clear dirty flag
        if obj_id in dirty_ids and dirty_ids[obj_id][0] == last_update_time:
            log.debug(f"clearing dirty flag for {obj_id}")
            del dirty_ids[obj_id]
        
    # add to set so that AN can be notified about changed objects
    notify_objs.add(obj_id)

    # calculate time to do the write
    elapsed_time = time.time() - now
    log.info(f"s3 write for {s3key} took {elapsed_time:.3f}s") 
    return obj_id

async def s3sync(app):
    """ Periodic method that writes dirty objects in the metadata cache to S3"""
    MAX_PENDING_WRITE_REQUESTS=20
    dirty_ids = app["dirty_ids"]
    pending_s3_write = app["pending_s3_write"]
    pending_s3_write_tasks = app["pending_s3_write_tasks"]
    s3_sync_interval = config.get("s3_sync_interval")
    dirty_count = len(dirty_ids)
    if not dirty_count:
        log.info("s3sync nothing to update")
        return 0

    log.info(f"s3sync update - dirtyid count: {dirty_count}, active write tasks: {len(pending_s3_write_tasks)}/{MAX_PENDING_WRITE_REQUESTS}")
    log.debug(f"s3sync dirty_ids: {dirty_ids}")
    log.debug(f"sesync pending write s3keys: {list(pending_s3_write.keys())}")
    log.debug(f"s3sync write tasks: {list(pending_s3_write_tasks.keys())}")

    def callback(future):
        try:
            obj_id = future.result()  # returns a objid
            log.info(f"write_s3_obj callback result: {obj_id}")
        except HTTPInternalServerError as hse:
            log.error(f"write_s3_obj callback got 500: {hse}")
        except Exception as e:
            log.error(f"write_s3_obj callback unexpected exception: {e}")
    
    update_count = 0
    s3sync_start = time.time()
        
    for obj_id in dirty_ids:
        bucket = dirty_ids[1]
        if not bucket:
            bucket = app["bucket_name"]
        s3key = getS3Key(obj_id)
        log.debug(f"s3sync dirty id: {obj_id}, s3key: {s3key} bucket: {bucket}")
        create_task = True
        if s3key in pending_s3_write:
            log.debug(f"key {s3key} has been pending for {s3sync_start - pending_s3_write[s3key]}")
            if s3sync_start - pending_s3_write[s3key] > s3_sync_interval * 2:
                log.warn(f"obj {obj_id} has been in pending_s3_write for {s3sync_start - pending_s3_write[s3key]} seconds, restarting")
                del pending_s3_write[s3key]
                if obj_id not in pending_s3_write_tasks:
                    log.error(f"Expected to find write task for {obj_id}")
                else:
                    task = pending_s3_write_tasks[obj_id]
                    task.cancel()
                    del pending_s3_write_tasks[obj_id]
            else:
                log.debug(f"key {s3key} has a pending write task")
                create_task = False
                if obj_id not in pending_s3_write_tasks:
                    log.error(f"expected to find {obj_id} in pending_s3_write_tasks")
        if create_task and len(pending_s3_write_tasks) < MAX_PENDING_WRITE_REQUESTS:
            # create a task to write this object
            log.debug(f"s3sync - ensure future for {obj_id}")
            task = asyncio.ensure_future(write_s3_obj(app, obj_id, bucket=bucket))
            task.add_done_callback(callback)
            pending_s3_write_tasks[obj_id] = task
            update_count += 1


    # notify AN of key updates 
    an_url = getAsyncNodeUrl(app)
    
    notify_objs = app["an_notify_objs"]
    if len(notify_objs) > 0:           
        log.info(f"Notifying AN for {len(notify_objs)} S3 Updates")
        body = { "objs": list(notify_objs) }
        notify_objs.clear()

        req = an_url + "/objects"
        try:
            log.info("ASync PUT notify: {} body: {}".format(req, body))
            await http_put(app, req, data=body)
        except HTTPInternalServerError as hpe:
            msg = "got error notifying async node: {}".format(hpe)
            log.error(msg)

    # return number of objects written
    return update_count



async def s3syncCheck(app):
    s3_sync_interval = config.get("s3_sync_interval")
    long_sleep = config.get("node_sleep_time")
    short_sleep = long_sleep/100.0
    last_write_time = 0

    while True:
        if app["node_state"] != "READY":
            log.info("s3sync - clusterstate is not ready, sleeping")
            await asyncio.sleep(long_sleep)
            continue
       
        update_count = await s3sync(app)
        now = time.time()
        if update_count:
            log.info(f"s3syncCheck {update_count} objects updated")
            last_write_time = time.time()

        if now - last_write_time < s3_sync_interval:
            log.debug(f"s3syncCheck sleeping for {short_sleep}")
            # this will sleep for ~0.1s by default
            await asyncio.sleep(short_sleep)
        else:
            log.info(f"s3syncCheck no objects to write, sleeping for {long_sleep}")
            await asyncio.sleep(long_sleep)
