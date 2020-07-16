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
import numpy as np
from aiohttp.web_exceptions import HTTPGone, HTTPInternalServerError, HTTPBadRequest, HTTPNotFound, HTTPForbidden, HTTPServiceUnavailable
from .util.idUtil import validateInPartition, getS3Key, isValidUuid, isValidChunkId, getDataNodeUrl, isSchema2Id, getRootObjId, isRootObjId
from .util.storUtil import getStorJSONObj, putStorJSONObj, putStorBytes, getStorBytes, isStorObj, deleteStorObj
from .util.domainUtil import isValidDomain, getBucketForDomain
from .util.attrUtil import getRequestCollectionName
from .util.httpUtil import http_post
from .util.dsetUtil import getChunkLayout, getDeflateLevel, isShuffle, getFillValue
from .util.chunkUtil import getDatasetId
from .util.arrayUtil import arrayToBytes, bytesToArray
from .util.hdf5dtype import createDataType

from . import config
from . import hsds_logger as log


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
        msg = f"Invalid obj id: {obj_id}"
        log.error(msg)
        raise HTTPInternalServerError()

    try:
        validateInPartition(app, obj_id)
    except KeyError:
        log.error("Domain not in partition")
        raise HTTPInternalServerError()

    return obj_id

async def notify_root(app, root_id, bucket=None):
    # flag to write to S3

    log.info(f"notify_root: {root_id}")
    if not isValidUuid(root_id) or not isSchema2Id(root_id):
        log.error(f"unexpected call to notify with invalid id: {root_id}")
        return
    notify_req = getDataNodeUrl(app, root_id) + "/roots/" + root_id
    log.info(f"Notify: {notify_req} [{bucket}]")
    params = {}
    if bucket:
        params["bucket"] = bucket
    await http_post(app, notify_req, data={}, params=params)

async def check_metadata_obj(app, obj_id, bucket=None):
    """ Return False is obj does not exist
    """
    if isValidDomain(obj_id):
        bucket = getBucketForDomain(obj_id)

    try:
        validateInPartition(app, obj_id)
    except KeyError:
        log.error("Domain not in partition")
        raise HTTPInternalServerError()

    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        msg = f"{obj_id} has been deleted"
        log.info(msg)
        return False

    meta_cache = app['meta_cache']
    if obj_id in meta_cache:
        found = True
    else:
        # Not in chache, check s3 obj exists
        s3_key = getS3Key(obj_id)
        log.debug(f"check_metadata_obj({s3_key})")
        # does key exist?
        found = await isStorObj(app, s3_key, bucket=bucket)
    return found



async def get_metadata_obj(app, obj_id, bucket=None):
    """ Get object from metadata cache (if present).
        Otherwise fetch from S3 and add to cache
    """
    log.info(f"get_metadata_obj: {obj_id} bucket: {bucket}")
    if isValidDomain(obj_id):
        bucket = getBucketForDomain(obj_id)

    # don't call validateInPartition since this is used to pull in
    # immutable data from other nodes

    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        msg = f"{obj_id} has been deleted"
        log.warn(msg)
        raise HTTPGone()

    meta_cache = app['meta_cache']
    obj_json = None
    if obj_id in meta_cache:
        log.debug(f"{obj_id} found in meta cache")
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
            log.debug(f"getS3JSONObj({s3_key}, bucket={bucket})")
            if obj_id not in pending_s3_read:
                pending_s3_read[obj_id] = time.time()
            # read S3 object as JSON
            try:
                obj_json = await getStorJSONObj(app, s3_key, bucket=bucket)
            except HTTPNotFound:
                log.warn(f"HTTPpNotFound error for {s3_key} bucket:{bucket}")
                if obj_id in pending_s3_read:
                    del pending_s3_read[obj_id]
                raise
            except HTTPForbidden:
                log.warn(f"HTTPForbidden error for {s3_key} bucket:{bucket}")
                if obj_id in pending_s3_read:
                    del pending_s3_read[obj_id]
                raise
            except HTTPInternalServerError:
                log.warn(f"HTTPInternalServerError error for {s3_key} bucket:{bucket}")
                if obj_id in pending_s3_read:
                    del pending_s3_read[obj_id]
                raise
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

    if not isinstance(obj_json, dict):
        log.error("Passed non-dict obj to save_metadata_obj")
        raise HTTPInternalServerError()

    try:
        validateInPartition(app, obj_id)
    except KeyError:
        log.error("Domain not in partition")
        raise HTTPInternalServerError()

    if  isValidChunkId(obj_id):
        log.warn(f"save_metadata_obj {obj_id} not supported for chunks")
        raise HTTPBadRequest()

    dirty_ids = app["dirty_ids"]
    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        if isValidUuid(obj_id):
            # domain objects may be re-created, but shouldn't see repeats of
            # deleted uuids
            log.warn(f"{obj_id} has been deleted")
            raise HTTPInternalServerError()
        elif obj_id in deleted_ids:
            deleted_ids.remove(obj_id)  # un-gone the domain id

    # update meta cache
    meta_cache = app['meta_cache']
    log.debug(f"save: {obj_id} to cache")
    meta_cache[obj_id] = obj_json

    meta_cache.setDirty(obj_id)
    now = time.time()
    log.debug(f"setting dirty_ids[{obj_id}] = ({now}, {bucket})")
    if isValidUuid(obj_id) and not bucket:
        log.warn(f"bucket is not defined for save_metadata_obj: {obj_id}")
    dirty_ids[obj_id] = (now, bucket)

    if flush:
        # write to S3 immediately
        if isValidChunkId(obj_id):
            log.warn("flush not supported for save_metadata_obj with chunks")
            raise HTTPBadRequest()
        try:
            await write_s3_obj(app, obj_id, bucket=bucket)
        except KeyError as ke:
            log.error(f"s3 sync got key error: {ke}")
            raise HTTPInternalServerError()
        except HTTPInternalServerError:
            log.warn(f" failed to write {obj_id}")
            raise  # re-throw
        if obj_id in dirty_ids:
            log.warn(f"save_metadata_obj flush - object {obj_id} is still dirty")
        # message AN immediately if notify flag is set
        # otherwise node for root will be notified at next S3 sync
        if notify:
            log.debug(f"save_metadata_obj - sending notify for {obj_id}")
            if isValidUuid(obj_id) and isSchema2Id(obj_id):
                root_id = getRootObjId(obj_id)
                await notify_root(app, root_id, bucket=bucket)
        



async def delete_metadata_obj(app, obj_id, notify=True, root_id=None, bucket=None):
    """ Delete the given object """
    meta_cache = app['meta_cache']
    dirty_ids = app["dirty_ids"]
    log.info(f"delete_meta_data_obj: {obj_id} notify: {notify}")
    if isValidDomain(obj_id):
        bucket = getBucketForDomain(obj_id)

    try:
        validateInPartition(app, obj_id)
    except KeyError:
        log.error(f"obj: {obj_id} not in partition")
        raise HTTPInternalServerError()

    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        log.warn(f"{obj_id} has already been deleted")
    else:
        log.debug(f"adding {obj_id} to deleted ids")
        deleted_ids.add(obj_id)

    if obj_id in meta_cache:
        log.debug(f"removing {obj_id} from meta_cache")
        del meta_cache[obj_id]

    if obj_id in dirty_ids:
        log.debug(f"removing dirty_ids for: {obj_id}")
        del dirty_ids[obj_id]

    # remove from S3 (if present)
    s3key = getS3Key(obj_id)

    if await isStorObj(app, s3key, bucket=bucket):
        await deleteStorObj(app, s3key, bucket=bucket)
    else:
        log.info(f"delete_metadata_obj - key {s3key} not found (never written)?")

    if isValidUuid(obj_id) and isSchema2Id(obj_id):
        if isRootObjId(obj_id):
            # add to gc ids so sub-objects will be deleted
            gc_ids = app["gc_ids"]
            log.info(f"adding root id: {obj_id} for GC cleanup")
            gc_ids.add(obj_id)
        elif notify:
            root_id = getRootObjId(obj_id)
            await notify_root(app, root_id, bucket=bucket)
        # no notify for domain deletes since the root group is being deleted

    log.debug(f"delete_metadata_obj for {obj_id} done")

"""
Utility method for GET_Chunk, PUT_Chunk, and POST_CHunk
Get a numpy array for the chunk (possibly initizaling a new chunk if requested)
"""
async def get_chunk(app, chunk_id, dset_json, bucket=None, s3path=None, s3offset=0, s3size=0, chunk_init=False):
    # if the chunk cache has too many dirty items, wait till items get flushed to S3
    MAX_WAIT_TIME = 10.0  # TBD - make this a config
    chunk_cache = app['chunk_cache']
    if chunk_init and s3offset > 0:
        log.error(f"unable to initiale chunk {chunk_id} for reference layouts ")
        raise  HTTPInternalServerError()

    log.debug(f"getChunk cache utilization: {chunk_cache.cacheUtilizationPercent} per, dirty_count: {chunk_cache.dirtyCount}, mem_dirty: {chunk_cache.memDirty}")

    chunk_arr = None
    dims = getChunkLayout(dset_json)
    type_json = dset_json["type"]
    dt = createDataType(type_json)
    # note - officially we should follow the order in which the filters are defined in the filter_list,
    # but since we currently have just deflate and shuffle we will always apply deflate then shuffle on read,
    # and shuffle then deflate on write
    # also note - get deflate and shuffle will update the deflate and shuffle map so that the s3sync will do the right thing
    deflate_level = getDeflateLevel(dset_json)
    shuffle = isShuffle(dset_json)
    s3key = None

    if s3path:
        if s3path.startswith("s3://"):
            # trim off the s3:// if found
            path = s3path[5:]
        else:
            path = s3path
        index = path.find('/')   # split bucket and key
        if index < 1:
            log.error(f"s3path is invalid: {s3path}")
            raise HTTPInternalServerError()
        bucket = path[:index]
        s3key = path[(index+1):]
        log.debug(f"Using s3path bucket: {bucket} and  s3key: {s3key}")
    else:
        s3key = getS3Key(chunk_id)
        log.debug(f"getChunk chunkid: {chunk_id} bucket: {bucket}")
    if chunk_id in chunk_cache:
        chunk_arr = chunk_cache[chunk_id]
    else:
        if s3path and s3size == 0:
            obj_exists = False
        else:
            obj_exists = await isStorObj(app, s3key, bucket=bucket)
        # TBD - potential race condition?
        if obj_exists:
            pending_s3_read = app["pending_s3_read"]

            if chunk_id in pending_s3_read:
                # already a read in progress, wait for it to complete
                read_start_time = pending_s3_read[chunk_id]
                log.info(f"s3 read request for {chunk_id} was requested at: {read_start_time}")
                while time.time() - read_start_time < 2.0:
                    log.debug("waiting for pending s3 read, sleeping")
                    await asyncio.sleep(1)  # sleep for sub-second?
                    if chunk_id in chunk_cache:
                        log.info(f"Chunk {chunk_id} has arrived!")
                        chunk_arr = chunk_cache[chunk_id]
                        break
                if chunk_arr is None:
                    log.warn(f"s3 read for chunk {chunk_id} timed-out, initiaiting a new read")

            if chunk_arr is None:
                if chunk_id not in pending_s3_read:
                    pending_s3_read[chunk_id] = time.time()
                log.debug(f"Reading chunk {s3key} from S3")

                chunk_bytes = await getStorBytes(app, s3key, shuffle=shuffle, deflate_level=deflate_level, offset=s3offset, length=s3size, bucket=bucket)
                if chunk_id in pending_s3_read:
                    # read complete - remove from pending map
                    elapsed_time = time.time() - pending_s3_read[chunk_id]
                    log.info(f"s3 read for {s3key} took {elapsed_time}")
                    del pending_s3_read[chunk_id]
                else:
                    log.warn(f"expected to find {chunk_id} in pending_s3_read map")
                chunk_arr = bytesToArray(chunk_bytes, dt, dims)

            log.debug(f"chunk size: {chunk_arr.size}")

        elif chunk_init:
            log.debug(f"Initializing chunk {chunk_id}")
            fill_value = getFillValue(dset_json)
            if fill_value:
                # need to convert list to tuples for numpy broadcast
                if isinstance(fill_value, list):
                    fill_value = tuple(fill_value)
                chunk_arr = np.empty(dims, dtype=dt, order='C')
                chunk_arr[...] = fill_value
            else:
                chunk_arr = np.zeros(dims, dtype=dt, order='C')
        else:
            log.debug(f"Chunk {chunk_id} not found")

        if chunk_arr is not None:
            # check that there's room in the cache before adding it
            if chunk_cache.memTarget - chunk_cache.memDirty < chunk_arr.size:
                # no room in the cache, wait till space is freed by the s3sync task
                wait_start = time.time()
                while chunk_cache.memTarget - chunk_cache.memDirty < chunk_arr.size:
                    log.warn(f"getChunk, cache utilization: {chunk_cache.cacheUtilizationPercent}, sleeping till items are flushed")
                    if time.time() - wait_start > MAX_WAIT_TIME:
                        log.warn(f"unable to save chunk {chunk_id} to cache returning 503 error")
                        raise HTTPServiceUnavailable()
                    await asyncio.sleep(1)

            chunk_cache[chunk_id] = chunk_arr  # store in cache
    return chunk_arr

"""
Mark the given chunk as dirty to write to storage
"""
def save_chunk(app, chunk_id, bucket=None):
    """ Persist the given object """
    log.info(f"save_chunk {chunk_id} bucket={bucket}")

    try:
        validateInPartition(app, chunk_id)
    except KeyError:
        log.error("Domain not in partition")
        raise HTTPInternalServerError()

    chunk_cache = app["chunk_cache"]
    chunk_cache.setDirty(chunk_id)
    log.info(f"chunk cache dirty count: {chunk_cache.dirtyCount}")

    # async write to S3
    dirty_ids = app["dirty_ids"]
    now = time.time()
    dirty_ids[chunk_id] = (now, bucket)

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
    shuffle_map = app['shuffle_map']
    notify_objs = app["root_notify_ids"]
    deleted_ids = app['deleted_ids']
    success = False

    if isValidDomain(obj_id):
        domain_bucket = getBucketForDomain(obj_id)
        if bucket and bucket != domain_bucket:
            log.error(f"expected bucket for domain: {obj_id} to match what wsas passed to write_s3_obj")
        else:
            bucket = domain_bucket

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
        log.warn(f"Canceling write for {obj_id} since it has been deleted")
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
    else:
        log.debug(f"write_s3_obj - {obj_id} not in dirty_ids, assuming flush write")
    if last_update_time > now:
        msg = f"last_update time {last_update_time} is in the future for obj_id: {obj_id}"
        log.error(msg)
        raise ValueError(msg)

    pending_s3_write[s3key] = now
    # do the following in the try block so we can always remove the pending_s3_write at the end

    try:
        if isValidChunkId(obj_id):
            if obj_id not in chunk_cache:
                log.error(f"expected to find obj_id: {obj_id} in chunk cache")
                raise KeyError(f"{obj_id} not found in chunk cache")
            if not chunk_cache.isDirty(obj_id):
                log.error(f"expected chunk cache obj {obj_id} to be dirty")
                raise ValueError("bad dirty state for obj")
            chunk_arr = chunk_cache[obj_id]
            chunk_bytes = arrayToBytes(chunk_arr)
            dset_id = getDatasetId(obj_id)
            deflate_level = None
            shuffle = 0
            if dset_id in shuffle_map:
                shuffle = shuffle_map[dset_id]
            if dset_id in deflate_map:
                deflate_level = deflate_map[dset_id]
                log.debug(f"got deflate_level: {deflate_level} for dset: {dset_id}")
            if dset_id in shuffle_map:
                shuffle = shuffle_map[dset_id]
                log.debug(f"got shuffle size: {shuffle} for dset: {dset_id}")

            await putStorBytes(app, s3key, chunk_bytes, shuffle=shuffle, deflate_level=deflate_level, bucket=bucket)
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
                log.error(f"expected to find obj_id: {obj_id} in meta cache")
                raise KeyError(f"{obj_id} not found in meta cache")
            if not meta_cache.isDirty(obj_id):
                log.error(f"expected meta cache obj {obj_id} to be dirty")
                raise ValueError("bad dirty state for obj")
            obj_json = meta_cache[obj_id]

            await putStorJSONObj(app, s3key, obj_json, bucket=bucket)
            success = True
            # should still be in meta_cache...
            if obj_id in deleted_ids:
                log.info(f"obj {obj_id} has been deleted while write was in progress")
            elif obj_id not in meta_cache:
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
        if obj_id not in dirty_ids:
            log.warn(f"write_s3_obj - expected to find id: {obj_id} in dirty_ids")
        elif not success:
            log.warn(f"write_s3_obj - write not successful, for {obj_id} keeping dirty flag")
        elif dirty_ids[obj_id][0] > last_update_time:
            log.warn(f"write_s3_obj - {obj_id} has been modified during write, keeping dirty flag")
        else:
            log.debug(f"clearing dirty flag for {obj_id}")
            del dirty_ids[obj_id]

    # add to map so that root can be notified about changed objects
    if isValidUuid(obj_id) and isSchema2Id(obj_id):
        root_id = getRootObjId(obj_id)
        notify_objs[root_id] = bucket

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
    log.debug(f"s3sync pending write s3keys: {list(pending_s3_write.keys())}")
    log.debug(f"s3sync write tasks: {list(pending_s3_write_tasks.keys())}")

    def callback(future):
        try:
            obj_id = future.result()  # returns a objid
            log.info(f"write_s3_obj callback result: {obj_id}")
        except HTTPInternalServerError as hse:
            log.error(f"write_s3_obj callback got 500: {hse}")
        except Exception as e:
            log.error(f"write_s3_obj callback unexpected exception {type(e)}: {e}")

    update_count = 0
    s3sync_start = time.time()

    log.info(f"s3sync - processing {len(dirty_ids)} dirty_ids")
    for obj_id in dirty_ids:
        item = dirty_ids[obj_id]
        log.debug(f"got item: {item} for obj_id: {obj_id}")
        bucket = item[1]
        if not bucket:
            if "bucket_name" in app and app["bucket_name"]:
                bucket = app["bucket_name"]
            else:
                log.error(f"can not determine bucket for s3sync obj_id: {obj_id}")
                continue
        s3key = getS3Key(obj_id)
        log.debug(f"s3sync dirty id: {obj_id}, s3key: {s3key} bucket: {bucket}")

        create_task = True
        if s3key in pending_s3_write:
            log.debug(f"key {s3key} has been pending for {s3sync_start - pending_s3_write[s3key]}")
            if s3sync_start - pending_s3_write[s3key] > s3_sync_interval * 2:
                log.warn(f"obj {obj_id} has been in pending_s3_write for {s3sync_start - pending_s3_write[s3key]} seconds, restarting")
                del pending_s3_write[s3key]
                if obj_id not in pending_s3_write_tasks:
                    log.warn(f"Expected to find write task for {obj_id}")
                else:
                    task = pending_s3_write_tasks[obj_id]
                    task.cancel()
                    del pending_s3_write_tasks[obj_id]
            else:
                log.debug(f"key {s3key} has a pending write task")
                create_task = False
                if obj_id not in pending_s3_write_tasks:
                    log.error(f"expected to find {obj_id} in pending_s3_write_tasks")
        if create_task:
            if len(pending_s3_write_tasks) < MAX_PENDING_WRITE_REQUESTS:
                # create a task to write this object
                log.debug(f"s3sync - ensure future for {obj_id}")
                task = asyncio.ensure_future(write_s3_obj(app, obj_id, bucket=bucket))
                task.add_done_callback(callback)
                pending_s3_write_tasks[obj_id] = task
                update_count += 1
            else:
                log.debug(f"s3sync - too many pending tasks, not creating task for: {obj_id} now")


    # notify root of obj updates
    notify_ids = app["root_notify_ids"]
    if len(notify_ids) > 0:
        log.info(f"Notifying for {len(notify_ids)} S3 Updates")
        # create a set since we are not allowed to change
        root_ids = set()
        for root_id in notify_ids:
            root_ids.add(root_id)

        for root_id in root_ids:
            bucket = notify_ids[root_id]
            await notify_root(app, root_id, bucket=bucket)
            del notify_ids[root_id]
        log.info("root notify complete")

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
        else:
            log.debug("s3sync - clusterstate is {}".format(app["node_state"]))

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
