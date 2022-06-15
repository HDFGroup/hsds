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
# utility methods for datanode handlers
#

import asyncio
import time
import numpy as np
from aiohttp.web_exceptions import HTTPGone, HTTPInternalServerError
from aiohttp.web_exceptions import HTTPNotFound, HTTPForbidden
from aiohttp.web_exceptions import HTTPServiceUnavailable, HTTPBadRequest
from .util.idUtil import validateInPartition, getS3Key, isValidUuid
from .util.idUtil import isValidChunkId, getDataNodeUrl, isSchema2Id
from .util.idUtil import getRootObjId, isRootObjId
from .util.storUtil import getStorJSONObj, putStorJSONObj, putStorBytes
from .util.storUtil import getStorBytes, isStorObj, deleteStorObj
from .util.storUtil import getBucketFromStorURI, getKeyFromStorURI
from .util.domainUtil import isValidDomain, getBucketForDomain
from .util.attrUtil import getRequestCollectionName
from .util.httpUtil import http_post
from .util.dsetUtil import getChunkLayout, getFilterOps, getFillValue
from .util.chunkUtil import getDatasetId
from .util.arrayUtil import arrayToBytes, bytesToArray
from .util.hdf5dtype import createDataType, getItemSize

from . import config
from . import hsds_logger as log


def get_obj_id(request, body=None):
    """Get object id from request
    Raise HTTPException on errors.
    """
    obj_id = None
    collection = None
    app = request.app
    if body and "id" in body:
        obj_id = body["id"]
    else:
        # returns datasets|groups|datatypes
        collection = getRequestCollectionName(request)
        obj_id = request.match_info.get("id")

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
        log.error(f"Object {obj_id} not in partition")
        raise HTTPInternalServerError()

    return obj_id


async def notify_root(app, root_id, bucket=None):
    """flag to write to S3"""

    log.info(f"notify_root: {root_id}, bucket={bucket}")
    if not isValidUuid(root_id) or not isSchema2Id(root_id):
        log.error(f"unexpected call to notify with invalid id: {root_id}")
        return
    if not bucket:
        log.error("notify_root - bucket not set")
        return
    notify_req = getDataNodeUrl(app, root_id) + "/roots/" + root_id
    log.info(f"Notify: {notify_req} [{bucket}]")
    params = {}
    if bucket:
        params["bucket"] = bucket
    await http_post(app, notify_req, data={}, params=params)


async def check_metadata_obj(app, obj_id, bucket=None):
    """Return False is obj does not exist"""
    if isValidDomain(obj_id):
        bucket = getBucketForDomain(obj_id)

    try:
        validateInPartition(app, obj_id)
    except KeyError:
        log.error(f"Object {obj_id} not in partition")
        raise HTTPInternalServerError()

    deleted_ids = app["deleted_ids"]
    if obj_id in deleted_ids:
        msg = f"{obj_id} has been deleted"
        log.info(msg)
        return False

    meta_cache = app["meta_cache"]
    if obj_id in meta_cache:
        found = True
    else:
        # Not in chache, check s3 obj exists
        s3_key = getS3Key(obj_id)
        log.debug(f"check_metadata_obj({s3_key})")
        # does key exist?
        found = await isStorObj(app, s3_key, bucket=bucket)
    return found


async def write_s3_obj(app, obj_id, bucket=None):
    """writes the given object to s3"""
    s3key = getS3Key(obj_id)
    msg = f"write_s3_obj for obj_id: {obj_id} / s3_key: {s3key}  "
    msg += f"bucket: {bucket}"
    log.info(msg)
    pending_s3_write = app["pending_s3_write"]
    pending_s3_write_tasks = app["pending_s3_write_tasks"]
    dirty_ids = app["dirty_ids"]
    chunk_cache = app["chunk_cache"]
    meta_cache = app["meta_cache"]
    filter_map = app["filter_map"]
    notify_objs = app["root_notify_ids"]
    deleted_ids = app["deleted_ids"]
    success = False

    if isValidDomain(obj_id):
        domain_bucket = getBucketForDomain(obj_id)
        if bucket and bucket != domain_bucket:
            msg = f"expected bucket for domain: {obj_id} to match what was "
            msg += "passed to write_s3_obj"
            log.error(msg)
        else:
            bucket = domain_bucket

    if obj_id in pending_s3_write:
        msg = f"write_s3_key - not expected for key {obj_id} to be in "
        msg += "pending_s3_write map"
        log.error(msg)
        raise KeyError(msg)

    if obj_id in deleted_ids and isValidUuid(obj_id):
        # if this objid has been deleted (and its unique since this is
        # not a domain id) cancel any pending task and return
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
        # timestamp is first element of two-tuple
        last_update_time = dirty_ids[obj_id][0]
    else:
        msg = f"write_s3_obj - {obj_id} not in dirty_ids, "
        msg += "assuming flush write"
        log.debug(msg)
    if last_update_time > now:
        msg = f"last_update time {last_update_time} is in the future for "
        msg += f"obj_id: {obj_id}"
        log.error(msg)
        raise ValueError(msg)

    pending_s3_write[obj_id] = now
    # do the following in the try block so we can always remove the
    # pending_s3_write at the end

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
            if dset_id in filter_map:
                filter_ops = filter_map[dset_id]
                msg = f"write_s3_obj: got filter_op: {filter_ops} "
                msg += f"for dset: {dset_id}"
                log.debug(msg)
            else:
                filter_ops = None
                log.debug(f"write_s3_obj: no filter_op for dset: {dset_id}")

            kwargs = {"bucket": bucket, "filter_ops": filter_ops}
            await putStorBytes(app, s3key, chunk_bytes, **kwargs)
            success = True

            # if chunk has been evicted from cache something has gone wrong
            if obj_id not in chunk_cache:
                msg = f"write_s3_obj: expected to find {obj_id} "
                msg += "in chunk_cache"
                log.error(msg)
            else:
                if obj_id in dirty_ids:
                    timestamp = dirty_ids[obj_id][0]
                else:
                    timestamp = 0
                if timestamp > last_update_time:
                    msg = f"write_s3_obj {obj_id} got updated while s3 "
                    msg += "write was in progress"
                    log.info(msg)
                else:
                    # no new write, can clear dirty
                    # allow eviction from cache
                    chunk_cache.clearDirty(obj_id)
                    cache_utilization = chunk_cache.cacheUtilizationPercent
                    dirty_count = chunk_cache.dirtyCount
                    msg = f"write_s3_obj: {obj_id} updated - "
                    msg += f"Chunk cache utilization: {cache_utilization} "
                    msg += "per, dirty_count: {dirty_count}"
                    log.debug(msg)
        else:
            # meta data update
            # check for object in meta cache
            if obj_id not in meta_cache:
                msg = f"write_s3_obj: expected to find obj_id: {obj_id} "
                msg += "in meta cache"
                log.error(msg)
                raise KeyError(f"{obj_id} not found in meta cache")
            if not meta_cache.isDirty(obj_id):
                msg = f"write_s3_obj: expected meta cache obj {obj_id} "
                msg == "to be dirty"
                log.error(msg)
                raise ValueError("bad dirty state for obj")
            obj_json = meta_cache[obj_id]

            await putStorJSONObj(app, s3key, obj_json, bucket=bucket)
            success = True
            # should still be in meta_cache...
            if obj_id in deleted_ids:
                msg = f"write_s3_obj: obj {obj_id} has been deleted "
                msg += "while write was in progress"
                log.info(msg)
            elif obj_id not in meta_cache:
                msg = f"write_s3_obj: expected to find {obj_id} in meta_cache"
                log.error(msg)
            else:
                if obj_id in dirty_ids:
                    timestamp = dirty_ids[obj_id][0]
                else:
                    timestamp = 0
                if timestamp > last_update_time:
                    msg = f"write_s3_obj: {obj_id} got updated while s3 "
                    msg += "write was in progress"
                    log.info(msg)
                else:
                    log.debug(f"write_s3obj: clear dirty for {obj_id} ")
                    meta_cache.clearDirty(obj_id)  # allow eviction from cache
                    cache_utilization = chunk_cache.cacheUtilizationPercent
                    dirty_count = chunk_cache.dirtyCount
                    msg = f"write_s3_obj: {obj_id} updated - "
                    msg += f"Meta cache utilization: {cache_utilization} per, "
                    msg += f"dirty_count: {dirty_count}"
                    log.debug(msg)

    finally:
        # clear pending_s3_write item
        log.debug(f"write_s3_obj finally block, success={success}")
        if obj_id in pending_s3_write:
            if pending_s3_write[obj_id] != now:
                msg = "pending_s3_write timestamp got updated unexpectedly "
                msg += f"for {obj_id}"
                log.error(msg)
            del pending_s3_write[obj_id]
        # clear task
        if obj_id not in pending_s3_write_tasks:
            log.debug(f"no pending s3 write task for {obj_id}")
        else:
            log.debug(f"removing pending s3 write task for {obj_id}")
            del pending_s3_write_tasks[obj_id]
        # clear dirty flag
        if obj_id not in dirty_ids:
            msg = f"write_s3_obj - expected to find id: {obj_id} in dirty_ids"
            log.warn(msg)
        elif not success:
            msg = f"write_s3_obj - write not successful, for {obj_id} "
            msg += "keeping dirty flag"
            log.warn(msg)
        elif dirty_ids[obj_id][0] > last_update_time:
            msg = f"write_s3_obj - {obj_id} has been modified during "
            msg += "write, keeping dirty flag"
            log.warn(msg)
        else:
            log.debug(f"clearing dirty flag for {obj_id}")
            del dirty_ids[obj_id]

    # add to map so that root can be notified about changed objects
    if isValidUuid(obj_id) and isSchema2Id(obj_id):
        root_id = getRootObjId(obj_id)
        notify_objs[root_id] = bucket

    # calculate time to do the write
    elapsed_time = time.time() - now
    log.info(f"s3 write for {obj_id} took {elapsed_time:.3f}s")
    return obj_id


async def get_metadata_obj(app, obj_id, bucket=None):
    """Get object from metadata cache (if present).
    Otherwise fetch from S3 and add to cache
    """
    log.info(f"get_metadata_obj: {obj_id} bucket: {bucket}")
    if isValidDomain(obj_id):
        domain_bucket = getBucketForDomain(obj_id)
        if bucket and domain_bucket and bucket != domain_bucket:
            msg = (
                f"get_metadata_obj for domain: {obj_id} but bucket param was: {bucket}"
            )
            log.error(msg)
            raise HTTPInternalServerError()
        if not bucket:
            bucket = domain_bucket

    if not bucket:
        log.warn("get_metadata_obj - bucket is None")

    # don't call validateInPartition since this is used to pull in
    # immutable data from other nodes

    deleted_ids = app["deleted_ids"]
    # don't raise 410 for domains since a domain might have been
    # re-created outside the server
    if obj_id in deleted_ids and not isValidDomain(obj_id):
        msg = f"{obj_id} has been deleted"
        log.warn(msg)
        raise HTTPGone()

    meta_cache = app["meta_cache"]
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
            msg = f"s3 read request for {s3_key} was "
            msg += f"requested at: {read_start_time}"
            log.info(msg)
            store_read_timeout = float(config.get("store_read_timeout", default=2.0))
            log.debug(f"store_read_timeout: {store_read_timeout}")
            store_read_sleep_interval = float(
                config.get("store_read_sleep_interval", default=0.1)
            )
            while time.time() - read_start_time < store_read_timeout:
                log.debug(f"waiting for pending s3 read {s3_key}, sleeping")
                await asyncio.sleep(store_read_sleep_interval)  # sleep for sub-second?
                if obj_id in meta_cache:
                    log.info(f"object {obj_id} has arrived!")
                    obj_json = meta_cache[obj_id]
                    break
            if not obj_json:
                msg = f"s3 read for object {obj_id} timed-out, "
                msg += "initiaiting a new read"
                log.warn(msg)

        # invoke S3 read unless the object has just come in from pending read
        if not obj_json:
            log.debug(f"getS3JSONObj({obj_id}, bucket={bucket})")
            if obj_id not in pending_s3_read:
                pending_s3_read[obj_id] = time.time()
            # read S3 object as JSON
            try:
                obj_json = await getStorJSONObj(app, s3_key, bucket=bucket)
                # read complete - remove from pending map
                if obj_id in pending_s3_read:
                    elapsed_time = time.time() - pending_s3_read[obj_id]
                    log.info(f"s3 read for {obj_id} took {elapsed_time}")
                else:
                    log.warn(f"s3 read complete but pending object: {obj_id} not found")
                meta_cache[obj_id] = obj_json  # add to cache
            except HTTPNotFound:
                msg = f"HTTPNotFound for {obj_id} bucket:{bucket} "
                msg += f"s3key: {s3_key}"
                log.warn(msg)
                if obj_id in deleted_ids and isValidDomain(obj_id):
                    raise HTTPGone()
                raise
            except HTTPForbidden:
                msg = f"HTTPForbidden error for {obj_id} bucket:{bucket} "
                msg += f"s3key: {s3_key}"
                log.warn(msg)
                raise
            except HTTPInternalServerError:
                msg = f"HTTPInternalServerError error for {obj_id} "
                msg += f"bucket:{bucket} s3key: {s3_key}"
                log.warn(msg)
                raise
            finally:
                if obj_id in pending_s3_read:
                    del pending_s3_read[obj_id]

    return obj_json


async def save_metadata_obj(
    app, obj_id, obj_json, bucket=None, notify=False, flush=False
):
    """Persist the given object"""
    msg = f"save_metadata_obj {obj_id} bucket={bucket} notify={notify} "
    msg += f"flush={flush}"
    log.info(msg)
    if notify and not flush:
        log.error("notify not valid when flush is false")
        raise HTTPInternalServerError()

    if not isinstance(obj_json, dict):
        log.error("Passed non-dict obj to save_metadata_obj")
        raise HTTPInternalServerError()

    try:
        validateInPartition(app, obj_id)
    except KeyError:
        log.error(f"Object {obj_id} not in partition")
        raise HTTPInternalServerError()

    if isValidChunkId(obj_id):
        log.warn(f"save_metadata_obj {obj_id} not supported for chunks")
        raise HTTPBadRequest()

    dirty_ids = app["dirty_ids"]
    deleted_ids = app["deleted_ids"]
    if obj_id in deleted_ids:
        if isValidUuid(obj_id):
            # domain objects may be re-created, but shouldn't see repeats of
            # deleted uuids
            log.warn(f"{obj_id} has been deleted")
            raise HTTPInternalServerError()
        elif obj_id in deleted_ids:
            deleted_ids.remove(obj_id)  # un-gone the domain id

    # update meta cache
    meta_cache = app["meta_cache"]
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
            log.debug(f"calling write_s3_obj with {obj_id}")
            await write_s3_obj(app, obj_id, bucket=bucket)
        except KeyError as ke:
            log.error(f"s3 sync got key error: {ke}")
            raise HTTPInternalServerError()
        except HTTPInternalServerError:
            log.warn(f" failed to write {obj_id}")
            raise  # re-throw
        if obj_id in dirty_ids:
            msg = f"save_metadata_obj flush - object {obj_id} is still dirty"
            log.warn(msg)
        # message immediately if notify flag is set
        # otherwise node for root will be notified at next S3 sync
        if notify:
            log.debug(f"save_metadata_obj - sending notify for {obj_id}")
            if isValidUuid(obj_id) and isSchema2Id(obj_id):
                root_id = getRootObjId(obj_id)
                await notify_root(app, root_id, bucket=bucket)


async def delete_metadata_obj(app, obj_id, notify=True, root_id=None, bucket=None):
    """Delete the given object"""
    meta_cache = app["meta_cache"]
    dirty_ids = app["dirty_ids"]
    log.info(f"delete_meta_data_obj: {obj_id} notify: {notify}")
    if isValidDomain(obj_id):
        bucket = getBucketForDomain(obj_id)
        log.debug(f"delete_meta_data_obj: using bucket: {bucket}")

    if not bucket:
        log.error("delete_metadata_obj - bucket not set")
        raise HTTPInternalServerError()

    log.info(f"delete_meta_obj - using bucket:{bucket}")

    try:
        validateInPartition(app, obj_id)
    except KeyError:
        log.error(f"obj: {obj_id} not in partition")
        raise HTTPInternalServerError()

    deleted_ids = app["deleted_ids"]
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
        msg = f"delete_metadata_obj - key {s3key} not found (never written)?"
        log.info(msg)

    if isValidUuid(obj_id) and isSchema2Id(obj_id):
        if isRootObjId(obj_id):
            # add to gc ids so sub-objects will be deleted
            gc_buckets = app["gc_buckets"]
            if bucket not in gc_buckets:
                gc_buckets[bucket] = set()
            gc_ids = gc_buckets[bucket]
            log.info(f"adding root id: {bucket}/{obj_id} for GC cleanup")
            gc_ids.add(obj_id)
        elif notify:
            root_id = getRootObjId(obj_id)
            await notify_root(app, root_id, bucket=bucket)
        # no notify for domain deletes since the root group is being deleted

    log.debug(f"delete_metadata_obj for {obj_id} done")


async def get_chunk(
    app,
    chunk_id,
    dset_json,
    bucket=None,
    s3path=None,
    s3offset=0,
    s3size=0,
    chunk_init=False,
):
    """
    Utility method for GET_Chunk, PUT_Chunk, and POST_CHunk
    Get a numpy array for the chunk (possibly initizaling a new chunk
    if requested)
    """
    # if the chunk cache has too many dirty items, wait till items
    # get flushed to S3
    chunk_cache = app["chunk_cache"]
    if chunk_init and s3offset > 0:
        msg = f"unable to initiale chunk {chunk_id} for reference layouts "
        log.error(msg)
        raise HTTPInternalServerError()

    # validate arguments
    if s3path:
        if s3size == 0:
            msg = f"Unexpected get_chunk parameter - s3path: {s3path} with size 0"
            log.error(msg)
            raise HTTPInternalServerError()
        if bucket:
            msg = "get_chunk - bucket arg should not be used with s3path"
            log.error(msg)
            raise HTTPInternalServerError()
        log.debug(f"get_chunk - chunk_id: {chunk_id} s3path: {s3path}")

    else:
        if not bucket:
            msg = "get_chunk - bucket not set"
            log.error(msg)
            raise HTTPInternalServerError()
        log.debug(f"get_chunk - chunk_id: {chunk_id} bucket: {bucket}")

    if "oio_proxy" in app or "is_k8s" in app:
        # TBD - rangeget proxy not supported on k8s yet
        use_proxy = False
    else:
        use_proxy = True
    msg = f"getChunk cache utilization: {chunk_cache.cacheUtilizationPercent}"
    msg += f" per, dirty_count: {chunk_cache.dirtyCount}, "
    msg += f"mem_dirty: {chunk_cache.memDirty}"
    log.debug(msg)

    chunk_arr = None
    dims = getChunkLayout(dset_json)
    type_json = dset_json["type"]
    item_size = getItemSize(type_json)
    layout = dset_json["layout"]
    layout_class = None
    if "class" in layout:
        layout_class = layout["class"]

    dt = createDataType(type_json)
    # note - officially we should follow the order in which the filters are
    # defined in the filter_list,
    # but since we currently have just deflate and shuffle we will always
    # apply deflate then shuffle on read, and shuffle then deflate on write
    # also note - get deflate and shuffle will update the deflate and
    # shuffle map so that the s3sync will do the right thing
    filter_ops = getFilterOps(app, dset_json, item_size)

    if s3path:
        bucket = getBucketFromStorURI(s3path)
        s3key = getKeyFromStorURI(s3path)
        msg = f"Using s3path bucket: {bucket} and  s3key: {s3key} "
        msg += f"offset: {s3offset} length: {s3size}"
        log.debug(msg)
    else:
        s3key = getS3Key(chunk_id)
        log.debug(f"getChunk chunkid: {chunk_id} bucket: {bucket}")
    if chunk_id in chunk_cache:
        log.debug(f"getChunk chunkid: {chunk_id} found in cache")
        chunk_arr = chunk_cache[chunk_id]
    else:
        # TBD - potential race condition?
        pending_s3_read = app["pending_s3_read"]

        if chunk_id in pending_s3_read:
            # already a read in progress, wait for it to complete
            read_start_time = pending_s3_read[chunk_id]
            msg = f"s3 read request for {chunk_id} was requested at: "
            msg += f"{read_start_time}"
            log.info(msg)
            store_read_timeout = float(config.get("store_read_timeout", default=2.0))
            log.debug(f"store_read_timeout: {store_read_timeout}")
            store_read_sleep_interval = float(
                config.get("store_read_sleep_interval", default=0.1)
            )

            while time.time() - read_start_time < store_read_timeout:
                log.debug("waiting for pending s3 read, sleeping")
                await asyncio.sleep(store_read_sleep_interval)
                if chunk_id in chunk_cache:
                    log.info(f"Chunk {chunk_id} has arrived!")
                    chunk_arr = chunk_cache[chunk_id]
                    break
            if chunk_arr is None:
                msg = f"s3 read for chunk {chunk_id} timed-out, "
                msg += "initiaiting a new read"
                log.warn(msg)

        if chunk_arr is None:
            if chunk_id not in pending_s3_read:
                pending_s3_read[chunk_id] = time.time()
                log.debug(f"Reading chunk {chunk_id} from S3")

            try:
                kwargs = {
                    "filter_ops": filter_ops,
                    "offset": s3offset,
                    "length": s3size,
                    "bucket": bucket,
                    "use_proxy": use_proxy,
                }
                chunk_bytes = await getStorBytes(app, s3key, **kwargs)
                if chunk_id in pending_s3_read:
                    # read complete - remove from pending map
                    elapsed_time = time.time() - pending_s3_read[chunk_id]
                    log.info(f"s3 read for {chunk_id} took {elapsed_time}")
                else:
                    msg = f"expected to find {chunk_id} in "
                    msg += "pending_s3_read map"
                    log.warn(msg)
                if chunk_bytes is None:
                    msg = f"read {chunk_id} bucket: {bucket} returned None"
                    raise ValueError(msg)
                if layout_class == "H5D_CONTIGUOUS_REF":
                    if len(chunk_bytes) < s3size:
                        # we may get less than expected bytes if this chunk
                        # is close to the end of the file
                        # expand to expected number of bytes
                        msg = "extending returned bytearray for "
                        msg += "H5D_CONTIGUOUS layout from "
                        msg += f"{len(chunk_bytes)} to {s3size}"
                        log.info(msg)
                        tmp_buffer = bytearray(s3size)
                        tmp_buffer[: len(chunk_bytes)] = chunk_bytes
                        chunk_bytes = bytes(tmp_buffer)
                chunk_arr = bytesToArray(chunk_bytes, dt, dims)
                log.debug(f"chunk size: {chunk_arr.size}")
            except HTTPNotFound:
                if not chunk_init:
                    log.info(f"chunk not found for id: {chunk_id}")
                    raise  # not found return 404
            except ValueError as ve:
                log.error(f"Unable to retrieve chunk array: {ve}")
                raise HTTPInternalServerError()
            finally:
                # exception or not, no longer pending
                if chunk_id in pending_s3_read:
                    del pending_s3_read[chunk_id]

        if chunk_arr is not None:
            # check that there's room in the cache before adding it
            if chunk_id in chunk_cache or chunk_cache.memFree >= chunk_arr.size:
                chunk_cache[chunk_id] = chunk_arr  # store in cache
            else:
                # no room in the cache, just skip caching
                msg = "getChunk, cache utilization: "
                msg += f"{chunk_cache.cacheUtilizationPercent}, "
                msg += "skip cache for chunk_id {chunk_id}"
                log.warn(msg)

        if chunk_arr is None and chunk_init:
            log.debug(f"Initializing chunk {chunk_id}")
            fill_value = getFillValue(dset_json)
            if fill_value:
                # need to convert list to tuples for numpy broadcast
                if isinstance(fill_value, list):
                    fill_value = tuple(fill_value)
                chunk_arr = np.empty(dims, dtype=dt, order="C")
                chunk_arr[...] = fill_value
            else:
                chunk_arr = np.zeros(dims, dtype=dt, order="C")
        else:
            log.debug(f"Chunk {chunk_id} not found")

    return chunk_arr


def save_chunk(app, chunk_id, dset_json, chunk_arr, bucket=None):
    """Persist the given chunk"""
    log.info(f"save_chunk {chunk_id} bucket={bucket}")

    try:
        validateInPartition(app, chunk_id)
    except KeyError:
        log.error(f"Chunk {chunk_id} not in partition")
        raise HTTPInternalServerError()

    item_size = getItemSize(dset_json["type"])
    # will store filter options into app['filter_map']
    getFilterOps(app, dset_json, item_size)

    chunk_cache = app["chunk_cache"]
    if chunk_id not in chunk_cache:
        # check that we have enough room to store the chunk
        # TBD: there could be issues with the free space calculation
        # not working precisely with variable types
        log.debug(f"chunk_cache free space: {chunk_cache.memFree}")
        if chunk_cache.memFree < chunk_arr.size:
            msg = f"unable to save chunk: {chunk_id}, "
            msg += f"chunk_cache free space: {chunk_cache.memFree}, "
            msg += f"chunk_size:{chunk_arr.size}"
            log.warn(msg)
            raise HTTPServiceUnavailable()

    chunk_cache[chunk_id] = chunk_arr
    chunk_cache.setDirty(chunk_id)
    log.debug(f"chunk cache dirty count: {chunk_cache.dirtyCount}")

    # async write to S3
    dirty_ids = app["dirty_ids"]
    now = time.time()
    dirty_ids[chunk_id] = (now, bucket)


async def s3sync(app, s3_age_time=0):
    """Periodic method that writes dirty objects in
    the metadata cache to S3
    """
    max_pending_write_requests = config.get("max_pending_write_requests")
    dirty_ids = app["dirty_ids"]
    pending_s3_write = app["pending_s3_write"]
    pending_s3_write_tasks = app["pending_s3_write_tasks"]
    s3_sync_task_timeout = config.get("s3_sync_task_timeout")

    dirty_count = len(dirty_ids)
    if not dirty_count:
        log.info("s3sync nothing to update")
        return 0
    msg = f"s3sync update - dirtyid count: {dirty_count}, "
    msg += f"active write tasks: {len(pending_s3_write_tasks)}/"
    msg += f"{max_pending_write_requests}"
    log.info(msg)
    log.debug(f"s3sync dirty_ids: {dirty_ids}")
    pending_keys = list(pending_s3_write.keys())
    log.debug(f"s3sync pending write obj_ids: {pending_keys}")
    pending_tasks = list(pending_s3_write_tasks.keys())
    log.debug(f"s3sync write tasks: {pending_tasks}")

    def callback(future):
        try:
            obj_id = future.result()  # returns a objid
            log.info(f"write_s3_obj callback result: {obj_id}")
        except HTTPInternalServerError as hse:
            log.error(f"write_s3_obj callback got 500: {hse}")
        except HTTPNotFound as nfe:
            log.error(f"write_s3_obj callback got 404: {nfe}")
        except Exception as e:
            msg = f"write_s3_obj callback unexpected exception {type(e)}: {e}"
            log.error(msg)

    update_count = 0
    s3sync_start = time.time()

    log.info(f"s3sync - processing {len(dirty_ids)} dirty_ids")
    for obj_id in dirty_ids:
        if len(pending_s3_write_tasks) >= max_pending_write_requests:
            msg = "max_pending_write requests in flight, not processing "
            msg += "more dirtyids for this run"
            log.debug(msg)
            break
        item = dirty_ids[obj_id]
        log.debug(f"s3sync - got item: {item} for obj_id: {obj_id}")
        time_since_dirty = s3sync_start - item[0]
        if time_since_dirty < 0.0:
            msg = "s3sync: expected time since dirty to be positive, "
            msg += f"but was {time_since_dirty}"
            log.warn(msg)
        bucket = item[1]
        if not bucket:
            if app["bucket_name"]:
                bucket = app["bucket_name"]
            else:
                msg = f"can not determine bucket for s3sync obj_id: {obj_id}"
                log.error(msg)
                continue
        s3key = getS3Key(obj_id)
        msg = f"s3sync - dirty id: {obj_id}, s3key: {s3key} bucket: {bucket}"
        log.debug(msg)

        if obj_id in pending_s3_write:
            pending_time = s3sync_start - pending_s3_write[obj_id]
            msg = f"s3sync - key {obj_id} has been pending for "
            msg += f"{pending_time:.3f}"
            log.debug(msg)
            if s3sync_start - pending_s3_write[obj_id] > s3_sync_task_timeout:
                msg = f"s3sync - obj {obj_id} has been in pending_s3_write "
                msg += f"for {pending_time:.3f} seconds, restarting"
                log.warn(msg)
                del pending_s3_write[obj_id]
                if obj_id not in pending_s3_write_tasks:
                    log.warn(f"s3sync - no pending task for {obj_id}")
                else:
                    log.info(f"s3sync - canceling task for {obj_id}")
                    task = pending_s3_write_tasks[obj_id]
                    task.cancel()
                    del pending_s3_write_tasks[obj_id]
                create_task = True
            else:
                log.debug(f"s3sync - key {obj_id} has a pending write")
                if obj_id not in pending_s3_write_tasks:
                    msg = f"s3sync - no pending task for {obj_id} in "
                    msg += "pending_s3_write_tasks"
                    log.info(msg)
                create_task = False

        elif time_since_dirty < s3_age_time:
            msg = f"s3sync - obj {obj_id} last written {time_since_dirty:.3f} "
            msg += "seconds ago, waiting to age"
            log.debug(msg)
            create_task = False
        else:
            msg = f"s3sync - obj {obj_id} last written {time_since_dirty:.3f} "
            msg += "seconds ago, creating write task"
            log.debug(msg)
            create_task = True

        if create_task:
            # create a task to write this object
            log.debug(f"s3sync - ensure future for {obj_id}")
            kwargs = {"bucket": bucket}
            task = asyncio.ensure_future(write_s3_obj(app, obj_id, **kwargs))
            task.add_done_callback(callback)
            pending_s3_write_tasks[obj_id] = task
            update_count += 1

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
            try:
                await notify_root(app, root_id, bucket=bucket)
                del notify_ids[root_id]
            except HTTPServiceUnavailable:
                # this can happen if we go into a WAITING state
                log.warning(
                    f"got HTTPServiceUnavailable exception try to notify root_id: {root_id}"
                )
        log.info("root notify complete")

    # return number of objects written
    return update_count


async def s3syncCheck(app):
    s3_sync_interval = config.get("s3_sync_interval")
    s3_age_time = config.get("s3_age_time", default=1)
    last_update = time.time()
    if app["node_state"] != "TERMINATING":
        s3_dirty_age_to_write = config.get("s3_dirty_age_to_write", default=20)
        log.debug(f"s3sync - s3_dirty_age_to_write is {s3_dirty_age_to_write}")
    else:
        log.info("s3sync - node is terminating, using s3_dirty_age_to_write of 0")
        s3_dirty_age_to_write = 0

    while True:
        node_state = app["node_state"]
        if node_state == "INITIALIZING":
            log.info("s3sync - nodestate is INITIALIZING, sleeping")
            await asyncio.sleep(s3_sync_interval)
            continue
        elif node_state == "TERMINATING":
            s3_age_time = 0
            log.debug("s3sync - nodestate is TERMINATING, set s3_age_time to 0")
        else:
            log.debug(
                f"s3sync - nodestate is {node_state}, using s3_age_time of: {s3_age_time}"
            )

        update_count = 0
        try:
            update_count = await s3sync(app, s3_age_time=s3_age_time)
            if update_count:
                log.info(f"s3syncCheck {update_count} objects updated")
        except Exception as e:
            # catch any exception so don't prematurely end the s3sync task
            log.warn(f"s3syncCheck - got {type(e)} exception: {e}")

        pending_s3_write_tasks = app["pending_s3_write_tasks"]
        log.debug(f"pending_write_tasks count: {len(pending_s3_write_tasks)}")
        dirty_ids = app["dirty_ids"]
        log.debug(f"dirty_ids: {dirty_ids}")

        if update_count > 0:
            log.debug("s3syncCheck short sleep")
            last_update = time.time()
            # give other tasks a chance to run
            await asyncio.sleep(0)
        else:
            last_update_delta = time.time() - last_update
            if last_update_delta > s3_sync_interval:
                sleep_time = s3_sync_interval
            else:
                sleep_time = last_update_delta
            msg = "s3syncCheck no objects to write, "
            msg += f"sleeping for {sleep_time:.2f}"
            log.info(msg)
            await asyncio.sleep(sleep_time)
