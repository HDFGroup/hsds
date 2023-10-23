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

import asyncio
import numpy as np

from aiohttp.client_exceptions import ClientError

from .util.hdf5dtype import createDataType
from .util.arrayUtil import getNumpyValue
from .util.dsetUtil import getChunkLayout
from .util.chunkUtil import getChunkCoordinate
from .util.idUtil import getDataNodeUrl, isSchema2Id, getS3Key, getObjId
from .util.storUtil import getStorKeys
from .util.httpUtil import http_delete

from . import hsds_logger as log
from . import config
from .chunk_crawl import ChunkCrawler


def getFillValue(dset_json):
    """ Return the fill value of the given dataset as a numpy array.
      If no fill value is defined, return an zero array of given type """

    fill_value = None
    type_json = dset_json["type"]
    dt = createDataType(type_json)

    if "creationProperties" in dset_json:
        cprops = dset_json["creationProperties"]
        if "fillValue" in cprops:
            fill_value_prop = cprops["fillValue"]
            log.debug(f"got fo;;+value_prop: {fill_value_prop}")
            encoding = cprops.get("fillValue_encoding")
            fill_value = getNumpyValue(fill_value_prop, dt=dt, encoding=encoding)
    if fill_value:
        arr = np.empty((1,), dtype=dt, order="C")
        arr[...] = fill_value
    else:
        arr = np.zeros([1,], dtype=dt, order="C")

    return arr


async def removeChunks(app, chunk_ids, bucket=None):
    """ Remove chunks with the given ids """

    # this should only be called from a SN

    log.info(f"removeChunks, {len(chunk_ids)} chunks")
    log.debug(f"removeChunks for: {chunk_ids}")

    dn_urls = app["dn_urls"]
    if not dn_urls:
        log.error("removeChunks request, but no dn_urls")
        raise ValueError()

    log.debug(f"doFlush - dn_urls: {dn_urls}")
    params = {}
    if bucket:
        params["bucket"] = bucket
    failed_count = 0

    try:
        tasks = []
        for chunk_id in chunk_ids:
            dn_url = getDataNodeUrl(app, chunk_id)
            req = dn_url + "/chunks/" + chunk_id
            task = asyncio.ensure_future(http_delete(app, req, params=params))
            tasks.append(task)
        done, pending = await asyncio.wait(tasks)
        if pending:
            # should be empty since we didn't use return_when parameter
            log.error("removeChunks - got pending tasks")
            raise ValueError()
        for task in done:
            if task.exception():
                exception_type = type(task.exception())
                msg = f"removeChunks - task had exception: {exception_type}"
                log.warn(msg)
                failed_count += 1

    except ClientError as ce:
        msg = f"removeChunks - ClientError: {ce}"
        log.error(msg)
        raise ValueError()
    except asyncio.CancelledError as cle:
        log.error(f"removeChunks - CancelledError: {cle}")
        raise ValueError()

    if failed_count:
        msg = f"removeChunks, failed count: {failed_count}"
        log.error(msg)
    else:
        log.info(f"removeChunks complete for {len(chunk_ids)} chunks - no errors")


async def getAllocatedChunkIds(app, dset_id, bucket=None):
    """ Return the set of allocated chunk ids for the give dataset.
        If slices is given, just return chunks that interesect with the slice region """

    log.info(f"getAllocatedChunkIds for {dset_id}")

    if not isSchema2Id(dset_id):
        msg = f"no tabulation for schema v1 id: {dset_id} returning "
        msg += "null results"
        log.warn(msg)
        return {}

    if not bucket:
        bucket = config.get("bucket_name")
    if not bucket:
        raise ValueError(f"no bucket defined for getAllocatedChunkIds for {dset_id}")

    root_key = getS3Key(dset_id)
    log.debug(f"got root_key: {root_key}")

    if not root_key.endswith("/.dataset.json"):
        raise ValueError("unexpected root key")

    root_prefix = root_key[: -(len(".dataset.json"))]

    log.debug(f"scanRoot - using prefix: {root_prefix}")

    kwargs = {
        "prefix": root_prefix,
        "include_stats": False,
        "bucket": bucket,
    }
    s3keys = await getStorKeys(app, **kwargs)

    # getStoreKeys will pick up the dataset.json as well,
    # so go through and discard
    chunk_ids = []
    for s3key in s3keys:
        if s3key.endswith("json"):
            # ignore metadata items
            continue
        try:
            chunk_id = getObjId(s3key)
        except ValueError:
            log.warn(f"ignoring s3key: {s3key}")
            continue
        chunk_ids.append(chunk_id)

    log.debug(f"getAllocattedChunkIds - got {len(chunk_ids)} ids")
    return chunk_ids


async def reduceShape(app, dset_json, shape_update, bucket=None):
    """ Given an existing dataset and a new shape,
        Reinitialize and edge chunks and delete any chunks
        that fall entirely out of the new shape region """

    dset_id = dset_json["id"]
    log.info(f"reduceShape for {dset_id} to {shape_update}")

    # get the current shape dims
    shape_orig = dset_json["shape"]
    if shape_orig["class"] != "H5S_SIMPLE":
        raise ValueError("reduceShape can only be called on simple datasets")
    dims = shape_orig["dims"]
    rank = len(dims)

    # get the fill value
    arr = getFillValue(dset_json)

    # and the chunk layout
    layout = tuple(getChunkLayout(dset_json))
    log.debug(f"got layout: {layout}")

    # get all chunk ids for chunks that have been allocated
    chunk_ids = await getAllocatedChunkIds(app, dset_id, bucket=bucket)
    chunk_ids.sort()

    log.debug(f"got chunkIds: {chunk_ids}")

    # separate ids into three groups:
    #   A: those are entirely inside the new shape region - no action needed
    #   B: those that overlap the new shape - will need the edge portion reinitialized
    #   C: those that are entirely outside the new shape - will need to be deleted

    delete_ids = []  # chunk ids for chunk that that will need to be deleted
    update_ids = []  # chunk ids for chunks that will need to be reinitialized

    for chunk_id in chunk_ids:
        log.debug(f"chunk_id: {chunk_id}")
        chunk_coord = getChunkCoordinate(chunk_id, layout)
        log.debug(f"chunk_coord: {chunk_coord}")
        skip = True
        for i in range(rank):
            if chunk_coord[i] + layout[i] > shape_update[i]:
                skip = False
                break
        if skip:
            log.debug(f"chunk_id {chunk_id} no action needed")
            continue

        reinit = False
        for n in range(rank):
            if chunk_coord[n] < shape_update[n]:
                reinit = True
                break
        if reinit:
            log.debug("chunk reinit")
            update_ids.append(chunk_id)
        else:
            log.debug("chunk delete")
            delete_ids.append(chunk_id)

    msg = f"reduceShape - from {len(chunk_ids)} chunks, {len(update_ids)} will need to be "
    msg += f"updated and {len(delete_ids)} will need to deleted"
    log.info(msg)

    if update_ids:
        log.debug(f"these ids will need to be updated: {update_ids}")

        # For multidimensional datasets, may need multiple hyperslab writes
        # go through each dimension and calculate region to update

        for n in range(rank):
            slices = []

            for m in range(rank):
                if m == n:
                    s = slice(shape_update[m], dims[m], 1)
                else:
                    # just select the entire extent
                    s = slice(0, dims[m], 1)
                slices.append(s)

            crawler = ChunkCrawler(
                app,
                update_ids,
                dset_json=dset_json,
                bucket=bucket,
                slices=slices,
                arr=arr,
                action="write_chunk_hyperslab",
            )
            await crawler.crawl()

            crawler_status = crawler.get_status()

            if crawler_status not in (200, 201):
                msg = f"crawler failed for shape reinitialize with status: {crawler_status}"
                log.warn(msg)
            else:
                msg = f"crawler success for reinitialization with slices: {slices}"
                log.info(msg)
    else:
        log.info("no chunks need updating for shape reduction")

    log.debug("chunk reinitialization complete")

    if delete_ids:
        delete_ids = list(delete_ids)
        delete_ids.sort()
        log.debug(f"these ids will need to be deleted: {delete_ids}")
        await removeChunks(app, delete_ids, bucket=bucket)
    else:
        log.info("no chunks need deletion for shape reduction")
