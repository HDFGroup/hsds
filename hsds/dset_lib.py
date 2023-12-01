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
import math
import numpy as np

from aiohttp.client_exceptions import ClientError
from aiohttp.web_exceptions import HTTPBadRequest, HTTPInternalServerError
from .util.hdf5dtype import createDataType, getItemSize
from .util.arrayUtil import getNumpyValue
from .util.dsetUtil import isNullSpace, getDatasetLayout, getDatasetLayoutClass
from .util.dsetUtil import getChunkLayout, getSelectionShape, getShapeDims, get_slices
from .util.chunkUtil import getChunkCoordinate, getChunkIndex, getChunkSuffix
from .util.chunkUtil import getNumChunks, getChunkIds, getChunkId
from .util.chunkUtil import getChunkCoverage, getDataCoverage
from .util.chunkUtil import getQueryDtype, get_chunktable_dims

from .util.idUtil import getDataNodeUrl, isSchema2Id, getS3Key, getObjId
from .util.storUtil import getStorKeys
from .util.httpUtil import http_delete

from .servicenode_lib import getDsetJson
from .chunk_crawl import ChunkCrawler
from . import config
from . import hsds_logger as log


CHUNK_REF_LAYOUTS = (
    "H5D_CONTIGUOUS_REF",
    "H5D_CHUNKED_REF",
    "H5D_CHUNKED_REF_INDIRECT",
)


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
            log.debug(f"got fill_value_prop: {fill_value_prop}")
            encoding = cprops.get("fillValue_encoding")
            fill_value = getNumpyValue(fill_value_prop, dt=dt, encoding=encoding)
    if fill_value:
        arr = np.empty((1,), dtype=dt, order="C")
        arr[...] = fill_value
    else:
        arr = None

    return arr


async def getChunkLocations(app, dset_id, dset_json, chunkinfo_map, chunk_ids, bucket=None):
    """
    Get info for chunk locations (for reference layouts)
    """
    layout_class = getDatasetLayoutClass(dset_json)

    if layout_class not in CHUNK_REF_LAYOUTS:
        msg = f"skip getChunkLocations for layout class: {layout_class}"
        log.debug(msg)
        return

    chunk_dims = None
    if "layout" in dset_json:
        dset_layout = dset_json["layout"]
        log.debug(f"dset_json layout: {dset_layout}")
        if "dims" in dset_layout:
            chunk_dims = dset_layout["dims"]
    if chunk_dims is None:
        msg = "no chunk dimensions set in dataset layout"
        log.error(msg)
        raise HTTPInternalServerError()

    datashape = dset_json["shape"]
    datatype = dset_json["type"]
    if isNullSpace(dset_json):
        log.error("H5S_NULL shape class used with reference chunk layout")
        raise HTTPInternalServerError()
    dims = getShapeDims(datashape)
    rank = len(dims)
    # chunk_ids = list(chunkinfo_map.keys())
    # chunk_ids.sort()
    num_chunks = len(chunk_ids)
    msg = f"getChunkLocations for dset: {dset_id} bucket: {bucket} "
    msg += f"rank: {rank} num chunk_ids: {num_chunks}"
    log.info(msg)
    log.debug(f"getChunkLocations layout: {layout_class}")

    def getChunkItem(chunkid):
        if chunk_id in chunkinfo_map:
            chunk_item = chunkinfo_map[chunk_id]
        else:
            chunk_item = {}
            chunkinfo_map[chunk_id] = chunk_item
        return chunk_item

    if layout_class == "H5D_CONTIGUOUS_REF":
        layout = getDatasetLayout(dset_json)
        log.debug(f"cpl layout: {layout}")
        s3path = layout["file_uri"]
        s3size = layout["size"]
        if s3size == 0:
            msg = "getChunkLocations - H5D_CONTIGUOUS_REF layout size 0, "
            msg += "no allocation"
            log.info(msg)
            return
        item_size = getItemSize(datatype)
        chunk_size = item_size
        for dim in chunk_dims:
            chunk_size *= dim
        log.debug(f"using chunk_size: {chunk_size} for H5D_CONTIGUOUS_REF")

        for chunk_id in chunk_ids:
            log.debug(f"getChunkLocations - getting data for chunk: {chunk_id}")
            chunk_item = getChunkItem(chunk_id)
            chunk_index = getChunkIndex(chunk_id)
            if len(chunk_index) != rank:
                log.error("Unexpected chunk_index")
                raise HTTPInternalServerError()
            extent = item_size
            if "offset" not in layout:
                msg = "getChunkLocations - expected to find offset in chunk "
                msg += "layout for H5D_CONTIGUOUS_REF"
                log.error(msg)
                continue
            s3offset = layout["offset"]
            if not isinstance(s3offset, int):
                msg = "getChunkLocations - expected offset to be an int but "
                msg += f"got: {s3offset}"
                log.error(msg)
                continue
            log.debug(f"getChunkLocations s3offset: {s3offset}")
            for i in range(rank):
                dim = rank - i - 1
                index = chunk_index[dim]
                s3offset += index * chunk_dims[dim] * extent
                extent *= dims[dim]
            msg = f"setting chunk_info_map to s3offset: {s3offset} "
            msg == f"s3size: {s3size} for chunk_id: {chunk_id}"
            log.debug(msg)
            if s3offset > layout["offset"] + layout["size"]:
                msg = f"range get of s3offset: {s3offset} s3size: {s3size} "
                msg += "extends beyond end of contiguous dataset for "
                msg += f"chunk_id: {chunk_id}"
                log.warn(msg)
            chunk_item["s3path"] = s3path
            chunk_item["s3offset"] = s3offset
            chunk_item["s3size"] = chunk_size
    elif layout_class == "H5D_CHUNKED_REF":
        layout = getDatasetLayout(dset_json)
        log.debug(f"cpl layout: {layout}")
        s3path = layout["file_uri"]
        chunks = layout["chunks"]

        for chunk_id in chunk_ids:
            chunk_item = getChunkItem(chunk_id)
            s3offset = 0
            s3size = 0
            chunk_key = getChunkSuffix(chunk_id)
            if chunk_key in chunks:
                item = chunks[chunk_key]
                s3offset = item[0]
                s3size = item[1]
            chunk_item["s3path"] = s3path
            chunk_item["s3offset"] = s3offset
            chunk_item["s3size"] = s3size

    elif layout_class == "H5D_CHUNKED_REF_INDIRECT":
        layout = getDatasetLayout(dset_json)
        log.debug(f"cpl layout: {layout}")
        if "chunk_table" not in layout:
            log.error("Expected to find chunk_table in dataset layout")
            raise HTTPInternalServerError()
        chunktable_id = layout["chunk_table"]
        # get  state for dataset from DN.
        chunktable_json = await getDsetJson(app, chunktable_id, bucket=bucket)
        # log.debug(f"chunktable_json: {chunktable_json}")
        chunktable_dims = getShapeDims(chunktable_json["shape"])
        chunktable_layout = chunktable_json["layout"]
        if chunktable_layout.get("class") == "H5D_CHUNKED_REF_INDIRECT":
            # We don't support recursive chunked_ref_indirect classes
            msg = "chunktable layout: H5D_CHUNKED_REF_INDIRECT is invalid"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        if len(chunktable_dims) != rank:
            msg = "Rank of chunktable should be same as the dataset"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        # convert the list of chunk_ids into a set of points to query in
        # the chunk table
        log.debug(f"datashape: {dims}")
        log.debug(f"chunk_dims: {chunk_dims}")
        log.debug(f"chunktable_dims: {chunktable_dims}")
        default_chunktable_dims = get_chunktable_dims(dims, chunk_dims)
        log.debug(f"default_chunktable_dims: {default_chunktable_dims}")
        table_factors = []
        if "hyper_dims" in layout:
            hyper_dims = layout["hyper_dims"]
        else:
            # assume 1 to 1 matching
            hyper_dims = chunk_dims
        ref_num_chunks = num_chunks
        for dim in range(rank):
            if chunk_dims[dim] % hyper_dims[dim] != 0:
                msg = f"expected hyper_dims [{hyper_dims[dim]}] to be a factor"
                msg += f" of {chunk_dims[dim]}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            factor = chunk_dims[dim] // hyper_dims[dim]
            table_factors.append(factor)
            ref_num_chunks *= factor
        log.debug(f"table_factors: {table_factors}")
        log.debug(f"ref_num_chunks: {ref_num_chunks}")
        log.debug(f"hyper_dims: {hyper_dims}")

        if rank == 1:
            arr_points = np.zeros((ref_num_chunks,), dtype=np.dtype("u8"))
            table_factor = table_factors[0]
            for i in range(num_chunks):
                chunk_id = chunk_ids[i]
                log.debug(f"chunk_id: {chunk_id}")
                chunk_index = getChunkIndex(chunk_id)
                chunk_index = chunk_index[0]
                log.debug(f"chunk_index: {chunk_index}")
                for j in range(table_factor):
                    index = chunk_index * table_factor + j
                    arr_index = i * table_factor + j
                    arr_points[arr_index] = index
        else:
            if ref_num_chunks != num_chunks:
                msg = "hyperchunks not supported for multidimensional datasets"
                log.warn(msg)
                raise HTTPBadRequest(msg=msg)
            arr_points = np.zeros((num_chunks, rank), dtype=np.dtype("u8"))
            for i in range(num_chunks):
                chunk_id = chunk_ids[i]
                log.debug(f"chunk_id for chunktable: {chunk_id}")
                indx = getChunkIndex(chunk_id)
                log.debug(f"get chunk indx: {indx}")
                arr_points[i] = indx

        msg = f"got chunktable points: {arr_points}, calling getSelectionData"
        log.debug(msg)
        # this call won't lead to a circular loop of calls since we've checked
        # that the chunktable layout is not H5D_CHUNKED_REF_INDIRECT
        kwargs = {"points": arr_points, "bucket": bucket}
        point_data = await getSelectionData(app, chunktable_id, chunktable_json, **kwargs)

        log.debug(f"got chunktable data: {point_data}")
        if "file_uri" in layout:
            s3_layout_path = layout["file_uri"]
            log.debug(f"got s3_layout_path: {s3_layout_path}")
        else:
            s3_layout_path = None

        for i in range(num_chunks):
            chunk_id = chunk_ids[i]
            chunk_item = getChunkItem(chunk_id)
            item = point_data[i]
            if s3_layout_path is None:
                if len(item) < 3:
                    msg = "expected chunk table to have three fields"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
                e = item[2]
                if e:
                    s3path = e.decode("utf-8")
                    log.debug(f"got s3path: {s3path}")
            else:
                s3path = s3_layout_path
            chunk_item["s3path"] = s3path

            if ref_num_chunks == num_chunks:
                item = point_data[i]
                s3offset = int(item[0])
                s3size = int(item[1])
                chunk_item["s3offset"] = s3offset
                chunk_item["s3size"] = s3size
            else:
                factor = ref_num_chunks // num_chunks
                s3offsets = []
                s3sizes = []
                for j in range(factor):
                    item = point_data[i * factor + j]
                    s3offset = int(item[0])
                    s3offsets.append(s3offset)
                    s3size = int(item[1])
                    s3sizes.append(s3size)
                chunk_item["s3offset"] = s3offsets
                chunk_item["s3size"] = s3sizes
                chunk_item["hyper_dims"] = hyper_dims

    else:
        log.error(f"Unexpected chunk layout: {layout['class']}")
        raise HTTPInternalServerError()

    log.debug(f"returning chunkinfo_map: {chunkinfo_map}")
    return chunkinfo_map


def get_chunkmap_selections(chunk_map, chunk_ids, slices, dset_json):
    """Update chunk_map with chunk and data selections for the
    given set of slices
    """
    log.debug(f"get_chunkmap_selections - chunk_ids: {chunk_ids}")
    if not slices:
        log.debug("no slices set, returning")
        return  # nothing to do
    log.debug(f"slices: {slices}")
    layout = getChunkLayout(dset_json)
    for chunk_id in chunk_ids:
        if chunk_id in chunk_map:
            item = chunk_map[chunk_id]
        else:
            item = {}
            chunk_map[chunk_id] = item

        chunk_sel = getChunkCoverage(chunk_id, slices, layout)
        log.debug(
            f"get_chunk_selections - chunk_id: {chunk_id}, chunk_sel: {chunk_sel}"
        )
        item["chunk_sel"] = chunk_sel
        data_sel = getDataCoverage(chunk_id, slices, layout)
        log.debug(f"get_chunk_selections - data_sel: {data_sel}")
        item["data_sel"] = data_sel


def get_chunk_selections(chunk_map, chunk_ids, slices, dset_json):
    """Update chunk_map with chunk and data selections for the
    given set of slices
    """
    log.debug(f"get_chunk_selections - chunk_ids: {chunk_ids}")
    if not slices:
        log.debug("no slices set, returning")
        return  # nothing to do
    log.debug(f"slices: {slices}")
    layout = getChunkLayout(dset_json)
    for chunk_id in chunk_ids:
        if chunk_id in chunk_map:
            item = chunk_map[chunk_id]
        else:
            item = {}
            chunk_map[chunk_id] = item

        chunk_sel = getChunkCoverage(chunk_id, slices, layout)
        log.debug(
            f"get_chunk_selections - chunk_id: {chunk_id}, chunk_sel: {chunk_sel}"
        )
        item["chunk_sel"] = chunk_sel
        data_sel = getDataCoverage(chunk_id, slices, layout)
        log.debug(f"get_chunk_selections - data_sel: {data_sel}")
        item["data_sel"] = data_sel


async def getSelectionData(
    app,
    dset_id,
    dset_json,
    slices=None,
    select_dtype=None,
    points=None,
    query=None,
    query_update=None,
    bucket=None,
    limit=0,
    method="GET",
):
    """Read selected slices and return numpy array"""
    log.debug("getSelectionData")
    if slices is None and points is None:
        log.error("getSelectionData - expected either slices or points to be set")
        raise HTTPInternalServerError()

    layout = getChunkLayout(dset_json)

    chunkinfo = {}

    if slices is not None:
        num_chunks = getNumChunks(slices, layout)
        log.debug(f"num_chunks: {num_chunks}")

        max_chunks = int(config.get("max_chunks_per_request", default=1000))
        if num_chunks > max_chunks:
            msg = f"num_chunks over {max_chunks} limit, but will attempt to fetch with crawler"
            log.warn(msg)

        chunk_ids = getChunkIds(dset_id, slices, layout)
    else:
        # points - already checked it is not None
        num_points = len(points)
        chunk_ids = []
        for pt_indx in range(num_points):
            point = points[pt_indx]
            chunk_id = getChunkId(dset_id, point, layout)
            if chunk_id in chunkinfo:
                chunk_entry = chunkinfo[chunk_id]
            else:
                chunk_entry = {}
                chunkinfo[chunk_id] = chunk_entry
                chunk_ids.append(chunk_id)
            if "points" in chunk_entry:
                point_list = chunk_entry["points"]
            else:
                point_list = []
                chunk_entry["points"] = point_list
            if "indices" in chunk_entry:
                point_index = chunk_entry["indices"]
            else:
                point_index = []
                chunk_entry["indices"] = point_index

            point_list.append(point)
            point_index.append(pt_indx)

    # Get information about where chunks are located
    #   Will be None except for H5D_CHUNKED_REF_INDIRECT type
    await getChunkLocations(app, dset_id, dset_json, chunkinfo, chunk_ids, bucket=bucket)

    if slices is None:
        slices = get_slices(None, dset_json)

    if points is None:
        # get chunk selections for hyperslab select
        get_chunk_selections(chunkinfo, chunk_ids, slices, dset_json)

    log.debug(f"chunkinfo_map: {chunkinfo}")

    if method == "OPTIONS":
        # skip doing any big data load for options request
        return None

    arr = await doReadSelection(
        app,
        chunk_ids,
        dset_json,
        slices=slices,
        select_dtype=select_dtype,
        points=points,
        query=query,
        query_update=query_update,
        limit=limit,
        chunk_map=chunkinfo,
        bucket=bucket,
    )

    return arr


async def doReadSelection(
    app,
    chunk_ids,
    dset_json,
    slices=None,
    select_dtype=None,
    points=None,
    query=None,
    query_update=None,
    chunk_map=None,
    bucket=None,
    limit=0,
):
    """read selection utility function"""
    log.info(f"doReadSelection - number of chunk_ids: {len(chunk_ids)}")
    log.debug(f"doReadSelection - chunk_ids: {chunk_ids}")
    log.debug(f"doReadSelection - select_dtype: {select_dtype}")

    type_json = dset_json["type"]
    item_size = getItemSize(type_json)
    log.debug(f"item size: {item_size}")
    dset_dtype = createDataType(type_json)  # np datatype
    if select_dtype is None:
        select_dtype = dset_dtype
    if query is None:
        query_dtype = None
    else:
        log.debug(f"query: {query} limit: {limit}")
        query_dtype = getQueryDtype(dset_dtype)

    # create array to hold response data
    arr = None

    if points is not None:
        # point selection
        np_shape = [len(points), ]
    elif query is not None:
        # return shape will be determined by number of matches
        np_shape = None
    elif slices is not None:
        log.debug(f"get np_shape for slices: {slices}")
        np_shape = getSelectionShape(slices)
    else:
        log.error("doReadSelection - expected points or slices to be set")
        raise HTTPInternalServerError()
    log.debug(f"selection shape: {np_shape}")

    if np_shape is not None:
        # check that the array size is reasonable
        request_size = math.prod(np_shape)
        if item_size == "H5T_VARIABLE":
            request_size *= 512  # random guess of avg item_size
        else:
            request_size *= item_size
            log.debug(f"request_size: {request_size}")
        max_request_size = int(config.get("max_request_size"))
        if request_size >= max_request_size:
            msg = f"Attempting to fetch {request_size} bytes (greater than "
            msg += f"{max_request_size} limit"
            log.error(msg)
            raise HTTPBadRequest(reason=msg)

        # initialize to fill_value if specified
        fill_value = getFillValue(dset_json)

        if fill_value is not None:
            arr = np.empty(np_shape, dtype=select_dtype, order="C")
            arr[...] = fill_value
        else:
            arr = np.zeros(np_shape, dtype=select_dtype, order="C")

    crawler = ChunkCrawler(
        app,
        chunk_ids,
        dset_json=dset_json,
        chunk_map=chunk_map,
        bucket=bucket,
        slices=slices,
        query=query,
        query_update=query_update,
        limit=limit,
        arr=arr,
        action="read_chunk_hyperslab",
    )
    await crawler.crawl()

    crawler_status = crawler.get_status()

    log.info(f"doReadSelection complete - status:  {crawler_status}")
    if crawler_status == 400:
        log.info(f"doReadSelection raising BadRequest error:  {crawler_status}")
        raise HTTPBadRequest()
    if crawler_status not in (200, 201):
        log.info(
            f"doReadSelection raising HTTPInternalServerError for status:  {crawler_status}"
        )
        raise HTTPInternalServerError()

    if query is not None:
        # combine chunk responses and return
        if limit > 0 and crawler._hits > limit:
            nrows = limit
        else:
            nrows = crawler._hits
        arr = np.empty((nrows,), dtype=query_dtype)
        start = 0
        for chunkid in chunk_ids:
            if chunkid not in chunk_map:
                continue
            chunk_item = chunk_map[chunkid]
            if "query_rsp" not in chunk_item:
                continue
            query_rsp = chunk_item["query_rsp"]
            if len(query_rsp) == 0:
                continue
            stop = start + len(query_rsp)
            if stop > nrows:
                rsp_stop = len(query_rsp) - (stop - nrows)
                arr[start:] = query_rsp[0:rsp_stop]
            else:
                arr[start:stop] = query_rsp[:]
            start = stop
            if start >= nrows:
                log.debug(f"got {nrows} rows for query, quitting")
                break
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

    params = {}
    if bucket:
        params["bucket"] = bucket
    failed_count = 0

    try:
        tasks = []
        # TBD - this may be problematic if the number of chunks to
        # be deleted is very large - may need to implement some sort of crawler
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
        Reinitialize any edge chunks and delete any chunks
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

    type_json = dset_json["type"]
    dt = createDataType(type_json)

    if arr is None:
        arr = np.zeros([1], dtype=dt, order="C")

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

        if np.all(np.add(chunk_coord, layout) <= shape_update):
            log.debug(f"chunk_id {chunk_id} no action needed")
            continue

        if np.any(chunk_coord < shape_update):
            log.debug(f"{chunk_id} reinit")
            update_ids.append(chunk_id)
        else:
            log.debug(f"{chunk_id} delete")
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
            update_element_count = 1
            for m in range(rank):
                if m == n:
                    s = slice(shape_update[m], dims[m], 1)
                    update_element_count *= dims[m] - shape_update[m]
                else:
                    # just select the entire extent
                    s = slice(0, dims[m], 1)
                    update_element_count *= dims[m]
                slices.append(s)

            if update_element_count == 0:
                log.debug(f"empty hyperslab update for dim {n}")
                continue

            log.debug(f"update {update_element_count} elements for dim {n}")

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
