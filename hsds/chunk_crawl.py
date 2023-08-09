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
# ChunkCrawler class for async processing of per chunk actions
#
#

import asyncio
import time
import random
from asyncio import CancelledError
import numpy as np
from aiohttp.web_exceptions import HTTPBadRequest, HTTPNotFound, HTTPServiceUnavailable
from aiohttp.web_exceptions import HTTPInternalServerError
from aiohttp.client_exceptions import ClientError

from .util.httpUtil import http_get, http_put, http_post, get_http_client
from .util.httpUtil import isUnixDomainUrl
from .util.idUtil import getDataNodeUrl, getNodeCount
from .util.hdf5dtype import createDataType
from .util.dsetUtil import getSliceQueryParam
from .util.dsetUtil import getSelectionShape, getChunkLayout
from .util.chunkUtil import getChunkCoverage, getDataCoverage
from .util.chunkUtil import getChunkIdForPartition, getQueryDtype
from .util.arrayUtil import jsonToArray, getShapeDims, getNumpyValue
from .util.arrayUtil import getNumElements, arrayToBytes, bytesToArray
from . import config
from . import hsds_logger as log

CHUNK_REF_LAYOUTS = (
    "H5D_CONTIGUOUS_REF",
    "H5D_CHUNKED_REF",
    "H5D_CHUNKED_REF_INDIRECT",
)


async def write_chunk_hyperslab(
    app, chunk_id, dset_json, slices, arr, bucket=None, client=None
):
    """write the chunk selection to the DN
    chunk_id: id of chunk to write to
    chunk_sel: chunk-relative selection to write to
    np_arr: numpy array of data to be written
    """

    if not bucket:
        bucket = config.get("bucket_name")

    msg = f"write_chunk_hyperslab, chunk_id:{chunk_id}, slices:{slices}, "
    msg += f"bucket: {bucket}"
    log.info(msg)
    if "layout" not in dset_json:
        log.error(f"No layout found in dset_json: {dset_json}")
        raise HTTPInternalServerError()
    partition_chunk_id = getChunkIdForPartition(chunk_id, dset_json)
    if partition_chunk_id != chunk_id:
        log.debug(f"using partition_chunk_id: {partition_chunk_id}")
        chunk_id = partition_chunk_id  # replace the chunk_id

    if "type" not in dset_json:
        log.error(f"No type found in dset_json: {dset_json}")
        raise HTTPInternalServerError()

    layout = getChunkLayout(dset_json)
    chunk_sel = getChunkCoverage(chunk_id, slices, layout)
    log.debug(f"chunk_sel: {chunk_sel}")
    data_sel = getDataCoverage(chunk_id, slices, layout)
    log.debug(f"data_sel: {data_sel}")
    log.debug(f"arr.shape: {arr.shape}")
    arr_chunk = arr[data_sel]
    req = getDataNodeUrl(app, chunk_id)
    req += "/chunks/" + chunk_id

    log.debug(f"PUT chunk req: {req}")
    data = arrayToBytes(arr_chunk)
    # pass itemsize, type, dimensions, and selection as query params
    params = {}
    select = getSliceQueryParam(chunk_sel)
    params["select"] = select
    if bucket:
        params["bucket"] = bucket

    json_rsp = await http_put(app, req, data=data, params=params, client=client)
    msg = f"got rsp: {json_rsp} for put binary request: {req}, "
    msg += f"{len(data)} bytes"
    log.debug(msg)


async def read_chunk_hyperslab(
    app,
    chunk_id,
    dset_json,
    np_arr,
    query=None,
    query_update=None,
    limit=0,
    chunk_map=None,
    bucket=None,
    client=None,
):
    """read the chunk selection from the DN
    chunk_id: id of chunk to write to
    chunk_sel: chunk-relative selection to read from
    np_arr: numpy array to store read bytes
    chunk_map: map of chunk_id to chunk_offset and chunk_size
        chunk_offset: location of chunk with the s3 object
        chunk_size: size of chunk within the s3 object (or 0 if the
           entire object)
    bucket: s3 bucket to read from
    """
    if not bucket:
        bucket = config.get("bucket_name")

    if chunk_map is None:
        log.error("expected chunk_map to be set")
        return

    msg = f"read_chunk_hyperslab, chunk_id: {chunk_id},"
    """
    msg += " slices: ["
    for s in slices:
        if isinstance(s, slice):
            msg += f"{s},"
        else:
            if len(s) > 5:
                # avoid large output lines
                msg += f"[{s[0]}, {s[1]}, ..., {s[-2]}, {s[-1]}],"
            else:
                msg += f"{s},"
    """
    msg += f" bucket: {bucket}"
    if query is not None:
        msg += f" query: {query} limit: {limit}"
    log.info(msg)
    if chunk_id not in chunk_map:
        log.warn(f"expected to find {chunk_id} in chunk_map")
        return
    chunk_info = chunk_map[chunk_id]
    log.debug(f"using chunk_map entry for {chunk_id}: {chunk_info}")

    partition_chunk_id = getChunkIdForPartition(chunk_id, dset_json)
    if partition_chunk_id != chunk_id:
        log.debug(f"using partition_chunk_id: {partition_chunk_id}")
        chunk_id = partition_chunk_id  # replace the chunk_id

    if "type" not in dset_json:
        log.error(f"No type found in dset_json: {dset_json}")
        raise HTTPInternalServerError()

    chunk_shape = None  # expected return array shape
    chunk_sel = None  # for hyperslab
    data_sel = None  # for hyperslab
    point_list = None  # for point sel
    point_index = None  # for point sel
    select = None  # select query string
    method = "GET"  # default http method
    # for hyperslab selections, chunk_sel and data_sel keys are used
    if "chunk_sel" in chunk_info:
        chunk_sel = chunk_info["chunk_sel"]
        log.debug(f"read_chunk_hyperslab - chunk_sel: {chunk_sel}")
        select = getSliceQueryParam(chunk_sel)

    if "data_sel" in chunk_info:
        data_sel = chunk_info["data_sel"]
        log.debug(f"read_chunk_hyperslab - data_sel: {data_sel}")
        chunk_shape = getSelectionShape(chunk_sel)
        log.debug(f"hyperslab selection - chunk_shape: {chunk_shape}")

    if "points" in chunk_info:
        point_list = chunk_info["points"]
        if "indices" not in chunk_info:
            log.error(f"expected to find 'indices' in item: {chunk_info}")
            raise HTTPInternalServerError()
        point_index = chunk_info["indices"]
        method = "POST"
        chunk_shape = [
            len(point_list),
        ]
        log.debug(f"point selection - chunk_shape: {chunk_shape}")

    type_json = dset_json["type"]
    dt = createDataType(type_json)
    if query is None and query_update is None:
        query_dtype = None
    else:
        query_dtype = getQueryDtype(dt)

    chunk_arr = None
    array_data = None

    # pass dset json and selection as query params
    params = {}
    # params["select"] = select
    if "s3path" in chunk_info:
        params["s3path"] = chunk_info["s3path"]

    if "s3offset" in chunk_info:
        s3offset = chunk_info["s3offset"]
        if isinstance(s3offset, list):
            # convert to a colon seperated string
            s3offset = ":".join(map(str, s3offset))
        params["s3offset"] = s3offset

    if "s3size" in chunk_info:
        s3size = chunk_info["s3size"]
        if isinstance(s3size, list):
            # convert to a colon seperated string
            s3size = ":".join(map(str, s3size))
        params["s3size"] = s3size

    if "hyper_dims" in chunk_info:
        hyper_dims = chunk_info["hyper_dims"]
        if isinstance(hyper_dims, list):
            # convert to colon seperated string
            hyper_dims = ":".join(map(str, hyper_dims))
        params["hyper_dims"] = hyper_dims

    # set query-based params
    if query is not None:
        params["query"] = query
        if limit > 0:
            params["Limit"] = limit

    # bucket will be used to get dset json even when s3path is used for
    # the chunk data
    params["bucket"] = bucket

    if point_list is not None:
        # set query params for point selection
        log.debug(f"read_chunk_hyperslab - point selection {len(point_list)} points")
        params["action"] = "get"
        params["count"] = len(point_list)
        method = "POST"
    elif query_update is not None:
        method = "PUT"

    req = getDataNodeUrl(app, chunk_id)
    req += "/chunks/" + chunk_id

    if select is not None:
        # use post if the select param is long
        max_select_len = config.get("http_max_url_length", default=512)
        max_select_len //= 2  # use up to half the alloted url length for select
        if len(select) > max_select_len:
            method = "POST"

    body = None
    if method == "POST":
        if point_list is not None:
            num_points = len(point_list)
            log.debug(f"read_point_sel: {num_points}")
            point_dt = np.dtype("u8")  # use unsigned long for point index
            np_arr_points = np.asarray(point_list, dtype=point_dt)
            body = np_arr_points.tobytes()
        elif select is not None:
            body = {"select": select}
        else:
            log.error("read_chunk_hyperslab - expected hyperslab or point selection")
            raise HTTPInternalServerError()
    elif method == "PUT":
        # query update
        body = query_update
    else:
        if select is not None:
            params["select"] = select

    # send request
    try:
        log.debug(f"read_chunk_hyperslab - {method} chunk req: {req}")
        log.debug(f"params: {params}")
        if method == "GET":
            array_data = await http_get(app, req, params=params, client=client)
            log.debug(f"http_get {req}, returned {len(array_data)} bytes")
        elif method == "PUT":
            array_data = await http_put(
                app, req, data=body, params=params, client=client
            )
            log.debug(f"http_put {req}, returned {len(array_data)} bytes")
        else:  # POST
            array_data = await http_post(
                app, req, data=body, params=params, client=client
            )
            log.debug(f"http_post {req}, returned {len(array_data)} bytes")
    except HTTPNotFound:
        if query is None and "s3path" in params:
            s3path = params["s3path"]
            # external HDF5 file, should exist
            log.warn(f"chunk {chunk_id} with s3path: {s3path} not found")

    # process response
    if array_data is None:
        log.debug(f"read_chunk_hyperslab - No data returned for chunk: {chunk_id}")
    elif not isinstance(array_data, bytes):
        log.warn(f"read_chunk_hyperslab - expected bytes but got: {array_data}")
        raise HTTPInternalServerError()
    else:
        log.debug(f"got data for chunk: {chunk_id}")
        log.debug(f"data: {len(array_data)} bytes")
        if query is not None or query_update is not None:
            # TBD: this needs to be fixed up for variable length dtypes
            nrows = len(array_data) // query_dtype.itemsize
            try:
                chunk_arr = bytesToArray(
                    array_data,
                    query_dtype,
                    [
                        nrows,
                    ],
                )
            except ValueError as ve:
                log.warn(f"bytesToArray ValueError: {ve}")
                raise HTTPBadRequest()
            # save result to chunk_info
            # chunk results will be merged later
            chunk_info["query_rsp"] = chunk_arr
        else:
            # convert binary data to numpy array
            try:
                chunk_arr = bytesToArray(array_data, dt, chunk_shape)
            except ValueError as ve:
                log.warn(f"bytesToArray ValueError: {ve}")
                raise HTTPBadRequest()
            nelements_read = getNumElements(chunk_arr.shape)
            nelements_expected = getNumElements(chunk_shape)
            if nelements_read != nelements_expected:
                msg = f"Expected {nelements_expected} points, "
                msg += f"but got: {nelements_read}"
                log.error(msg)
                raise HTTPInternalServerError()
            chunk_arr = chunk_arr.reshape(chunk_shape)

            log.info(f"chunk_arr shape: {chunk_arr.shape}")
            log.info(f"data_sel: {data_sel}")
            log.info(f"np_arr shape: {np_arr.shape}")

            if point_list is not None:
                # point selection
                # Fill in the return array based on passed in index values
                np_arr[point_index] = chunk_arr
            else:
                # hyperslab selection
                np_arr[data_sel] = chunk_arr
    log.debug(f"read_chunk_hyperslab {chunk_id} - done")


async def read_point_sel(
    app,
    chunk_id,
    dset_json,
    point_list,
    point_index,
    np_arr,
    chunk_map=None,
    bucket=None,
    client=None,
):
    """
    Read point selection
    --
    app: application object
    chunk_id: id of chunk to read from
    dset_json: dset JSON
    point_list: array of points to read
    point_index: index of arr element to update for a given point
    arr: numpy array to store read bytes
    """

    if not bucket:
        bucket = config.get("bucket_name")

    msg = f"read_point_sel, chunk_id: {chunk_id}, bucket: {bucket}"
    log.info(msg)

    partition_chunk_id = getChunkIdForPartition(chunk_id, dset_json)
    if partition_chunk_id != chunk_id:
        log.debug(f"using partition_chunk_id: {partition_chunk_id}")
        chunk_id = partition_chunk_id  # replace the chunk_id

    point_dt = np.dtype("u8")  # use unsigned long for point index

    if "type" not in dset_json:
        log.error(f"No type found in dset_json: {dset_json}")
        raise HTTPInternalServerError()

    num_points = len(point_list)
    log.debug(f"read_point_sel: {num_points}")
    np_arr_points = np.asarray(point_list, dtype=point_dt)
    post_data = np_arr_points.tobytes()

    # set action as query params
    params = {}
    params["action"] = "get"
    params["count"] = num_points

    np_arr_rsp = None
    dt = np_arr.dtype

    fill_value = None
    # initialize to fill_value if specified
    if "creationProperties" in dset_json:
        cprops = dset_json["creationProperties"]
        if "fillValue" in cprops:
            fill_value_prop = cprops["fillValue"]
            encoding = cprops.get("fillValue_encoding")
            fill_value = getNumpyValue(fill_value_prop, dt=dt, encoding=encoding)

    def defaultArray():
        # no data, return zero array
        if fill_value:
            arr = np.empty((num_points,), dtype=dt)
            arr[...] = fill_value
        else:
            arr = np.zeros((num_points,), dtype=dt)
        return arr

    np_arr_rsp = None
    if chunk_map:
        if chunk_id not in chunk_map:
            msg = f"{chunk_id} not found in chunk_map, returning default arr"
            log.debug(msg)
            np_arr_rsp = defaultArray()
        else:
            chunk_info = chunk_map[chunk_id]
            params["s3path"] = chunk_info["s3path"]
            params["s3offset"] = chunk_info["s3offset"]
            params["s3size"] = chunk_info["s3size"]

    # bucket will be used to get dset json even when s3path is used for
    # the chunk data
    params["bucket"] = bucket

    if np_arr_rsp is None:
        # make request to DN node
        req = getDataNodeUrl(app, chunk_id)
        req += "/chunks/" + chunk_id
        log.debug(f"GET chunk req: {req}")
        try:
            kwargs = {"params": params, "data": post_data, "client": client}
            rsp_data = await http_post(app, req, **kwargs)
            msg = f"got rsp for http_post({req}): {len(rsp_data)} bytes"
            log.debug(msg)
            np_arr_rsp = bytesToArray(rsp_data, dt, (num_points,))
        except HTTPNotFound:
            if "s3path" in params:
                s3path = params["s3path"]
                # external HDF5 file, should exist
                log.warn(f"s3path: {s3path} for S3 range get found")
                raise
            # no data, return zero array
            np_arr_rsp = defaultArray()

    npoints_read = len(np_arr_rsp)
    log.info(f"got {npoints_read} points response")

    if npoints_read != num_points:
        msg = f"Expected {num_points} points, but got: {npoints_read}"
        log.error(msg)
        raise HTTPInternalServerError()

    # Fill in the return array based on passed in index values
    for i in range(num_points):
        index = point_index[i]
        np_arr[index] = np_arr_rsp[i]


async def write_point_sel(
    app, chunk_id, dset_json, point_list, point_data, bucket=None, client=None
):
    """
    Write point selection
    --
      app: application object
      chunk_id: id of chunk to write to
      dset_json: dset JSON
      point_list: array of points to write
      point_data: index of arr element to update for a given point
    """

    if not bucket:
        bucket = config.get("bucket_name")

    msg = f"write_point_sel, chunk_id: {chunk_id}, points: {point_list}, "
    msg += f"data: {point_data}"
    log.info(msg)
    if "type" not in dset_json:
        log.error(f"No type found in dset_json: {dset_json}")
        raise HTTPInternalServerError()

    datashape = dset_json["shape"]
    dims = getShapeDims(datashape)
    rank = len(dims)
    type_json = dset_json["type"]
    dset_dtype = createDataType(type_json)  # np datatype

    partition_chunk_id = getChunkIdForPartition(chunk_id, dset_json)
    if partition_chunk_id != chunk_id:
        log.debug(f"using partition_chunk_id: {partition_chunk_id}")
        chunk_id = partition_chunk_id  # replace the chunk_id

    req = getDataNodeUrl(app, chunk_id)
    req += "/chunks/" + chunk_id
    log.debug("POST chunk req: " + req)

    num_points = len(point_list)
    log.debug(f"write_point_sel - {num_points}")

    # create a numpy array with point_data
    data_arr = jsonToArray((num_points,), dset_dtype, point_data)

    # create a numpy array with the following type:
    #   (coord1, coord2, ...) | dset_dtype
    if rank == 1:
        coord_type_str = "uint64"
    else:
        coord_type_str = f"({rank},)uint64"
    type_fields = [("coord", np.dtype(coord_type_str)), ("value", dset_dtype)]
    comp_type = np.dtype(type_fields)
    np_arr = np.zeros((num_points,), dtype=comp_type)

    # Zip together coordinate and point_data to one numpy array
    for i in range(num_points):
        if rank == 1:
            elem = (point_list[i], data_arr[i])
        else:
            elem = (tuple(point_list[i]), data_arr[i])
        np_arr[i] = elem

    # TBD - support VLEN data
    post_data = np_arr.tobytes()

    # pass dset_json as query params
    params = {}
    params["action"] = "put"
    params["count"] = num_points
    params["bucket"] = bucket

    json_rsp = await http_post(app, req, params=params, data=post_data, client=client)
    log.debug(f"post to {req} returned {json_rsp}")


class ChunkCrawler:
    """ChunkCrawler class is instanted by chunk_sn request handlers to dispatch per-chunk
    requests to DN nodes.  Asyncio.Task is used to setup workers to parallelize DN requests."""

    def __init__(
        self,
        app,
        chunk_ids,
        dset_json=None,
        chunk_map=None,
        bucket=None,
        slices=None,
        arr=None,
        query=None,
        query_update=None,
        limit=0,
        points=None,
        action=None,
    ):

        max_tasks_per_node = config.get("max_tasks_per_node_per_request", default=16)
        client_pool_count = config.get("client_pool_count", default=10)
        log.info(f"ChunkCrawler.__init__  {len(chunk_ids)} chunks, action={action}")
        log.debug(f"ChunkCrawler - chunk_ids: {chunk_ids}")

        self._app = app
        self._slices = slices
        self._chunk_ids = chunk_ids
        self._chunk_map = chunk_map
        self._dset_json = dset_json
        self._arr = arr
        self._points = points
        self._query = query
        self._query_update = query_update
        self._hits = 0
        self._limit = limit
        self._status_map = {}  # map of chunk_ids to status code
        self._q = asyncio.Queue()
        self._fail_count = 0
        self._action = action

        for chunk_id in chunk_ids:
            self._q.put_nowait(chunk_id)

        self._bucket = bucket
        max_tasks = max_tasks_per_node * getNodeCount(app)
        if len(chunk_ids) > max_tasks:
            self._max_tasks = max_tasks
        else:
            self._max_tasks = len(chunk_ids)

        if self._max_tasks >= client_pool_count:
            self._client_pool = 1
        else:
            self._client_pool = client_pool_count - self._max_tasks
        log.info(f"ChunkCrawler - client_pool count: {self._client_pool}")

        # create one ClientSession per dn_url
        if "cc_clients" not in app:
            app["cc_clients"] = {}
        self._clients = app["cc_clients"]

    def get_status(self):
        if len(self._status_map) != len(self._chunk_ids):
            msg = "get_status code while crawler not complete"
            log.error(msg)
            raise ValueError(msg)
        for chunk_id in self._chunk_ids:
            if chunk_id not in self._status_map:
                msg = f"expected to find chunk_id {chunk_id} in ChunkCrawler status_map"
                log.error(msg)
                raise KeyError(msg)
            chunk_status = self._status_map[chunk_id]
            if chunk_status not in (200, 201):
                log.info(
                    f"returning chunk_status: {chunk_status} for chunk: {chunk_id}"
                )
                return chunk_status

        return 200  # all good

    async def crawl(self):
        workers = [
            asyncio.Task(self.work(), name=f"cc_task_{i}")
            for i in range(self._max_tasks)
        ]
        # When all work is done, exit.
        msg = f"ChunkCrawler max_tasks {self._max_tasks} = await queue.join "
        msg += f"- count: {len(self._chunk_ids)}"
        log.info(msg)
        await self._q.join()
        msg = f"ChunkCrawler - join complete - count: {len(self._chunk_ids)}"
        log.info(msg)

        for w in workers:
            w.cancel()
        log.debug("ChunkCrawler - workers canceled")

    async def work(self):
        """Process chunk ids from queue till we are done"""
        this_task = asyncio.current_task()
        task_name = this_task.get_name()
        log.info(f"ChunkCrawler - work method for task: {task_name}")
        client_name = f"{task_name}.{random.randrange(0,self._client_pool)}"
        log.info(f"ChunkCrawler - client_name: {client_name}")
        while True:
            try:
                start = time.time()
                chunk_id = await self._q.get()
                if self._limit > 0 and self._hits >= self._limit:
                    msg = f"ChunkCrawler - maxhits exceeded, skipping fetch for chunk: {chunk_id}"
                    log.debug(msg)
                else:
                    dn_url = getDataNodeUrl(self._app, chunk_id)
                    if isUnixDomainUrl(dn_url):
                        # need a client per url for unix sockets
                        client = get_http_client(self._app, url=dn_url, cache_client=True)
                    else:
                        # create a pool of clients and store the handles in the app dict
                        if client_name not in self._clients:
                            client = get_http_client(
                                self._app, url=dn_url, cache_client=False
                            )
                            msg = "ChunkCrawler - creating new SessionClient for "
                            msg += f"task: {client_name}"
                            log.info(msg)
                            self._clients[client_name] = client
                        else:
                            client = self._clients[client_name]
                    await self.do_work(chunk_id, client=client)

                self._q.task_done()
                elapsed = time.time() - start
                msg = f"ChunkCrawler - task {chunk_id} start: {start:.3f} "
                msg += f"elapsed: {elapsed:.3f}"
                log.debug(msg)
            except asyncio.CancelledError:
                log.debug("ChunkCrawler - worker has been cancelled")
                # raise the exception so worker is truly cancelled
                raise

    async def do_work(self, chunk_id, client=None):
        """fetch the indicated chunk and update status map"""
        msg = f"ChunkCrawler - do_work for chunk: {chunk_id} bucket: "
        msg += f"{self._bucket}"
        log.debug(msg)
        max_retries = config.get("dn_max_retries", default=3)
        retry_exp = config.get("dn_retry_backoff_exp", 0.1)
        log.debug(f"ChunkCrawler - retry_exp: {retry_exp:.3f}")
        retry = 0
        status_code = None
        while retry < max_retries:
            try:
                if self._action == "read_chunk_hyperslab":
                    await read_chunk_hyperslab(
                        self._app,
                        chunk_id,
                        self._dset_json,
                        self._arr,
                        query=self._query,
                        query_update=self._query_update,
                        limit=self._limit,
                        chunk_map=self._chunk_map,
                        bucket=self._bucket,
                        client=client,
                    )
                    log.debug(
                        f"read_chunk_hyperslab - got 200 status for chunk_id: {chunk_id}"
                    )
                    status_code = 200
                elif self._action == "write_chunk_hyperslab":
                    await write_chunk_hyperslab(
                        self._app,
                        chunk_id,
                        self._dset_json,
                        self._slices,
                        self._arr,
                        bucket=self._bucket,
                        client=client,
                    )
                    log.debug(
                        f"write_chunk_hyperslab - got 200 status for chunk_id: {chunk_id}"
                    )
                    status_code = 200
                elif self._action == "read_point_sel":
                    if not isinstance(self._points, dict):
                        log.error("ChunkCrawler - expected dict for points")
                        status_code = 500
                        break
                    if chunk_id not in self._points:
                        log.error(
                            f"ChunkCrawler - read_point_sel, no entry for chunk: {chunk_id}"
                        )
                        status_code = 500
                        break
                    item = self._points[chunk_id]
                    point_list = item["indices"]
                    point_data = item["points"]

                    await read_point_sel(
                        self._app,
                        chunk_id,
                        self._dset_json,
                        point_list,
                        point_data,
                        self._arr,
                        chunk_map=self._chunk_map,
                        bucket=self._bucket,
                        client=client,
                    )
                    log.debug(
                        f"read_point_sel - got 200 status for chunk_id: {chunk_id}"
                    )
                    status_code = 200
                elif self._action == "write_point_sel":
                    if not isinstance(self._points, dict):
                        log.error("ChunkCrawler - expected dict for points")
                        status_code = 500
                        break
                    if chunk_id not in self._points:
                        log.error(
                            f"ChunkCrawler - read_point_sel, no entry for chunk: {chunk_id}"
                        )
                        status_code = 500
                        break
                    item = self._points[chunk_id]
                    log.debug(f"item[{chunk_id}]: {item}")
                    point_list = item["indices"]
                    point_data = item["points"]

                    await write_point_sel(
                        self._app,
                        chunk_id,
                        self._dset_json,
                        point_list,
                        point_data,
                        bucket=self._bucket,
                        client=client,
                    )
                    log.debug(
                        f"read_point_sel - got 200 status for chunk_id: {chunk_id}"
                    )
                    status_code = 200
                else:
                    log.error(f"ChunkCrawler - unexpected action: {self._action}")
                    status_code = 500
                    break

            except ClientError as ce:
                status_code = 500
                log.warn(
                    f"ClientError {type(ce)} for {self._action}({chunk_id}): {ce} "
                )
            except CancelledError as cle:
                status_code = 503
                log.warn(f"CancelledError for {self._action}({chunk_id}): {cle}")
            except HTTPBadRequest as hbr:
                status_code = 400
                log.error(f"HTTPBadRequest for {self._action}({chunk_id}): {hbr}")
            except HTTPNotFound as nfe:
                status_code = 404
                log.info(f"HTTPNotFoundRequest for {self._action}({chunk_id}): {nfe}")
                break
            except HTTPInternalServerError as ise:
                status_code = 500
                log.warn(
                    f"HTTPInternalServerError for {self._action}({chunk_id}): {ise}"
                )
            except HTTPServiceUnavailable as sue:
                status_code = 503
                log.warn(
                    f"HTTPServiceUnavailable for {self._action}({chunk_id}): {sue}"
                )
            except Exception as e:
                status_code = 500
                log.error(
                    f"Unexpected exception {type(e)} for {self._action}({chunk_id}): {e} "
                )
            retry += 1
            if status_code == 200:
                break
            if retry == max_retries:
                log.error(
                    f"ChunkCrawler action: {self._action} failed after: {retry} retries"
                )
            else:
                sleep_time = retry_exp * 2 ** retry + random.uniform(0, 0.1)
                log.warn(
                    f"ChunkCrawler.doWork - retry: {retry}, sleeping for {sleep_time:.2f}"
                )
                await asyncio.sleep(sleep_time)

        # save status_code
        self._status_map[chunk_id] = status_code
        if self._query is not None and status_code == 200:
            item = self._chunk_map[chunk_id]
            if "query_rsp" in item:
                query_rsp = item["query_rsp"]
                self._hits += len(query_rsp)
        log.info(
            f"ChunkCrawler - worker status for chunk {chunk_id}: {self._status_map[chunk_id]}"
        )
