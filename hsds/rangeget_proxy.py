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
from aiohttp.web import run_app
from aiohttp.web import Application, StreamResponse
from . import config
from .util.lruCache import LruCache
from .util.storUtil import getStorBytes, getStorObjStats
from . import hsds_logger as log
from aiohttp.web_exceptions import  HTTPInternalServerError, HTTPNotFound, HTTPBadRequest

"""
read indicated bytes from LRU cache (or S3 if not in cache)
"""
async def read_page(app, key, buffer, obj_size=0, offset=0, start=0, end=0, bucket=None):
    log.info(f"read_page(key={key}, offset={offset}, start={start}, end={end}, bucket={bucket}")
    page_size = int(config.get("data_cache_page_size"))
    length = end - start
    if length <= 0:
        msg = "Invalid parameter - end <= start"
        log.error(msg)
        raise HTTPInternalServerError()
    if start // page_size != (end-1) // page_size:
        msg = "Invalid parameter - start and end not in same page"
        log.error(msg)
        raise HTTPInternalServerError()
    if offset > start:
        msg = "Invalid parameter - offset greater than start"
        log.error(msg)
        raise HTTPInternalServerError()

    if not key:
        msg = "Invalid parameter - key"
        log.error(msg)
        raise HTTPInternalServerError()
   
    if not bucket:
        log.error("read_page - Invalid parameter - bucket not set")
        raise HTTPInternalServerError()
    page_start = (start // page_size) * page_size
    page_number = page_start // page_size
    
    data_cache = app["data_cache"]
    cache_key = f"{bucket}/{key}:{page_number}"
    if cache_key not in data_cache:
        log.debug(f"cache_key: {cache_key} not found in data cache")
        page_length = page_size
        if page_start + page_size > obj_size:
            page_length = obj_size - page_start
        else:
            page_length = page_size
        # set use_proxy to False sense we are the proxy!
        page_bytes = await getStorBytes(app, key, offset=page_start, length=page_length, bucket=bucket, use_proxy=False)
        if page_bytes is None:
            log.debug(f"getStorBytes {bucket}/{key} not found")
            raise HTTPNotFound()
        log.debug(f"got page_bytes, add key: {cache_key} to cache")
        data_cache[cache_key] = page_bytes
    else:
        log.debug(f"cache_key: {cache_key} found in data cache")
        page_bytes = data_cache[cache_key]
    page_start = start % page_size
    page_end = page_start + length
    buffer_start = start - offset
    buffer[buffer_start:(buffer_start+length)] = page_bytes[page_start:page_end] 
  


"""
Return data from requested chunk and selection
"""
async def GET_ByteRange(request):
    log.request(request)
    app = request.app
    # app = request.app
    params = request.rel_url.query
    log.info("GET_ByteRange")
    for k in params:
        v = params[k]
        log.debug(f"    {k}: {v}")

    if "bucket" not in params:
        msg = "Missing bucket"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    bucket = params["bucket"]

    if "key" not in params:
        msg = "Missing key"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    key = params["key"]

    if "offset" not in params:
        msg =- "missing offset"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    offset = int(params["offset"])

    if "length" not in params:
        msg = "Missing length"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    length = int(params["length"])
    if length <= 0:
        msg = "Invalid length parameter"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    if "Content-Type" in request.headers:
        # client should use "application/octet-stream" for binary transfer
        content_type = request.headers["Content-Type"]
        if content_type != "application/octet-stream":
            msg = f"Unexpected content_type: {content_type}"
            log.error(msg)
            raise HTTPBadRequest(reason=msg)

    page_size = int(config.get("data_cache_page_size"))
    log.debug(f"page_size: {page_size}")
    log.info(f"GET_ByteRange(bucket={bucket}, key={key}, offset={offset}, length={length})")

    obj_stat_map = app["obj_stat_map"]
    if f"{bucket}/{key}" not in obj_stat_map:
        log.debug("getStorObjStats")
        key_stats = await getStorObjStats(app, key, bucket=bucket)  
        obj_size = key_stats["Size"]
        obj_stat_map[f"{bucket}/{key}"] = key_stats
    else: 
        key_stats = obj_stat_map[f"{bucket}/{key}"]
        obj_size = key_stats["Size"]

    log.debug(f"{bucket}/{key} size: {obj_size}")
    if offset + length > obj_size:
        msg = "ByteRange selection invalid"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)


    # create bytearray to store data to be returned
    buffer = bytearray(length)       

    max_concurrent_read = config.get("data_cache_max_concurrent_read") 
    tasks = set()
    loop = asyncio.get_event_loop()
    page_start = offset
    page_end = page_start
    while page_end < offset + length:
        if len(tasks) >= max_concurrent_read:
            # Wait for some download to finish before adding a new one
            _done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        page_end = page_start + page_size
        page_end -= page_end % page_size  # trim to page boundry
        if page_end > offset + length:
            page_end = offset + length
        log.debug(f"read page {page_start} - {page_end}")  
        task = loop.create_task(read_page(app, key, buffer, obj_size=obj_size, offset=offset, start=page_start, end=page_end, bucket=bucket))
        tasks.add(task)
        page_start = page_end
    # Wait for the remaining downloads to finish
    await asyncio.wait(tasks)

    log.info(f"GET_ByteRange - returning: {len(buffer)} bytes")

    # write response
     
    try:
        resp = StreamResponse()
        resp.headers['Content-Type'] = "application/octet-stream"
        resp.content_length = len(buffer)
        await resp.prepare(request)
        await resp.write(buffer)
    except Exception as e:
        log.error(f"Exception during binary data write: {e}")
        raise HTTPInternalServerError()
    finally:
        await resp.write_eof()
     
    return resp


#
# Main
#

def main():
    log.info("rangeget_proxy start")

    cache_size = int(config.get("data_cache_size"))
    log.info(f"Using data cache size of: {cache_size}")
    page_size = int(config.get("data_cache_page_size"))
    log.info(f"Setting data page size to: {page_size}")
    expire_time = int(config.get("data_cache_expire_time"))
    log.info(f"Setting data cache expire time to: {expire_time}")
     
    # create the app object
    app = Application() 

    #
    # call app.router.add_get() here to add node-specific routes
    #
    app.router.add_route('GET', '/', GET_ByteRange)
    app['data_cache'] = LruCache(mem_target=cache_size, name="DataCache", expire_time=expire_time)
    app['obj_stat_map'] = {}

    # run the app
    port = int(config.get("rangeget_port"))
    log.info(f"run_app on port: {port}")
    run_app(app, port=port)

if __name__ == '__main__':
    main()
