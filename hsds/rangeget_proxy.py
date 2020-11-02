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
from aiohttp.web import run_app
from aiohttp.web import Application, StreamResponse
from . import config
from .util.lruCache import LruCache
from .util.storUtil import getStorBytes
from . import hsds_logger as log
#from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError, HTTPBadRequest
from aiohttp.web_exceptions import  HTTPInternalServerError, HTTPBadRequest

"""
read indicated bytes from LRU cache (or S3 if not in cache)
"""
async def read_page(app, key, buffer, offset=0, page_start=0, page_end=0, bucket=None):
    log.info(f"read_page(key={key}, offset={offset}, page_start={page_start}, page_end={page_end}, bucket={bucket}")
    if page_end <= page_start:
        msg = "Invalid parameter - page_end <= page_start"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    if not key:
        msg = "Invalid parameter - key"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    if not key.startswith("/"):
        msg = "Invalid parameter - expected key to start with /"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    if not bucket:
        msg = "Invalid parameter - bucket"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    data_cache = app["data_cache"]
    cache_key = f"{bucket}{key}:{page_start}"
    length = page_end - page_start
    stor_bytes = await getStorBytes(app, key, offset=page_start, length=length, bucket=bucket)
    buffer[(page_start - offset): (page_end - offset)] = stor_bytes


    





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

    page_size = app["data_page_size"]
    log.info(f"page_size: {page_size}")

    # create bytearray to store data to be returned
    buffer = bytearray(length)        

    page_start = offset
    page_end = page_start
    while page_end < offset + length:
        page_end = page_start + page_size
        page_end -= page_end % page_size  # trim to page boundry
        if page_end > offset + length:
            page_end = offset + length
        print(f"read page {page_start} - {page_end}")  
        await read_page(app, key, buffer, offset=offset, page_start=page_start, page_end=page_end, bucket=bucket)
        page_start = page_end
  
        
    log.info(f"got: {len(buffer)} bytes")

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

    data_cache_size = int(config.get("data_cache_size"))
    log.info(f"Using data cache size of: {data_cache_size}")
    data_page_size = int(config.get("data_page_size"))
    log.info(f"Setting data page size to: {data_page_size}")
    data_cache_expire = int(config.get("data_cache_expire"))
    log.info(f"Setting data cache expire time to: {data_cache_expire}")
     
    # create the app object
    app = Application() 

    app["data_page_size"] = data_page_size
    #
    # call app.router.add_get() here to add node-specific routes
    #
    app.router.add_route('GET', '/', GET_ByteRange)
    app['data_cache'] = LruCache(mem_target=data_cache_size, name="DataCache", expire_time=data_cache_expire)

    # run the app
    port = int(config.get("rangeget_port"))
    log.info(f"run_app on port: {port}")
    run_app(app, port=port)

if __name__ == '__main__':
    main()
