##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of HSDS (HDF5 Scalable Data Service), Libraries and      #
# Utilities.  The full HSDS copyright notice, including                      #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from  help@hdfgroup.org.                                     #
##############################################################################
#
# httpUtil:
# http-related helper functions
#
from asyncio import CancelledError
from aiohttp.web import json_response
from aiohttp import  ClientSession, TCPConnector
from aiohttp.web_exceptions import HTTPForbidden, HTTPNotFound, HTTPConflict, HTTPGone, HTTPInternalServerError, HTTPRequestEntityTooLarge, HTTPServiceUnavailable
from aiohttp.client_exceptions import ClientError


from .. import hsds_logger as log
from .. import config

def isOK(http_response):
    if http_response < 300:
        return True
    return False


def getUrl(host, port):
    return f"http://{host}:{port}"

"""
get aiobotocore http client
"""
def get_http_client(app):
    """ get http client """
    if "client" in app:
        return app["client"]

    # first time call, create client interface
    # use shared client so that all client requests
    #   will share the same connection pool
    max_tcp_connections = int(config.get("max_tcp_connections"))
    log.info(f"Initiating TCPConnector with limit {max_tcp_connections} connections")
    client = ClientSession(connector=TCPConnector(limit_per_host=max_tcp_connections))
    #create the app object
    app['client'] = client
    return client


"""
Replacement for aiohttp Request.read using our max request limit
"""
async def request_read(request) -> bytes:
    """Read request body if present.

    Returns bytes object with full request content.
    """
    log.debug("request_read")
    if request._read_bytes is None:
        body = bytearray()
        max_request_size = int(config.get("max_request_size"))
        while True:
            chunk = await request._payload.readany()
            body.extend(chunk)
            body_size = len(body)
            if body_size >= max_request_size:
                raise HTTPRequestEntityTooLarge(
                        max_size=max_request_size,
                        actual_size=body_size
                    )
            if not chunk:
                break
        request._read_bytes = bytes(body)
    return request._read_bytes

"""
Helper function  - async HTTP GET
"""
async def http_get(app, url, params=None, format="json"):
    log.info(f"http_get('{url}')")
    client = get_http_client(app)
    data = None
    status_code = None
    timeout = config.get("timeout")
    try:
        async with client.get(url, params=params, timeout=timeout) as rsp:
            log.info(f"http_get status: {rsp.status}")
            status_code = rsp.status
            if rsp.status != 200:
                log.warn(f"request to {url} failed with code: {status_code}")
            else:
                # 200, so read the response
                if format == "json":
                    data = await rsp.json()
                else:
                    data = await rsp.read()  # read response as bytes
    except ClientError as ce:
        log.debug(f"ClientError: {ce}")
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.error(f"CancelledError for http_get({url}): {cle}")
        raise HTTPInternalServerError()

    if status_code == 403:
        log.warn(f"Forbiden to access {url}")
        raise HTTPForbidden()
    elif status_code == 404:
        log.warn(f"Object: {url} not found")
        raise HTTPNotFound()
    elif status_code == 410:
        log.warn(f"Object: {url} removed")
        raise HTTPGone()
    elif status_code == 503:
        log.warn(f"503 error for http_get_Json {url}")
        raise HTTPServiceUnavailable()
    elif status_code != 200:
        log.error(f"Error for http_get_json({url}): {status_code}")
        raise HTTPInternalServerError()

    return data

"""
Helper function  - async HTTP POST
"""
async def http_post(app, url, data=None, params=None):
    log.info(f"http_post('{url}', {data})")
    client = get_http_client(app)
    rsp_json = None
    timeout = config.get("timeout")

    try:
        async with client.post(url, json=data, params=params, timeout=timeout ) as rsp:
            log.info(f"http_post status: {rsp.status}")
            if rsp.status == 200:
                pass  # ok
            elif rsp.status == 201:
                pass # also ok
            elif rsp.status == 204: # no data
                return None
            elif rsp.status == 404:
                log.info(f"POST  reqest HTTPNotFound error for url: {url}")
            elif rsp.status == 410:
                log.info(f"POST  reqest HTTPGone error for url: {url}")
            elif rsp.status == 503:
                log.warn(f"503 error for http_get_Json {url}")
                raise HTTPServiceUnavailable()

            else:
                log.warn(f"POST request error for url: {url} - status: {rsp.status}")
                raise HTTPInternalServerError()
            rsp_json = await rsp.json()
            log.debug(f"http_post({url}) response: {rsp_json}")
    except ClientError as ce:
        log.error(f"Error for http_post({url}): {ce} ")
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.error(f"CancelledError for http_post({url}): {cle}")
        raise HTTPInternalServerError()
    return rsp_json

"""
Helper function  - async HTTP PUT for json data
"""
async def http_put(app, url, data=None, params=None):
    log.info(f"http_put('{url}', data: {data})")
    rsp = None
    client = get_http_client(app)
    timeout = config.get("timeout")

    try:
        async with client.put(url, json=data, params=params, timeout=timeout) as rsp:
            log.info(f"http_put status: {rsp.status}")
            if rsp.status in (200, 201):
                pass # expected
            elif rsp.status == 404:
                # can come up for replace ops
                log.info(f"HTTPNotFound for: {url}")
            elif rsp.status == 409:
                log.info(f"HTTPConflict for: {url}")
                raise HTTPConflict()
            elif rsp.status == 503:
                log.warn(f"503 error for http_put url: {url}")
                raise HTTPServiceUnavailable()
            else:
                log.error(f"PUT request error for url: {url} - status: {rsp.status}")
                raise HTTPInternalServerError()

            rsp_json = await rsp.json()
            log.debug(f"http_put({url}) response: {rsp_json}")
    except ClientError as ce:
        log.error(f"ClientError for http_put({url}): {ce} ")
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.error(f"CancelledError for http_put({url}): {cle}")
        raise HTTPInternalServerError()
    return rsp_json

"""
Helper function  - async HTTP PUT for binary data
"""

async def http_put_binary(app, url, data=None, params=None):
    log.info(f"http_put_binary('{url}') nbytes: {len(data)}")
    rsp_json = None
    client = get_http_client(app)
    timeout = config.get("timeout")

    try:
        async with client.put(url, data=data, params=params, timeout=timeout) as rsp:
            log.info(f"http_put_binary status: {rsp.status}")
            if rsp.status != 201:
                log.error(f"PUT (binary) request error for {url}: status {rsp.status}")
                raise HTTPInternalServerError()
            elif rsp.status == 503:
                log.warn(f"503 error for http_put_binary {url}")
                raise HTTPServiceUnavailable()

            rsp_json = await rsp.json()
            log.debug(f"http_put_binary({url}) response: {rsp_json}")
    except ClientError as ce:
        log.error(f"Error for http_put_binary({url}): {ce} ")
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.error(f"CancelledError for http_put_binary({url}): {cle}")
        raise HTTPInternalServerError()
    return rsp_json

"""
Helper function  - async HTTP DELETE
"""
async def http_delete(app, url, data=None, params=None):
    # TBD - do we really need a data param?
    log.info(f"http_delete('{url}')")
    #client = get_http_client(app)
    rsp_json = None
    timeout = config.get("timeout")
    import aiohttp

    try:
        async with aiohttp.ClientSession() as session:
            async with session.delete(url, json=data, params=params, timeout=timeout) as rsp:
                log.info(f"http_delete status: {rsp.status}")
                if rsp.status == 200:
                    pass  # expectred
                elif rsp.status == 404:
                    log.info(f"NotFound response for DELETE for url: {url}")
                elif rsp.status == 503:
                    log.warn(f"503 error for http_delete {url}")
                    raise HTTPServiceUnavailable()
                else:
                    log.error(f"DELETE request error for url: {url} - status: {rsp.status}")
                    raise HTTPInternalServerError()

            #rsp_json = await rsp.json()
            #log.debug(f"http_delete({url}) response: {rsp_json}")
    except ClientError as ce:
        log.error(f"ClientError for http_delete({url}): {ce} ")
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.error(f"CancelledError for http_delete({url}): {cle}")
        raise HTTPInternalServerError()
    except ConnectionResetError as cre:
        log.error(f"ConnectionResetError for http_delete({url}): {cre}")
        raise HTTPInternalServerError()

    return rsp_json

"""
Helper function, create a response object using the provided
JSON data
"""
async def jsonResponse(request, data, status=200):
    return json_response(data, headers={}, status=status)

"""
Convenience method to compute href links
"""
def getHref(request, uri, query=None, domain=None):
    params = request.rel_url.query
    href = config.get("hsds_endpoint")
    if not href:
        href = request.scheme + "://127.0.0.1"
    href += uri
    delimiter = '?'
    if domain:
        href += "?domain=" + domain
        delimiter = '&'
    elif "domain" in params:
        href += "?domain=" + params["domain"]
        delimiter = '&'
    elif "host" in params:
        href  += "?host=" + params["host"]
        delimiter = '&'

    if query is not None:
        if type(query) is str:
            href += delimiter + query
        else:
            # list or tuple
            for item in query:
                href += delimiter + item
                delimiter = '&'
    return href

"""
Get requested content type.  Returns either "binary" if the accept header is
octet stream, otherwise json.
Currently does not support q fields.
"""
def getAcceptType(request):
    accept_type = "json"  # default to JSON
    if "accept" in request.headers:
        accept = request.headers["accept"]
        # treat everything as json unless octet-stream is given
        if accept != "application/octet-stream":
            msg = f"Ignoring accept value: {accept}"
            log.info(msg)
        else:
            accept_type = "binary"
    return accept_type
