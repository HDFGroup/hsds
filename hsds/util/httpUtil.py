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
from asyncio import CancelledError, TimeoutError
import os
import socket
import numpy as np
from aiohttp.web import json_response
import simplejson
from aiohttp import ClientSession, UnixConnector, TCPConnector
from aiohttp.web_exceptions import HTTPForbidden, HTTPNotFound, HTTPConflict
from aiohttp.web_exceptions import HTTPGone, HTTPInternalServerError
from aiohttp.web_exceptions import HTTPRequestEntityTooLarge
from aiohttp.web_exceptions import HTTPServiceUnavailable, HTTPBadRequest
from aiohttp.client_exceptions import ClientError
from hsds.util.idUtil import isValidUuid

from .. import hsds_logger as log
from .. import config


def isOK(http_response):
    """return True for successful http_status codes"""
    if http_response < 300:
        return True
    return False


def getUrl(host, port):
    """return url for host and port"""
    return f"http://{host}:{port}"


def getBooleanParam(params, key):
    """ return False if the given key is not in the
        params dict, or is it, but has the value, 0, or "0".
        return True otherwise """

    if not isinstance(key, str):
        raise TypeError("expected str value for key")

    if key not in params:
        return False

    value = params[key]
    if not value:
        return False

    try:
        int_value = int(value)
    except ValueError:
        return True

    if int_value:
        return True
    else:
        return False


def getPortFromUrl(url):
    """Get Port number for given url"""
    if not url:
        raise ValueError("url undefined")
    if url.startswith("http://"):
        default_port = 80
    elif url.startswith("https://"):
        default_port = 443
    elif url.startswith("http+unix://"):
        # unix domain socket
        return None
    else:
        raise ValueError(f"Invalid Url: {url}")

    start = url.find("//")
    port = None
    dns = url[start:]
    index = dns.find(":")
    port_str = ""
    if index > 0:
        for i in range(index + 1, len(dns)):
            ch = dns[i]
            if ch.isdigit():
                port_str += ch
            else:
                break
    if port_str:
        port = int(port_str)
    else:
        port = default_port

    return port


def isUnixDomainUrl(url):
    # return True if url is a Unix Socket domain
    # e.g. http://unix:%2Ftmp%2Fdn_1.sock/about -> True
    #      http://localhost:80 -> False
    if not url:
        raise ValueError("url undefined")
    if not url.startswith("http"):
        raise ValueError(f"invalid url, no http: {url}")
    if url.startswith("http+unix:"):
        if not url.startswith("http+unix://"):
            raise ValueError(f"invalid socket url: {url}")
        return True
    else:
        return False


def getSocketPath(url):
    # return socket path part of the url
    # E.g. for "http+unix://%2Ftmp%2Fdn_1.sock/about" return "/tmp/dn_1.sock"
    if not isUnixDomainUrl(url):
        return None
    # url must start with http+unix://:
    skip = len("http+unix://")
    chars = []
    # TBD - replace with proper url-decode
    for i in range(len(url)):
        if skip:
            skip -= 1
        elif url[i] == "/":
            break
        elif url[i] == "%" and url[i + 1] == "2" and url[i + 2] == "F":
            chars.append("/")
            skip = 2
        else:
            chars.append(url[i])
    return "".join(chars)


def bindToSocket(url):
    """
    Bind to socket specified by http+unix url
    """
    if not isUnixDomainUrl(url):
        raise ValueError(f"Invalid url for bindToSocket: {url}")
    # use a unix domain socket path
    path = getSocketPath(url)
    log.debug(f"got socketpath: {path}")
    # first, make sure the socket does not already exist
    try:
        os.unlink(path)
    except OSError:
        if os.path.exists(path):
            raise
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.bind(path)
    return s


def get_http_std_url(url):
    # replace socket path (if exists) with 127.0.0.1
    if not isUnixDomainUrl(url):
        return url
    index = url.find(".sock")
    n = index + 5
    url = "http://127.0.0.1" + url[n:]
    return url


def get_base_url(url):
    """return protocal+dns+port part of url.
    Returns just url if a non-standard protocol is given."""
    n = len(url)
    for protocol in ("http://", "https://", "http+unix://"):
        if url.startswith(protocol):
            start = len(protocol)
            n = url.find("/", start)
            if n < 0:
                n = len(url)
            break
    s = url[:n]
    return s


def get_http_client(app, url=None, cache_client=True):
    """get http client"""
    log.debug(f"get_http_client, url: {url}")
    if url is None or not isUnixDomainUrl(url):
        socket_path = None
    else:
        socket_path = getSocketPath(url)
        log.debug(f"socket_path: {socket_path}")

    if cache_client:
        if "client" in app and not socket_path:
            return app["client"]
        if socket_path:
            if "socket_clients" not in app:
                app["socket_clients"] = {}
            socket_clients = app["socket_clients"]
            if socket_path in socket_clients:
                return socket_clients[socket_path]

    # first time call, create client interface
    # use shared client so that all client requests
    #   will share the same connection pool

    if socket_path:
        log.info(f"Initiating UnixConnector with path: {socket_path}")
        client = ClientSession(connector=UnixConnector(path=socket_path))
        if cache_client:
            socket_clients[socket_path] = client
        log.info(f"Socket Ready: {socket_path}")
    else:
        max_tcp_connections = int(config.get("max_tcp_connections"))
        msg = f"Initiating TCPConnector for {url} with limit "
        msg += f"{max_tcp_connections} connections"
        log.info(msg)
        kwargs = {"limit_per_host": max_tcp_connections}
        # not yet supported in this aiohttp version
        # read_buf_size = config.get("read_buf_size", default=10*1024*1024)
        # log.debug(f"setting read_buf_size to: {read_buf_size}")
        # kwargs['read_bufsize'] = read_buf_size
        client = ClientSession(connector=TCPConnector(**kwargs))
        if cache_client:
            app["client"] = client

    # return client instance
    return client


async def release_http_client(app):
    """
    Release any http clients
    """
    log.info("releasing http clients")
    if "client" in app:
        client = app["client"]
        await client.close()
        del app["client"]
    if "socket_clients" in app:
        socket_clients = app["socket_clients"]
        for socket_path in socket_clients:
            client = socket_clients[socket_path]
            await client.close()
        app["socket_clients"] = {}


async def request_read(request, count=None) -> bytes:
    """
    Replacement for aiohttp Request.read using our max request limit
    Read request body if present.

    Returns bytes object with full request content,
       or next count bytes if count is set
    """
    log.debug(f"request_read - count: {count}")
    body = bytearray()
    max_request_size = int(config.get("max_request_size"))
    while True:
        if count is not None:
            chunk = await request._payload.readexactly(count)
            count -= len(chunk)
        else:
            chunk = await request._payload.readany()
        body.extend(chunk)
        body_size = len(body)
        if body_size >= max_request_size:
            raise HTTPRequestEntityTooLarge(
                max_size=max_request_size, actual_size=body_size
            )
        if not chunk:
            break
        if count is not None and count <= 0:
            break
    return bytes(body)


async def http_get(app, url, params=None, client=None):
    """
    Helper function  - async HTTP GET
    """
    log.info(f"http_get('{url}')")
    if client is None:
        client = get_http_client(app, url=url)
    url = get_http_std_url(url)
    status_code = None
    timeout = config.get("timeout")
    # TBD: use read_bufsize parameter to optimize read for large responses
    try:
        async with client.get(url, params=params, timeout=timeout) as rsp:
            log.info(f"http_get status: {rsp.status} for req: {url}")
            status_code = rsp.status
            if rsp.status == 200:
                # 200, so read the response
                if isBinaryResponse(rsp):
                    # return binary data
                    retval = await rsp.read()  # read response as bytes
                else:
                    retval = await rsp.json()
            elif status_code == 400:
                log.warn(f"BadRequest to {url}")
                raise HTTPBadRequest(reason="Bad Request")
            elif status_code == 403:
                log.warn(f"Forbidden to access {url}")
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
            else:
                log.error(f"request to {url} failed with code: {status_code}")
                raise HTTPInternalServerError()

    except ClientError as ce:
        log.warn(f"ClientError: {ce}")
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.warn(f"CancelledError for http_get({url}): {cle}")
        raise HTTPInternalServerError()
    except ConnectionResetError as cre:
        log.warn(f"ConnectionResetError for http_get({url}): {cre}")
        raise HTTPInternalServerError()
    except TimeoutError as toe:
        log.warn(f"TimeoutError for http_get({url}: {toe})")
        raise HTTPServiceUnavailable()

    return retval


async def http_post(app, url, data=None, params=None, client=None):
    """
    Helper function  - async HTTP POST
    """
    if not url:
        log.error("http_post with no url")
        return
    if url.startswith("http://head"):
        # just use debug for health check traffic
        logmsg = log.debug
    else:
        logmsg = log.info
    msg = f"http_post('{url}'"
    if isinstance(data, bytes):
        msg += f" {len(data)} bytes"
    logmsg(msg)
    if client is None:
        client = get_http_client(app, url=url)
    url = get_http_std_url(url)
    if isinstance(data, bytes):
        log.debug("setting http_post for binary")
        kwargs = {"data": data}
    else:
        kwargs = {"json": data}
    timeout = config.get("timeout")
    if timeout:
        kwargs["timeout"] = timeout
    if params:
        kwargs["params"] = params

    try:
        async with client.post(url, **kwargs) as rsp:
            logmsg(f"http_post status: {rsp.status}")
            if rsp.status == 200:
                pass  # ok
            elif rsp.status == 201:
                pass  # also ok
            elif rsp.status == 204:  # no data
                return None
            elif rsp.status == 400:
                msg = f"POST request HTTPBadRequest error for url: {url}"
                log.warn(msg)
                raise HTTPBadRequest(reason="Bad Request")
            elif rsp.status == 404:
                msg = f"POST  request HTTPNotFound error for url: {url}"
                log.warn(msg)
                raise HTTPNotFound()
            elif rsp.status == 410:
                log.warn(f"POST  request HTTPGone error for url: {url}")
                raise HTTPGone()
            elif rsp.status == 503:
                log.warn(f"503 error for http_get_Json {url}")
                raise HTTPServiceUnavailable()
            else:
                msg = f"POST request error for url: {url} status: {rsp.status}"
                log.error(msg)
                raise HTTPInternalServerError()
            if isBinaryResponse(rsp):
                # return binary data
                retval = await (rsp.read())
                log.debug(f"http_post({url}) returning {len(retval)} bytes")
            else:
                retval = await rsp.json()
                log.debug(f"http_post({url}) response: {retval}")

    except ClientError as ce:
        log.warn(f"ClientError for http_post({url}): {ce} ")
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.warn(f"CancelledError for http_post({url}): {cle}")
        raise HTTPInternalServerError()
    except ConnectionResetError as cre:
        log.warn(f"ConnectionResetError for http_post({url}): {cre}")
        raise HTTPInternalServerError()
    except TimeoutError as toe:
        log.warn(f"TimeoutError for http_post({url}: {toe})")
        raise HTTPServiceUnavailable()

    return retval


async def http_put(app, url, data=None, params=None, client=None):
    """
    Helper function  - async HTTP PUT
    """
    log.info(f"http_put('{url}')")
    if client is None:
        client = get_http_client(app, url=url)
    url = get_http_std_url(url)
    if isinstance(data, bytes):
        log.debug(f"setting http_put for binary, {len(data)} bytes")
        kwargs = {"data": data}
    else:
        log.debug("setting http_put for json")
        kwargs = {"json": data}

    rsp_json = None
    if params is not None:
        kwargs["params"] = params
    timeout = config.get("timeout")
    if timeout:
        kwargs["timeout"] = timeout

    try:
        async with client.put(url, **kwargs) as rsp:
            log.info(f"http_put status: {rsp.status}")
            if rsp.status in (200, 201):
                pass  # expected
            elif rsp.status == 400:
                msg = f"PUT request HTTPBadRequest error for url: {url}"
                log.warn(msg)
                raise HTTPBadRequest(reason="Bad Request")
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
                msg = f"PUT request error for url: {url} status: {rsp.status}"
                log.error(msg)
                raise HTTPInternalServerError()
            if isBinaryResponse(rsp):
                # return binary data
                retval = await rsp.read()  # read response as bytes
                log.debug(f"http_put({url}): return {len(retval)} bytes")
            else:
                retval = await rsp.json()
                log.debug(f"http_put({url}) response: {rsp_json}")
    except ClientError as ce:
        log.warn(f"ClientError for http_put({url}): {ce} ")
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.warn(f"CancelledError for http_put({url}): {cle}")
        raise HTTPInternalServerError()
    except ConnectionResetError as cre:
        log.warn(f"ConnectionResetError for http_put({url}): {cre}")
        raise HTTPInternalServerError()
    except TimeoutError as toe:
        log.warn(f"TimeoutError for http_put({url}: {toe})")
        raise HTTPServiceUnavailable()
    return retval


async def http_delete(app, url, data=None, params=None, client=None):
    """
    Helper function  - async HTTP DELETE
    """
    # TBD - do we really need a data param?
    log.info(f"http_delete('{url}')")
    if client is None:
        client = get_http_client(app, url=url)
    url = get_http_std_url(url)

    rsp_json = None
    kwargs = {}
    timeout = config.get("timeout")
    if timeout:
        kwargs["timeout"] = timeout
    if params:
        kwargs["params"] = params

    try:
        async with client.delete(url, **kwargs) as rsp:
            log.info(f"http_delete status: {rsp.status}")
            if rsp.status == 200:
                pass  # expected
            elif rsp.status == 404:
                log.info(f"NotFound response for DELETE for url: {url}")
            elif rsp.status == 503:
                log.warn(f"503 error for http_delete {url}")
                raise HTTPServiceUnavailable()
            else:
                msg = f"DELETE request error for url: {url} "
                msg += f"status: {rsp.status}"
                log.error(msg)
                raise HTTPInternalServerError()

            # rsp_json = await rsp.json()
            # log.debug(f"http_delete({url}) response: {rsp_json}")
    except ClientError as ce:
        log.warn(f"ClientError for http_delete({url}): {ce} ")
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.warn(f"CancelledError for http_delete({url}): {cle}")
        raise HTTPInternalServerError()
    except ConnectionResetError as cre:
        log.warn(f"ConnectionResetError for http_delete({url}): {cre}")
        raise HTTPInternalServerError()
    except TimeoutError as toe:
        log.warn(f"TimeoutError for http_delete({url}: {toe})")
        raise HTTPServiceUnavailable()

    return rsp_json


async def jsonResponse(resp, data, status=200, ignore_nan=False, body_only=False):
    """
    Helper function, create a response object using the provided
    JSON data
    """
    # tbd - remove resp parameter - not used

    try:
        text = simplejson.dumps(data, ignore_nan=ignore_nan, allow_nan=True)
    except ValueError as ve:
        # this exception started to get raised around 04/12/2023
        # "out of range float values" when nan is eturned and ignore_nan is False
        # some change in numpy behaviour?
        log.warn(f"got exception {ve} trying to do json dump of: {data}")
        raise HTTPInternalServerError()
    if body_only:
        return text
    else:
        server_name = config.get("server_name")
        xss_protection = config.get("xss_protection", default="1; mode=block")
        headers = {"Server": server_name}
        if xss_protection:
            headers["X-XSS-Protection"] = xss_protection
        return json_response(text=text, headers=headers, status=status)


def respJsonAssemble(obj_json, params, id):
    """
    Populate response fields based on object type
    """
    log.debug("enter assemble")

    if isValidUuid(id, "dataset"):
        log.debug("assemble dataset")
        resp_json = {}
        resp_json["id"] = obj_json["id"]
        resp_json["root"] = obj_json["root"]
        resp_json["shape"] = obj_json["shape"]
        resp_json["type"] = obj_json["type"]
        if "creationProperties" in obj_json:
            if "ignore_nan" in params and params["ignore_nan"]:
                # convert fillValue to "nan" if it is a np.nan
                s = obj_json["creationProperties"]
                d = {}
                for k in s:
                    v = s[k]
                    if k == "fillValue" and isinstance(v, float) and np.isnan(v):
                        d[k] = "nan"
                    else:
                        d[k] = v
                resp_json["creationProperties"] = d
            else:
                # just return the dset_json creation props as is
                resp_json["creationProperties"] = obj_json["creationProperties"]
        else:
            resp_json["creationProperties"] = {}

        if "layout" in obj_json:
            resp_json["layout"] = obj_json["layout"]
        resp_json["attributeCount"] = obj_json["attributeCount"]
        resp_json["created"] = obj_json["created"]
        resp_json["lastModified"] = obj_json["lastModified"]
        if "include_attrs" in params and params["include_attrs"]:
            resp_json["attributes"] = obj_json["attributes"]
        return resp_json
    elif isValidUuid(id, "group"):
        log.debug("assemble group")
        return obj_json
    elif isValidUuid(id, "type"):
        log.debug("assemble type")
        return obj_json
    elif isValidUuid(id, "chunk"):
        log.debug("assemble chunk")
        return obj_json
    else:
        return obj_json


def getHeader(uri):
    """
    Determine domain header based on id
    """
    if isValidUuid(uri, "group"):
        return "/groups/"
    elif isValidUuid(uri, "dataset"):
        return "/datasets/"
    elif isValidUuid(uri, "type"):
        return "/datatypes/"
    else:
        log.error("Couldn't determine proper header for type")
        raise HTTPInternalServerError()


def getObjectClass(uri):
    """
    Determine object based on id
    """
    if isValidUuid(uri, "group"):
        return "group"
    elif isValidUuid(uri, "dataset"):
        return "dataset"
    elif isValidUuid(uri, "type"):
        return "datatype"
    else:
        log.error("Couldn't determine proper object class for id")
        raise HTTPInternalServerError()


def getHref(request, uri, query=None, domain=None):
    """
    Convenience method to compute href links
    """
    params = request.rel_url.query
    href = config.get("hsds_endpoint")
    if not href:
        href = request.scheme + "://127.0.0.1"
    href += uri
    delimiter = "?"
    if domain:
        href += "?domain=" + domain
        delimiter = "&"
    elif "domain" in params:
        href += "?domain=" + params["domain"]
        delimiter = "&"
    elif "host" in params:
        href += "?host=" + params["host"]
        delimiter = "&"

    if query is not None:
        if type(query) is str:
            href += delimiter + query
        else:
            # list or tuple
            for item in query:
                href += delimiter + item
                delimiter = "&"
    return href


def getAcceptType(request):
    """
    Get requested content type.  Returns either "binary" if the accept
    header is octet stream, otherwise json.
    Currently does not support q fields.
    """
    accept_type = "json"  # default to JSON
    if "accept" in request.headers:
        accept = request.headers["accept"]
        # treat everything as json unless octet-stream is given
        if accept != "application/octet-stream":
            msg = f"Ignoring accept value: {accept}"
            log.debug(msg)
        else:
            accept_type = "binary"
    return accept_type


def isAWSLambda(request):
    """
    Return true if this is a lambda request
    """
    is_lambda = False
    if "User-Agent" in request.headers:
        if request.headers["User-Agent"] == "AWSLambda":
            is_lambda = True
    return is_lambda


def getContentType(request):
    """
    Get the content type from request headers.
    Default to json if not specified
    """
    if "Content-Type" in request.headers:
        # client should use "application/octet-stream" for binary transfer
        content_type = request.headers["Content-Type"]
        if "application/octet-stream" in content_type:
            request_type = "binary"
        elif "application/json" in content_type:
            request_type = "json"
        else:
            msg = f"Unknown content_type: {content_type}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        request_type = "json"
    return request_type


def isBinaryResponse(rsp):
    """
    Return True if response is binary data
    """
    is_binary = False
    if "Content-Type" in rsp.headers:
        content_type = rsp.headers["Content-Type"]
        if "application/octet-stream" in content_type:
            is_binary = True
    return is_binary
