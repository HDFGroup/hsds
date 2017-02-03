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
# httpUtil:
# http-related helper functions
# 
import json
from asyncio import CancelledError
from aiohttp.web import StreamResponse
from aiohttp import HttpProcessingError 
from aiohttp.errors import ClientError, HttpBadRequest


import hsds_logger as log
import config

def isOK(http_response):
    if http_response < 300:
        return True
    return False


def getUrl(host, port):
    return "http://{}:{}".format(host, port)

"""
Helper function  - async HTTP GET
""" 
async def http_get(app, url, params=None):
    log.info("http_get('{}')".format(url))
    client = app['client']
    data = None
    timeout = config.get("timeout")
    try:
        async with client.get(url, params=params, timeout=timeout) as rsp:
            log.info("http_get status: {}".format(rsp.status))
            if rsp.status != 200:
                msg = "request to {} failed with code: {}".format(url, rsp.status)
                log.warn(msg)
                raise HttpProcessingError(message=msg, code=rsp.status)
            #log.info("http_get({}) response: {}".format(url, rsp))  
            data = await rsp.read()  # read response as bytes
    except ClientError as ce:
        log.error("Error for http_get({}): {} ".format(url, str(ce)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    except CancelledError as cle:
        log.error("CancelledError for http_get({}): {}".format(url, str(cle)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    return data

"""
Helper function  - async HTTP GET, return response as JSON
""" 
async def http_get_json(app, url, params=None):
    log.info("http_get('{}')".format(url))
    client = app['client']
    rsp_json = None
    timeout = config.get("timeout")
     
    try:    
        async with client.get(url, params=params, timeout=timeout) as rsp:
            log.info("http_get status: {}".format(rsp.status))
            if rsp.status != 200:
                msg = "request to {} failed with code: {}".format(url, rsp.status)
                log.warn(msg)
                raise HttpProcessingError(message=msg, code=rsp.status)
            rsp_json = await rsp.json()
            #log.info("http_get({}) response: {}".format(url, rsp_json))  
    except ClientError as ce:
        log.error("Error for http_get_json({}): {} ".format(url, str(ce)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    except CancelledError as cle:
        log.error("CancelledError for http_get_json({}): {}".format(url, str(cle)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    if isinstance(rsp_json, str):
        log.warn("converting str to json")
        rsp_json = json.loads(rsp_json)
    return rsp_json

"""
Helper function  - async HTTP POST
""" 
async def http_post(app, url, data=None, params=None):
    log.info("http_post('{}', data)".format(url, data))
    client = app['client']
    rsp_json = None
    timeout = config.get("timeout")
    
    try:
        async with client.post(url, data=json.dumps(data), params=params, timeout=timeout ) as rsp:
            log.info("http_post status: {}".format(rsp.status))
            if rsp.status not in (200, 201):
                msg = "request error - status: ".format(rsp.status)  # tbd - pull error from rsp
                log.warn(msg)
                raise HttpProcessingError(message=msg, code=rsp.status)
            rsp_json = await rsp.json()
            log.info("http_post({}) response: {}".format(url, rsp_json))
    except ClientError as ce:
        log.error("Error for http_post({}): {} ".format(url, str(ce)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    except CancelledError as cle:
        log.error("CancelledError for http_post({}): {}".format(url, str(cle)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    return rsp_json

"""
Helper function  - async HTTP PUT for json data
""" 
async def http_put(app, url, data=None, params=None):
    log.info("http_put('{}', data: {})".format(url, data))
    rsp = None
    client = app['client']
    timeout = config.get("timeout")
      
    try:
        async with client.put(url, data=json.dumps(data), params=params, timeout=timeout) as rsp:
            log.info("http_put status: {}".format(rsp.status))
            if rsp.status != 201:
                msg = "request error"  # tbd - pull error from rsp
                log.warn(msg)
                raise HttpProcessingError(message=msg, code=rsp.status)

            rsp_json = await rsp.json()
            log.info("http_put({}) response: {}".format(url, rsp_json))
    except ClientError as ce:
        log.error("ClientError for http_put({}): {} ".format(url, str(ce)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    except CancelledError as cle:
        log.error("CancelledError for http_put({}): {}".format(url, str(cle)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    return rsp_json

"""
Helper function  - async HTTP PUT for binary data
""" 

async def http_put_binary(app, url, data=None, params=None):
    log.info("http_put_binary('{}') nbytes: {}".format(url, len(data)))
    rsp_json = None
    client = app['client']
    timeout = config.get("timeout")
    
    try:
        async with client.put(url, data=data, params=params, timeout=timeout) as rsp:
            log.info("http_put status: {}".format(rsp.status))
            if rsp.status != 201:
                msg = "request error for {}: {}".format(url, str(rsp))
                log.error(msg)
                raise HttpProcessingError(message=msg, code=rsp.status)

            rsp_json = await rsp.json()
            log.info("http_put({}) response: {}".format(url, rsp_json))
    except ClientError as ce:
        log.error("Error for http_put_binary({}): {} ".format(url, str(ce)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    except CancelledError as cle:
        log.error("CancelledError for http_put_binary({}): {}".format(url, str(cle)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    return rsp_json

"""
Helper function  - async HTTP DELETE
""" 
async def http_delete(app, url, data=None, params=None):
    log.info("http_delete('{}')".format(url))
    client = app['client']
    rsp_json = None
    timeout = config.get("timeout")
    
    try:
        async with client.delete(url, data=json.dumps(data), params=params, timeout=timeout) as rsp:
            log.info("http_delete status: {}".format(rsp.status))
            if rsp.status != 200:
                msg = "request error"  # tbd - pull error from rsp
                log.warn(msg)
                raise HttpProcessingError(message=msg, code=rsp.status)

            rsp_json = await rsp.json()
            log.info("http_put({}) response: {}".format(url, rsp_json))
    except ClientError as ce:
        log.error("Error for http_delete({}): {} ".format(url, str(ce)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    except CancelledError as cle:
        log.error("CancelledError for http_delete({}): {}".format(url, str(cle)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    return rsp_json

"""
Helper funciton, create a response object using the provided 
JSON data
"""
async def jsonResponse(request, data, status=200):
    resp = StreamResponse(status=status)
    resp.headers['Content-Type'] = 'application/json'
    answer = json.dumps(data)
    answer = answer.encode('utf8')
    resp.content_length = len(answer)
    await resp.prepare(request)
    resp.write(answer)
    await resp.write_eof()
    return resp

"""
Convience method to compute href links
"""
def getHref(request, uri, query=None, domain=None):
    href = config.get("hsds_endpoint")
    if not href:
        href = request.scheme + "://" + request.host  
    href += uri
    delimiter = '?'
    if domain:
        href += "?domain=" + domain
        delimiter = '&'
    elif "domain" in request.GET:
        href += "?domain=" + request.GET["domain"]
        delimiter = '&'
    elif "host" in request.GET:
        href  += "?host=" + request.GET["host"]
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
        if request.headers["accept"] not in ("application/json", "application/octet-stream", "*/*"):
            msg = "Unexpected accept value: {}".format(accept_type)
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        if request.headers["accept"] == "application/octet-stream":
            accept_type = "binary"
    return accept_type

    
     





 
