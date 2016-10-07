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
from aiohttp.web import StreamResponse
from aiohttp import HttpProcessingError 
from aiohttp.errors import ClientError

import hsds_logger as log

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
    rsp = None
    try:
        async with client.get(url, params=params) as rsp:
            log.info("http_get status: {}".format(rsp.status))
            rsp = await rsp.text()
            #log.info("http_get({}) response: {}".format(url, rsp))  
    except ClientError as ce:
        log.error("Error for http_get({}): {} ".format(url, str(ce)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    return rsp

"""
Helper function  - async HTTP GET, return response as JSON
""" 
async def http_get_json(app, url, params=None):
    log.info("http_get('{}')".format(url))
    client = app['client']
    rsp_json = None
     
    try:    
        async with client.get(url, params=params) as rsp:
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
    if isinstance(rsp_json, str):
        log.warn("converting str to json")
        rsp_json = json.loads(rsp_json)
    return rsp_json

"""
Helper function  - async HTTP GET
""" 
async def http_post(app, url, data=None, params=None):
    log.info("http_post('{}', data)".format(url, data))
    client = app['client']
    rsp_json = None
    
    try:
        async with client.post(url, data=json.dumps(data), params=params ) as rsp:
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
    return rsp_json

"""
Helper function  - async HTTP PUT
""" 
async def http_put(app, url, data=None, params=None):
    log.info("http_put('{}', data: {})".format(url, data))
    rsp_json = None
    client = app['client']
    
    try:
        async with client.put(url, data=json.dumps(data), params=params) as rsp:
            log.info("http_put status: {}".format(rsp.status))
            if rsp.status != 201:
                print("bad response:", str(rsp))
                msg = "request error"  # tbd - pull error from rsp
                log.warn(msg)
                raise HttpProcessingError(message=msg, code=rsp.status)

            rsp_json = await rsp.json()
            log.info("http_put({}) response: {}".format(url, rsp_json))
    except ClientError as ce:
        log.error("Error for http_post({}): {} ".format(url, str(ce)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    return rsp_json

"""
Helper function  - async HTTP DELETE
""" 
async def http_delete(app, url, params=None):
    log.info("http_delete('{}'".format(url))
    client = app['client']
    rsp_json = None
    
    try:
        async with client.delete(url, params=params) as rsp:
            log.info("http_delete status: {}".format(rsp.status))
            if rsp.status != 200:
                print("bad response:", str(rsp))
                msg = "request error"  # tbd - pull error from rsp
                log.warn(msg)
                raise HttpProcessingError(message=msg, code=rsp.status)

            rsp_json = await rsp.json()
            log.info("http_put({}) response: {}".format(url, rsp_json))
    except ClientError as ce:
        log.error("Error for http_delete({}): {} ".format(url, str(ce)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    return rsp_json

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





 
