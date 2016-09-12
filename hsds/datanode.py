#
# data node of hsds cluster
# 
import asyncio
import json
import time
import sys

from aiohttp.web import Application, Response, StreamResponse, run_app
from aiohttp import ClientSession, TCPConnector, HttpProcessingError 
from aiohttp.errors import HttpBadRequest, ClientOSError
from botocore.exceptions import ClientError
 

import config
from timeUtil import unixTimeToUTC, elapsedTime
from hsdsUtil import isOK, http_post, createNodeId, createObjId, jsonResponse 
from hsdsUtil import getS3Partition, getS3JSONObj, putS3JSONObj, isS3Obj 
from basenode import register, healthCheck, info, baseInit
from domainUtil import getS3KeyForDomain
import hsds_logger as log

async def GET_Domain(request):
    """HTTP GET method to return JSON for /domains/
    """
    log.request(request)
    app = request.app
    domain_key = request.match_info.get('key')
    log.info("domain: {}".format(domain_key))
    s3_key = None
    try:
        s3_key = getS3KeyForDomain(domain_key)
        log.info("s3_key for domain {}: {}".format(domain_key, s3_key))
    except ValueError as ve:
        msg = "Invalid domain key: {}".format(str(ve))
        log.warn(msg)
        raise HttpBadRequest(msg)

    if getS3Partition(s3_key, app['node_count']) != app['node_number']:
        # The request shouldn't have come to this node'
        raise HttpBadRequest(message="wrong node for 'key':{}".format(s3_key))

    meta_cache = app['meta_cache'] 
    domain_json = None 
    if s3_key in meta_cache:
        log.info("{} found in meta cache".format(s3_key))
        domain_json = meta_cache[s3_key]
    else:
        try:
            log.info("getS3JSONObj({})".format(s3_key))
            domain_json = await getS3JSONObj(app, s3_key, addprefix=False)
        except ClientError as ce:
            # key does not exist?
            log.warn("got ClientError on s3 get: {}".format(str(ce)))
            is_s3obj = await isS3Obj(app, s3_key, addprefix=False)
            if is_s3obj:
                msg = "Error getting s3 obj: " + str(ce)
                log.response(request, code=500, message=msg)
                raise HttpProcessingError(code=500, message=msg)
            else:
                msg = "{} not found".format(s3_key)
                log.response(request, code=404, message=msg)
                raise HttpProcessingError(code=404, message=msg)
        meta_cache[s3_key] = domain_json

    resp = await jsonResponse(request, domain_json)
    log.response(request, resp=resp)
    return resp

async def PUT_Domain(request):
    """HTTP PUT method to create a domain
    """
    log.request(request)
    app = request.app
    domain_key = request.match_info.get('key')
    log.info("domain: {}".format(domain_key))
    s3_key = None
    try:
        s3_key = getS3KeyForDomain(domain_key)
        log.info("s3_key for domain {}: {}".format(domain_key, s3_key))
    except ValueError as ve:
        msg = "Invalid domain key: {}".format(str(ve))
        log.warn(msg)
        raise HttpBadRequest(msg)

    if getS3Partition(s3_key, app['node_count']) != app['node_number']:
        # The request shouldn't have come to this node'
        raise HttpBadRequest(message="wrong node for 'key':{}".format(s3_key))

    meta_cache = app['meta_cache'] 
    
    domain_exist = False
    if s3_key in meta_cache:
        log.info("{} found in meta cache".format(s3_key))
        domain_exist = True
    else:
        domain_exist = await isS3Obj(app, s3_key, addprefix=False)
    if domain_exist:
        # this domain already exists, client must delete it first
        msg = "Conflict: resource exists: " + domain_key
        log.info(msg)
        raise HttpProcessingError(code=409, message=msg)   

    if not request.has_body:
        msg = "Expected Body to be in request"
        log.warn(msg)
        raise HttpProcessingError(code=500, message=msg) 

    body_json = await request.json()
    if "owner" not in body_json:
        msg = "Expected Owner Key in Body"
        log.warn(msg)
        raise HttpProcessingError(code=500, message=msg) 
    if "acls" not in body_json:
        msg = "Expected Owner Key in Body"
        log.warn(msg)
        raise HttpProcessingError(code=500, message=msg) 

    # create a root group for the new domain
    root_id = createObjId("group") 
    log.info("new root group id: {}".format(root_id))
    group_json = {"id": root_id, "root": root_id, "domain": domain_key, "links": [], "attributes": [] }
    try:
        await putS3JSONObj(app, root_id, group_json)  # write to S3
    except ClientError as ce:
        msg = "Error writing s3 obj: " + str(ce)
        log.response(request, code=500, message=msg)
        raise HttpProcessingError(code=500, message=msg)

    domain_json = { "root": root_id }
    domain_json["owner"] = body_json["owner"]
    domain_json["acls"] = body_json["acls"]

    try:
        await putS3JSONObj(app, s3_key, domain_json, addprefix=False)  # write to S3
    except ClientError as ce:
        msg = "Error writing s3 obj: " + str(ce)
        log.response(request, code=500, message=msg)
        raise HttpProcessingError(code=500, message=msg)

    # read back from S3 (will add timestamps metakeys) 
    log.info("getS3JSONObj({})".format(s3_key))
    try:
        domain_json = await getS3JSONObj(app, s3_key, addprefix=False)
    except ClientError as ce:
        msg = "Error reading s3 obj: " + s3_key
        log.response(request, code=500, message=msg)
        raise HttpProcessingError(code=500, message=msg)
    meta_cache[s3_key] = domain_json

    resp = await jsonResponse(request, domain_json, status=201)
    log.response(request, resp=resp)
    return resp

async def GET_Group(request):
    """HTTP GET method to return JSON for /groups/
    """
    log.request(request)
    app = request.app
    group_id = request.match_info.get('id')
    
    if getS3Partition(group_id, app['node_count']) != app['node_number']:
        # The request shouldn't have come to this node'
        raise HttpBadRequest(message="wrong node for 'id':{}".format(group_id))

    meta_cache = app['meta_cache'] 
    group_json = None 
    if group_id in meta_cache:
        log.info("{} found in meta cache".format(group_id))
        group_json = meta_cache[group_id]
    else:
        try:
            log.info("getS3JSONObj({})".format(group_id))
            group_json = await getS3JSONObj(app, group_id)
        except ClientError as ce:
            # key does not exist?
            is_s3obj = await isS3Obj(app, group_id)
            if is_s3obj:
                msg = "Error getting s3 obj: " + str(ce)
                log.response(request, code=500, message=msg)
                raise HttpProcessingError(code=500, message=msg)
            # not a S3 Key
            msg = "{} not found".format(group_id)
            log.response(request, code=404, message=msg)
            raise HttpProcessingError(code=404, message=msg)
        meta_cache[group_id] = group_json
    resp = await jsonResponse(request, group_json)
    log.response(request, resp=resp)
    return resp

async def POST_Group(request):
    """ Hander for POST /groups"""
    log.request(request)
    data = await request.post()
    root_id = None
    if data is not None:
        if "root" in data:
            root_id = data["root"]
            if not root_id.startswith("g-"):
                msg = "Bad createGroup request, malformed root id"
                log.response(request, code=400, mesage=msg)
                raise HttpBadRequest(message=msg)
            is_obj = await isS3Obj(app, root_id)
            if not is_obj:
                msg = "Bad createGroup request, root id does not exist"
                log.response(request, code=400, mesage=msg)
                raise HttpBadRequest(message=msg)

    group_id = createObjId("group") 
    now = int(time.time())
    if root_id is None:
        # no root_id passed, so treat this group as a root group
        root_id = group_id
        log.info("new root group id: {}".format(group_id))
    else:
        log.info("new group id: {} with root: {}".format(group_id, root_id))

    group_json = {"id": group_id, "root": root_id, "created": now, "lastModified": now, "links": [], "attributes": [] }
    await putS3JSONObj(app, group_id, group_json)  # write to S3

    resp = await jsonResponse(request, group_json, status=201)
    log.response(request, resp=resp)
    return resp
               

async def init(loop):
    """Intitialize application and return app object"""
    app = baseInit(loop, 'dn')

    #
    # call app.router.add_get() here to add node-specific routes
    #
    app.router.add_route('GET', '/domains/{key}', GET_Domain)
    app.router.add_route('PUT', '/domains/{key}', PUT_Domain)
    app.router.add_route('GET', '/groups/{id}', GET_Group)
    app.router.add_route('POST', '/groups', POST_Group)
      
    return app

#
# Main
#

if __name__ == '__main__':
    log.info("datanode start")
    loop = asyncio.get_event_loop()

    # create a client Session here so that all client requests 
    #   will share the same connection pool
    max_tcp_connections = int(config.get("max_tcp_connections"))
    client = ClientSession(loop=loop, connector=TCPConnector(limit=max_tcp_connections))

    #create the app object
    app = loop.run_until_complete(init(loop))
    app['client'] = client
    app['meta_cache'] = {}
    app['data_cache'] = {}

    # run background task
    asyncio.ensure_future(healthCheck(app), loop=loop)
   
    # run the app
    run_app(app, port=config.get("dn_port"))
