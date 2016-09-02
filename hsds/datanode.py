#
# data node of hsds cluster
# 
import asyncio
import uuid
import json
import time
import sys

from aiohttp.web import Application, Response, StreamResponse, run_app
from aiohttp import log, ClientSession, TCPConnector 
from aiohttp.errors import HttpBadRequest, ClientOSError
from botocore.exceptions import ClientError
 

import config
from timeUtil import unixTimeToUTC, elapsedTime
from hsdsUtil import http_get, isOK, http_post, getS3Key, getS3Partition, getS3JSONObj, putS3JSONObj, isS3Obj, getRootTocUuid
from basenode import register, healthCheck, info, baseInit


async def getGroup(request):
    """HTTP method to return JSON for group"""
    group_id = request.match_info.get('id')
    print("getGroup:", group_id)
    app = request.app
    if getS3Partition(group_id, app['node_count']) != app['node_number']:
        # The request shouldn't have come to this node'
        raise HttpBadRequest(message="wrong node for 'id':{}".format(group_id))

    meta_cache = app['meta_cache'] 
    group_json = None 
    if group_id in meta_cache:
        group_json = meta_cache[group_id]
    else:
        try:
            print("{} found in meta cache".format(group_id))
            group_json = await getS3JSONObj(app, group_id)
        except ClientError as ce:
            # key doesn not exist?
            is_s3obj = await isS3Obj(app, group_id)
            if is_s3obj:
                msg = "Error getting s3 obj: " + str(ce)
                print(msg)
                raise HttpProcessingError(code=500, message=msg)
            # not a S3 Key
            if group_id == getRootTocUuid():
                print("TOC group uuid not found, initializing TOC Root for this bucket")
                now = time.time()
                group_json = {"id": group_id, "created": now, "lastModified": now, "links": [], "attributes": [] }
                await putS3JSONObj(app, group_id, group_json)  # write to S3
            else:
                msg = "{} not found".format(group_id)
                print("Status 404: " + msg)
                raise HttpProcessingError(code=404, message=msg)
        meta_cache[group_id] = group_json

    
    #group_id == getRootTocUuid()
    
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
     
    answer = json.dumps(group_json)
    answer = answer.encode('utf8')
    resp.content_length = len(answer)
    await resp.prepare(request)
    resp.write(answer)
    await resp.write_eof()
    return resp



async def init(loop):
    """Intitialize application and return app object"""
    app = baseInit(loop, 'dn')

    #
    # call app.router.add_get() here to add node-specific routes
    #
    app.router.add_get('/groups/{id}', getGroup)
      
    return app

#
# Main
#

if __name__ == '__main__':

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
