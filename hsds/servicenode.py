#
# service node of hsds cluster
# 
import asyncio
import uuid
import json
import time
import sys

from aiohttp.web import Application, Response, StreamResponse, run_app
from aiohttp import log, ClientSession, TCPConnector 
from aiohttp.errors import HttpBadRequest, ClientOSError
 

import config
from timeUtil import unixTimeToUTC, elapsedTime
from hsdsUtil import http_get, isOK, http_post
from basenode import register, healthCheck, info, baseInit


async def init(loop):
    """Intitialize application and return app object"""
    app = baseInit(loop, 'sn')

    #
    # call app.router.add_get() here to add node-specific routes
    #
      
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

    # run background task
    asyncio.ensure_future(healthCheck(app), loop=loop)
   
    # run the app
    run_app(app, port=config.get("sn_port"))
