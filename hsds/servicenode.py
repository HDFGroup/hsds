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
# service node of hsds cluster
#
import asyncio

from aiohttp.web import run_app
import aiohttp_cors
from .util.lruCache import LruCache

from . import config
from .basenode import healthCheck,  baseInit
from . import hsds_logger as log
from .util.authUtil import initUserDB
from .domain_sn import GET_Domain, PUT_Domain, DELETE_Domain, GET_Domains
from .domain_sn import GET_Datasets, GET_Groups, GET_Datatypes
from .domain_sn import GET_ACL, GET_ACLs, PUT_ACL
from .group_sn import GET_Group, POST_Group, DELETE_Group
from .link_sn import GET_Links, GET_Link, PUT_Link, DELETE_Link
from .attr_sn import GET_Attributes, GET_Attribute, PUT_Attribute
from .attr_sn import DELETE_Attribute, GET_AttributeValue, PUT_AttributeValue
from .ctype_sn import GET_Datatype, POST_Datatype, DELETE_Datatype
from .dset_sn import GET_Dataset, POST_Dataset, DELETE_Dataset, GET_DatasetShape, PUT_DatasetShape, GET_DatasetType
from .chunk_sn import PUT_Value, GET_Value, POST_Value


async def init(loop):
    """Intitialize application and return app object"""
    app = baseInit(loop, 'sn')

    # call app.router.add_get() here to add node-specific routes
    #
    app.router.add_route('GET', '/', GET_Domain)
    app.router.add_route('DELETE', '/', DELETE_Domain)
    app.router.add_route('PUT', '/', PUT_Domain)
    app.router.add_route('GET', '/domains', GET_Domains)
    app.router.add_route('GET', '/acls/{username}', GET_ACL)
    app.router.add_route('PUT', '/acls/{username}', PUT_ACL)
    app.router.add_route('GET', '/acls', GET_ACLs)
    app.router.add_route('GET', '/groups/{id}', GET_Group)
    app.router.add_route('GET', '/groups/', GET_Group)
    app.router.add_route('GET', '/groups', GET_Groups)
    app.router.add_route('DELETE', '/groups/{id}', DELETE_Group)
    app.router.add_route('POST', '/groups', POST_Group)
    app.router.add_route('GET', '/groups/{id}/links', GET_Links)
    app.router.add_route('GET', '/groups/{id}/links/{title}', GET_Link)
    app.router.add_route('DELETE', '/groups/{id}/links/{title}', DELETE_Link)
    app.router.add_route('PUT', '/groups/{id}/links/{title}', PUT_Link)
    app.router.add_route('GET', '/groups/{id}/attributes', GET_Attributes)
    app.router.add_route('GET', '/groups/{id}/attributes/{name}', GET_Attribute)
    app.router.add_route('DELETE', '/groups/{id}/attributes/{name}', DELETE_Attribute)
    app.router.add_route('PUT', '/groups/{id}/attributes/{name}', PUT_Attribute)
    app.router.add_route('GET', '/groups/{id}/attributes/{name}/value', GET_AttributeValue)
    app.router.add_route('PUT', '/groups/{id}/attributes/{name}/value', PUT_AttributeValue)
    app.router.add_route('GET', '/groups/{id}/acls/{username}', GET_ACL)
    app.router.add_route('PUT', '/groups/{id}/acls/{username}', PUT_ACL)
    app.router.add_route('GET', '/groups/{id}/acls', GET_ACLs)
    app.router.add_route('GET', '/datatypes/{id}', GET_Datatype)
    app.router.add_route('GET', '/datatypes/', GET_Datatype)
    app.router.add_route('GET', '/datatypes', GET_Datatypes)
    app.router.add_route('DELETE', '/datatypes/{id}', DELETE_Datatype)
    app.router.add_route('POST', '/datatypes', POST_Datatype)
    app.router.add_route('GET', '/datatypes/{id}/attributes', GET_Attributes)
    app.router.add_route('GET', '/datatypes/{id}/attributes/{name}', GET_Attribute)
    app.router.add_route('DELETE', '/datatypes/{id}/attributes/{name}', DELETE_Attribute)
    app.router.add_route('PUT', '/datatypes/{id}/attributes/{name}', PUT_Attribute)
    app.router.add_route('GET', '/datatypes/{id}/attributes/{name}/value', GET_AttributeValue)
    app.router.add_route('PUT', '/datatypes/{id}/attributes/{name}/value', PUT_AttributeValue)
    app.router.add_route('GET', '/datatypes/{id}/acls/{username}', GET_ACL)
    app.router.add_route('PUT', '/datatypes/{id}/acls/{username}', PUT_ACL)
    app.router.add_route('GET', '/datatypes/{id}/acls', GET_ACLs)
    app.router.add_route('GET', '/datasets/{id}', GET_Dataset)
    app.router.add_route('GET', '/datasets/', GET_Dataset)
    app.router.add_route('GET', '/datasets', GET_Datasets)
    app.router.add_route('DELETE', '/datasets/{id}', DELETE_Dataset)
    app.router.add_route('POST', '/datasets', POST_Dataset)
    app.router.add_route('GET', '/datasets/{id}/shape', GET_DatasetShape)
    app.router.add_route('PUT', '/datasets/{id}/shape', PUT_DatasetShape)
    app.router.add_route('GET', '/datasets/{id}/type', GET_DatasetType)
    app.router.add_route('GET', '/datasets/{id}/attributes', GET_Attributes)
    app.router.add_route('GET', '/datasets/{id}/attributes/{name}', GET_Attribute)
    app.router.add_route('DELETE', '/datasets/{id}/attributes/{name}', DELETE_Attribute)
    app.router.add_route('PUT', '/datasets/{id}/attributes/{name}', PUT_Attribute)
    app.router.add_route('GET', '/datasets/{id}/attributes/{name}/value', GET_AttributeValue)
    app.router.add_route('PUT', '/datasets/{id}/attributes/{name}/value', PUT_AttributeValue)
    app.router.add_route('PUT', '/datasets/{id}/value', PUT_Value)
    app.router.add_route('GET', '/datasets/{id}/value', GET_Value)
    app.router.add_route('POST', '/datasets/{id}/value', POST_Value)
    app.router.add_route('GET', '/datasets/{id}/acls/{username}', GET_ACL)
    app.router.add_route('PUT', '/datasets/{id}/acls/{username}', PUT_ACL)
    app.router.add_route('GET', '/datasets/{id}/acls', GET_ACLs)

    # Add CORS to all routes
    cors_domain = config.get("cors_domain")
    if cors_domain:
        cors = aiohttp_cors.setup(app, defaults={cors_domain: aiohttp_cors.ResourceOptions(allow_credentials=True, expose_headers="*", allow_headers="*",)})
        for route in list(app.router.routes()):
            log.info(f"CORS add route: {route}")
            cors.add(route)


    return app

async def start_background_tasks(app):
    loop = app['loop']
    loop.create_task(healthCheck(app))


def create_app(loop):
    """Create servicenode aiohttp application

    :param loop: The asyncio loop to use for the application
    :rtype: aiohttp.web.Application
    """
    app = loop.run_until_complete(init(loop))

    metadata_mem_cache_size = int(config.get("metadata_mem_cache_size"))
    log.info("Using metadata memory cache size of: {}".format(metadata_mem_cache_size))
    app['meta_cache'] = LruCache(mem_target=metadata_mem_cache_size, chunk_cache=False)
    app['domain_cache'] = LruCache(mem_target=metadata_mem_cache_size, chunk_cache=False)

    app['loop'] = loop
    if config.get("allow_noauth"):
        allow_noauth = config.get("allow_noauth")
        if isinstance(allow_noauth, str):
            if allow_noauth in ("0", "F", "False"):
                allow_noauth = False
            else:
                allow_noauth = True
        log.info(f"allow_noauth = {allow_noauth}")
        app['allow_noauth'] = allow_noauth
    else:
        log.info("allow_noauth = False")
        app['allow_noauth'] = False

    initUserDB(app)

    app.on_startup.append(start_background_tasks)

    return app


#
# Main
#

def main():
    log.info("Service node initializing")
    app = create_app(asyncio.get_event_loop())

    # run the app
    port = int(config.get("sn_port"))
    log.info(f"run_app on port: {port}")
    run_app(app, port=port)


if __name__ == '__main__':
    main()
