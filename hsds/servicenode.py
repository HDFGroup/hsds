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
import time
from aiohttp.web import run_app
import aiohttp_cors
from .util.lruCache import LruCache
from .util.httpUtil import isUnixDomainUrl, bindToSocket, getPortFromUrl
from .util.httpUtil import release_http_client, jsonResponse

from . import config
from .basenode import healthCheck, baseInit
from . import hsds_logger as log
from .util.authUtil import initUserDB, initGroupDB, setPassword
from .domain_sn import GET_Domain, PUT_Domain, DELETE_Domain, GET_Domains, POST_Domain
from .domain_sn import GET_Datasets, GET_Groups, GET_Datatypes
from .domain_sn import GET_ACL, GET_ACLs, PUT_ACL
from .group_sn import GET_Group, POST_Group, DELETE_Group
from .link_sn import GET_Links, POST_Links, GET_Link, PUT_Link, PUT_Links
from .link_sn import DELETE_Link, DELETE_Links
from .attr_sn import GET_Attributes, GET_Attribute, PUT_Attribute, PUT_Attributes, POST_Attributes
from .attr_sn import DELETE_Attributes, DELETE_Attribute, GET_AttributeValue, PUT_AttributeValue
from .ctype_sn import GET_Datatype, POST_Datatype, DELETE_Datatype
from .dset_sn import GET_Dataset, POST_Dataset, DELETE_Dataset
from .dset_sn import GET_DatasetShape, PUT_DatasetShape, GET_DatasetType
from .chunk_sn import PUT_Value, GET_Value, POST_Value


async def init():
    """Intitialize application and return app object"""
    app = baseInit("sn")

    # call app.router.add_get() here to add node-specific routes
    #

    #
    # domain paths
    #
    path = "/"
    app.router.add_route("GET", path, GET_Domain)
    app.router.add_route("DELETE", path, DELETE_Domain)
    app.router.add_route("PUT", path, PUT_Domain)
    app.router.add_route("POST", path, POST_Domain)

    path = "/domains"
    app.router.add_route("GET", path, GET_Domains)

    #
    # acls paths
    #
    path = "/acls/{username}"
    app.router.add_route("GET", path, GET_ACL)
    app.router.add_route("PUT", path, PUT_ACL)

    path = "/acls"
    app.router.add_route("GET", path, GET_ACLs)

    #
    # groups paths
    #
    path = "/groups/"
    app.router.add_route("GET", path, GET_Group)

    path = "/groups"
    app.router.add_route("GET", path, GET_Groups)
    app.router.add_route("POST", path, POST_Group)

    path = "/groups/{id}"
    app.router.add_route("GET", path, GET_Group)
    app.router.add_route("DELETE", path, DELETE_Group)

    path = "/groups/{id}/links"
    app.router.add_route("GET", path, GET_Links)
    app.router.add_route("POST", path, POST_Links)
    app.router.add_route("PUT", path, PUT_Links)
    app.router.add_route("DELETE", path, DELETE_Links)

    path = "/groups/{id}/links/{title}"
    app.router.add_route("GET", path, GET_Link)
    app.router.add_route("DELETE", path, DELETE_Link)
    app.router.add_route("PUT", path, PUT_Link)

    path = "/groups/{id}/attributes"
    app.router.add_route("GET", path, GET_Attributes)
    app.router.add_route("POST", path, POST_Attributes)
    app.router.add_route("PUT", path, PUT_Attributes)
    app.router.add_route("DELETE", path, DELETE_Attributes)

    path = "/groups/{id}/attributes/{name}"
    app.router.add_route("GET", path, GET_Attribute)
    app.router.add_route("DELETE", path, DELETE_Attribute)
    app.router.add_route("PUT", path, PUT_Attribute)

    path = "/groups/{id}/attributes/{name}/value"
    app.router.add_route("GET", path, GET_AttributeValue)
    app.router.add_route("PUT", path, PUT_AttributeValue)

    #
    # datatypes paths
    #
    path = "/datatypes"
    app.router.add_route("GET", path, GET_Datatypes)
    app.router.add_route("POST", path, POST_Datatype)

    path = "/datatypes/"
    app.router.add_route("GET", path, GET_Datatype)

    path = "/datatypes/{id}"
    app.router.add_route("GET", path, GET_Datatype)
    app.router.add_route("DELETE", path, DELETE_Datatype)

    path = "/datatypes/{id}/attributes"
    app.router.add_route("GET", path, GET_Attributes)
    app.router.add_route("POST", path, POST_Attributes)
    app.router.add_route("PUT", path, PUT_Attributes)
    app.router.add_route("DELETE", path, DELETE_Attributes)

    path = "/datatypes/{id}/attributes/{name}"
    app.router.add_route("GET", path, GET_Attribute)
    app.router.add_route("DELETE", path, DELETE_Attribute)
    app.router.add_route("PUT", path, PUT_Attribute)

    path = "/datatypes/{id}/attributes/{name}/value"
    app.router.add_route("GET", path, GET_AttributeValue)
    app.router.add_route("PUT", path, PUT_AttributeValue)

    #
    # datasets paths
    #
    path = "/datasets/{id}"
    app.router.add_route("GET", path, GET_Dataset)
    app.router.add_route("DELETE", path, DELETE_Dataset)

    path = "/datasets/"
    app.router.add_route("GET", path, GET_Dataset)

    path = "/datasets"
    app.router.add_route("GET", path, GET_Datasets)
    app.router.add_route("POST", path, POST_Dataset)

    path = "/datasets/{id}/shape"
    app.router.add_route("GET", path, GET_DatasetShape)
    app.router.add_route("PUT", path, PUT_DatasetShape)

    path = "/datasets/{id}/type"
    app.router.add_route("GET", path, GET_DatasetType)

    path = "/datasets/{id}/attributes"
    app.router.add_route("GET", path, GET_Attributes)
    app.router.add_route("POST", path, POST_Attributes)
    app.router.add_route("PUT", path, PUT_Attributes)
    app.router.add_route("DELETE", path, DELETE_Attributes)

    path = "/datasets/{id}/attributes/{name}"
    app.router.add_route("GET", path, GET_Attribute)
    app.router.add_route("DELETE", path, DELETE_Attribute)
    app.router.add_route("PUT", path, PUT_Attribute)

    path = "/datasets/{id}/attributes/{name}/value"
    app.router.add_route("GET", path, GET_AttributeValue)
    app.router.add_route("PUT", path, PUT_AttributeValue)

    path = "/datasets/{id}/value"
    app.router.add_route("PUT", path, PUT_Value)
    app.router.add_route("GET", path, GET_Value)
    app.router.add_route("POST", path, POST_Value)

    # Add CORS to all routes
    cors_domain = config.get("cors_domain")
    if cors_domain:
        kwargs = {
            "allow_credentials": True,
            "expose_headers": "*",
            "allow_headers": "*",
            "allow_methods": ["POST", "PUT", "GET", "DELETE"],
        }
        cors_defaults = {cors_domain: aiohttp_cors.ResourceOptions(**kwargs)}
        cors = aiohttp_cors.setup(app, defaults=cors_defaults)
        for route in list(app.router.routes()):
            cors.add(route)

    return app


async def start_background_tasks(app):
    if "is_standalone" in app:
        return  # don't need health check
    loop = asyncio.get_event_loop()
    loop.create_task(healthCheck(app))


async def on_shutdown(app):
    """Release any held resources"""
    log.info("on_shutdown")
    # finally release any http_clients
    await release_http_client(app)

    log.info("on_shutdown - done")


async def preStop(request):
    """HTTP Method used by K8s to signal the container is shutting down"""

    log.request(request)
    app = request.app

    shutdown_start = time.time()
    log.warn(f"preStop request calling on_shutdown at {shutdown_start:.2f}")
    await on_shutdown(app)
    shutdown_elapse_time = time.time() - shutdown_start
    msg = f"shutdown took: {shutdown_elapse_time:.2f} seconds"
    if shutdown_elapse_time > 2.0:
        # 2.0 is the default grace period for kubernetes
        log.warn(msg)
    else:
        log.info(msg)
    resp = await jsonResponse(request, {})
    log.response(request, resp=resp)
    return resp


def create_app():
    """Create servicenode aiohttp application"""
    log.info("service node initializing")

    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init())

    metadata_mem_cache_size = int(config.get("metadata_mem_cache_size"))
    msg = f"Using metadata memory cache size of: {metadata_mem_cache_size}"
    log.info(msg)
    kwargs = {"mem_target": metadata_mem_cache_size}
    kwargs["name"] = "MetaCache"
    app["meta_cache"] = LruCache(**kwargs)
    kwargs["name"] = "DomainCache"
    app["domain_cache"] = LruCache(**kwargs)

    if config.get("allow_noauth"):
        allow_noauth = config.get("allow_noauth")
        if isinstance(allow_noauth, str):
            if allow_noauth in ("0", "F", "False"):
                allow_noauth = False
            else:
                allow_noauth = True
        log.info(f"allow_noauth = {allow_noauth}")
        app["allow_noauth"] = allow_noauth
    else:
        log.info("allow_noauth = False")
        app["allow_noauth"] = False

    initUserDB(app)
    initGroupDB(app)

    # typically these are null
    hs_username = config.getCmdLineArg("hs_username")
    hs_password = config.getCmdLineArg("hs_password")
    if hs_username:
        log.info(f"getCmdLine hs_username: {hs_username}")
    if hs_password:
        log.info(f"getCmdLine hs_password: {'*' * len(hs_password)}")
    if hs_username:
        setPassword(app, hs_username, hs_password)

    app.on_startup.append(start_background_tasks)
    app.on_shutdown.append(on_shutdown)

    return app


def main():
    """
    Main - entry point for service node
    """
    log.info("Service node initializing")
    app = create_app()

    # run app using either socket or tcp
    sn_url = config.getCmdLineArg("sn_url")
    if sn_url:
        sn_port = getPortFromUrl(sn_url)
    else:
        # create TCP url based on port address
        sn_port = int(config.get("sn_port"))
        sn_url = f"http://localhost:{sn_port}"

    if isUnixDomainUrl(sn_url):
        print("binding to socket:", sn_url)
        try:
            s = bindToSocket(sn_url)
        except OSError as oe:
            log.error(f"unable to find to socket: {oe}")
            raise
        except ValueError as ve:
            log.error(f"unable to find to socket: {ve}")
            raise
        try:
            run_app(app, sock=s, handle_signals=True)
        except KeyboardInterrupt:
            log.info("got keyboard interrupt")
        except SystemExit:
            log.info("got system exit")
        except Exception as e:
            log.error(f"got exception: {e}")
        log.info("run_app done")
        # close socket?
    else:
        # Use TCP connection
        log.info(f"run_app on port: {sn_port}")
        run_app(app, port=sn_port)

    log.info("Service node exiting")


if __name__ == "__main__":
    main()
