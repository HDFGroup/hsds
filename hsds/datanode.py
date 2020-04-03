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
import asyncio

from aiohttp.web import run_app
from . import config
from .util.lruCache import LruCache
from .util.idUtil import isValidUuid, isSchema2Id, getCollectionForId, isRootObjId
from .basenode import healthCheck, baseInit, preStop
from . import hsds_logger as log
from .domain_dn import GET_Domain, PUT_Domain, DELETE_Domain, PUT_ACL
from .group_dn import GET_Group, POST_Group, DELETE_Group, PUT_Group, POST_Root
from .link_dn import GET_Links, GET_Link, PUT_Link, DELETE_Link
from .attr_dn import GET_Attributes, GET_Attribute, PUT_Attribute, DELETE_Attribute
from .ctype_dn import GET_Datatype, POST_Datatype, DELETE_Datatype
from .dset_dn import GET_Dataset, POST_Dataset, DELETE_Dataset, PUT_DatasetShape
from .chunk_dn import PUT_Chunk, GET_Chunk, POST_Chunk, DELETE_Chunk
from .datanode_lib import s3syncCheck
from .async_lib import scanRoot, removeKeys
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError, HTTPForbidden, HTTPBadRequest



async def init(loop):
    """Intitialize application and return app object"""
    app = baseInit(loop, 'dn')

    #
    # call app.router.add_get() here to add node-specific routes
    #
    app.router.add_route('GET', '/domains', GET_Domain)
    app.router.add_route('PUT', '/domains', PUT_Domain)
    app.router.add_route('DELETE', '/domains', DELETE_Domain)
    app.router.add_route('PUT', '/acls/{username}', PUT_ACL)
    app.router.add_route('GET', '/groups/{id}', GET_Group)
    app.router.add_route('DELETE', '/groups/{id}', DELETE_Group)
    app.router.add_route('PUT', '/groups/{id}', PUT_Group)
    app.router.add_route('POST', '/groups', POST_Group)
    app.router.add_route('GET', '/groups/{id}/links', GET_Links)
    app.router.add_route('GET', '/groups/{id}/links/{title}', GET_Link)
    app.router.add_route('DELETE', '/groups/{id}/links/{title}', DELETE_Link)
    app.router.add_route('PUT', '/groups/{id}/links/{title}', PUT_Link)
    app.router.add_route('GET', '/groups/{id}/attributes', GET_Attributes)
    app.router.add_route('GET', '/groups/{id}/attributes/{name}', GET_Attribute)
    app.router.add_route('DELETE', '/groups/{id}/attributes/{name}', DELETE_Attribute)
    app.router.add_route('PUT', '/groups/{id}/attributes/{name}', PUT_Attribute)
    app.router.add_route('GET', '/datatypes/{id}', GET_Datatype)
    app.router.add_route('DELETE', '/datatypes/{id}', DELETE_Datatype)
    app.router.add_route('POST', '/datatypes', POST_Datatype)
    app.router.add_route('GET', '/datatypes/{id}/attributes', GET_Attributes)
    app.router.add_route('GET', '/datatypes/{id}/attributes/{name}', GET_Attribute)
    app.router.add_route('DELETE', '/datatypes/{id}/attributes/{name}', DELETE_Attribute)
    app.router.add_route('PUT', '/datatypes/{id}/attributes/{name}', PUT_Attribute)
    app.router.add_route('GET', '/datasets/{id}', GET_Dataset)
    app.router.add_route('DELETE', '/datasets/{id}', DELETE_Dataset)
    app.router.add_route('POST', '/datasets', POST_Dataset)
    app.router.add_route('PUT', '/datasets/{id}/shape', PUT_DatasetShape)
    app.router.add_route('GET', '/datasets/{id}/attributes', GET_Attributes)
    app.router.add_route('GET', '/datasets/{id}/attributes/{name}', GET_Attribute)
    app.router.add_route('DELETE', '/datasets/{id}/attributes/{name}', DELETE_Attribute)
    app.router.add_route('PUT', '/datasets/{id}/attributes/{name}', PUT_Attribute)
    app.router.add_route('PUT', '/chunks/{id}', PUT_Chunk)
    app.router.add_route('GET', '/chunks/{id}', GET_Chunk)
    app.router.add_route('POST', '/chunks/{id}', POST_Chunk)
    app.router.add_route('DELETE', '/chunks/{id}', DELETE_Chunk)
    app.router.add_route("POST", '/roots/{id}', POST_Root)
    app.router.add_route("DELETE", '/prestop', preStop)


    return app

async def bucketScan(app):
    """ Scan v2 keys and update .info.json
    """

    log.info("bucketScan start")

    async_sleep_time = int(config.get("async_sleep_time"))
    log.info("async_sleep_time: {}".format(async_sleep_time))

    # update/initialize root object before starting node updates

    while True:
        if app["node_state"] != "READY":
            log.info("bucketScan waiting for Node state to be READY")
            await asyncio.sleep(async_sleep_time)
            continue  # wait for READY state

        root_scan_ids = app["root_scan_ids"]
        root_ids = {}
        # copy ids to a new map so we don't need to worry about race conditions
        for root_id in root_scan_ids:
            root_ids[root_id] = root_scan_ids[root_id]
        # remove from map
        for root_id in root_ids:
            del root_scan_ids[root_id]

        for root_id in root_ids:
            bucket = root_ids[root_id]
            log.info(f"bucketScan for: {root_id} bucket: {bucket}")
            try:
                await scanRoot(app, root_id, update=True, bucket=bucket)
            except HTTPNotFound as nfe:
                log.warn(f"bucketScan - HTTPNotFound error scanning {root_id}: {nfe}")
            except HTTPForbidden as fe:
                log.warn(f"bucketScan - HTTPForbidden error scanning {root_id}: {fe}")
            except HTTPBadRequest as bre:
                log.error(f"bucketScan - HTTPBadRequest error scanning {root_id}: {bre}")
            except HTTPInternalServerError as ise:
                log.error(f"bucketScan - HTTPInternalServer error scanning {root_id}: {ise}")
            except Exception as e:
                log.error(f"bucketScan - Unexpected exception scanning {root_id}: {e}")

        log.info(f"bucketScan - sleep: {async_sleep_time}")
        await asyncio.sleep(async_sleep_time)

    # shouldn't ever get here
    log.error("bucketScan terminating unexpectedly")

async def bucketGC(app):
    """ remove objects from db for any deleted root groups or datasets
    """
    log.info("bucketGC start")
    async_sleep_time = int(config.get("async_sleep_time"))
    log.info("async_sleep_time: {}".format(async_sleep_time))

    # update/initialize root object before starting GC

    while True:
        if app["node_state"] != "READY":
            log.info("bucketGC - waiting for Node state to be READY")
            await asyncio.sleep(async_sleep_time)
            continue  # wait for READY state

        gc_ids = app["gc_ids"]
        while len(gc_ids) > 0:
            obj_id = gc_ids.pop()
            log.info(f"got gc id: {obj_id}")
            if not isValidUuid(obj_id):
                log.error(f"bucketGC - got unexpected gc id: {obj_id}")
                continue
            if not isSchema2Id(obj_id):
                log.warn(f"bucketGC - ignoring v1 id: {obj_id}")
                continue
            if getCollectionForId(obj_id) == "groups":
                if not isRootObjId(obj_id):
                    log.error(f"bucketGC - unexpected non-root id: {obj_id}")
                    continue
                log.info(f"bucketGC - delete root objs: {obj_id}")
                await removeKeys(app, obj_id)
            elif getCollectionForId(obj_id) == "datasets":
                log.info(f"bucketGC - delete dataset: {obj_id}")
                await removeKeys(app, obj_id)
            else:
                log.error(f"bucketGC - unexpected obj_id class: {obj_id}")

        log.info(f"bucketGC - sleep: {async_sleep_time}")
        await asyncio.sleep(async_sleep_time)

    # shouldn't ever get here
    log.error("bucketGC terminating unexpectedly")

#
# Main
#

def main():
    log.info("datanode start")
    loop = asyncio.get_event_loop()

    metadata_mem_cache_size = int(config.get("metadata_mem_cache_size"))
    log.info("Using metadata memory cache size of: {}".format(metadata_mem_cache_size))
    chunk_mem_cache_size = int(config.get("chunk_mem_cache_size"))
    log.info("Using chunk memory cache size of: {}".format(chunk_mem_cache_size))

    #create the app object
    app = loop.run_until_complete(init(loop))
    app['meta_cache'] = LruCache(mem_target=metadata_mem_cache_size, chunk_cache=False)
    app['chunk_cache'] = LruCache(mem_target=chunk_mem_cache_size, chunk_cache=True)
    app['deleted_ids'] = set()
    app['dirty_ids'] = {}  # map of objids to timestamp and bucket of which they were last updated
    app['deflate_map'] = {} # map of dataset ids to deflate levels (if compressed)
    app["shuffle_map"] = {} # map of dataset ids to shuffle items size (if shuffle filter is applied)
    app["pending_s3_read"] = {} # map of s3key to timestamp for in-flight read requests
    app["pending_s3_write"] = {} # map of s3key to timestamp for in-flight write requests
    app["pending_s3_write_tasks"] = {} # map of objid to asyncio Task objects for writes
    app["root_notify_ids"] = {}   # map of root_id to bucket name used for notify root of changes in domain
    app["root_scan_ids"] = {}   # map of root_id to bucket name for pending root scans
    app["gc_ids"] = set()       # set of root or dataset ids for deletion
    app["objDelete_prefix"] = None  # used by async_lib removeKeys
    # TODO - there's nothing to prevent the deflate_map from getting ever larger
    # (though it is only one int per dataset id)
    # add a timestamp and remove at a certain time?
    # delete entire map whenver the synch queue is empty?

    # run background tasks
    try:
        task = asyncio.ensure_future(healthCheck(app), loop=loop)
    except Exception as hcEx:
        log.error(f"Failed to run health checks {hcEx}")
    finally:
        if task.done() and not task.cancelled():
            log.info("health check received {}".format(task.exception()))

    # run data sync tasks
    asyncio.ensure_future(s3syncCheck(app), loop=loop)

    # run root scan
    asyncio.ensure_future(bucketScan(app), loop=loop)

    # run root/dataset GC
    asyncio.ensure_future(bucketGC(app), loop=loop)

    # run the app
    port = int(config.get("dn_port"))
    log.info(f"run_app on port: {port}")
    run_app(app, port=port)

if __name__ == '__main__':
    main()
