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
import time
import traceback
from aiohttp.web import run_app

from . import config
from .util.lruCache import LruCache
from .util.idUtil import isValidUuid, isSchema2Id, getCollectionForId
from .util.idUtil import isRootObjId
from .util.httpUtil import isUnixDomainUrl, bindToSocket, getPortFromUrl
from .util.httpUtil import jsonResponse, release_http_client
from .util.storUtil import setBloscThreads, getBloscThreads
from .basenode import healthCheck, baseInit
from . import hsds_logger as log
from .domain_dn import GET_Domain, PUT_Domain, DELETE_Domain, PUT_ACL
from .group_dn import GET_Group, POST_Group, DELETE_Group, PUT_Group
from .group_dn import POST_Root
from .link_dn import GET_Links, GET_Link, PUT_Link, DELETE_Link
from .attr_dn import GET_Attributes, GET_Attribute, PUT_Attribute
from .attr_dn import DELETE_Attribute
from .ctype_dn import GET_Datatype, POST_Datatype, DELETE_Datatype
from .dset_dn import GET_Dataset, POST_Dataset, DELETE_Dataset
from .dset_dn import PUT_DatasetShape
from .chunk_dn import PUT_Chunk, GET_Chunk, POST_Chunk, DELETE_Chunk
from .datanode_lib import s3syncCheck
from .async_lib import scanRoot, removeKeys
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError
from aiohttp.web_exceptions import HTTPForbidden, HTTPBadRequest


async def init():
    """Intitialize application and return app object"""
    app = baseInit("dn")

    #
    # call app.router.add_get() here to add node-specific routes
    #
    app.router.add_route("GET", "/domains", GET_Domain)
    app.router.add_route("PUT", "/domains", PUT_Domain)
    app.router.add_route("DELETE", "/domains", DELETE_Domain)
    app.router.add_route("PUT", "/acls/{username}", PUT_ACL)
    app.router.add_route("GET", "/groups/{id}", GET_Group)
    app.router.add_route("DELETE", "/groups/{id}", DELETE_Group)
    app.router.add_route("PUT", "/groups/{id}", PUT_Group)
    app.router.add_route("POST", "/groups", POST_Group)
    app.router.add_route("GET", "/groups/{id}/links", GET_Links)
    app.router.add_route("GET", "/groups/{id}/links/{title}", GET_Link)
    app.router.add_route("DELETE", "/groups/{id}/links/{title}", DELETE_Link)
    app.router.add_route("PUT", "/groups/{id}/links/{title}", PUT_Link)
    app.router.add_route("GET", "/groups/{id}/attributes", GET_Attributes)
    app.router.add_route("GET", "/groups/{id}/attributes/{name}", GET_Attribute)
    app.router.add_route("DELETE", "/groups/{id}/attributes/{name}", DELETE_Attribute)
    app.router.add_route("PUT", "/groups/{id}/attributes/{name}", PUT_Attribute)
    app.router.add_route("GET", "/datatypes/{id}", GET_Datatype)
    app.router.add_route("DELETE", "/datatypes/{id}", DELETE_Datatype)
    app.router.add_route("POST", "/datatypes", POST_Datatype)
    app.router.add_route("GET", "/datatypes/{id}/attributes", GET_Attributes)
    app.router.add_route("GET", "/datatypes/{id}/attributes/{name}", GET_Attribute)
    app.router.add_route(
        "DELETE", "/datatypes/{id}/attributes/{name}", DELETE_Attribute
    )
    app.router.add_route("PUT", "/datatypes/{id}/attributes/{name}", PUT_Attribute)
    app.router.add_route("GET", "/datasets/{id}", GET_Dataset)
    app.router.add_route("DELETE", "/datasets/{id}", DELETE_Dataset)
    app.router.add_route("POST", "/datasets", POST_Dataset)
    app.router.add_route("PUT", "/datasets/{id}/shape", PUT_DatasetShape)
    app.router.add_route("GET", "/datasets/{id}/attributes", GET_Attributes)
    app.router.add_route("GET", "/datasets/{id}/attributes/{name}", GET_Attribute)
    app.router.add_route("DELETE", "/datasets/{id}/attributes/{name}", DELETE_Attribute)
    app.router.add_route("PUT", "/datasets/{id}/attributes/{name}", PUT_Attribute)
    app.router.add_route("PUT", "/chunks/{id}", PUT_Chunk)
    app.router.add_route("GET", "/chunks/{id}", GET_Chunk)
    app.router.add_route("POST", "/chunks/{id}", POST_Chunk)
    app.router.add_route("DELETE", "/chunks/{id}", DELETE_Chunk)
    app.router.add_route("POST", "/roots/{id}", POST_Root)
    app.router.add_route("DELETE", "/prestop", preStop)

    return app


async def bucketScan(app):
    """Scan v2 keys and update .info.json"""
    log.info("bucketScan start")

    async_sleep_time = int(config.get("async_sleep_time"))
    short_sleep_time = float(async_sleep_time) / 10.0
    scan_wait_time = async_sleep_time  # default to ~1min
    log.info(f"scan_wait_time: {scan_wait_time}")
    last_action = time.time()  # keep track of the last time any work was done

    # update/initialize root object before starting node updates

    while True:
        if app["node_state"] != "READY":
            log.info("bucketScan waiting for Node state to be READY")
            await asyncio.sleep(async_sleep_time)
            continue  # wait for READY state

        root_scan_ids = app["root_scan_ids"]
        root_ids = {}
        now = time.time()
        # copy ids to a new map so we don't need to worry about
        # race conditions
        for root_id in root_scan_ids:
            # bucket and timestamp in tuple
            item = root_scan_ids[root_id]
            bucket = item[0]
            timestamp = item[1]
            msg = f"root_scan id {root_id}: bucket: {bucket} "
            msg += f"timestamp: {timestamp}"
            log.info(msg)
            if now - timestamp > scan_wait_time:
                log.info(f"add {root_id} to scan list")
                root_ids[root_id] = bucket
            else:
                msg = f"waiting for {root_id} to age before "
                msg += "adding to scan list"
                log.debug(msg)
        # remove from map
        for root_id in root_ids:
            del root_scan_ids[root_id]

        for root_id in root_ids:
            bucket = root_ids[root_id]
            log.info(f"bucketScan for: {root_id} bucket: {bucket}")
            try:
                await scanRoot(app, root_id, update=True, bucket=bucket)
            except HTTPNotFound as nfe:
                msg = f"bucketScan - HTTPNotFound error scanning {root_id}: "
                msg += f"{nfe}"
                log.warn(msg)
            except HTTPForbidden as fe:
                msg = f"bucketScan - HTTPForbidden error scanning {root_id}: "
                msg += f"{fe}"
                log.warn(msg)
            except HTTPBadRequest as bre:
                msg = f"bucketScan - HTTPBadRequest error scanning {root_id}: "
                msg += f"{bre}"
                log.error(msg)
                tb = traceback.format_exc()
                print("traceback:", tb)
            except HTTPInternalServerError as ise:
                msg = "bucketScan - HTTPInternalServer error scanning "
                msg += f"{root_id}: {ise}"
                log.error(msg)
                tb = traceback.format_exc()
                print("traceback:", tb)
            except Exception as e:
                msg = "bucketScan - Unexpected exception scanning "
                msg += f"{root_id}: {e}"
                log.error(msg)
                tb = traceback.format_exc()
                print("traceback:", tb)

            last_action = time.time()

        now = time.time()
        if (now - last_action) > async_sleep_time:
            sleep_time = async_sleep_time  # long nap
        else:
            sleep_time = short_sleep_time  # shot nap

        log.info(f"bucketScan - sleep: {sleep_time}")
        await asyncio.sleep(sleep_time)

    # shouldn't ever get here
    log.error("bucketScan terminating unexpectedly")


def get_gc_count(app):
    """Return number of items in gc queue"""
    count = 0
    gc_buckets = app["gc_buckets"]
    for bucket in gc_buckets:
        log.debug(f"gc_count - getting count for bucket: {bucket}")
        gc_ids = gc_buckets[bucket]
        count += len(gc_ids)
    return count


async def bucketGC(app):
    """remove objects from db for any deleted root groups or datasets"""
    async_sleep_time = int(config.get("async_sleep_time"))
    log.info(f"bucketGC start - async_sleep_time: {async_sleep_time}")

    # update/initialize root object before starting GC

    while True:
        if app["node_state"] not in ("READY", "TERMINATING"):
            log.info("bucketGC - waiting for Node state to be READY")
            await asyncio.sleep(async_sleep_time)
            continue  # wait for READY state

        gc_buckets = app["gc_buckets"]
        for bucket in gc_buckets:
            log.debug(f"bucketGC - getting keys for bucket: {bucket}")
            gc_ids = gc_buckets[bucket]
            while len(gc_ids) > 0:
                obj_id = gc_ids.pop()
                log.info(f"got gc id: {obj_id}")
                if not isValidUuid(obj_id):
                    log.error(f"bucketGC - got unexpected gc id: {bucket}/{obj_id}")
                    continue
                if not isSchema2Id(obj_id):
                    log.warn(f"bucketGC - ignoring v1 id: {bucket}/{obj_id}")
                    continue
                if getCollectionForId(obj_id) == "groups":
                    if not isRootObjId(obj_id):
                        log.error(
                            f"bucketGC - unexpected non-root id: {bucket}/{obj_id}"
                        )
                        continue
                    log.info(f"bucketGC - delete root objs: {bucket}/{obj_id}")
                    await removeKeys(app, obj_id, bucket=bucket)
                elif getCollectionForId(obj_id) == "datasets":
                    log.info(f"bucketGC - delete dataset: {bucket}/{obj_id}")
                    await removeKeys(app, obj_id, bucket=bucket)
                else:
                    log.error(f"bucketGC - unexpected obj_id class: {bucket}/{obj_id}")

        log.info(f"bucketGC - sleep: {async_sleep_time}")
        await asyncio.sleep(async_sleep_time)

    # shouldn't ever get here
    log.error("bucketGC terminating unexpectedly")


async def start_background_tasks(app):
    loop = asyncio.get_event_loop()

    if "is_standalone" not in app:
        loop.create_task(healthCheck(app))

    if "is_readonly" not in app:
        # run data sync tasks
        loop.create_task(s3syncCheck(app))

        # run root scan
        loop.create_task(bucketScan(app))

        # run root/dataset GC
        loop.create_task(bucketGC(app))


def create_app():
    """Create datanode aiohttp application

    :param loop: The asyncio loop to use for the application
    :rtype: aiohttp.web.Application
    """

    log.info("data node initializing")

    metadata_mem_cache_size = int(config.get("metadata_mem_cache_size"))
    msg = f"Using metadata memory cache size of: {metadata_mem_cache_size}"
    log.debug(msg)
    metadata_mem_cache_expire = int(config.get("metadata_mem_cache_expire"))
    msg = f"Setting metadata cache expire time to: {metadata_mem_cache_expire}"
    log.debug(msg)
    chunk_mem_cache_size = int(config.get("chunk_mem_cache_size"))
    log.debug(f"Using chunk memory cache size of: {chunk_mem_cache_size}")
    chunk_mem_cache_expire = int(config.get("chunk_mem_cache_expire"))
    log.debug(f"Setting chunk cache expire time to: {chunk_mem_cache_expire}")
    blosc_nthreads = int(config.get("blosc_nthreads"))
    if blosc_nthreads > 0:
        log.debug(f"Setting blosc nthreads to: {blosc_nthreads}")
        setBloscThreads(blosc_nthreads)
    else:
        # let Blosc select thread count based on processor type
        blosc_nthreads = getBloscThreads()
        log.debug(f"Using default blosc nthreads: {blosc_nthreads}")

    # create the app object
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init())
    kwargs = {
        "mem_target": metadata_mem_cache_size,
        "name": "MetaCache",
        "expire_time": metadata_mem_cache_expire,
    }
    app["meta_cache"] = LruCache(**kwargs)
    kwargs = {
        "mem_target": chunk_mem_cache_size,
        "name": "ChunkCache",
        "expire_time": chunk_mem_cache_expire,
    }
    app["chunk_cache"] = LruCache(**kwargs)
    app["deleted_ids"] = set()
    # map of objids to timestamp and bucket of which they were last updated
    app["dirty_ids"] = {}
    # map of dataset ids to deflate levels (if compressed)
    app["filter_map"] = {}
    # map of objid to timestamp for in-flight read requests
    app["pending_s3_read"] = {}
    # map of objid to timestamp for in-flight write requests
    app["pending_s3_write"] = {}
    # map of objid to asyncio Task objects for writes
    app["pending_s3_write_tasks"] = {}
    # map of root_id to bucket name used for notify root of changes in domain
    app["root_notify_ids"] = {}
    # map of root_id to bucket name for pending root scans
    app["root_scan_ids"] = {}
    # set of root or dataset ids for deletion
    app["gc_buckets"] = {}
    app["objDelete_prefix"] = None  # used by async_lib removeKeys

    # TODO - there's nothing to prevent the deflate_map from getting
    # ever larger
    # (though it is only one int per dataset id)
    # add a timestamp and remove at a certain time?
    # delete entire map whenver the synch queue is empty?

    # run background tasks
    app.on_startup.append(start_background_tasks)
    # set method to run when app is being terminated
    app.on_shutdown.append(on_shutdown)

    return app


async def on_shutdown(app):
    """Release any held resources"""
    log.info("on_shutdown - setting node_state to TERMINATING")
    app["node_state"] = "TERMINATING"
    s3_sync_interval = config.get("s3_sync_interval")
    sleep_interval = float(s3_sync_interval) / 4.0
    pending_s3_write_tasks = app["pending_s3_write_tasks"]

    # wait for s3sync queue to drain
    while True:
        pending_write_count = len(pending_s3_write_tasks)
        if pending_write_count == 0:
            log.debug("on_shutdown - no pending write tasks")
            break
        msg = f"on_shutdown - waiting on {pending_write_count} write tasks "
        msg += f"sleeping for {sleep_interval:.2f}"
        log.warning(msg)
        await asyncio.sleep(sleep_interval)

    # wait on gc tasks to complete
    while True:
        gc_count = get_gc_count(app)
        if gc_count == 0:
            log.debug("on_shutdown - no gc items")
            break
        msg = f"on_shutdown - waiting on {gc_count} gc tasks "
        msg += f"sleeping for {sleep_interval:.2f}"
        log.warning(msg)
        await asyncio.sleep(sleep_interval)

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


#
# Main
#


def main():
    log.info("Data node initializing")
    app = create_app()

    # run app using either socket or tcp

    if app["dn_urls"] and app["node_number"] >= 0:
        dn_urls = app["dn_urls"]
        node_number = app["node_number"]
        if node_number >= len(dn_urls):
            msg = f"Invalid node_number: {node_number} "
            msg += f"must be less than {len(dn_urls)}"
            msg += f" dn_urls: {dn_urls}"
            raise ValueError(msg)
        dn_url = dn_urls[node_number]
        dn_port = getPortFromUrl(dn_url)
    else:
        dn_port = int(config.get("dn_port"))
        dn_url = f"http://localhost:{dn_port}"

    if isUnixDomainUrl(dn_url):
        try:
            s = bindToSocket(dn_url)
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
        log.info(f"run_app on port: {dn_port}")
        run_app(app, port=dn_port)

    log.info("datanode exiting")


if __name__ == "__main__":
    main()
