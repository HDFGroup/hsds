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

import time
import asyncio

from aiohttp.web_exceptions import HTTPBadRequest, HTTPInternalServerError
from aiohttp.web_exceptions import HTTPNotFound, HTTPServiceUnavailable
from aiohttp.web import json_response

from .util.idUtil import isValidUuid, isSchema2Id, isRootObjId, getRootObjId
from .datanode_lib import get_obj_id, check_metadata_obj, get_metadata_obj
from .datanode_lib import save_metadata_obj, delete_metadata_obj
from . import hsds_logger as log
from . import config


async def GET_Group(request):
    """HTTP GET method to return JSON for /groups/"""
    log.request(request)
    app = request.app
    params = request.rel_url.query
    group_id = get_obj_id(request)
    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = None
    log.info(f"GET group: {group_id} bucket: {bucket}")

    if not isValidUuid(group_id, obj_class="group"):
        log.error(f"Unexpected group_id: {group_id}")
        raise HTTPInternalServerError()

    group_json = await get_metadata_obj(app, group_id, bucket=bucket)

    resp_json = {}
    resp_json["id"] = group_json["id"]
    resp_json["root"] = group_json["root"]
    resp_json["created"] = group_json["created"]
    resp_json["lastModified"] = group_json["lastModified"]
    resp_json["linkCount"] = len(group_json["links"])
    resp_json["attributeCount"] = len(group_json["attributes"])

    if "include_links" in params and params["include_links"]:
        resp_json["links"] = group_json["links"]
    if "include_attrs" in params and params["include_attrs"]:
        resp_json["attributes"] = group_json["attributes"]

    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp


async def POST_Group(request):
    """Handler for POST /groups"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    if not request.has_body:
        msg = "POST_Group with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    body = await request.json()
    if "bucket" in params:
        bucket = params["bucket"]
    elif "bucket" in body:
        bucket = params["bucket"]
    else:
        bucket = None

    group_id = get_obj_id(request, body=body)

    log.info(f"POST group: {group_id} bucket: {bucket}")
    if not isValidUuid(group_id, obj_class="group"):
        log.error(f"Unexpected group_id: {group_id}")
        raise HTTPInternalServerError()
    if "root" not in body:
        msg = "POST_Group with no root"
        log.error(msg)
        raise HTTPInternalServerError()

    # verify the id doesn't already exist
    obj_found = await check_metadata_obj(app, group_id, bucket=bucket)
    if obj_found:
        log.error(f"Post with existing group_id: {group_id}")
        raise HTTPInternalServerError()

    root_id = body["root"]

    if not isValidUuid(root_id, obj_class="group"):
        msg = "Invalid root_id: " + root_id
        log.error(msg)
        raise HTTPInternalServerError()

    # ok - all set, create group obj
    now = time.time()

    group_json = {
        "id": group_id,
        "root": root_id,
        "created": now,
        "lastModified": now,
        "links": {},
        "attributes": {},
    }

    kwargs = {"bucket": bucket, "notify": True, "flush": True}
    await save_metadata_obj(app, group_id, group_json, **kwargs)

    # formulate response
    resp_json = {}
    resp_json["id"] = group_id
    resp_json["root"] = root_id
    resp_json["created"] = group_json["created"]
    resp_json["lastModified"] = group_json["lastModified"]
    resp_json["linkCount"] = 0
    resp_json["attributeCount"] = 0

    resp = json_response(resp_json, status=201)
    log.response(request, resp=resp)
    return resp


async def PUT_Group(request):
    """Handler for PUT /groups
    Used to flush all objects under a root group to S3
    """

    flush_time_out = config.get("s3_sync_interval") * 2
    flush_sleep_interval = config.get("flush_sleep_interval")
    log.request(request)
    app = request.app
    params = request.rel_url.query

    root_id = request.match_info.get("id")
    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = None
    log.info(f"PUT group (flush): {root_id}  bucket: {bucket}")
    # don't really need bucket param since the dirty ids know which bucket
    # they should write too

    if not isValidUuid(root_id, obj_class="group"):
        log.error(f"Unexpected group_id: {root_id}")
        raise HTTPInternalServerError()

    schema2 = isSchema2Id(root_id)

    if schema2 and not isRootObjId(root_id):
        log.error(f"Expected root id for flush but got: {root_id}")
        raise HTTPInternalServerError()

    flush_start = time.time()
    flush_set = set()
    dirty_ids = app["dirty_ids"]

    for obj_id in dirty_ids:
        if schema2:
            if isValidUuid(obj_id) and getRootObjId(obj_id) == root_id:
                flush_set.add(obj_id)
        else:
            # for schema1 not easy to determine if a given id is in a
            # domain, so just wait on all of them
            flush_set.add(obj_id)

    log.debug(f"flushop - waiting on {len(flush_set)} items")

    if len(flush_set) > 0:
        while time.time() - flush_start < flush_time_out:
            await asyncio.sleep(flush_sleep_interval)  # wait a bit
            # check to see if the items in our flush set are still there
            remaining_set = set()
            for obj_id in flush_set:
                if obj_id not in dirty_ids:
                    log.debug(f"flush - {obj_id} has been written")
                elif dirty_ids[obj_id][0] > flush_start:
                    msg = f"flush - {obj_id} has been updated after "
                    msg += "flush start"
                    log.debug(msg)
                else:
                    log.debug(f"flush - {obj_id} still pending")
                    remaining_set.add(obj_id)
            flush_set = remaining_set
            if len(flush_set) == 0:
                log.debug("flush op - all objects have been written")
                break
            msg = f"flushop - {len(flush_set)} item remaining, sleeping "
            msg += f"for {flush_sleep_interval}"
            log.debug(msg)

    if len(flush_set) > 0:
        msg = f"flushop - {len(flush_set)} items not updated after "
        msg += f"{flush_time_out} seconds"
        log.warn(msg)
        log.debug(f"flush set: {flush_set}")
        raise HTTPServiceUnavailable()

    rsp_json = {"id": app["id"]}  # return the node id
    log.debug(f"flush returning: {rsp_json}")
    resp = json_response(rsp_json, status=200)
    log.response(request, resp=resp)
    return resp


async def DELETE_Group(request):
    """HTTP DELETE method for /groups/"""
    log.request(request)
    app = request.app
    params = request.rel_url.query
    group_id = get_obj_id(request)

    if not isValidUuid(group_id, obj_class="group"):
        log.error(f"Unexpected group_id: {group_id}")
        raise HTTPInternalServerError()

    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = app["bucket_name"]

    if not bucket:
        msg = "DELETE_Group - bucket not set"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    log.info(f"DELETE group: {group_id} bucket: {bucket}")

    # verify the id exist
    obj_found = await check_metadata_obj(app, group_id, bucket=bucket)
    if not obj_found:
        log.debug(f"delete called on non-exsistet obj: {group_id}")
        raise HTTPNotFound()

    log.debug(f"deleting group: {group_id}, bucket:{bucket}")

    if "Notify" in params and not params["Notify"]:
        notify = False
    else:
        notify = True

    await delete_metadata_obj(app, group_id, bucket=bucket, notify=notify)

    resp_json = {}

    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp


async def POST_Root(request):
    """Notify root that content in the domain has been modified."""
    log.request(request)
    app = request.app
    root_id = request.match_info.get("id")
    if not root_id:
        log.error("missing id in request")
        raise HTTPInternalServerError()
    if not isSchema2Id(root_id):
        log.error(f"expected schema2 id but got: {root_id}")
        raise HTTPInternalServerError()
    if not isRootObjId(root_id):
        log.error(f"Expected root id but got: {root_id}")
        raise HTTPInternalServerError()
    params = request.rel_url.query
    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = None

    log.info(f"POST_Root: {root_id} bucket: {bucket}")

    # add id to be scanned by the s3sync task
    root_scan_ids = app["root_scan_ids"]
    root_scan_ids[root_id] = (bucket, time.time())

    resp_json = {}

    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp
