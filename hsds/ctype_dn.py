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

from aiohttp.web_exceptions import HTTPBadRequest, HTTPNotFound
from aiohttp.web_exceptions import HTTPInternalServerError
from aiohttp.web import json_response

from .util.idUtil import isValidUuid, validateUuid
from .datanode_lib import get_obj_id, get_metadata_obj, save_metadata_obj
from .datanode_lib import delete_metadata_obj, check_metadata_obj
from . import hsds_logger as log


async def GET_Datatype(request):
    """HTTP GET method to return JSON for /groups/"""
    log.request(request)
    app = request.app
    params = request.rel_url.query
    ctype_id = get_obj_id(request)

    if not isValidUuid(ctype_id, obj_class="type"):
        log.error(f"Unexpected type_id: {ctype_id}")
        raise HTTPInternalServerError()

    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = None

    ctype_json = await get_metadata_obj(app, ctype_id, bucket=bucket)

    resp_json = {}
    resp_json["id"] = ctype_json["id"]
    resp_json["root"] = ctype_json["root"]
    resp_json["created"] = ctype_json["created"]
    resp_json["lastModified"] = ctype_json["lastModified"]
    resp_json["type"] = ctype_json["type"]
    resp_json["attributeCount"] = len(ctype_json["attributes"])
    if "include_attrs" in params and params["include_attrs"]:
        resp_json["attributes"] = ctype_json["attributes"]

    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp


async def POST_Datatype(request):
    """Handler for POST /datatypes"""
    log.info("Post_Datatype")
    log.request(request)
    app = request.app
    params = request.rel_url.query

    if not request.has_body:
        msg = "POST_Datatype with no body"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    body = await request.json()
    if "bucket" in params:
        bucket = params["bucket"]
    elif "bucket" in body:
        bucket = params["bucket"]
    else:
        bucket = None

    ctype_id = get_obj_id(request, body=body)
    if not isValidUuid(ctype_id, obj_class="datatype"):
        log.error("Unexpected type_id: {ctype_id}")
        raise HTTPInternalServerError()

    # verify the id doesn't already exist
    obj_found = await check_metadata_obj(app, ctype_id, bucket=bucket)
    if obj_found:
        log.error(f"Post with existing type_id: {ctype_id}")
        raise HTTPInternalServerError()

    root_id = None

    if "root" not in body:
        msg = "POST_Datatype with no root"
        log.error(msg)
        raise HTTPInternalServerError()
    root_id = body["root"]
    try:
        validateUuid(root_id, "group")
    except ValueError:
        msg = f"Invalid root_id: {root_id}"
        log.error(msg)
        raise HTTPInternalServerError()

    if "type" not in body:
        msg = "POST_Datatype with no type"
        log.error(msg)
        raise HTTPInternalServerError()
    type_json = body["type"]

    # ok - all set, create committed type obj
    now = time.time()

    log.info(f"POST_datatype, typejson: {type_json}")

    ctype_json = {
        "id": ctype_id,
        "root": root_id,
        "created": now,
        "lastModified": now,
        "type": type_json,
        "attributes": {},
    }

    kwargs = {"bucket": bucket, "notify": True, "flush": True}
    await save_metadata_obj(app, ctype_id, ctype_json, **kwargs)

    resp_json = {}
    resp_json["id"] = ctype_id
    resp_json["root"] = root_id
    resp_json["created"] = ctype_json["created"]
    resp_json["lastModified"] = ctype_json["lastModified"]
    resp_json["attributeCount"] = 0
    resp = json_response(resp_json, status=201)

    log.response(request, resp=resp)
    return resp


async def DELETE_Datatype(request):
    """HTTP DELETE method for datatype"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    ctype_id = get_obj_id(request)
    log.info(f"DELETE ctype: {ctype_id}")

    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = app["bucket_name"]

    if not bucket:
        msg = "DELETE_Datatype - bucket not set"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    # verify the id  exist
    obj_found = await check_metadata_obj(app, ctype_id)
    if not obj_found:
        log.warn(f"Delete on non-existent obj: {ctype_id}")
        raise HTTPNotFound

    log.info("deleting ctype: {}".format(ctype_id))

    if "Notify" in params and not params["Notify"]:
        notify = False
    else:
        notify = True
    log.info(f"Delete datatype, notify: {notify}")

    await delete_metadata_obj(app, ctype_id, bucket=bucket, notify=notify)

    resp_json = {}
    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp
