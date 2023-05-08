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
# attribute handling routines
#
import time
from bisect import bisect_left

from aiohttp.web_exceptions import HTTPBadRequest, HTTPConflict, HTTPNotFound
from aiohttp.web_exceptions import HTTPInternalServerError
from aiohttp.web import json_response

from .util.attrUtil import validateAttributeName
from .datanode_lib import get_obj_id, get_metadata_obj, save_metadata_obj
from . import hsds_logger as log


def _index(items, marker, create_order=False):
    """Locate the leftmost value exactly equal to x"""
    if create_order:
        # list is not ordered, juse search linearly
        for i in range(len(items)):
            if items[i] == marker:
                return i
    else:
        i = bisect_left(items, marker)
        if i != len(items) and items[i] == marker:
            return i
    # not found
    return -1


async def GET_Attributes(request):
    """ Return JSON for attribute collection
    """
    log.request(request)
    app = request.app
    params = request.rel_url.query

    obj_id = get_obj_id(request)
    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = None

    create_order = False
    if "CreateOrder" in params and params["CreateOrder"]:
        create_order = True

    include_data = False
    if "IncludeData" in params and params["IncludeData"]:
        include_data = True

    limit = None
    if "Limit" in params:
        try:
            limit = int(params["Limit"])
            log.info("GET_Links - using Limit: {}".format(limit))
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.error(msg)  # should be validated by SN
            raise HTTPInternalServerError()

    marker = None
    if "Marker" in params:
        marker = params["Marker"]
        log.info(f"GET_Links - using Marker: {marker}")

    obj_json = await get_metadata_obj(app, obj_id, bucket=bucket)

    log.debug(f"GET attributes obj_id: {obj_id} got json")
    if "attributes" not in obj_json:
        msg = f"unexpected data for obj id: {obj_id}"
        msg.error(msg)
        raise HTTPInternalServerError()

    # return a list of attributes based on sorted dictionary keys
    attr_dict = obj_json["attributes"]

    titles = []
    if create_order:
        order_dict = {}
        for title in attr_dict:
            item = attr_dict[title]
            if "created" not in item:
                log.warning(f"expected to find 'created' key in attr item {title}")
                continue
            order_dict[title] = item["created"]
        log.debug(f"order_dict: {order_dict}")
        # now sort by created
        for k in sorted(order_dict.items(), key=lambda item: item[1]):
            titles.append(k[0])
        log.debug(f"attrs by create order: {titles}")
    else:
        titles = list(attr_dict.keys())
        titles.sort()  # sort by key
        log.debug(f"attrs by lexographic order: {titles}")

    start_index = 0
    if marker is not None:
        start_index = _index(titles, marker, create_order=create_order) + 1
        if start_index == 0:
            # marker not found, return 404
            msg = f"attribute marker: {marker}, not found"
            log.warn(msg)
            raise HTTPNotFound()

    end_index = len(titles)
    if limit is not None and (end_index - start_index) > limit:
        end_index = start_index + limit

    attr_list = []
    for i in range(start_index, end_index):
        attr_name = titles[i]
        src_attr = attr_dict[attr_name]
        des_attr = {}
        des_attr["created"] = src_attr["created"]
        des_attr["type"] = src_attr["type"]
        des_attr["shape"] = src_attr["shape"]
        des_attr["name"] = attr_name
        if include_data:
            des_attr["value"] = src_attr["value"]
        attr_list.append(des_attr)

    resp_json = {"attributes": attr_list}
    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp


async def GET_Attribute(request):
    """HTTP GET method to return JSON for /(obj)/<id>/attributes/<name>
    """
    log.request(request)
    app = request.app
    params = request.rel_url.query

    obj_id = get_obj_id(request)

    attr_name = request.match_info.get('name')
    validateAttributeName(attr_name)
    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = None

    obj_json = await get_metadata_obj(app, obj_id, bucket=bucket)
    msg = f"GET attribute obj_id: {obj_id} name: {attr_name} bucket: {bucket}"
    log.info(msg)
    log.debug(f"got obj_json: {obj_json}")

    if "attributes" not in obj_json:
        log.error(f"unexpected obj data for id: {obj_id}")
        raise HTTPInternalServerError()

    attributes = obj_json["attributes"]
    if attr_name not in attributes:
        msg = f"Attribute  '{attr_name}' not found for id: {obj_id}"
        log.warn(msg)
        raise HTTPNotFound()

    attr_json = attributes[attr_name]

    resp = json_response(attr_json)
    log.response(request, resp=resp)
    return resp


async def PUT_Attribute(request):
    """ Handler for PUT /(obj)/<id>/attributes/<name>
    """
    log.request(request)
    app = request.app
    params = request.rel_url.query
    obj_id = get_obj_id(request)

    attr_name = request.match_info.get('name')
    log.info("PUT attribute {} in {}".format(attr_name, obj_id))
    validateAttributeName(attr_name)

    if not request.has_body:
        log.error("PUT_Attribute with no body")
        raise HTTPBadRequest(message="body expected")

    body = await request.json()
    if "bucket" in params:
        bucket = params["bucket"]
    elif "bucket" in body:
        bucket = params["bucket"]
    else:
        bucket = None

    replace = False
    if "replace" in params and params["replace"]:
        replace = True
        log.info("replace attribute")
    datatype = None
    shape = None
    value = None

    if "type" not in body:
        log.error("PUT attribute with no type in body")
        raise HTTPInternalServerError()

    datatype = body["type"]

    if "shape" not in body:
        log.error("PUT attribute with no shape in body")
        raise HTTPInternalServerError()
    shape = body["shape"]

    if "value" in body:
        value = body["value"]

    obj_json = await get_metadata_obj(app, obj_id, bucket=bucket)
    log.debug(f"PUT attribute obj_id: {obj_id} bucket: {bucket} got json")

    if "attributes" not in obj_json:
        log.error(f"unexpected obj data for id: {obj_id}")
        raise HTTPInternalServerError()

    attributes = obj_json["attributes"]
    if attr_name in attributes and not replace:
        # Attribute already exists, return a 409
        msg = f"Attempt to overwrite attribute: {attr_name} "
        msg += f"in obj_id: {obj_id}"
        log.warn(msg)
        raise HTTPConflict()

    if replace and attr_name not in attributes:
        # Replace requires attribute exists
        msg = f"Attempt to update missing attribute: {attr_name} "
        msg += f"in obj_id: {obj_id}"
        log.warn()
        raise HTTPNotFound()

    if replace:
        orig_attr = attributes[attr_name]
        create_time = orig_attr["created"]
    else:
        create_time = time.time()

    # ok - all set, create attribute obj
    attr_json = {"type": datatype,
                 "shape": shape,
                 "value": value,
                 "created": create_time}
    attributes[attr_name] = attr_json

    # write back to S3, save to metadata cache
    await save_metadata_obj(app, obj_id, obj_json, bucket=bucket)

    resp_json = {}

    resp = json_response(resp_json, status=201)
    log.response(request, resp=resp)
    return resp


async def DELETE_Attribute(request):
    """HTTP DELETE method for /(obj)/<id>/attributes/<name>
    """
    log.request(request)
    app = request.app
    params = request.rel_url.query

    obj_id = get_obj_id(request)
    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = None

    attr_name = request.match_info.get('name')
    log.info(f"DELETE attribute {attr_name} in {obj_id} bucket: {bucket}")
    validateAttributeName(attr_name)

    obj_json = await get_metadata_obj(app, obj_id, bucket=bucket)

    log.debug(f"DELETE attribute obj_id: {obj_id} got json")
    if "attributes" not in obj_json:
        msg = f"unexpected data for obj id: {obj_id}"
        msg.error(msg)
        raise HTTPInternalServerError()

    # return a list of attributes based on sorted dictionary keys
    attributes = obj_json["attributes"]

    if attr_name not in attributes:
        msg = f"Attribute  {attr_name} not found in objid: {obj_id}"
        log.warn(msg)
        raise HTTPNotFound()

    del attributes[attr_name]

    await save_metadata_obj(app, obj_id, obj_json, bucket=bucket)

    resp_json = {}
    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp
