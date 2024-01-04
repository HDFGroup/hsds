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
from .util.hdf5dtype import getItemSize, createDataType
from .util.dsetUtil import getShapeDims
from .util.arrayUtil import arrayToBytes, jsonToArray, decodeData
from .util.arrayUtil import bytesToArray, bytesArrayToList
from .datanode_lib import get_obj_id, get_metadata_obj, save_metadata_obj
from . import hsds_logger as log


def _index(items, marker, create_order=False):
    """Locate the leftmost value exactly equal to x"""
    if create_order:
        # list is not ordered, just search linearly
        for i in range(len(items)):
            if items[i] == marker:
                return i
    else:
        i = bisect_left(items, marker)
        if i != len(items) and items[i] == marker:
            return i
    # not found
    return -1


def _getAttribute(attr_name, obj_json, include_data=True, encoding=None):
    """ copy relevant fields from src to target """

    if not isinstance(obj_json, dict):
        msg = f"expected dict but got: {type(obj_json)}"
        log.error(msg)
        raise HTTPInternalServerError()

    if "attributes" not in obj_json:
        msg = "expected to find attributes key in obj_json"
        log.error(msg)
        raise HTTPInternalServerError()

    attributes = obj_json["attributes"]
    if attr_name not in attributes:
        # this should be checked before calling this function
        msg = f"attribute {attr_name} not found"
        log.error(msg)
        raise HTTPInternalServerError()

    src_attr = attributes[attr_name]
    log.debug(f"_getAttribute - src_attr: {src_attr}")

    for key in ("created", "type", "shape", "value"):
        if key not in src_attr:
            msg = f"Expected to find key: {key} in {src_attr}"
            log.error(msg)
            raise HTTPInternalServerError()

    des_attr = {}
    type_json = src_attr["type"]
    shape_json = src_attr["shape"]
    des_attr["created"] = src_attr["created"]
    des_attr["type"] = type_json
    des_attr["shape"] = shape_json
    des_attr["name"] = attr_name

    if encoding:
        item_size = getItemSize(type_json)
        if item_size == "H5T_VARIABLE":
            msg = "encoded value request but only json can be returned for "
            msg = f"{attr_name} since it has variable length type"
            log.warn(msg)
            encoding = None
        log.debug("base64 encoding requested")

    if include_data:
        value_json = src_attr["value"]
        if "encoding" in src_attr:
            des_attr["encoding"] = src_attr["encoding"]
            # just copy the encoded value
            des_attr["value"] = value_json
        elif encoding:
            # return base64 encoded value
            if value_json is None:
                des_attr["value"] = None
            else:
                arr_dtype = createDataType(type_json)
                np_shape = getShapeDims(shape_json)
                try:
                    arr = jsonToArray(np_shape, arr_dtype, value_json)
                except ValueError as e:
                    msg = f"Bad Request: input data doesn't match selection: {e}"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
                output_data = arrayToBytes(arr, encoding=encoding)
                des_attr["value"] = output_data.decode("ascii")
                des_attr["encoding"] = encoding
        else:
            des_attr["value"] = src_attr["value"]
    return des_attr


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
        msg = "POST Attributes without bucket param"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    create_order = False
    if params.get("CreateOrder"):
        create_order = True

    encoding = None
    if params.get("encoding"):
        encoding = params["encoding"]

    include_data = False
    if params.get("IncludeData"):
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
        kwargs = {"include_data": include_data, "encoding": encoding}
        log.debug(f"_getAttribute kwargs: {kwargs}")
        des_attr = _getAttribute(attr_name, obj_json, **kwargs)
        attr_list.append(des_attr)

    resp_json = {"attributes": attr_list}
    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp


async def POST_Attributes(request):
    """ Return JSON for attribute collection
    """
    log.request(request)
    app = request.app

    if not request.has_body:
        msg = "POST_Attributes with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    body = await request.json()
    if "attributes" not in body:
        msg = f"POST_Attributes expected attributes in body but got: {body.keys()}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    titles = body["attributes"]  # list of attribute names to fetch

    params = request.rel_url.query

    obj_id = get_obj_id(request)
    if "bucket" in params:
        bucket = params["bucket"]
    else:
        msg = "POST Attributes without bucket param"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    include_data = False
    log.debug(f"got params: {params}")
    if "IncludeData" in params and params["IncludeData"]:
        include_data = True
        log.debug("include attr data")
    if params.get("encoding"):
        encoding = params["encoding"]
        log.debug("POST_Attributes requested base64 encoding")
    else:
        encoding = None

    obj_json = await get_metadata_obj(app, obj_id, bucket=bucket)

    log.debug(f"Get attributes obj_id: {obj_id} got json")
    if "attributes" not in obj_json:
        msg = f"unexpected data for obj id: {obj_id}"
        msg.error(msg)
        raise HTTPInternalServerError()

    # return a list of attributes based on sorted dictionary keys
    attr_dict = obj_json["attributes"]
    attr_list = []
    kwargs = {"include_data": include_data}
    if encoding:
        kwargs["encoding"] = encoding

    for attr_name in titles:
        if attr_name not in attr_dict:
            continue
        des_attr = _getAttribute(attr_name, obj_json, **kwargs)
        attr_list.append(des_attr)

    resp_json = {"attributes": attr_list}
    if not attr_list:
        msg = f"POST attributes - requested {len(titles)} but none were found"
        log.warn(msg)
        raise HTTPNotFound()
    if len(attr_list) != len(titles):
        msg = f"POST attributes - requested {len(titles)} attributes but only "
        msg += f"{len(attr_list)} were found"
        log.warn(msg)
        raise HTTPNotFound()
    log.debug(f"POST attributes returning: {resp_json}")
    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp


async def PUT_Attributes(request):
    """ Handler for PUT /(obj)/<id>/attributes
    """
    log.request(request)
    app = request.app
    params = request.rel_url.query
    log.debug(f"got PUT_Attributes params: {params}")
    obj_id = get_obj_id(request)

    if not request.has_body:
        log.error("PUT_Attribute with no body")
        raise HTTPBadRequest(message="body expected")

    body = await request.json()
    log.debug(f"got body: {body}")
    if "bucket" in params:
        bucket = params["bucket"]
    elif "bucket" in body:
        bucket = params["bucket"]
    else:
        msg = "PUT Attributes without bucket param"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    replace = False
    if "replace" in params and params["replace"]:
        replace = True
        log.info("replace attribute")

    if "attributes" in body:
        items = body["attributes"]
    else:
        # make it look like a dictionary anyway to make
        # the processing more consistent
        items = {}
        if "name" not in body:
            log.error("PUT attribute with no name in body")
            raise HTTPInternalServerError()
        attr_name = body["name"]
        attribute = {}
        if "type" in body:
            attribute["type"] = body["type"]
        if "shape" in body:
            attribute["shape"] = body["shape"]
        if "value" in body:
            attribute["value"] = body["value"]
        if "encoding" in body:
            attribute["encoding"] = body["encoding"]
        items[attr_name] = attribute

    # validate input
    for attr_name in items:
        validateAttributeName(attr_name)
        attr_json = items[attr_name]
        if "type" not in attr_json:
            log.error("PUT attribute with no type in body")
            raise HTTPInternalServerError()
        if "shape" not in attr_json:
            log.error("PUT attribute with no shape in body")
            raise HTTPInternalServerError()
        if "value" in attr_json and attr_json.get("encoding"):
            # decode and store as JSON if possible
            value = attr_json["value"]
            arr_dtype = createDataType(attr_json["type"])  # np datatype
            attr_shape = attr_json["shape"]
            np_dims = getShapeDims(attr_shape)
            log.debug(f"np_dims: {np_dims}")
            try:
                arr = bytesToArray(value, arr_dtype, np_dims, encoding="base64")
            except ValueError as e:
                msg = f"Bad Request: encoded input data doesn't match shape and type: {e}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            log.debug(f"got arr: {arr}")
            log.debug(f"arr.shape: {arr.shape}")
            data = arr.tolist()
            try:
                json_data = bytesArrayToList(data)
                log.debug(f"converted encoded data to {json_data}")
                if attr_shape["class"] == "H5S_SCALAR":
                    attr_json["value"] = json_data[0]  # just store the scalar
                else:
                    attr_json["value"] = json_data
                del attr_json["encoding"]  # don't need to store as base64
            except ValueError as err:
                msg = f"Cannot decode bytes to list: {err}, will store as base64"
                log.warn(msg)
                attr_json["value"] = value  # use the base64 data

        log.debug(f"attribute {attr_name}: {attr_json}")

    log.info(f"PUT {len(items)} attributes to obj_id: {obj_id} bucket: {bucket}")

    obj_json = await get_metadata_obj(app, obj_id, bucket=bucket)
    if "attributes" not in obj_json:
        log.error(f"unexpected obj data for id: {obj_id}")
        raise HTTPInternalServerError()

    attributes = obj_json["attributes"]

    # check for conflicts, also set timestamp
    create_time = time.time()
    new_attribute = False  # set this if we have any new attributes
    for attr_name in items:
        attribute = items[attr_name]
        if attr_name in attributes:
            log.debug(f"attribute {attr_name} exists")
            if replace:
                # don't change the create timestamp
                log.debug(f"attribute {attr_name} exists, but will be updated")
                old_item = attributes[attr_name]
                attribute["created"] = old_item["created"]
            else:
                # Attribute already exists, return a 409
                msg = f"Attempt to overwrite attribute: {attr_name} "
                msg += f"in obj_id: {obj_id}"
                log.warn(msg)
                raise HTTPConflict()
        else:
            # set the timestamp
            log.debug(f"new attribute {attr_name}")
            attribute["created"] = create_time
            new_attribute = True

    # ok - all set, create the attributes
    for attr_name in items:
        log.debug(f"adding attribute {attr_name}")
        attr_json = items[attr_name]
        attributes[attr_name] = attr_json

    # write back to S3, save to metadata cache
    await save_metadata_obj(app, obj_id, obj_json, bucket=bucket)

    if new_attribute:
        status = 201
    else:
        status = 200

    resp_json = {"status": status}

    resp = json_response(resp_json, status=status)
    log.response(request, resp=resp)
    return resp


async def DELETE_Attributes(request):
    """HTTP DELETE method for /(obj)/<id>/attributes
    """
    log.request(request)
    app = request.app
    params = request.rel_url.query

    obj_id = get_obj_id(request)
    if "bucket" in params:
        bucket = params["bucket"]
    else:
        msg = "DELETE Attributes without bucket param"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if "encoding" in params:
        encoding = params["encoding"]
        if encoding != "base64":
            msg = "only base64 encoding is supported"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        encoding = None

    if "separator" in params:
        separator = params["separator"]
    else:
        separator = "/"

    if "attr_names" not in params:
        msg = "expected attr_names for DELETE attributes"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    attr_names_param = params["attr_names"]
    if encoding:
        attr_names_param = decodeData(attr_names_param).decode("utf-8")

    attr_names = attr_names_param.split(separator)

    log.info(f"DELETE attribute {attr_names} in {obj_id} bucket: {bucket}")

    obj_json = await get_metadata_obj(app, obj_id, bucket=bucket)

    log.debug(f"DELETE attributes obj_id: {obj_id} got json")
    if "attributes" not in obj_json:
        msg = f"unexpected data for obj id: {obj_id}"
        msg.error(msg)
        raise HTTPInternalServerError()

    # return a list of attributes based on sorted dictionary keys
    attributes = obj_json["attributes"]

    for attr_name in attr_names:
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
