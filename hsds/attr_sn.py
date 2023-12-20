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
# attribute methods for SN
#

import numpy as np
from aiohttp.web_exceptions import HTTPBadRequest, HTTPInternalServerError
from aiohttp.web import StreamResponse
from json import JSONDecodeError

from .util.httpUtil import http_get, http_put, http_delete, getHref
from .util.httpUtil import getAcceptType, jsonResponse
from .util.idUtil import isValidUuid, getDataNodeUrl, getCollectionForId, getRootObjId
from .util.authUtil import getUserPasswordFromRequest, validateUserPassword
from .util.domainUtil import getDomainFromRequest, isValidDomain
from .util.domainUtil import getBucketForDomain, verifyRoot
from .util.attrUtil import validateAttributeName, getRequestCollectionName
from .util.hdf5dtype import validateTypeItem, getBaseTypeJson
from .util.hdf5dtype import createDataType, getItemSize
from .util.arrayUtil import jsonToArray, getNumElements
from .util.arrayUtil import bytesArrayToList
from .util.dsetUtil import getShapeDims

from .servicenode_lib import getDomainJson, getObjectJson, validateAction, getAttributes
from .domain_crawl import DomainCrawler
from . import hsds_logger as log
from . import config


async def GET_Attributes(request):
    """HTTP method to return JSON for attribute collection"""
    log.request(request)
    app = request.app
    params = request.rel_url.query
    # returns datasets|groups|datatypes
    collection = getRequestCollectionName(request)

    obj_id = request.match_info.get("id")
    if not obj_id:
        msg = "Missing object id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if not isValidUuid(obj_id, obj_class=collection):
        msg = f"Invalid obj id: {obj_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    include_data = False
    ignore_nan = False
    if "IncludeData" in params and params["IncludeData"]:
        include_data = True
        if "ignore_nan" in params and params["ignore_nan"]:
            ignore_nan = True
    create_order = False
    if "CreateOrder" in params and params["CreateOrder"]:
        create_order = True

    limit = None
    if "Limit" in params:
        try:
            limit = int(params["Limit"])
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    marker = None
    if "Marker" in params:
        marker = params["Marker"]

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app["allow_noauth"]:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "read")

    req = getDataNodeUrl(app, obj_id)

    req += "/" + collection + "/" + obj_id + "/attributes"
    params = {}
    if limit is not None:
        params["Limit"] = str(limit)
    if marker is not None:
        params["Marker"] = marker
    if include_data:
        params["IncludeData"] = 1
    if bucket:
        params["bucket"] = bucket
    if create_order:
        params["CreateOrder"] = 1

    log.debug(f"get attributes: {req}")
    dn_json = await http_get(app, req, params=params)
    log.debug(f"got attributes json from dn for obj_id: {obj_id}")
    attributes = dn_json["attributes"]

    # mixin hrefs
    for attribute in attributes:
        attr_name = attribute["name"]
        attr_href = f"/{collection}/{obj_id}/attributes/{attr_name}"
        attribute["href"] = getHref(request, attr_href)

    resp_json = {}
    resp_json["attributes"] = attributes

    hrefs = []
    obj_uri = "/" + collection + "/" + obj_id
    href = getHref(request, obj_uri + "/attributes")
    hrefs.append({"rel": "self", "href": href})
    hrefs.append({"rel": "home", "href": getHref(request, "/")})
    hrefs.append({"rel": "owner", "href": getHref(request, obj_uri)})
    resp_json["hrefs"] = hrefs

    resp = await jsonResponse(request, resp_json, ignore_nan=ignore_nan)
    log.response(request, resp=resp)
    return resp


async def GET_Attribute(request):
    """HTTP method to return JSON for an attribute"""
    log.request(request)
    app = request.app
    # returns datasets|groups|datatypes
    collection = getRequestCollectionName(request)

    obj_id = request.match_info.get("id")
    if not obj_id:
        msg = "Missing object id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(obj_id, obj_class=collection):
        msg = f"Invalid object id: {obj_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    attr_name = request.match_info.get("name")
    validateAttributeName(attr_name)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app["allow_noauth"]:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "read")
    params = request.rel_url.query
    if "ignore_nan" in params and params["ignore_nan"]:
        ignore_nan = True
    else:
        ignore_nan = False

    req = getDataNodeUrl(app, obj_id)
    req += f"/{collection}/{obj_id}/attributes/{attr_name}"
    log.debug(f"get Attribute: {req}")
    params = {}
    if bucket:
        params["bucket"] = bucket
    dn_json = await http_get(app, req, params=params)
    log.debug(f"got attributes json from dn for obj_id: {obj_id}")

    resp_json = {}
    resp_json["name"] = attr_name
    resp_json["type"] = dn_json["type"]
    resp_json["shape"] = dn_json["shape"]
    if "value" in dn_json:
        resp_json["value"] = dn_json["value"]
    resp_json["created"] = dn_json["created"]
    # attributes don't get modified, so use created timestamp as lastModified
    resp_json["lastModified"] = dn_json["created"]

    hrefs = []
    obj_uri = "/" + collection + "/" + obj_id
    attr_uri = obj_uri + "/attributes/" + attr_name
    hrefs.append({"rel": "self", "href": getHref(request, attr_uri)})
    hrefs.append({"rel": "home", "href": getHref(request, "/")})
    hrefs.append({"rel": "owner", "href": getHref(request, obj_uri)})
    resp_json["hrefs"] = hrefs
    resp = await jsonResponse(request, resp_json, ignore_nan=ignore_nan)
    log.response(request, resp=resp)
    return resp


async def _getTypeFromRequest(app, body, obj_id=None, bucket=None):
    """ return a type json from the request body """
    if "type" not in body:
        msg = "PUT attribute with no type in body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    datatype = body["type"]

    if isinstance(datatype, str) and datatype.startswith("t-"):
        # Committed type - fetch type json from DN
        ctype_id = datatype
        log.debug(f"got ctypeid: {ctype_id}")
        ctype_json = await getObjectJson(app, ctype_id, bucket=bucket)
        log.debug(f"ctype {ctype_id}: {ctype_json}")
        root_id = getRootObjId(obj_id)
        if ctype_json["root"] != root_id:
            msg = "Referenced committed datatype must belong in same domain"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        datatype = ctype_json["type"]
        # add the ctype_id to the type
        datatype["id"] = ctype_id
    elif isinstance(datatype, str):
        try:
            # convert predefined type string (e.g. "H5T_STD_I32LE") to
            # corresponding json representation
            datatype = getBaseTypeJson(datatype)
        except TypeError:
            msg = "PUT attribute with invalid predefined type"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    try:
        validateTypeItem(datatype)
    except KeyError as ke:
        msg = f"KeyError creating type: {ke}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    except TypeError as te:
        msg = f"TypeError creating type: {te}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    except ValueError as ve:
        msg = f"ValueError creating type: {ve}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    return datatype


def _getShapeFromRequest(body):
    """ get shape json from request body """
    shape_json = {}
    if "shape" in body:
        shape_body = body["shape"]
        shape_class = None
        if isinstance(shape_body, dict) and "class" in shape_body:
            shape_class = shape_body["class"]
        elif isinstance(shape_body, str):
            shape_class = shape_body
        if shape_class:
            if shape_class == "H5S_NULL":
                shape_json["class"] = "H5S_NULL"
                if isinstance(shape_body, dict) and "dims" in shape_body:
                    msg = "can't include dims with null shape"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
                if isinstance(shape_body, dict) and "value" in body:
                    msg = "can't have H5S_NULL shape with value"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
            elif shape_class == "H5S_SCALAR":
                shape_json["class"] = "H5S_SCALAR"
                dims = getShapeDims(shape_body)
                if len(dims) != 1 or dims[0] != 1:
                    msg = "dimensions aren't valid for scalar attribute"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
            elif shape_class == "H5S_SIMPLE":
                shape_json["class"] = "H5S_SIMPLE"
                dims = getShapeDims(shape_body)
                shape_json["dims"] = dims
            else:
                msg = f"Unknown shape class: {shape_class}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
        else:
            # no class, interpet shape value as dimensions and
            # use H5S_SIMPLE as class
            if isinstance(shape_body, list) and len(shape_body) == 0:
                shape_json["class"] = "H5S_SCALAR"
            else:
                shape_json["class"] = "H5S_SIMPLE"
                dims = getShapeDims(shape_body)
                shape_json["dims"] = dims
    else:
        shape_json["class"] = "H5S_SCALAR"

    return shape_json


def _getValueFromRequest(body, data_type, data_shape):
    """ Get attribute value from request json """
    dims = getShapeDims(data_shape)
    if "value" in body:
        if dims is None:
            msg = "Bad Request: data can not be included with H5S_NULL space"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        value = body["value"]
        # validate that the value agrees with type/shape
        arr_dtype = createDataType(data_type)  # np datatype
        if len(dims) == 0:
            np_dims = [1, ]
        else:
            np_dims = dims

        # verify that the input data matches the array shape and type
        try:
            jsonToArray(np_dims, arr_dtype, value)
        except ValueError as e:
            msg = f"Bad Request: input data doesn't match selection: {e}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        value = None

    return value


async def _getAttributeFromRequest(app, req_json, obj_id=None, bucket=None):
    """ return attribute from given request json """
    attr_item = {}
    attr_type = await _getTypeFromRequest(app, req_json, obj_id=obj_id, bucket=bucket)
    attr_shape = _getShapeFromRequest(req_json)
    attr_item = {"type": attr_type, "shape": attr_shape}
    attr_value = _getValueFromRequest(req_json, attr_type, attr_shape)
    if attr_value is not None:
        attr_item["value"] = attr_value
    else:
        attr_item["value"] = None
    return attr_item


async def _getAttributesFromRequest(request, req_json, obj_id=None, bucket=None):
    """ read the given JSON dictinary and return dict of attribute json """

    app = request.app
    attr_items = {}
    kwargs = {"obj_id": obj_id}
    if bucket:
        kwargs["bucket"] = bucket
    if "attributes" in req_json:
        attributes = req_json["attributes"]
        if not isinstance(attributes, dict):
            msg = f"expected list for attributes but got: {type(attributes)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        # read each attr_item and canonicalize the shape, type, verify value
        for attr_name in attributes:
            attr_json = attributes[attr_name]
            attr_item = await _getAttributeFromRequest(app, attr_json, **kwargs)
            attr_items[attr_name] = attr_item

    elif "type" in req_json:
        # single attribute create - fake an item list
        attr_item = await _getAttributeFromRequest(app, req_json, **kwargs)
        if "name" in req_json:
            attr_name = req_json["name"]
        else:
            attr_name = request.match_info.get("name")
            validateAttributeName(attr_name)
        if not attr_name:
            msg = "Missing attribute name"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        attr_items[attr_name] = attr_item
    else:
        log.debug(f"_getAttributes from request - no attribute defined in {req_json}")

    return attr_items


async def PUT_Attribute(request):
    """HTTP method to create a new attribute"""
    log.request(request)
    app = request.app
    req_params = request.rel_url.query
    # returns datasets|groups|datatypes
    collection = getRequestCollectionName(request)

    obj_id = request.match_info.get("id")
    if not obj_id:
        msg = "Missing object id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(obj_id, obj_class=collection):
        msg = f"Invalid object id: {obj_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    attr_name = request.match_info.get("name")
    if attr_name:
        log.debug(f"Attribute name: [{attr_name}]")
        validateAttributeName(attr_name)

    log.info(f"PUT Attributes id: {obj_id} name: {attr_name}")
    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    await validateUserPassword(app, username, pswd)

    if not request.has_body:
        msg = "PUT Attribute with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    try:
        body = await request.json()
    except JSONDecodeError:
        msg = "Unable to load JSON body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    verifyRoot(domain_json)

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "create")

    kwargs = {"obj_id": obj_id, "bucket": bucket}
    attr_json = await _getAttributeFromRequest(app, body, **kwargs)
    attr_json["name"] = attr_name
    log.debug(f"got attr_json: {attr_json}")

    # ready to add attribute now
    req = getDataNodeUrl(app, obj_id)
    req += f"/{collection}/{obj_id}/attributes"
    log.info(f"PUT Attribute: {req}")

    params = {}
    if "replace" in req_params and req_params["replace"]:
        # allow attribute to be overwritten
        log.debug("setting replace for PUT Atttribute")
        params["replace"] = 1
    else:
        log.debug("replace is not set for PUT Attribute")

    if bucket:
        params["bucket"] = bucket

    put_rsp = await http_put(app, req, data=attr_json, params=params)
    log.info(f"PUT Attribute resp: {put_rsp}")

    if "status" in put_rsp:
        status = put_rsp["status"]
    else:
        status = 201

    hrefs = []  # TBD
    req_rsp = {"hrefs": hrefs}
    # attribute creation successful
    resp = await jsonResponse(request, req_rsp, status=status)
    log.response(request, resp=resp)
    return resp


async def PUT_Attributes(request):
    """HTTP method to create a new attribute"""
    log.request(request)
    req_params = request.rel_url.query
    app = request.app
    status = None

    log.debug("PUT_Attributes")

    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    await validateUserPassword(app, username, pswd)

    if not request.has_body:
        msg = "PUT Attribute with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    try:
        body = await request.json()
    except JSONDecodeError:
        msg = "Unable to load JSON body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    params = {}
    if "replace" in req_params and req_params["replace"]:
        # allow attribute to be overwritten
        log.debug("setting replace for PUT Atttribute")
        params["replace"] = 1
    else:
        log.debug("replace is not set for PUT Attribute")

    if bucket:
        params["bucket"] = bucket

    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    verifyRoot(domain_json)

    req_obj_id = request.match_info.get("id")
    if not req_obj_id:
        req_obj_id = domain_json["root"]
    kwargs = {"obj_id": req_obj_id, "bucket": bucket}
    attr_items = await _getAttributesFromRequest(request, body, **kwargs)

    if attr_items:
        log.debug(f"PUT Attribute {len(attr_items)} attibutes to add")
    else:
        log.debug("no attributes defined yet")

    # next, sort out where these attributes are going to

    obj_ids = {}
    if "obj_ids" in body:
        body_ids = body["obj_ids"]
        if isinstance(body_ids, list):
            # multi cast the attributes - each attribute  in attr-items
            # will be written to each of the objects identified by obj_id
            if not attr_items:
                msg = "no attributes provided"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            else:
                for obj_id in body_ids:
                    if not isValidUuid(obj_id):
                        msg = f"Invalid object id: {obj_id}"
                        log.warn(msg)
                        raise HTTPBadRequest(reason=msg)
                    obj_ids[obj_id] = attr_items

                msg = f"{len(attr_items)} attributes will be multicast to "
                msg += f"{len(obj_ids)} objects"
                log.info(msg)
        elif isinstance(body_ids, dict):
            # each value is body_ids is a set of attriutes to write to the object
            # unlike the above case, different attributes can be written to
            # different objects
            if attr_items:
                msg = "attributes defined outside the obj_ids dict"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            else:
                for obj_id in body_ids:
                    if not isValidUuid(obj_id):
                        msg = f"Invalid object id: {obj_id}"
                        log.warn(msg)
                        raise HTTPBadRequest(reason=msg)
                    id_json = body_ids[obj_id]

                    kwargs = {"obj_id": obj_id, "bucket": bucket}
                    obj_items = await _getAttributesFromRequest(request, id_json, **kwargs)
                    if obj_items:
                        obj_ids[obj_id] = obj_items

                # write different attributes to different objects
                msg = f"put attributes over {len(obj_ids)} objects"
        else:
            msg = f"unexpected type for obj_ids: {type(obj_ids)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        # get the object id from the request
        obj_id = request.match_info.get("id")
        if not obj_id:
            msg = "Missing object id"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        obj_ids[obj_id] = attr_items  # make it look like a list for consistency

    log.debug(f"got {len(obj_ids)} obj_ids")

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, req_obj_id, username, "create")

    count = len(obj_ids)
    if count == 0:
        msg = "no obj_ids defined"
        log.warn(f"PUT_Attributes: {msg}")
        raise HTTPBadRequest(reason=msg)
    elif count == 1:
        # just send one PUT Attributes request to the dn
        obj_id = list(obj_ids.keys())[0]
        attr_json = obj_ids[obj_id]
        log.debug(f"got attr_json: {attr_json}")
        req = getDataNodeUrl(app, obj_id)
        collection = getCollectionForId(obj_id)
        req += f"/{collection}/{obj_id}/attributes"
        log.info(f"PUT Attributes: {req}")
        data = {"attributes": attr_json}
        put_rsp = await http_put(app, req, data=data, params=params)
        log.info(f"PUT Attribute sresp: {put_rsp}")

        if "status" in put_rsp:
            status = put_rsp["status"]
        else:
            status = 201
    else:
        # put multi obj

        # mixin some additonal kwargs
        crawler_params = {"follow_links": False}
        if bucket:
            crawler_params["bucket"] = bucket

        crawler = DomainCrawler(app, obj_ids, action="put_attr", params=crawler_params)
        await crawler.crawl()

        status = 200
        for obj_id in crawler._obj_dict:
            item = crawler._obj_dict[obj_id]
            log.debug(f"got item from crawler for {obj_id}: {item}")
            if "status" in item:
                item_status = item["status"]
                if item_status > status:
                    # return the more sever error
                    log.debug(f"setting status to {item_status}")
                    status = item_status

        log.info("DomainCrawler done for put_attrs action")

    hrefs = []  # TBD
    req_rsp = {"hrefs": hrefs}
    # attribute creation successful
    log.debug(f"PUT_Attributes returning status: {status}")
    resp = await jsonResponse(request, req_rsp, status=status)
    log.response(request, resp=resp)
    return resp


async def DELETE_Attribute(request):
    """HTTP method to delete a attribute resource"""
    log.request(request)
    app = request.app
    # returns datasets|groups|datatypes
    collection = getRequestCollectionName(request)

    obj_id = request.match_info.get("id")
    if not obj_id:
        msg = "Missing object id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(obj_id, obj_class=collection):
        msg = f"Invalid object id: {obj_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    attr_name = request.match_info.get("name")
    log.debug(f"Attribute name: [{attr_name}]")
    validateAttributeName(attr_name)

    username, pswd = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    verifyRoot(domain_json)

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "delete")

    req = getDataNodeUrl(app, obj_id)
    req += "/" + collection + "/" + obj_id + "/attributes/" + attr_name
    log.info("PUT Attribute: " + req)
    params = {}
    if bucket:
        params["bucket"] = bucket
    rsp_json = await http_delete(app, req, params=params)

    log.info(f"PUT Attribute resp: {rsp_json}")

    hrefs = []  # TBD
    req_rsp = {"hrefs": hrefs}
    resp = await jsonResponse(request, req_rsp)
    log.response(request, resp=resp)
    return resp


async def GET_AttributeValue(request):
    """HTTP method to return an attribute value"""
    log.request(request)
    app = request.app
    log.info("GET_AttributeValue")
    # returns datasets|groups|datatypes
    collection = getRequestCollectionName(request)

    obj_id = request.match_info.get("id")
    if not obj_id:
        msg = "Missing object id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(obj_id, obj_class=collection):
        msg = f"Invalid object id: {obj_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    attr_name = request.match_info.get("name")
    validateAttributeName(attr_name)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app["allow_noauth"]:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain value: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    verifyRoot(domain_json)

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "read")

    params = request.rel_url.query
    if "ignore_nan" in params and params["ignore_nan"]:
        ignore_nan = True
    else:
        ignore_nan = False

    req = getDataNodeUrl(app, obj_id)
    req += "/" + collection + "/" + obj_id + "/attributes/" + attr_name
    log.debug("get Attribute: " + req)
    params = {}
    if bucket:
        params["bucket"] = bucket
    dn_json = await http_get(app, req, params=params)
    log.debug("got attributes json from dn for obj_id: " + str(dn_json))

    attr_shape = dn_json["shape"]
    log.debug(f"attribute shape: {attr_shape}")
    if attr_shape["class"] == "H5S_NULL":
        msg = "Null space attributes can not be read"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    accept_type = getAcceptType(request)
    response_type = accept_type  # will adjust later if binary not possible
    type_json = dn_json["type"]
    shape_json = dn_json["shape"]
    item_size = getItemSize(type_json)

    if item_size == "H5T_VARIABLE" and accept_type != "json":
        msg = "Client requested binary, but only JSON is supported for "
        msg += "variable length data types"
        log.info(msg)
        response_type = "json"

    if response_type == "binary":
        arr_dtype = createDataType(type_json)  # np datatype
        np_shape = getShapeDims(shape_json)
        try:
            arr = jsonToArray(np_shape, arr_dtype, dn_json["value"])
        except ValueError as e:
            if dn_json["value"] is None:
                arr = np.array([]).astype(arr_dtype)
            else:
                msg = f"Bad Request: input data doesn't match selection: {e}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
        output_data = arr.tobytes()
        msg = f"GET AttributeValue - returning {len(output_data)} "
        msg += "bytes binary data"
        log.debug(msg)
        # TBD: do we really need to manually add cors headers for binary
        # responses?
        cors_domain = config.get("cors_domain")
        # write response
        try:
            resp = StreamResponse()
            resp.content_type = "application/octet-stream"
            resp.content_length = len(output_data)
            # allow CORS
            if cors_domain:
                resp.headers["Access-Control-Allow-Origin"] = cors_domain
                cors_methods = "GET, POST, DELETE, PUT, OPTIONS"
                resp.headers["Access-Control-Allow-Methods"] = cors_methods
                cors_headers = "Content-Type, api_key, Authorization"
                resp.headers["Access-Control-Allow-Headers"] = cors_headers
            await resp.prepare(request)
            await resp.write(output_data)
        except Exception as e:
            log.error(f"Got exception: {e}")
            raise HTTPInternalServerError()
        finally:
            await resp.write_eof()

    else:
        resp_json = {}
        if "value" in dn_json:
            resp_json["value"] = dn_json["value"]

        hrefs = []
        obj_uri = "/" + collection + "/" + obj_id
        attr_uri = obj_uri + "/attributes/" + attr_name
        hrefs.append({"rel": "self", "href": getHref(request, attr_uri)})
        hrefs.append({"rel": "home", "href": getHref(request, "/")})
        hrefs.append({"rel": "owner", "href": getHref(request, obj_uri)})
        resp_json["hrefs"] = hrefs
        resp = await jsonResponse(request, resp_json, ignore_nan=ignore_nan)
        log.response(request, resp=resp)
    return resp


async def PUT_AttributeValue(request):
    """HTTP method to update an attributes data"""
    log.request(request)
    log.info("PUT_AttributeValue")
    app = request.app
    # returns datasets|groups|datatypes
    collection = getRequestCollectionName(request)
    obj_id = request.match_info.get("id")
    if not obj_id:
        msg = "Missing object id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(obj_id, obj_class=collection):
        msg = f"Invalid object id: {obj_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    attr_name = request.match_info.get("name")
    log.debug(f"Attribute name: [{attr_name}]")
    validateAttributeName(attr_name)

    log.info(f"PUT Attribute Value id: {obj_id} name: {attr_name}")
    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    await validateUserPassword(app, username, pswd)

    if not request.has_body:
        msg = "PUT AttributeValue with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    verifyRoot(domain_json)

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "update")

    req = getDataNodeUrl(app, obj_id)
    req += "/" + collection + "/" + obj_id + "/attributes/" + attr_name
    log.debug("get Attribute: " + req)
    params = {"replace": 1}  # allow overwrites
    if bucket:
        params["bucket"] = bucket
    dn_json = await http_get(app, req, params=params)
    log.debug("got attributes json from dn for obj_id: " + str(obj_id))
    log.debug(f"got dn_json: {dn_json}")

    attr_shape = dn_json["shape"]
    if attr_shape["class"] == "H5S_NULL":
        msg = "Null space attributes can not be updated"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    np_shape = getShapeDims(attr_shape)
    log.debug(f"np_shape: {np_shape}")
    type_json = dn_json["type"]
    np_dtype = createDataType(type_json)  # np datatype

    request_type = "json"
    if "Content-Type" in request.headers:
        # client should use "application/octet-stream" for binary transfer
        content_type = request.headers["Content-Type"]
        expected = ("application/json", "application/octet-stream")
        if content_type not in expected:
            msg = f"Unknown content_type: {content_type}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if content_type == "application/octet-stream":
            log.debug("PUT AttributeValue - request_type is binary")
            request_type = "binary"
        else:
            log.debug("PUT AttribueValue - request type is json")

    binary_data = None
    if request_type == "binary":
        item_size = getItemSize(type_json)

        if item_size == "H5T_VARIABLE":
            msg = "Only JSON is supported for variable length data types"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        # read binary data
        binary_data = await request.read()
        if len(binary_data) != request.content_length:
            msg = f"Read {len(binary_data)} bytes, expecting: "
            msg += f"{request.content_length}"
            log.error(msg)
            raise HTTPInternalServerError()

    arr = None  # np array to hold request data

    if binary_data:
        npoints = getNumElements(np_shape)
        if npoints * item_size != len(binary_data):
            msg = f"Expected: {npoints*item_size} bytes, "
            msg += f"but got {len(binary_data)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        arr = np.fromstring(binary_data, dtype=np_dtype)
        arr = arr.reshape(np_shape)  # conform to selection shape
        # convert to JSON for transmission to DN
        data = arr.tolist()

        try:
            value = bytesArrayToList(data)
        except ValueError as err:
            msg = f"Cannot decode bytes to list: {err}"
            raise HTTPBadRequest(reason=msg)

        if attr_shape["class"] == "H5S_SCALAR":
            # just send the value, not a list
            value = value[0]

    else:
        try:
            body = await request.json()
        except JSONDecodeError:
            msg = "Unable to load JSON body"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        if "value" not in body:
            msg = "PUT attribute value with no value in body"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        value = body["value"]

        # validate that the value agrees with type/shape
        try:
            arr = jsonToArray(np_shape, np_dtype, value)
        except ValueError as e:
            if value is None:
                arr = np.array([]).astype(np_dtype)
            else:
                msg = f"Bad Request: input data doesn't match selection: {e}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
    log.debug(f"Got: {arr.size} array elements")

    # ready to add attribute now
    attr_json = {}
    attr_json["name"] = attr_name
    attr_json["type"] = type_json
    attr_json["shape"] = attr_shape
    attr_json["value"] = value

    req = getDataNodeUrl(app, obj_id)
    req += "/" + collection + "/" + obj_id + "/attributes"
    log.info(f"PUT Attribute Value: {req}")

    dn_json["value"] = value
    params = {}
    params = {"replace": 1}  # let the DN know we can overwrite the attribute
    if bucket:
        params["bucket"] = bucket
    put_rsp = await http_put(app, req, params=params, data=attr_json)
    log.info(f"PUT Attribute Value resp: {put_rsp}")

    hrefs = []  # TBD
    req_rsp = {"hrefs": hrefs}
    # attribute creation successful
    resp = await jsonResponse(request, req_rsp)
    log.response(request, resp=resp)
    return resp


async def POST_Attributes(request):
    """HTTP method to get multiple attribute values"""
    log.request(request)
    app = request.app
    log.info("POST_Attributes")
    req_id = request.match_info.get("id")

    if not request.has_body:
        msg = "POST Attributes with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    try:
        body = await request.json()
    except JSONDecodeError:
        msg = "Unable to load JSON body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if "attr_names" in body:
        attr_names = body["attr_names"]
        if not isinstance(attr_names, list):
            msg = f"expected list for attr_names but got: {type(attr_names)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        attr_names = None

    if "obj_ids" in body:
        obj_ids = body["obj_ids"]
    else:
        obj_ids = None

    if attr_names is None and obj_ids is None:
        msg = "expected body to contain one of attr_names, obj_ids keys"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    # construct an item list from attr_names and obj_ids
    items = {}
    if obj_ids is None:
        if not req_id:
            msg = "no object id in request"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        items[req_id] = attr_names
    elif isinstance(obj_ids, list):
        if attr_names is None:
            msg = "attr_names must be provided if obj_ids is a list"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        for obj_id in obj_ids:
            items[obj_id] = attr_names
    elif isinstance(obj_ids, dict):
        if attr_names is not None:
            msg = "attr_names must not be proved if obj_ids is a dict"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        for obj_id in obj_ids:
            names_for_id = obj_ids[obj_id]
            if not isinstance(names_for_id, list):
                msg = "expected list of attribute names"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            items[obj_id] = names_for_id

    log.debug(f"POST Attributes items: {items}")

    # do a check that everything is as it should with the item list
    for obj_id in items:
        if not isValidUuid(obj_id):
            msg = f"Invalid object id: {obj_id}"
            log.warn(msg)

        attr_names = items[obj_id]

        if not isinstance(attr_names, list):
            msg = f"expected list for attr_names but got: {type(attr_names)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        for attr_name in attr_names:
            validateAttributeName(attr_name)  # raises HTTPBadRequest if invalid

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app["allow_noauth"]:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain value: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    accept_type = getAcceptType(request)
    if accept_type != "json":
        msg = f"{accept_type} response requested for POST Attributes, "
        msg += "but only json is supported"
        log.warn(msg)

    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    verifyRoot(domain_json)

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "read")

    params = request.rel_url.query
    log.debug(f"got params: {params}")
    if params.get("ignore_nan"):
        ignore_nan = True
    else:
        ignore_nan = False

    resp_json = {}

    if len(items) == 0:
        msg = "no obj ids specified for POST Attributes"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    elif len(items) == 1:
        # just make a request the datanode
        obj_id = list(items.keys())[0]
        collection = getCollectionForId(obj_id)
        attr_names = items[obj_id]
        kwargs = {"attr_names": attr_names, "bucket": bucket}
        if params.get("IncludeData"):
            kwargs["include_data"] = True
        if params.get("ignore_nan"):
            kwargs["ignore_nan"] = True
        attributes = await getAttributes(app, obj_id, **kwargs)

        # mixin hrefs
        for attribute in attributes:
            attr_name = attribute["name"]
            attr_href = f"/{collection}/{obj_id}/attributes/{attr_name}"
            attribute["href"] = getHref(request, attr_href)

        resp_json["attributes"] = attributes
    else:
        # get multi obj
        # don't follow links!
        crawler_params = {"follow_links": False, "bucket": bucket}
        # mixin params
        if params.get("IncludeData"):
            crawler_params["include_data"] = True
        if params.get("ignore_nan"):
            crawler_params["ignore_nan"] = True

        crawler = DomainCrawler(app, items, action="get_attr", params=crawler_params)
        await crawler.crawl()

        msg = f"DomainCrawler returning: {len(crawler._obj_dict)} objects"
        log.info(msg)
        attributes = crawler._obj_dict
        # mixin hrefs
        for obj_id in attributes.keys():
            obj_attributes = attributes[obj_id]
            collection = getCollectionForId(obj_id)
            for attribute in obj_attributes:
                attr_name = attribute["name"]
                attr_href = f"/{collection}/{obj_id}/attributes/{attr_name}"
                attribute["href"] = getHref(request, attr_href)
        log.debug(f"got {len(attributes)} attributes")
        resp_json["attributes"] = attributes

    hrefs = []
    collection = getCollectionForId(req_id)
    obj_uri = "/" + collection + "/" + req_id
    href = getHref(request, obj_uri + "/attributes")
    hrefs.append({"rel": "self", "href": href})
    hrefs.append({"rel": "home", "href": getHref(request, "/")})
    hrefs.append({"rel": "owner", "href": getHref(request, obj_uri)})
    resp_json["hrefs"] = hrefs

    resp = await jsonResponse(request, resp_json, ignore_nan=ignore_nan)
    log.response(request, resp=resp)
    return resp
