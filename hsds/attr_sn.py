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
from aiohttp.web_exceptions import HTTPBadRequest, HTTPNotFound, HTTPInternalServerError
from aiohttp.web import StreamResponse
from json import JSONDecodeError

from .util.httpUtil import getAcceptType, jsonResponse, getHref
from .util.globparser import globmatch
from .util.idUtil import isValidUuid, getRootObjId
from .util.authUtil import getUserPasswordFromRequest, validateUserPassword
from .util.domainUtil import getDomainFromRequest, isValidDomain
from .util.domainUtil import getBucketForDomain, verifyRoot
from .util.attrUtil import validateAttributeName, getRequestCollectionName
from .util.hdf5dtype import validateTypeItem, getBaseTypeJson
from .util.hdf5dtype import createDataType, getItemSize
from .util.arrayUtil import jsonToArray, getNumElements, bytesArrayToList
from .util.arrayUtil import bytesToArray, arrayToBytes, decodeData, encodeData
from .util.dsetUtil import getShapeDims

from .servicenode_lib import getDomainJson, getObjectJson, validateAction
from .servicenode_lib import getAttributes, putAttributes, deleteAttributes
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

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    bucket = getBucketForDomain(domain)
    log.debug(f"bucket: {bucket}")

    if "follow_links" in params and params["follow_links"]:
        if collection != "groups":
            msg = "follow_links can only be used with group ids"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        follow_links = True
    else:
        follow_links = False
    log.debug(f"getAttributes follow_links: {follow_links}")
    include_data = True
    if "IncludeData" in params:
        IncludeData = params["IncludeData"]
        if not IncludeData or IncludeData == "0":
            include_data = False
    log.debug(f"include_data: {include_data}")

    if "max_data_size" in params:
        try:
            max_data_size = int(params["max_data_size"])
        except ValueError:
            msg = "expected int for max_data_size"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        max_data_size = 0

    if "ignore_nan" in params and params["ignore_nan"]:
        ignore_nan = True
    else:
        ignore_nan = False

    if "CreateOrder" in params and params["CreateOrder"]:
        create_order = True
    else:
        create_order = False

    if "Limit" in params:
        try:
            limit = int(params["Limit"])
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        limit = None
    if "Marker" in params:
        marker = params["Marker"]
    else:
        marker = None
    if "encoding" in params:
        encoding = params["encoding"]
        if params["encoding"] != "base64":
            msg = "only base64 encoding is supported"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        encoding = "base64"
    else:
        encoding = None

    if "pattern" in params and params["pattern"]:
        pattern = params["pattern"]
        try:
            globmatch("abc", pattern)
        except ValueError:
            msg = f"invlaid pattern: {pattern} for attribute matching"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.debug(f"using pattern: {pattern} for GET_Attributes")
    else:
        pattern = None

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app["allow_noauth"]:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    await validateAction(app, domain, obj_id, username, "read")

    if follow_links:
        # setup kwargs for DomainCrawler
        kwargs = {"action": "get_attr", "follow_links": True, "bucket": bucket}
        # mixin params
        if include_data:
            kwargs["include_data"] = True
        if max_data_size > 0:
            kwargs["max_data_size"] = max_data_size
        if ignore_nan:
            kwargs["ignore_nan"] = True
        items = [obj_id, ]
        crawler = DomainCrawler(app, items, **kwargs)
        # will raise exception on NotFound, etc.
        await crawler.crawl()
        attributes = crawler._obj_dict
        msg = f"DomainCrawler returned: {len(attributes)} objects"
        log.info(msg)
    else:
        # just get attributes for this objects
        kwargs = {"bucket": bucket}
        if include_data:
            kwargs["include_data"] = True
        if max_data_size > 0:
            kwargs["max_data_size"] = max_data_size
        if ignore_nan:
            kwargs["ignore_nan"] = True
        if limit:
            kwargs["limit"] = limit
        if marker:
            kwargs["marker"] = marker
        if encoding:
            kwargs["encoding"] = encoding
        if pattern:
            kwargs["pattern"] = pattern
        if create_order:
            kwargs["create_order"] = True
        attributes = await getAttributes(app, obj_id, **kwargs)
        log.debug(f"got attributes json from dn for obj_id: {obj_id}")

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
    params = request.rel_url.query
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

    await validateAction(app, domain, obj_id, username, "read")

    if "ignore_nan" in params and params["ignore_nan"]:
        ignore_nan = True
    else:
        ignore_nan = False

    if "IncludeData" in params and not params["IncludeData"]:
        include_data = False
    else:
        include_data = True

    if params.get("encoding"):
        if params["encoding"] != "base64":
            msg = "only base64 encoding is supported"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        encoding = "base64"
    else:
        encoding = None

    kwargs = {"bucket": bucket, "include_data": include_data, "attr_names": [attr_name, ]}
    if ignore_nan:
        kwargs["ignore_nan"] = ignore_nan
    if encoding:
        kwargs["encoding"] = encoding

    attributes = await getAttributes(app, obj_id, **kwargs)
    if not attributes:
        log.error("no attributes returned")  # should have been raised by getAttributes
        raise HTTPInternalServerError()
    if len(attributes) > 1:
        log.error(f"expected one attribute but got: {len(attributes)}")
        raise HTTPInternalServerError()

    log.debug(f"got attributes: {attributes}")
    attribute = attributes[0]

    resp_json = {}
    resp_json["name"] = attr_name
    resp_json["type"] = attribute["type"]
    resp_json["shape"] = attribute["shape"]
    if "value" in attribute:
        resp_json["value"] = attribute["value"]
    resp_json["created"] = attribute["created"]
    # attributes don't get modified, so use created timestamp as lastModified
    # TBD: but they can if replace is set!
    resp_json["lastModified"] = attribute["created"]
    if "encoding" in attribute:
        resp_json["encoding"] = attribute["encoding"]

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

        if body.get("encoding"):
            item_size = getItemSize(data_type)
            if item_size == "H5T_VARIABLE":
                msg = "base64 encoding is not support for variable length attributes"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            try:
                data = decodeData(value)
            except ValueError:
                msg = "unable to decode data"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

            expected_numbytes = arr_dtype.itemsize * np.prod(dims)
            if len(data) != expected_numbytes:
                msg = f"expected: {expected_numbytes} but got: {len(data)}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

            # check to see if this works with our shape and type
            try:
                log.debug(f"data: {data}")
                log.debug(f"type: {arr_dtype}")
                log.debug(f"np_dims: {np_dims}")
                arr = bytesToArray(data, arr_dtype, np_dims)
            except ValueError as e:
                msg = f"Bad Request: encoded input data doesn't match shape and type: {e}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

            value_json = None
            # now try converting to JSON
            list_data = arr.tolist()
            try:
                value_json = bytesArrayToList(list_data)
            except ValueError as err:
                msg = f"Cannot decode bytes to list: {err}, will store as encoded bytes"
                log.warn(msg)
            if value_json:
                log.debug("will store base64 input as json")
                if data_shape["class"] == "H5S_SCALAR":
                    # just use the scalar value
                    value = value_json[0]
                else:
                    value = value_json  # return this
            else:
                value = data  # return bytes to signal that this needs to be encoded
        else:
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
        if isinstance(attr_value, bytes):
            attr_value = encodeData(attr_value)  # store as base64
            attr_item["encoding"] = "base64"
        else:
            # just store the JSON dict or primitive value
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
    params = request.rel_url.query
    # returns datasets|groups|datatypes
    collection = getRequestCollectionName(request)

    req_obj_id = request.match_info.get("id")
    if not req_obj_id:
        msg = "Missing object id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(req_obj_id, obj_class=collection):
        msg = f"Invalid object id: {req_obj_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    attr_name = request.match_info.get("name")
    if attr_name:
        log.debug(f"Attribute name: [{attr_name}]")
        validateAttributeName(attr_name)

    log.info(f"PUT Attributes id: {req_obj_id} name: {attr_name}")
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

    await validateAction(app, domain, req_obj_id, username, "create")

    # get attribute from request body
    kwargs = {"bucket": bucket, "obj_id": req_obj_id}
    attr_body = await _getAttributeFromRequest(app, body, **kwargs)

    # write attribute to DN
    attr_json = {attr_name: attr_body}
    log.debug(f"putting attr {attr_name} to DN: {attr_json}")

    kwargs = {"bucket": bucket}
    if "replace" in params and params["replace"]:
        # allow attribute to be overwritten
        log.debug("setting replace for PUT Atttribute")
        kwargs["replace"] = True
    else:
        log.debug("replace is not set for PUT Attribute")
    status = await putAttributes(app, req_obj_id, attr_json, **kwargs)
    log.info(f"PUT Attributes status: {status}")

    req_rsp = {}
    # attribute creation successful
    resp = await jsonResponse(request, req_rsp, status=status)
    log.response(request, resp=resp)
    return resp


async def PUT_Attributes(request):
    """HTTP method to create a new attribute"""
    log.request(request)
    params = request.rel_url.query
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
    log.debug(f"got bucket: {bucket}")
    if "replace" in params and params["replace"]:
        replace = True
    else:
        replace = False

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
        # use the object id from the request
        obj_id = request.match_info.get("id")
        if not obj_id:
            msg = "Missing object id"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        obj_ids[obj_id] = attr_items  # make it look like a list for consistency

    log.debug(f"got {len(obj_ids)} obj_ids")

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
        kwargs = {"bucket": bucket, "attr_json": attr_json}
        if replace:
            kwargs["replace"] = True

        status = await putAttributes(app, obj_id, **kwargs)

    else:
        # put multi obj
        kwargs = {"action": "put_attr", "bucket": bucket}
        if replace:
            kwargs["replace"] = True
        crawler = DomainCrawler(app, obj_ids, **kwargs)

        # will raise exception on not found, server busy, etc.
        await crawler.crawl()

        status = crawler.get_status()

        log.info("DomainCrawler done for put_attrs action")

    req_rsp = {}
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

    await validateAction(app, domain, obj_id, username, "delete")

    attr_names = [attr_name, ]
    kwargs = {"attr_names": attr_names, "bucket": bucket}

    await deleteAttributes(app, obj_id, **kwargs)

    req_rsp = {}
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

    await validateAction(app, domain, obj_id, username, "read")

    params = request.rel_url.query
    if "ignore_nan" in params and params["ignore_nan"]:
        ignore_nan = True
    else:
        ignore_nan = False
    if "encoding" in params:
        encoding = params["encoding"]
        if encoding and encoding != "base64":
            msg = f"invalid encoding value: {encoding}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        encoding = None

    attr_names = [attr_name, ]
    kwargs = {"attr_names": attr_names, "bucket": bucket, "include_data": True}
    if ignore_nan:
        kwargs["ignore_nan"] = True

    attributes = await getAttributes(app, obj_id, **kwargs)

    if not attributes:
        msg = f"attribute {attr_name} not found"
        log.warn(msg)
        raise HTTPNotFound()

    dn_json = attributes[0]

    log.debug(f"got attributes json from dn for obj_id: {dn_json}")

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

    log.debug(f"response_type: {response_type}")

    if response_type == "binary" or encoding:
        arr_dtype = createDataType(type_json)  # np datatype
        np_shape = getShapeDims(shape_json)
        if dn_json["value"] is None:
            arr = np.zeros(np_shape, dtype=arr_dtype)
        elif dn_json.get("encoding") == "base64":
            # data is a base64 string we can directly convert to a
            # np array
            data = dn_json["value"]
            if not isinstance(data, str):
                msg = "expected string for base64 encoded attribute"
                msg += f" but got: {type(data)}"
                log.error(msg)
                raise HTTPInternalServerError()
            arr = bytesToArray(data, arr_dtype, np_shape, encoding="base64")
        else:
            try:
                arr = jsonToArray(np_shape, arr_dtype, dn_json["value"])
            except ValueError as e:
                msg = f"Bad Request: input data doesn't match selection: {e}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
        output_data = arrayToBytes(arr)
    else:
        output_data = None  # will return as json if possible

    if response_type == "binary":
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
            json_value = dn_json["value"]
            if dn_json.get("encoding") == "base64":
                resp_json["value"] = json_value
                resp_json["encoding"] = "base64"
            elif output_data is not None:
                # query param requesting base64 encoded value
                # convert output_data bytes to base64 string
                output_data = encodeData(output_data)
                output_data = output_data.decode("ascii")  # convert to a string
                resp_json["value"] = output_data
                resp_json["encoding"] = "base64"
            else:
                # just return json data
                resp_json["value"] = json_value

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

    await validateAction(app, domain, obj_id, username, "update")

    attr_names = [attr_name, ]
    kwargs = {"attr_names": attr_names, "bucket": bucket}

    attributes = await getAttributes(app, obj_id, **kwargs)

    if not attributes:
        msg = f"attribute {attr_name} not found"
        log.warn(msg)
        raise HTTPNotFound()

    dn_json = attributes[0]

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
        log.debug(f"read {len(binary_data)} bytes of binary data")

    arr = None  # np array to hold request data

    if binary_data:
        npoints = getNumElements(np_shape)
        if npoints * item_size != len(binary_data):
            msg = f"Expected: {npoints*item_size} bytes, "
            msg += f"but got {len(binary_data)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        arr = np.fromstring(binary_data, dtype=np_dtype)
        if attr_shape["class"] == "H5S_SCALAR":
            arr = arr.reshape([])
        else:
            arr = arr.reshape(np_shape)  # conform to selection shape
        log.debug(f"got array {arr} from binary data")
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

        if value is None:
            # write empty array
            arr = np.zeros(np_shape, dtype=np_dtype)
        elif "encoding" in body and body["encoding"] == "base64":
            arr = bytesToArray(value, np_dtype, np_shape, encoding="base64")
        else:
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

    # convert to base64 for transmission to DN
    data = arrayToBytes(arr, encoding="base64")

    # ready to add attribute now
    attr_body = {}
    attr_body["type"] = type_json
    attr_body["shape"] = attr_shape
    attr_body["value"] = data.decode("ascii")
    attr_body["encoding"] = "base64"
    attr_json = {attr_name: attr_body}

    kwargs = {"bucket": bucket, "replace": True}

    status = await putAttributes(app, obj_id, attr_json, **kwargs)

    if status != 200:
        msg = "putAttributesValue, expected DN status of 200"
        msg += f" but got {status}"
        log.warn(msg)
    else:
        log.info("PUT AttributesValue status: 200")

    req_rsp = {}
    # attribute creation successful
    resp = await jsonResponse(request, req_rsp)
    log.response(request, resp=resp)
    return resp


async def POST_Attributes(request):
    """HTTP method to get multiple attributes """
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
            msg = "no attr_names - will return all attributes for each object"
            log.debug(msg)
        for obj_id in obj_ids:
            items[obj_id] = None
    elif isinstance(obj_ids, dict):
        if attr_names is not None:
            msg = "attr_names must not be provided if obj_ids is a dict"
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

        if attr_names is None:
            log.debug(f"getting all attributes for {obj_id}")
        elif isinstance(attr_names, list):
            for attr_name in attr_names:
                validateAttributeName(attr_name)  # raises HTTPBadRequest if invalid
        else:
            msg = f"expected list for attr_names but got: {type(attr_names)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

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

    await validateAction(app, domain, obj_id, username, "read")

    params = request.rel_url.query
    log.debug(f"got params: {params}")
    include_data = False
    max_data_size = 0
    if "IncludeData" in params:
        IncludeData = params["IncludeData"]
        log.debug(f"got IncludeData: [{IncludeData}], type: {type(IncludeData)}")
        if IncludeData and IncludeData != "0":
            include_data = True
        log.debug(f"include_data: {include_data}")
    if "max_data_size" in params:
        try:
            max_data_size = int(params["max_data_size"])
        except ValueError:
            msg = "expected int for max_data_size"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    if params.get("ignore_nan"):
        ignore_nan = True
    else:
        ignore_nan = False

    if params.get("encoding"):
        encoding = params["encoding"]
        if encoding != "base64":
            msg = "only base64 encoding is supported"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        encoding = None

    resp_json = {}

    if len(items) == 0:
        msg = "no obj ids specified for POST Attributes"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    elif len(items) == 1:
        # just make a request the datanode
        obj_id = list(items.keys())[0]
        attr_names = items[obj_id]
        kwargs = {"attr_names": attr_names, "bucket": bucket}
        if include_data:
            log.debug("setting include_data to True")
            kwargs["include_data"] = True
        if max_data_size > 0:
            kwargs["max_data_size"] = max_data_size
        if ignore_nan:
            kwargs["ignore_nan"] = True
        if encoding:
            kwargs["encoding"] = encoding
        log.debug(f"getAttributes kwargs: {kwargs}")
        attributes = await getAttributes(app, obj_id, **kwargs)

        resp_json["attributes"] = attributes
    else:
        # get multi obj
        # don't follow links!
        kwargs = {"action": "get_attr", "bucket": bucket, "follow_links": False}
        kwargs["include_attrs"] = True
        if include_data:
            log.debug("setting include_data to True")
            kwargs["include_data"] = True
        if max_data_size > 0:
            kwargs["max_data_size"] = max_data_size
        if ignore_nan:
            kwargs["ignore_nan"] = True
        if encoding:
            pass
            # TBD: crawler_params["encoding"] = encoding
        log.debug(f"DomainCrawler kwargs: {kwargs}")
        crawler = DomainCrawler(app, items, **kwargs)
        # will raise exception on NotFound, etc.
        await crawler.crawl()

        msg = f"DomainCrawler returned: {len(crawler._obj_dict)} objects"
        log.info(msg)
        attributes = crawler._obj_dict
        # log attributes returned for each obj_id
        for obj_id in attributes:
            obj_attributes = attributes[obj_id]
            msg = f"POST_Attributes, obj_id {obj_id} "
            msg += f"returned {len(obj_attributes)}"
            log.debug(msg)

        log.debug(f"got {len(attributes)} attributes")
        resp_json["attributes"] = attributes

    resp = await jsonResponse(request, resp_json, ignore_nan=ignore_nan)
    log.response(request, resp=resp)
    return resp


async def DELETE_Attributes(request):
    """HTTP method to delete multiple attribute values"""
    log.request(request)
    app = request.app
    log.info("DELETE_Attributes")
    obj_id = request.match_info.get("id")
    if not isValidUuid(obj_id):
        msg = f"Invalid object id: {obj_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    params = request.rel_url.query
    log.debug(f"got params: {params}")

    if "attr_names" not in params:
        msg = "expected attr_names query param"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    attr_names_query_string = params["attr_names"]
    if not attr_names_query_string:
        msg = "empty attr_names query param"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if "encoding" in params:
        encoding = params["encoding"]
        if encoding != "base64":
            msg = "only base64 encoding is supported for attribute names"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        encoding = None

    if "separator" in params:
        separator = params["separator"]
    else:
        separator = "/"

    if encoding:
        # this can be used to deal with non-url encodable names
        attr_names_query_string = decodeData(attr_names_query_string).decode("ascii")

    log.debug(f"got attr_names query string: {attr_names_query_string}")

    # Use the given separator character to construct a list from
    # the query string
    attr_names = attr_names_query_string.split(separator)
    log.info(f"delete {len(attr_names)} attributes for {obj_id}")
    log.debug(f"attr_names: {attr_names}")

    username, pswd = getUserPasswordFromRequest(request)
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

    await validateAction(app, domain, obj_id, username, "delete")

    kwargs = {"attr_names": attr_names, "bucket": bucket, "separator": separator}

    await deleteAttributes(app, obj_id, **kwargs)

    resp_json = {}
    hrefs = []
    resp_json["hrefs"] = hrefs

    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp
