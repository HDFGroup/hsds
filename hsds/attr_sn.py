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

from util.httpUtil import  http_get, http_put, http_delete, getHref, getAcceptType, jsonResponse
from util.idUtil import   isValidUuid, getDataNodeUrl
from util.authUtil import getUserPasswordFromRequest, validateUserPassword
from util.domainUtil import  getDomainFromRequest, isValidDomain, getBucketForDomain
from util.attrUtil import  validateAttributeName, getRequestCollectionName
from util.hdf5dtype import validateTypeItem, getBaseTypeJson, createDataType, getItemSize
from util.arrayUtil import jsonToArray, getShapeDims, getNumElements, bytesArrayToList
from servicenode_lib import getDomainJson, getObjectJson, validateAction
import hsds_logger as log


async def GET_Attributes(request):
    """HTTP method to return JSON for attribute collection"""
    log.request(request)
    app = request.app
    params = request.rel_url.query
    collection = getRequestCollectionName(request) # returns datasets|groups|datatypes

    obj_id = request.match_info.get('id')
    if not obj_id:
        msg = "Missing object id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if not isValidUuid(obj_id, obj_class=collection):
        msg = "Invalid obj id: {}".format(obj_id)
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    include_data = False
    if "IncludeData" in params and params["IncludeData"]:
        include_data = True
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
    if username is None and app['allow_noauth']:
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

    req += '/' + collection + '/' + obj_id + "/attributes"
    params = {}
    if limit is not None:
        params["Limit"] = str(limit)
    if marker is not None:
        params["Marker"] = marker
    if include_data:
        params["IncludeData"] = '1'
    if bucket:
        params["bucket"] = bucket

    log.debug(f"get attributes: {req}")
    dn_json = await http_get(app, req, params=params)
    log.debug(f"got attributes json from dn for obj_id: {obj_id}")
    attributes = dn_json["attributes"]

    # mixin hrefs
    for attribute in attributes:
        attr_name = attribute["name"]
        attr_href = '/' + collection + '/' + obj_id + '/attributes/' + attr_name
        attribute["href"] = getHref(request, attr_href)

    resp_json = {}
    resp_json["attributes"] = attributes

    hrefs = []
    obj_uri = '/' + collection + '/' + obj_id
    hrefs.append({'rel': 'self', 'href': getHref(request, obj_uri + '/attributes')})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})
    hrefs.append({'rel': 'owner', 'href': getHref(request, obj_uri)})
    resp_json["hrefs"] = hrefs

    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

async def GET_Attribute(request):
    """HTTP method to return JSON for an attribute"""
    log.request(request)
    app = request.app
    collection = getRequestCollectionName(request) # returns datasets|groups|datatypes

    obj_id = request.match_info.get('id')
    if not obj_id:
        msg = "Missing object id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(obj_id, obj_class=collection):
        msg = f"Invalid object id: {obj_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    attr_name = request.match_info.get('name')
    validateAttributeName(attr_name)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
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
    req += '/' + collection + '/' + obj_id + "/attributes/" + attr_name
    log.debug("get Attribute: " + req)
    params = {}
    if bucket:
        params["bucket"] = bucket
    dn_json = await http_get(app, req, params=params)
    log.debug("got attributes json from dn for obj_id: " + str(obj_id))


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
    obj_uri = '/' + collection + '/' + obj_id
    attr_uri = obj_uri + '/attributes/' + attr_name
    hrefs.append({'rel': 'self', 'href': getHref(request, attr_uri)})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})
    hrefs.append({'rel': 'owner', 'href': getHref(request, obj_uri)})
    resp_json["hrefs"] = hrefs

    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

async def PUT_Attribute(request):
    """HTTP method to create a new attribute"""
    log.request(request)
    app = request.app
    collection = getRequestCollectionName(request) # returns datasets|groups|datatypes

    obj_id = request.match_info.get('id')
    if not obj_id:
        msg = "Missing object id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(obj_id, obj_class=collection):
        msg = f"Invalid object id: {obj_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    attr_name = request.match_info.get('name')
    log.debug(f"Attribute name: [{attr_name}]")
    validateAttributeName(attr_name)

    log.info(f"PUT Attribute id: {obj_id} name: {attr_name}")
    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    await validateUserPassword(app, username, pswd)

    if not request.has_body:
        msg = "PUT Attribute with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    body = await request.json()

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    if "root" not in domain_json:
        log.error(f"Expected root key for domain: {domain}")
        raise HTTPBadRequest(reason="Unexpected Error")
    root_id = domain_json["root"]

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "create")

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
        if ctype_json["root"] != root_id:
            msg = "Referenced committed datatype must belong in same domain"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        datatype = ctype_json["type"]
        # add the ctype_id to type type
        datatype["id"] = ctype_id
    elif isinstance(datatype, str):
        try:
            # convert predefined type string (e.g. "H5T_STD_I32LE") to
            # corresponding json representation
            datatype = getBaseTypeJson(datatype)
            log.debug(f"got datatype: {datatype}")
        except TypeError:
            msg = "PUT attribute with invalid predefined type"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    validateTypeItem(datatype)

    dims = None
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
                dims = [1,]
            else:
                shape_json["class"] = "H5S_SIMPLE"
                dims = getShapeDims(shape_body)
                shape_json["dims"] = dims
    else:
        shape_json["class"] = "H5S_SCALAR"
        dims = [1,]


    if "value" in body:
        if dims is None:
            msg = "Bad Request: data can not be included with H5S_NULL space"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        value = body["value"]
        # validate that the value agrees with type/shape
        arr_dtype = createDataType(datatype)  # np datatype
        if len(dims) == 0:
            np_dims = [1,]
        else:
            np_dims = dims
        log.debug(f"attribute dims: {np_dims}")
        log.debug(f"attribute value: {value}")
        try:
            arr = jsonToArray(np_dims, arr_dtype, value)
        except ValueError:
            msg = "Bad Request: input data doesn't match selection"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.info(f"Got: {arr.size} array elements")
    else:
        value = None

    # ready to add attribute now
    req = getDataNodeUrl(app, obj_id)
    req += '/' + collection + '/' + obj_id + "/attributes/" + attr_name
    log.info("PUT Attribute: " + req)

    attr_json = {}
    attr_json["type"] = datatype
    attr_json["shape"] = shape_json
    if value is not None:
        attr_json["value"] = value
    params = {}
    if bucket:
        params["bucket"] = bucket

    put_rsp = await http_put(app, req, params=params, data=attr_json)
    log.info(f"PUT Attribute resp: {put_rsp}")

    hrefs = []  # TBD
    req_rsp = { "hrefs": hrefs }
    # attribute creation successful
    resp = await jsonResponse(request, req_rsp, status=201)
    log.response(request, resp=resp)
    return resp

async def DELETE_Attribute(request):
    """HTTP method to delete a attribute resource"""
    log.request(request)
    app = request.app
    collection = getRequestCollectionName(request) # returns datasets|groups|datatypes

    obj_id = request.match_info.get('id')
    if not obj_id:
        msg = "Missing object id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(obj_id, obj_class=collection):
        msg = f"Invalid object id: {obj_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    attr_name = request.match_info.get('name')
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
    if "root" not in domain_json:
        log.error(f"Expected root key for domain: {domain}")
        raise HTTPBadRequest(reason="Unexpected Error")

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "delete")

    req = getDataNodeUrl(app, obj_id)
    req += '/' + collection + '/' + obj_id + "/attributes/" + attr_name
    log.info("PUT Attribute: " + req)
    params = {}
    if bucket:
        params["bucket"] = bucket
    rsp_json = await http_delete(app, req, params=params)

    log.info(f"PUT Attribute resp: {rsp_json}")

    hrefs = []  # TBD
    req_rsp = { "hrefs": hrefs }
    resp = await jsonResponse(request, req_rsp)
    log.response(request, resp=resp)
    return resp

async def GET_AttributeValue(request):
    """HTTP method to return an attribute value"""
    log.request(request)
    app = request.app
    log.info("GET_AttributeValue")
    collection = getRequestCollectionName(request) # returns datasets|groups|datatypes

    obj_id = request.match_info.get('id')
    if not obj_id:
        msg = "Missing object id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(obj_id, obj_class=collection):
        msg = f"Invalid object id: {obj_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    attr_name = request.match_info.get('name')
    validateAttributeName(attr_name)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
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
    if "root" not in domain_json:
        log.error(f"Expected root key for domain: {domain}")
        raise HTTPBadRequest(reason="Unexpected Error")

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "read")

    req = getDataNodeUrl(app, obj_id)
    req += '/' + collection + '/' + obj_id + "/attributes/" + attr_name
    log.debug("get Attribute: " + req)
    params = {}
    if bucket:
        params["bucket"] = bucket
    dn_json = await http_get(app, req, params=params)
    log.debug("got attributes json from dn for obj_id: " + str(dn_json))

    attr_shape = dn_json["shape"]
    log.debug(f"attribute shape: {attr_shape}")
    if attr_shape["class"] == 'H5S_NULL':
        msg = "Null space attributes can not be read"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    accept_type = getAcceptType(request)
    response_type = accept_type    # will adjust later if binary not possible
    type_json = dn_json["type"]
    shape_json = dn_json["shape"]
    item_size = getItemSize(type_json)

    if item_size == 'H5T_VARIABLE' and accept_type != "json":
        msg = "Client requested binary, but only JSON is supported for variable length data types"
        log.info(msg)
        response_type = "json"

    if response_type == "binary":
        arr_dtype = createDataType(type_json)  # np datatype
        np_shape = getShapeDims(shape_json)
        try:
            arr = jsonToArray(np_shape, arr_dtype, dn_json["value"])
        except ValueError:
            msg = "Bad Request: input data doesn't match selection"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        output_data = arr.tobytes()
        log.debug(f"GET AttributeValue - returning {len(output_data)} bytes binary data")
        # write response
        try:
            resp = StreamResponse()
            resp.content_type = "application/octet-stream"
            resp.content_length = len(output_data)
            # allow CORS
            resp.headers['Access-Control-Allow-Origin'] = '*'
            resp.headers['Access-Control-Allow-Methods'] = "GET, POST, DELETE, PUT, OPTIONS"
            resp.headers['Access-Control-Allow-Headers'] = "Content-Type, api_key, Authorization"
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
        obj_uri = '/' + collection + '/' + obj_id
        attr_uri = obj_uri + '/attributes/' + attr_name
        hrefs.append({'rel': 'self', 'href': getHref(request, attr_uri)})
        hrefs.append({'rel': 'home', 'href': getHref(request, '/')})
        hrefs.append({'rel': 'owner', 'href': getHref(request, obj_uri)})
        resp_json["hrefs"] = hrefs
        resp = await jsonResponse(request, resp_json)
        log.response(request, resp=resp)
    return resp

async def PUT_AttributeValue(request):
    """HTTP method to update an attributes data"""
    log.request(request)
    log.info("PUT_AttributeValue")
    app = request.app
    collection = getRequestCollectionName(request) # returns datasets|groups|datatypes

    obj_id = request.match_info.get('id')
    if not obj_id:
        msg = "Missing object id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(obj_id, obj_class=collection):
        msg = f"Invalid object id: {obj_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    attr_name = request.match_info.get('name')
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
    if "root" not in domain_json:
        log.error(f"Expected root key for domain: {domain}")
        raise HTTPInternalServerError()

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "update")

    req = getDataNodeUrl(app, obj_id)
    req += '/' + collection + '/' + obj_id + "/attributes/" + attr_name
    log.debug("get Attribute: " + req)
    params = {}
    if bucket:
        params["bucket"] = bucket
    dn_json = await http_get(app, req, params=params)
    log.debug("got attributes json from dn for obj_id: " + str(obj_id))
    log.debug(f"got dn_json: {dn_json}")

    attr_shape = dn_json["shape"]
    if attr_shape["class"] == 'H5S_NULL':
        msg = "Null space attributes can not be updated"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    np_shape = getShapeDims(attr_shape)
    type_json = dn_json["type"]
    np_dtype = createDataType(type_json)  # np datatype

    request_type = "json"
    if "Content-Type" in request.headers:
        # client should use "application/octet-stream" for binary transfer
        content_type = request.headers["Content-Type"]
        if content_type not in ("application/json", "application/octet-stream"):
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

        if item_size == 'H5T_VARIABLE':
            msg = "Only JSON is supported for variable length data types"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        # read binary data
        binary_data = await request.read()
        if len(binary_data) != request.content_length:
            msg = f"Read {len(binary_data)} bytes, expecting: {request.content_length}"
            log.error(msg)
            raise HTTPInternalServerError()

    arr = None  # np array to hold request data

    if binary_data:
        npoints = getNumElements(np_shape)
        if npoints*item_size != len(binary_data):
            msg = "Expected: " + str(npoints*item_size) + " bytes, but got: " + str(len(binary_data))
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        arr = np.fromstring(binary_data, dtype=np_dtype)
        arr = arr.reshape(np_shape)  # conform to selection shape
        # convert to JSON for transmission to DN
        data = arr.tolist()
        value = bytesArrayToList(data)
    else:
        body = await request.json()

        if "value" not in body:
            msg = "PUT attribute value with no value in body"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        value = body["value"]

        # validate that the value agrees with type/shape
        try:
            arr = jsonToArray(np_shape, np_dtype, value)
        except ValueError:
            msg = "Bad Request: input data doesn't match selection"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    log.info(f"Got: {arr.size} array elements")

    # ready to add attribute now
    attr_json = {}
    attr_json["type"] = type_json
    attr_json["shape"] = attr_shape
    attr_json["value"] = value

    req = getDataNodeUrl(app, obj_id)
    req += '/' + collection + '/' + obj_id + "/attributes/" + attr_name
    log.info(f"PUT Attribute Value: {req}")

    dn_json["value"] = value
    params = {}
    params = {"replace": 1}  # let the DN know we can overwrite the attribute
    if bucket:
        params["bucket"] = bucket
    put_rsp = await http_put(app, req, params=params, data=attr_json)
    log.info(f"PUT Attribute Value resp: {put_rsp}")

    hrefs = []  # TBD
    req_rsp = { "hrefs": hrefs }
    # attribute creation successful
    resp = await jsonResponse(request, req_rsp)
    log.response(request, resp=resp)
    return resp
