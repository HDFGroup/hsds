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
 
from aiohttp.errors import HttpBadRequest 
 
from util.httpUtil import  http_get_json, http_put, http_delete, jsonResponse, getHref
from util.idUtil import   isValidUuid, getDataNodeUrl
from util.authUtil import getUserPasswordFromRequest, validateUserPassword
from util.domainUtil import  getDomainFromRequest, isValidDomain
from util.attrUtil import  validateAttributeName, getRequestCollectionName
from util.hdf5dtype import validateTypeItem, getBaseTypeJson
from servicenode_lib import getDomainJson, validateAction
import hsds_logger as log


async def GET_Attributes(request):
    """HTTP method to return JSON for attribute collection"""
    log.request(request)
    app = request.app 
    collection = getRequestCollectionName(request) # returns datasets|groups|datatypes

    obj_id = request.match_info.get('id')
    if not obj_id:
        msg = "Missing object id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    if not isValidUuid(obj_id, obj_class=collection):
        msg = "Invalid obj id: {}".format(obj_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    limit = None
    if "Limit" in request.GET:
        try:
            limit = int(request.GET["Limit"])
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.warn(msg)
            raise HttpBadRequest(message=msg)
    marker = None
    if "Marker" in request.GET:
        marker = request.GET["Marker"]
    
    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        validateUserPassword(username, pswd)
    
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    if "root" not in domain_json:
        log.error("Expected root key for domain: {}".format(domain))
        raise HttpBadRequest(message="Unexpected Error")

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "read")

    req = getDataNodeUrl(app, obj_id)
    
    req += '/' + collection + '/' + obj_id + "/attributes" 
    params = {}
    if limit is not None:
        params["Limit"] = str(limit)
    if marker is not None:
        params["Marker"] = marker
        
    log.info("get attributes: " + req)
    dn_json = await http_get_json(app, req, params=params)
    log.info("got attributes json from dn for obj_id: " + str(obj_id)) 
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
        raise HttpBadRequest(message=msg)
    if not isValidUuid(obj_id, obj_class=collection):
        msg = "Invalid object id: {}".format(obj_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    attr_name = request.match_info.get('name')
    validateAttributeName(attr_name)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        validateUserPassword(username, pswd)
    
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    if "root" not in domain_json:
        log.error("Expected root key for domain: {}".format(domain))
        raise HttpBadRequest(message="Unexpected Error")

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "read")

    req = getDataNodeUrl(app, obj_id)
    req += '/' + collection + '/' + obj_id + "/attributes/" + attr_name
    log.info("get Attribute: " + req)
    dn_json = await http_get_json(app, req)
    log.info("got attributes json from dn for obj_id: " + str(obj_id)) 
   
     
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
        raise HttpBadRequest(message=msg)
    if not isValidUuid(obj_id, obj_class=collection):
        msg = "Invalid object id: {}".format(obj_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    attr_name = request.match_info.get('name')
    log.info("Attribute name: [{}]".format(attr_name) )
    validateAttributeName(attr_name)

    log.info("PUT Attribute id: {} name: {}".format(obj_id, attr_name))
    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    validateUserPassword(username, pswd)

    if not request.has_body:
        msg = "PUT Attribute with no body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    body = await request.json()   

    dims = None
    datatype = None
    shape = None
    value = None

    if "type" not in body:
        msg = "PUT attribute with no type in body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    datatype = body["type"]
    if isinstance(datatype, str):
        try:
            # convert predefined type string (e.g. "H5T_STD_I32LE") to 
            # corresponding json representation
            datatype = getBaseTypeJson(datatype)
            log.info("got datatype: {}".format(datatype))
        except TypeError:
            # TBD: Handle the case where the string is a committed type reference
            msg = "POST Dataset with invalid predefined type"
            log.warn(msg)
            raise HttpBadRequest(message=msg) 

    validateTypeItem(datatype)

    dims = []  # default as empty tuple (will create a scalar attribute)
    if "shape" in body:
        shape = body["shape"]
        if isinstance(shape, int):
            dims = [shape,]
        elif isinstance(shape, list) or isinstance(shape, tuple):
            dims = shape  # can use as is
        elif shape == 'H5S_NULL':
            log.info("Put attribute with NULL shape")
        else:
            msg = "Bad Request: shape is invalid!"
            log.warn(msg)
            raise HttpBadRequest(message=msg)  

    if dims is not None:
        if "value" not in body:
            msg = "Bad Request: value not specified"
            log.warn(msg)
            raise HttpBadRequest(message=msg)  
                
        value = body["value"]
        # TBD - validate that the value agrees with type/shape

    if shape is None:
        # For no shape, if value is supplied, then this is a scalar
        # otherwise a null space attribute
        if value is None:
            shape = {"class": "H5S_NULL"}
        else:
            shape = {"class": "H5S_SCALAR"}

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    if "root" not in domain_json:
        log.error("Expected root key for domain: {}".format(domain))
        raise HttpBadRequest(message="Unexpected Error")

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "create")
     
    # ready to add attribute now
    req = getDataNodeUrl(app, obj_id)
    req += '/' + collection + '/' + obj_id + "/attributes/" + attr_name
    log.info("PUT Attribute: " + req)

    attr_json = {}
    attr_json["type"] = datatype
    attr_json["shape"] = shape
    attr_json["value"] = value
    
    put_rsp = await http_put(app, req, data=attr_json)
    log.info("PUT Attribute resp: " + str(put_rsp))
    
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
        raise HttpBadRequest(message=msg)
    if not isValidUuid(obj_id, obj_class=collection):
        msg = "Invalid object id: {}".format(obj_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    attr_name = request.match_info.get('name')
    log.info("Attribute name: [{}]".format(attr_name) )
    validateAttributeName(attr_name)

    username, pswd = getUserPasswordFromRequest(request)
    validateUserPassword(username, pswd)
    
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    if "root" not in domain_json:
        log.error("Expected root key for domain: {}".format(domain))
        raise HttpBadRequest(message="Unexpected Error")

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, obj_id, username, "delete")

    req = getDataNodeUrl(app, obj_id)
    req += '/' + collection + '/' + obj_id + "/attributes/" + attr_name
    log.info("PUT Attribute: " + req)
    rsp_json = await http_delete(app, req)
    
    log.info("PUT Attribute resp: " + str(rsp_json))
    
    hrefs = []  # TBD
    req_rsp = { "hrefs": hrefs }

    resp = await jsonResponse(request, req_rsp)
    log.response(request, resp=resp)
    return resp

 