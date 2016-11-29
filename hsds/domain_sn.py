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
# service node of hsds cluster
# 
import json

from aiohttp import  HttpProcessingError 
from aiohttp.errors import HttpBadRequest

from util.httpUtil import  http_post, http_put, http_get_json, http_delete, jsonResponse
from util.idUtil import  getDataNodeUrl, createObjId
from util.authUtil import getUserPasswordFromRequest, aclCheck
from util.authUtil import validateUserPassword, getAclKeys
from util.domainUtil import getParentDomain, getDomainFromRequest, isValidDomain
from servicenode_lib import getDomainJson
import hsds_logger as log


async def GET_Domain(request):
    """HTTP method to return JSON for given domain"""
    log.request(request)
    app = request.app

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        validateUserPassword(username, pswd)
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # don't use app["domain_cache"]  if a direct domain request is made 
    # as opposed to an implicit request as with other operations, query
    # the domain from the authoritative source (the dn node)
    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain 
    log.info("sending dn req: {}".format(req))
     
    domain_json = await http_get_json(app, req)
     
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    log.info("got domain_json: {}".format(domain_json))
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "read", username)  # throws exception if not authorized

    # return just the keys as per the REST API
    rsp_json = { }
    if "root" in domain_json:
        rsp_json["root"] = domain_json["root"]
    if "owner" in domain_json:
        rsp_json["owner"] = domain_json["owner"]
    if "created" in domain_json:
        rsp_json["created"] = domain_json["created"]
    if "lastModified" in domain_json:
        rsp_json["lastModified"] = domain_json["lastModified"]

    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

async def PUT_Domain(request):
    """HTTP method to create a new domain"""
    log.request(request)
    app = request.app
    # yet exist
    username, pswd = getUserPasswordFromRequest(request) # throws exception if user/password is not valid
    validateUserPassword(username, pswd)
    log.info("PUT domain request from: {}".format(username))
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    parent_domain = getParentDomain(domain)
    if parent_domain is None:
        msg = "creation of top-level domains is not supported"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    log.info("parent_domain: {}".format(parent_domain))
    parent_json = None
    try:
        log.info("get parent domain {}".format(parent_domain))
        parent_json = await getDomainJson(app, parent_domain)
    except HttpProcessingError as hpe:
        msg = "Parent domain not found"
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)

    aclCheck(parent_json, "create", username)  # throws exception if not allowed
    
    # create a root group for the new domain
    # TBD - fire off create group and create domain dn requests at the same time
    root_id = createObjId("groups") 
    log.info("new root group id: {}".format(root_id))
    group_json = {"id": root_id, "root": root_id, "domain": domain }
    log.info("create group for domain, body: " + json.dumps(group_json))
    req = getDataNodeUrl(app, root_id) + "/groups"
    try:
        group_json = await http_post(app, req, data=group_json)
    except HttpProcessingError as ce:
        msg="Error creating root group for domain -- " + str(ce)
        log.warn(msg)
        raise ce
 
    domain_json = { }

    # construct dn request to create new domain
    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain 
    body = { "owner": username }
    body["acls"] = parent_json["acls"]  # copy parent acls to new domain
    body["root"] = root_id

    try:
        domain_json = await http_put(app, req, data=body)
    except HttpProcessingError as ce:
        msg="Error creating domain state -- " + str(ce)
        log.warn(msg)
        raise ce

    # domain creation successful     
    resp = await jsonResponse(request, domain_json, status=201)
    log.response(request, resp=resp)
    return resp

async def DELETE_Domain(request):
    """HTTP method to delete a domain resource"""
    log.request(request)
    app = request.app 

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
 
    username, pswd = getUserPasswordFromRequest(request)
    validateUserPassword(username, pswd)

    # verify that this is not a top-level domain
    parent_domain = getParentDomain(domain)
    if parent_domain is None:
        msg = "Top level domain can not be deleted"
        log.warn(msg)
        raise HttpProcessingError(code=403, message="Forbidden")
    try:
        log.info("get parent domain {}".format(parent_domain))
        parent_json = await getDomainJson(app, parent_domain)
    except HttpProcessingError as hpe:
        msg = "Attempt to delete domain with no parent domain"
        log.warn(msg)
        raise HttpProcessingError(code=403, message="Forbidden")
    log.info("got parent json: {}".format(parent_json))
    
    domain_json = await getDomainJson(app, domain)
    aclCheck(domain_json, "delete", username)  # throws exception if not allowed

    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain
    
    rsp_json = await http_delete(app, req)
 
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

async def GET_ACL(request):
    """HTTP method to return JSON for given domain/ACL"""
    log.request(request)
    app = request.app

    acl_username = request.match_info.get('username')
    if not acl_username:
        msg = "Missing username for ACL"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        validateUserPassword(username, pswd)
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # don't use app["domain_cache"]  if a direct domain request is made 
    # as opposed to an implicit request as with other operations, query
    # the domain from the authoritative source (the dn node)
    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain 
    log.info("sending dn req: {}".format(req))
     
    domain_json = await http_get_json(app, req)
     
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    acls = domain_json["acls"]

    log.info("got domain_json: {}".format(domain_json))
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "readACL", username)  # throws exception if not authorized

    if acl_username not in acls:
        msg = "acl for username: [{}] not found".format(acl_username)
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)

    acl = acls[acl_username]

    # return just the keys as per the REST API
    rsp_json = { }
    rsp_json["acl"] = acl
    rsp_json["hrefs"] = []  # TBD

    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

async def GET_ACLs(request):
    """HTTP method to return JSON for domain/ACLs"""
    log.request(request)
    app = request.app

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        validateUserPassword(username, pswd)
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # don't use app["domain_cache"]  if a direct domain request is made 
    # as opposed to an implicit request as with other operations, query
    # the domain from the authoritative source (the dn node)
    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain 
    log.info("sending dn req: {}".format(req))
     
    domain_json = await http_get_json(app, req)
     
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    acls = domain_json["acls"]

    log.info("got domain_json: {}".format(domain_json))
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "readACL", username)  # throws exception if not authorized

    acl_list = []
    acl_usernames = list(acls.keys())
    acl_usernames.sort()
    for acl_username in acl_usernames:
        entry = {"userName": acl_username}
        acl = acls[acl_username]

        for k in acl.keys():
            entry[k] = acl[k]
        acl_list.append(entry)
    # return just the keys as per the REST API
    rsp_json = { }
    rsp_json["acls"] = acl_list
    rsp_json["hrefs"] = []  # TBD

    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp    

async def PUT_ACL(request):
    """HTTP method to add a new ACL for a domain"""
    log.request(request)
    app = request.app

    acl_username = request.match_info.get('username')
    if not acl_username:
        msg = "Missing username for ACL"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    (username, pswd) = getUserPasswordFromRequest(request)
    validateUserPassword(username, pswd)

    if not request.has_body:
        msg = "PUT ACL with no body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    body = await request.json()   
    acl_keys = getAclKeys()

    for k in body.keys():
        if k not in acl_keys:
            msg = "Unexpected key in request body: {}".format(k)
            log.warn(k)
            raise HttpBadRequest(message=msg)
        if body[k] not in (True, False):
            msg = "Unexpected value for key in request body: {}".format(k)
            log.warn(k)
            raise HttpBadRequest(message=msg)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # don't use app["domain_cache"]  if a direct domain request is made 
    # as opposed to an implicit request as with other operations, query
    # the domain from the authoritative source (the dn node)
    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain + "/acls/" + acl_username
    log.info("sending dn req: {}".format(req))

    put_rsp = await http_put(app, req, data=body)
    log.info("PUT ACL resp: " + str(put_rsp))
    
    hrefs = []  # TBD
    req_rsp = { "hrefs": hrefs }
    # ACL update successful     
    resp = await jsonResponse(request, req_rsp, status=201)
    log.response(request, resp=resp)
    return resp


async def GET_Datasets(request):
    """HTTP method to return dataset collection for given domain"""
    log.request(request)
    app = request.app

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        validateUserPassword(username, pswd)
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # don't use app["domain_cache"]  if a direct domain request is made 
    # as opposed to an implicit request as with other operations, query
    # the domain from the authoritative source (the dn node)
    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain 
    log.info("sending dn req: {}".format(req))
     
    domain_json = await http_get_json(app, req)
     
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    log.info("got domain_json: {}".format(domain_json))
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "read", username)  # throws exception if not authorized

    # TBD: return the actual list of dataset ids.
    # for now just return empty array and hrefs
    rsp_json = { }
    rsp_json["datasets"] = []
    rsp_json["hrefs"] = []
     
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

async def GET_Groups(request):
    """HTTP method to return groups collection for given domain"""
    log.request(request)
    app = request.app

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        validateUserPassword(username, pswd)
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # don't use app["domain_cache"]  if a direct domain request is made 
    # as opposed to an implicit request as with other operations, query
    # the domain from the authoritative source (the dn node)
    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain 
    log.info("sending dn req: {}".format(req))
     
    domain_json = await http_get_json(app, req)
     
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    log.info("got domain_json: {}".format(domain_json))
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "read", username)  # throws exception if not authorized

    # TBD: return the actual list of dataset ids.
    # for now just return empty array and hrefs
    rsp_json = { }
    rsp_json["groups"] = []
    rsp_json["hrefs"] = []
     
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

async def GET_Datatypes(request):
    """HTTP method to return datatype collection for given domain"""
    log.request(request)
    app = request.app

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        validateUserPassword(username, pswd)
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # don't use app["domain_cache"]  if a direct domain request is made 
    # as opposed to an implicit request as with other operations, query
    # the domain from the authoritative source (the dn node)
    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain 
    log.info("sending dn req: {}".format(req))
     
    domain_json = await http_get_json(app, req)
     
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    log.info("got domain_json: {}".format(domain_json))
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "read", username)  # throws exception if not authorized

    # TBD: return the actual list of datatype ids.
    # for now just return empty array and hrefs
    rsp_json = { }
    rsp_json["datatypes"] = []
    rsp_json["hrefs"] = []
     
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp
    


 