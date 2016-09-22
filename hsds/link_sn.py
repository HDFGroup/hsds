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
 
from aiohttp.errors import HttpBadRequest
 
from util.httpUtil import  http_get_json, http_put, http_delete, jsonResponse
from util.idUtil import   isValidUuid, getDataNodeUrl, getCollectionForId
from util.authUtil import getUserPasswordFromRequest, aclCheck, validateUserPassword
from util.domainUtil import  getDomainFromRequest, isValidDomain
from util.linkUtil import validateLinkName
from servicenode_lib import getDomainJson
import hsds_logger as log


async def GET_Link(request):
    """HTTP method to return JSON for group"""
    log.request(request)
    app = request.app 

    group_id = request.match_info.get('id')
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(group_id, "Group"):
        msg = "Invalid group id: {}".format(group_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    link_title = request.match_info.get('title')
    validateLinkName(link_title)

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
    
    domain_json = await getDomainJson(app, domain)
    aclCheck(domain_json, "read", username)  # throws exception if not allowed

    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links/" + link_title
    log.info("get LINK: " + req)
    link_json = await http_get_json(app, req)
    log.info("got link_json: " + str(link_json)) 
    resp_link = {}
    resp_link["title"] = link_title
    resp_link["class"] = link_json["class"]
    if link_json["class"] == "H5L_TYPE_HARD":
        resp_link["id"] = link_json["id"]
        resp_link["collection"] = getCollectionForId(link_json["id"])
    elif link_json["class"] == "H5L_TYPE_SOFT":
        resp_link["h5path"] = link_json["h5path"]
    elif link_json["class"] == "H5L_TYPE_EXTERNAL":
        resp_link["h5path"] = link_json["h5path"]
        resp_link["h5domain"] = link_json["h5domain"]
    else:
        log.warn("Unexpected link class: {}".format(link_json["class"]))
    resp_json = {}
    resp_json["link"] = resp_link
    resp_json["created"] = link_json["created"]
    # links don't get modified, so use created timestamp as lastModified
    resp_json["lastModified"] = link_json["created"]  
    resp_json["hrefs"] = [] # tbd
    
 
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

async def PUT_Link(request):
    """HTTP method to return JSON for group"""
    log.request(request)
    app = request.app
    meta_cache = app['meta_cache']

    group_id = request.match_info.get('id')
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(group_id, "Group"):
        msg = "Invalid group id: {}".format(group_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    link_title = request.match_info.get('title')
    validateLinkName(link_title)


    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    validateUserPassword(username, pswd)

    if not request.has_body:
        msg = "PUT Link with no body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    body = await request.json()   

    link_json = {}
    if "id" in body:
        if not isValidUuid(body["id"]):
            msg = "PUT Link with invalid id in body"
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        link_json["id"] = body["id"]
        link_json["class"] = "H5L_TYPE_HARD"

    elif "h5path" in body:
        link_json["h5path"] = body["h5path"]
        # could be hard or soft link
        if "h5domain" in body:
            link_json["h5domain"] = body["h5domain"]
            link_json["class"] = "H5L_TYPE_EXTERNAL"
        else:
            # soft link
            link_json["class"] = "H5L_TYPE_SOFT"
    else:
        msg = "PUT Link with no id or h5path keys"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

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

    group_json = None
    if group_id in meta_cache:
        group_json = meta_cache[group_id]
    else:
        # fetch from DN
        req = getDataNodeUrl(app, group_id)
        req += "/groups/" + group_id
        group_json = await http_get_json(app, req) 
        meta_cache[group_id] = group_json

    if group_json["root"] != domain_json["root"]:
        msg = "Group id is not a member of the given domain"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    aclCheck(domain_json, "create", username)  # throws exception if not allowed

    # for hard links, verify that the referenced id exists and is in this domain
    if "id" in body:
        ref_id = body["id"]
        req = getDataNodeUrl(app, ref_id)
        req += '/' + getCollectionForId(ref_id) + '/' + ref_id
        ref_json = await http_get_json(app, req)  # throws 404 if doesn't exist'
        meta_cache[ref_id] = ref_json
        if ref_json["root"] != group_json["root"]:
            msg = "Hard link must reference an object in the same domain"
            log.warn(msg)
            raise HttpBadRequest(message=msg)

    # ready to add link now
    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links/" + link_title
    log.info("PUT link - getting group: " + req)
    
    put_rsp = await http_put(app, req, data=link_json)
    log.info("PUT Link resp: " + str(put_rsp))
    
    hrefs = []  # TBD
    req_rsp = { "hrefs": hrefs }
    # link creation successful     
    resp = await jsonResponse(request, req_rsp, status=201)
    log.response(request, resp=resp)
    return resp

async def DELETE_Link(request):
    """HTTP method to delete a group resource"""
    log.request(request)
    app = request.app 

    group_id = request.match_info.get('id')
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(group_id, "Group"):
        msg = "Invalid group id: {}".format(group_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    link_title = request.match_info.get('title')
    validateLinkName(link_title)

    username, pswd = getUserPasswordFromRequest(request)
    validateUserPassword(username, pswd)
    
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    domain_json = await getDomainJson(app, domain)
    aclCheck(domain_json, "delete", username)  # throws exception if not allowed

    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links/" + link_title
    rsp_json = await http_delete(app, req)
    
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

 