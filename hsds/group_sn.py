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
from aiohttp.errors import HttpBadRequest, HttpProcessingError
 
from util.httpUtil import http_post, http_put, http_delete, jsonResponse, getHref
from util.idUtil import   isValidUuid, getDataNodeUrl, createObjId
from util.authUtil import getUserPasswordFromRequest, aclCheck, validateUserPassword
from util.domainUtil import  getDomainFromRequest, isValidDomain
from servicenode_lib import getDomainJson, getObjectJson, validateAction
import hsds_logger as log



async def GET_Group(request):
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
    
    # get authoritative state for group from DN (even if it's in the meta_cache).
    group_json = await getObjectJson(app, group_id, refresh=True)  
    
    await validateAction(app, domain, group_id, username, "read")

    hrefs = []
    group_uri = '/groups/'+group_id
    hrefs.append({'rel': 'self', 'href': getHref(request, group_uri)})
    hrefs.append({'rel': 'links', 'href': getHref(request, group_uri+'/links')})
    root_uri = '/groups/' + group_json["root"]    
    hrefs.append({'rel': 'root', 'href': getHref(request, root_uri)})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})
    hrefs.append({'rel': 'attributes', 'href': getHref(request, group_uri+'/attributes')})
    group_json["hrefs"] = hrefs

    resp = await jsonResponse(request, group_json)
    log.response(request, resp=resp)
    return resp

async def POST_Group(request):
    """HTTP method to create new Group object"""
    log.request(request)
    app = request.app

    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    validateUserPassword(username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    domain_json = await getDomainJson(app, domain)

    aclCheck(domain_json, "create", username)  # throws exception if not allowed

    if "root" not in domain_json:
        msg = "Expected root key for domain: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)

     
    link_id = None
    link_title = None
    if request.has_body:
        body = await request.json()  
        log.info("POST Group body: {}".format(body))
        if body:
            if "link" in body:
                link_body = body["link"]
                log.info("link_body: {}".format(link_body))
                if "id" in link_body:
                    link_id = link_body["id"]
                if "name" in link_body:
                    link_title = link_body["name"]
                if link_id and link_title:
                    log.info("link id: {}".format(link_id))
                    # verify that the referenced id exists and is in this domain
                    # and that the requestor has permissions to create a link
                    await validateAction(app, domain, link_id, username, "create")
            if not link_id or not link_title:
                log.warn("POST Group body with no link: {}".format(body))

    domain_json = await getDomainJson(app, domain) # get again in case cache was invalidated
         
    root_id = domain_json["root"]
    group_id = createObjId("groups") 
    log.info("new  group id: {}".format(group_id))
    group_json = {"id": group_id, "root": root_id, "domain": domain }
    log.info("create group, body: " + json.dumps(group_json))
    req = getDataNodeUrl(app, group_id) + "/groups"
    
    group_json = await http_post(app, req, data=group_json)

    # create link if requested
    if link_id and link_title:
        link_json={}
        link_json["id"] = link_id
        link_json["class"] = "H5L_TYPE_HARD"
        link_req = getDataNodeUrl(app, link_id)
        link_req += "/groups/" + link_id + "/links/" + link_title
        log.info("PUT link - : " + link_req)
        put_json_rsp = await http_put(app, link_req, data=link_json)
        log.info("PUT Link resp: {}".format(put_json_rsp))
    log.info("returning resp")
    # group creation successful     
    resp = await jsonResponse(request, group_json, status=201)
    log.response(request, resp=resp)
    return resp

async def DELETE_Group(request):
    """HTTP method to delete a group resource"""
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
    await validateAction(app, domain, group_id, username, "delete")

    if group_id == domain_json["root"]:
        msg = "Forbidden - deletion of root group is not allowed - delete domain first"
        log.warn(msg)
        raise HttpProcessingError(code=403, message=msg)

    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id
 
    rsp_json = await http_delete(app, req)

    if group_id in meta_cache:
        del meta_cache[group_id]  # remove from cache
 
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

 