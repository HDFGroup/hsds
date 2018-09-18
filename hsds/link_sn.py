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
 
from aiohttp.http_exceptions import HttpBadRequest
 
from util.httpUtil import  http_get_json, http_put, http_delete, jsonResponse, getHref
from util.idUtil import   isValidUuid, getDataNodeUrl, getCollectionForId
from util.authUtil import getUserPasswordFromRequest,   validateUserPassword
from util.domainUtil import  getDomainFromRequest, isValidDomain
from util.linkUtil import validateLinkName
from servicenode_lib import  validateAction, getObjectJson
import hsds_logger as log


async def GET_Links(request):
    """HTTP method to return JSON for link collection"""
    log.request(request)
    app = request.app 

    group_id = request.match_info.get('id')
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(group_id, obj_class="Group"):
        msg = "Invalid group id: {}".format(group_id)
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
        await validateUserPassword(app, username, pswd)
    
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    await validateAction(app, domain, group_id, username, "read")

    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links" 
    query_sep = '?'
    if limit is not None:
        req += query_sep + "Limit=" + str(limit)
        query_sep = '&'
    if marker is not None:
        req += query_sep + "Marker=" + marker
        
    log.debug("get LINKS: " + req)
    links_json = await http_get_json(app, req)
    log.debug("got links json from dn for group_id: {}".format(group_id)) 
    links = links_json["links"]

    # mix in collection key, target and hrefs
    for link in links:
        if link["class"] == "H5L_TYPE_HARD": 
            collection_name = getCollectionForId(link["id"])
            link["collection"] = collection_name
            target_uri = '/' + collection_name + '/' + link["id"]
            link["target"] = getHref(request, target_uri)
        link_uri = '/groups/' + group_id + '/links/' + link['title']
        link["href"] = getHref(request, link_uri)
 
    resp_json = {}
    resp_json["links"] = links
    hrefs = []
    group_uri = '/groups/'+group_id
    hrefs.append({'rel': 'self', 'href': getHref(request, group_uri+'/links')})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')}) 
    hrefs.append({'rel': 'owner', 'href': getHref(request, group_uri)})     
    resp_json["hrefs"] = hrefs
 
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

async def GET_Link(request):
    """HTTP method to return JSON for a group link"""
    log.request(request)
    app = request.app 

    group_id = request.match_info.get('id')
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(group_id, obj_class="Group"):
        msg = "Invalid group id: {}".format(group_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    link_title = request.match_info.get('title')
    validateLinkName(link_title)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)
    
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    await validateAction(app, domain, group_id, username, "read")
    
    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links/" + link_title
    log.debug("get LINK: " + req)
    link_json = await http_get_json(app, req)
    log.debug("got link_json: " + str(link_json)) 
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

    hrefs = []
    group_uri = '/groups/'+group_id
    hrefs.append({'rel': 'self', 'href': getHref(request, group_uri+'/links/'+link_title)})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')}) 
    hrefs.append({'rel': 'owner', 'href': getHref(request, group_uri)})
    if link_json["class"] == "H5L_TYPE_HARD":
        target = '/' + resp_link["collection"] + '/' + resp_link["id"]
        hrefs.append({'rel': 'target', 'href': getHref(request, target)})
     
    resp_json["hrefs"] = hrefs
    
 
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

async def PUT_Link(request):
    """HTTP method to create a new link"""
    log.request(request)
    app = request.app

    group_id = request.match_info.get('id')
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(group_id, obj_class="Group"):
        msg = "Invalid group id: {}".format(group_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    link_title = request.match_info.get('title')
    log.info("PUT Link_title: [{}]".format(link_title) )
    validateLinkName(link_title)


    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    await validateUserPassword(app, username, pswd)

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
    await validateAction(app, domain, group_id, username, "create")
    

    # for hard links, verify that the referenced id exists and is in this domain
    if "id" in body:
        ref_id = body["id"]
        ref_json = await getObjectJson(app, ref_id)
        group_json = await getObjectJson(app, group_id)
        if ref_json["root"] != group_json["root"]:
            msg = "Hard link must reference an object in the same domain"
            log.warn(msg)
            raise HttpBadRequest(message=msg)

    # ready to add link now
    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links/" + link_title
    log.debug("PUT link - getting group: " + req)
    
    put_rsp = await http_put(app, req, data=link_json)
    log.debug("PUT Link resp: " + str(put_rsp))

    hrefs = []  # TBD
    req_rsp = { "hrefs": hrefs }
    # link creation successful     
    resp = await jsonResponse(request, req_rsp, status=201)
    log.response(request, resp=resp)
    return resp

async def DELETE_Link(request):
    """HTTP method to delete a link"""
    log.request(request)
    app = request.app 

    group_id = request.match_info.get('id')
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(group_id, obj_class="Group"):
        msg = "Invalid group id: {}".format(group_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    link_title = request.match_info.get('title')
    validateLinkName(link_title)

    username, pswd = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)
    
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    await validateAction(app, domain, group_id, username, "delete")

    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links/" + link_title
    rsp_json = await http_delete(app, req)
    
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

 