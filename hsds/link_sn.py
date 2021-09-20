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

from aiohttp.web_exceptions import HTTPBadRequest, HTTPConflict

from .util.httpUtil import http_get, http_put, http_delete, getHref
from .util.httpUtil import jsonResponse
from .util.idUtil import isValidUuid, getDataNodeUrl, getCollectionForId
from .util.authUtil import getUserPasswordFromRequest,   validateUserPassword
from .util.domainUtil import getDomainFromRequest, isValidDomain
from .util.domainUtil import getBucketForDomain
from .util.linkUtil import validateLinkName
from .servicenode_lib import validateAction, getObjectJson
from . import config
from . import hsds_logger as log


async def GET_Links(request):
    """HTTP method to return JSON for link collection"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    group_id = request.match_info.get('id')
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(group_id, obj_class="Group"):
        msg = f"Invalid group id: {group_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
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
        msg = f"domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)
    if not bucket:
        bucket = config.get("bucket_name")

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
    params = {}
    if bucket:
        params["bucket"] = bucket
    links_json = await http_get(app, req, params=params)
    log.debug(f"got links json from dn for group_id: {group_id}")
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
    href = getHref(request, group_uri+'/links')
    hrefs.append({'rel': 'self', 'href': href})
    href = getHref(request, '/')
    hrefs.append({'rel': 'home', 'href': href})
    href = getHref(request, group_uri)
    hrefs.append({'rel': 'owner', 'href': href})
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
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(group_id, obj_class="Group"):
        msg = f"Invalid group id: {group_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    link_title = request.match_info.get('title')
    validateLinkName(link_title)

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
    if not bucket:
        bucket = config.get("bucket_name")

    await validateAction(app, domain, group_id, username, "read")

    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links/" + link_title
    log.debug("get LINK: " + req)
    params = {}
    if bucket:
        params["bucket"] = bucket
    link_json = await http_get(app, req, params=params)
    log.debug("got link_json: " + str(link_json))
    resp_link = {}
    resp_link["title"] = link_title
    link_class = link_json["class"]
    resp_link["class"] = link_class
    if link_class == "H5L_TYPE_HARD":
        resp_link["id"] = link_json["id"]
        resp_link["collection"] = getCollectionForId(link_json["id"])
    elif link_class == "H5L_TYPE_SOFT":
        resp_link["h5path"] = link_json["h5path"]
    elif link_class == "H5L_TYPE_EXTERNAL":
        resp_link["h5path"] = link_json["h5path"]
        resp_link["h5domain"] = link_json["h5domain"]
    else:
        log.warn(f"Unexpected link class: {link_class}")
    resp_json = {}
    resp_json["link"] = resp_link
    resp_json["created"] = link_json["created"]
    # links don't get modified, so use created timestamp as lastModified
    resp_json["lastModified"] = link_json["created"]

    hrefs = []
    group_uri = '/groups/'+group_id
    href = getHref(request, f"{group_uri}/links/{link_title}")
    hrefs.append({'rel': 'self', 'href': href})
    href = getHref(request, '/')
    hrefs.append({'rel': 'home', 'href': href})
    href = getHref(request, group_uri)
    hrefs.append({'rel': 'owner', 'href': href})
    if link_json["class"] == "H5L_TYPE_HARD":
        target = '/' + resp_link["collection"] + '/' + resp_link["id"]
        href = getHref(request, target)
        hrefs.append({'rel': 'target', 'href': href})

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
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(group_id, obj_class="Group"):
        msg = f"Invalid group id: {group_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    link_title = request.match_info.get('title')
    log.info(f"PUT Link_title: [{link_title}]")
    validateLinkName(link_title)

    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    await validateUserPassword(app, username, pswd)

    if not request.has_body:
        msg = "PUT Link with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    body = await request.json()

    link_json = {}
    if "id" in body:
        if not isValidUuid(body["id"]):
            msg = "PUT Link with invalid id in body"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
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
        raise HTTPBadRequest(reason=msg)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)
    if not bucket:
        bucket = config.get("bucket_name")
    await validateAction(app, domain, group_id, username, "create")

    # for hard links, verify that the referenced id exists and is in
    # this domain
    if "id" in body:
        ref_id = body["id"]
        ref_json = await getObjectJson(app, ref_id, bucket=bucket)
        group_json = await getObjectJson(app, group_id, bucket=bucket)
        if ref_json["root"] != group_json["root"]:
            msg = "Hard link must reference an object in the same domain"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    # ready to add link now
    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links/" + link_title
    log.debug("PUT link - getting group: " + req)
    params = {}
    if bucket:
        params["bucket"] = bucket
    try:
        put_rsp = await http_put(app, req, data=link_json, params=params)
        log.debug("PUT Link resp: " + str(put_rsp))
        dn_status = 201
    except HTTPConflict:
        # check to see if this is just a duplicate put of an existing link
        dn_status = 409
        log.warn(f"PUT Link: got conflict error for link_json: {link_json}")
        existing_link = await http_get(app, req, params=params)
        log.warn(f"PUT Link: fetched existing link: {existing_link}")
        for prop in ("class", "id", "h5path", "h5domain"):
            if prop in link_json:
                if prop not in existing_link:
                    msg = f"PUT Link - prop {prop} not found in existing "
                    msg += "link, returning 409"
                    log.warn(msg)
                    break
                if link_json[prop] != existing_link[prop]:
                    msg = f"PUT Link - prop {prop} value is different, old: "
                    msg += f"{existing_link[prop]}, new: {link_json[prop]}, "
                    msg += "returning 409"
                    log.warn(msg)
                    break
        else:
            log.info("PUT link is identical to existing value returning OK")
            # return 200 since we didn't actually create a resource
            dn_status = 200
        if dn_status == 409:
            raise  # return 409 to client
    hrefs = []  # TBD
    req_rsp = {"hrefs": hrefs}
    # link creation successful
    # returns 201 if new link was created, 200 if this is a duplicate
    # of an existing link
    resp = await jsonResponse(request, req_rsp, status=dn_status)
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
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(group_id, obj_class="Group"):
        msg = f"Invalid group id: {group_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    link_title = request.match_info.get('title')
    validateLinkName(link_title)

    username, pswd = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    bucket = getBucketForDomain(domain)
    if not bucket:
        bucket = config.get("bucket_name")
        
    await validateAction(app, domain, group_id, username, "delete")

    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links/" + link_title
    params = {}
    if bucket:
        params["bucket"] = bucket
    rsp_json = await http_delete(app, req, params=params)

    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp
