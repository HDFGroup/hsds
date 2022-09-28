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
# group handler for service node of hsds cluster
#

from aiohttp.web_exceptions import HTTPBadRequest, HTTPForbidden, HTTPNotFound
from json import JSONDecodeError

from .util.httpUtil import http_post, http_put, http_delete, getHref
from .util.httpUtil import jsonResponse
from .util.idUtil import isValidUuid, getDataNodeUrl, createObjId
from .util.authUtil import getUserPasswordFromRequest, aclCheck
from .util.authUtil import validateUserPassword
from .util.domainUtil import getDomainFromRequest, isValidDomain
from .util.domainUtil import getBucketForDomain, getPathForDomain, verifyRoot
from .servicenode_lib import getDomainJson, getObjectJson, validateAction
from .servicenode_lib import getObjectIdByPath, getPathForObjectId
from . import hsds_logger as log


async def GET_Group(request):
    """HTTP method to return JSON for group"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    h5path = None
    getAlias = False
    include_links = False
    include_attrs = False
    group_id = request.match_info.get("id")
    if not group_id and "h5path" not in params:
        # no id, or path provided, so bad request
        msg = "Missing group id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if group_id:
        log.info(f"GET_Group, id: {group_id}")
        # is the id a group id and not something else?
        if not isValidUuid(group_id, "Group"):
            msg = f"Invalid group id: {group_id}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if "getalias" in params:
            if params["getalias"]:
                getAlias = True
    if "h5path" in params:
        h5path = params["h5path"]
        if not group_id and h5path[0] != "/":
            msg = "h5paths must be absolute if no parent id is provided"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.info(f"GET_Group, h5path: {h5path}")
    if "include_links" in params and params["include_links"]:
        include_links = True
    if "include_attrs" in params and params["include_attrs"]:
        include_attrs = True

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

    if h5path and h5path[0] == "/":
        # ignore the request path id (if given) and start
        # from root group for absolute paths

        domain_json = await getDomainJson(app, domain)
        verifyRoot(domain_json)
        group_id = domain_json["root"]

    if h5path:
        # throws 404 if not found
        kwargs = {"bucket": bucket}
        group_id = await getObjectIdByPath(app, group_id, h5path, **kwargs)
        if not isValidUuid(group_id, "Group"):
            msg = f"No group exist with the path: {h5path}"
            log.warn(msg)
            raise HTTPNotFound()
        log.info(f"get group_id: {group_id} from h5path: {h5path}")

    # verify authorization to read the group
    await validateAction(app, domain, group_id, username, "read")

    # get authoritative state for group from DN (even if it's in the
    # meta_cache).
    kwargs = {
        "refresh": True,
        "include_links": include_links,
        "include_attrs": include_attrs,
        "bucket": bucket,
    }
    group_json = await getObjectJson(app, group_id, **kwargs)
    log.debug(f"domain from request: {domain}")
    group_json["domain"] = getPathForDomain(domain)
    if bucket:
        group_json["bucket"] = bucket

    if getAlias:
        root_id = group_json["root"]
        alias = []
        if group_id == root_id:
            alias.append("/")
        else:
            id_map = {root_id: "/"}
            kwargs = {"bucket": bucket, "tgt_id": group_id}
            h5path = await getPathForObjectId(app, root_id, id_map, **kwargs)
            if h5path:
                alias.append(h5path)
        group_json["alias"] = alias

    hrefs = []
    group_uri = "/groups/" + group_id
    href = getHref(request, group_uri)
    hrefs.append({"rel": "self", "href": href})
    href = getHref(request, group_uri + "/links")
    hrefs.append({"rel": "links", "href": href})
    root_uri = "/groups/" + group_json["root"]
    href = getHref(request, root_uri)
    hrefs.append({"rel": "root", "href": href})
    href = getHref(request, "/")
    hrefs.append({"rel": "home", "href": href})
    href = getHref(request, group_uri + "/attributes")
    hrefs.append({"rel": "attributes", "href": href})
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
    await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    domain_json = await getDomainJson(app, domain, reload=True)

    # throws exception if not allowed
    aclCheck(app, domain_json, "create", username)

    verifyRoot(domain_json)

    link_id = None
    link_title = None
    if request.has_body:

        try:
            body = await request.json()
        except JSONDecodeError:
            msg = "Unable to load JSON body"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        log.info(f"POST Group body: {body}")
        if body:
            if "link" in body:
                link_body = body["link"]
                log.debug(f"link_body: {link_body}")
                if "id" in link_body:
                    link_id = link_body["id"]
                if "name" in link_body:
                    link_title = link_body["name"]
                if link_id and link_title:
                    log.debug(f"link id: {link_id}")
                    # verify that the referenced id exists and is in this
                    # domainand that the requestor has permissions to create
                    # a link
                    act = "create"
                    await validateAction(app, domain, link_id, username, act)
            if not link_id or not link_title:
                log.warn(f"POST Group body with no link: {body}")

    # get again in case cache was invalidated
    domain_json = await getDomainJson(app, domain)

    root_id = domain_json["root"]
    group_id = createObjId("groups", rootid=root_id)
    log.info(f"new  group id: {group_id}")
    group_json = {"id": group_id, "root": root_id}
    log.debug(f"create group, body: {group_json}")
    req = getDataNodeUrl(app, group_id) + "/groups"
    params = {}
    if bucket:
        params["bucket"] = bucket

    group_json = await http_post(app, req, data=group_json, params=params)

    # create link if requested
    if link_id and link_title:
        link_json = {}
        link_json["id"] = group_id
        link_json["class"] = "H5L_TYPE_HARD"
        link_req = getDataNodeUrl(app, link_id)
        link_req += "/groups/" + link_id + "/links/" + link_title
        log.debug("PUT link - : " + link_req)
        kwargs = {"data": link_json, "params": params}
        put_json_rsp = await http_put(app, link_req, **kwargs)
        log.debug(f"PUT Link resp: {put_json_rsp}")
    log.debug("returning resp")
    # group creation successful
    resp = await jsonResponse(request, group_json, status=201)
    log.response(request, resp=resp)
    return resp


async def DELETE_Group(request):
    """HTTP method to delete a group resource"""
    log.request(request)
    app = request.app
    meta_cache = app["meta_cache"]

    group_id = request.match_info.get("id")
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(group_id, "Group"):
        msg = f"Invalid group id: {group_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

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

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, group_id, username, "delete")

    verifyRoot(domain_json)

    if group_id == domain_json["root"]:
        msg = "Forbidden - deletion of root group is not allowed - "
        msg += "delete domain first"
        log.warn(msg)
        raise HTTPForbidden()

    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id
    params = {}
    if bucket:
        params["bucket"] = bucket
    log.debug(f"http_delete req: {req} params: {params}")

    await http_delete(app, req, params=params)

    if group_id in meta_cache:
        del meta_cache[group_id]  # remove from cache

    resp = await jsonResponse(request, {})
    log.response(request, resp=resp)
    return resp
