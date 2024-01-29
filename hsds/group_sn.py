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

from .util.httpUtil import getHref, jsonResponse, getBooleanParam
from .util.idUtil import isValidUuid
from .util.authUtil import getUserPasswordFromRequest, aclCheck
from .util.authUtil import validateUserPassword
from .util.domainUtil import getDomainFromRequest, isValidDomain
from .util.domainUtil import getBucketForDomain, getPathForDomain, verifyRoot
from .util.linkUtil import validateLinkName
from .servicenode_lib import getDomainJson, getObjectJson, validateAction, deleteObj, createGroup
from .servicenode_lib import getObjectIdByPath, getPathForObjectId, createGroupByPath
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
        msg = f"GET_Group, h5path: {h5path}"
        if group_id:
            msg += f" group_id: {group_id}"
        log.info(msg)

    include_links = getBooleanParam(params, "include_links")
    include_attrs = getBooleanParam(params, "include_attrs")

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
        kwargs = {"bucket": bucket, "domain": domain}
        group_id, domain, obj_json = await getObjectIdByPath(app, group_id, h5path, **kwargs)

        if not isValidUuid(group_id, "Group"):
            msg = f"No group exist with the path: {h5path}"
            log.warn(msg)
            raise HTTPNotFound()
        log.info(f"get group_id: {group_id} from h5path: {h5path} in the domain: {domain}")

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
    params = request.rel_url.query

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
    root_id = domain_json["root"]

    # allow parent group creation or not
    implicit = getBooleanParam(params, "implicit")

    parent_id = None
    h5path = None
    creation_props = None

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
                if "h5path" in body:
                    msg = "link can't be used with h5path"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
                link_body = body["link"]
                log.debug(f"link_body: {link_body}")
                if "id" in link_body:
                    parent_id = link_body["id"]
                if "name" in link_body:
                    link_title = link_body["name"]
                    try:
                        # will throw exception if there's a slash in the name
                        validateLinkName(link_title)
                    except ValueError:
                        msg = f"invalid link title: {link_title}"
                        log.warn(msg)
                        raise HTTPBadRequest(reason=msg)

                if parent_id and link_title:
                    log.debug(f"parent id: {parent_id}, link_title: {link_title}")
                    h5path = link_title  # just use the link name as the h5path

            if "h5path" in body:
                h5path = body["h5path"]
                if "parent_id" not in body:
                    parent_id = root_id
                else:
                    parent_id = body["parent_id"]
            if "creationProperties" in body:
                creation_props = body["creationProperties"]

    if parent_id:
        kwargs = {"bucket": bucket, "parent_id": parent_id, "h5path": h5path}
        if creation_props:
            kwargs["creation_props"] = creation_props
        if implicit:
            kwargs["implicit"] = True
        log.debug(f"createGroupByPath args: {kwargs}")
        group_json = await createGroupByPath(app, **kwargs)
    else:
        # create an anonymous group
        kwargs = {"bucket": bucket, "root_id": root_id}
        if creation_props:
            kwargs["creation_props"] = creation_props
        group_json = await createGroup(app, **kwargs)

    log.debug(f"returning resp: {group_json}")
    # group creation successful
    resp = await jsonResponse(request, group_json, status=201)
    log.response(request, resp=resp)
    return resp


async def DELETE_Group(request):
    """HTTP method to delete a group resource"""
    log.request(request)
    app = request.app

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

    await validateAction(app, domain, group_id, username, "delete")

    verifyRoot(domain_json)

    if group_id == domain_json["root"]:
        msg = "Forbidden - deletion of root group is not allowed - "
        msg += "delete domain first"
        log.warn(msg)
        raise HTTPForbidden()

    await deleteObj(app, group_id, bucket=bucket)

    resp = await jsonResponse(request, {})
    log.response(request, resp=resp)
    return resp
