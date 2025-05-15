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

from h5json.objid import isValidUuid

from .util.httpUtil import getHref, jsonResponse, getBooleanParam
from .util.authUtil import getUserPasswordFromRequest, aclCheck
from .util.authUtil import validateUserPassword
from .util.domainUtil import getDomainFromRequest, isValidDomain
from .util.domainUtil import getBucketForDomain, getPathForDomain, verifyRoot
from .util.linkUtil import validateLinkName, getRequestLinks
from .servicenode_lib import getDomainJson, getObjectJson, validateAction
from .servicenode_lib import getObjectIdByPath, getPathForObjectId
from .servicenode_lib import createObject, createObjectByPath, deleteObject
from . import hsds_logger as log
from .post_crawl import createObjects
from . import config


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


async def _create_group(app, **kwargs):
    """ helper method for group creation """

    if kwargs.get("parent_id") and kwargs.get("h5path"):
        group_json = await createObjectByPath(app, **kwargs)
    else:
        # create an anonymous group
        log.debug(f"_create_group - kwargs: {kwargs}")
        group_json = await createObject(app, **kwargs)

    return group_json


def _get_create_args(body, root_id=None, bucket=None, implicit=False):
    """ get query args for _create_group from request body """
    kwargs = {"bucket": bucket}
    predate_max_time = config.get("predate_max_time", default=10.0)

    parent_id = None
    obj_id = None
    h5path = None

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

    if "parent_id" not in body:
        parent_id = root_id
    else:
        parent_id = body["parent_id"]

    if "h5path" in body:
        h5path = body["h5path"]
        # normalize the h5path
        if h5path.startswith("/"):
            if parent_id == root_id:
                # just adjust the path to be relative
                h5path = h5path[1:]
            else:
                msg = f"PostCrawler expecting relative h5path, but got: {h5path}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

        if h5path.endswith("/"):
            h5path = h5path[:-1]  # makes iterating through the links a bit easier

    if parent_id and h5path:
        # these are used by createObjectByPath
        kwargs["parent_id"] = parent_id
        kwargs["implicit"] = implicit
        kwargs["h5path"] = h5path
    else:
        kwargs["root_id"] = root_id

    if "id" in body:
        obj_id = body["id"]
        # tbd: validate this is a group id
        kwargs["obj_id"] = obj_id
        log.debug(f"POST group using client id: {obj_id}")

    if "creationProperties" in body:
        creation_props = body["creationProperties"]
        # tbd: validate creation_props
        kwargs["creation_props"] = creation_props

    if "attributes" in body:
        attrs = body["attributes"]
        if not isinstance(attrs, dict):
            msg = f"POST_Groups expected dict for for attributes, but got: {type(attrs)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.debug(f"POST Group attributes: {attrs}")

        # tbd: validate attributes
        kwargs["attrs"] = attrs
    if "links" in body:
        body_links = body["links"]
        log.debug(f"got links for new group: {body_links}")
        try:
            links = getRequestLinks(body["links"], predate_max_time=predate_max_time)
        except ValueError:
            msg = "invalid link item sent in request"
            raise HTTPBadRequest(reason=msg)
        log.debug(f"adding links to group POST request: {links}")
        kwargs["links"] = links

    return kwargs


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
    log.debug(f"got domain_json: {domain_json}")

    # throws exception if not allowed
    aclCheck(app, domain_json, "create", username)

    verifyRoot(domain_json)
    root_id = domain_json["root"]

    # allow parent group creation or not
    implicit = getBooleanParam(params, "implicit")
    kwargs = {}
    post_group_rsp = None
    if request.has_body:
        try:
            body = await request.json()
        except JSONDecodeError:
            msg = "Unable to load JSON body"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        log.info(f"POST Group body: {body}")
        if body:
            if isinstance(body, list):
                count = len(body)
                log.debug(f"multiple group create: {count} items")
                if count == 0:
                    # equivalent to no body, anonymous group case
                    kwargs = {"root_id": root_id, "bucket": bucket}
                elif count == 1:
                    # just create one object in typical way
                    kwargs = _get_create_args(body[0],
                                              root_id=root_id,
                                              bucket=bucket,
                                              implicit=implicit)
                else:
                    # create multiple group objects
                    kwarg_list = []  # list of kwargs for each object

                    for item in body:
                        log.debug(f"item: {item}")
                        if not isinstance(item, dict):
                            msg = f"PostGroup - invalid item type: {type(item)}"
                            log.warn(msg)
                            raise HTTPBadRequest(reason=msg)
                        kwargs = _get_create_args(item, root_id=root_id, bucket=bucket)
                        kwarg_list.append(kwargs)
                        kwargs = {"bucket": bucket, "root_id": root_id}
                    post_group_rsp = await createObjects(app, kwarg_list, **kwargs)
            else:
                kwargs = _get_create_args(body, root_id=root_id, bucket=bucket, implicit=implicit)
        else:
            kwargs["root_id"] = root_id
            kwargs["bucket"] = bucket
    else:
        kwargs = {"root_id": root_id, "bucket": bucket}

    if post_group_rsp is None:
        # Handle cases other than multi-group create here
        log.debug(f"_create_group - kwargs: {kwargs}")
        post_group_rsp = await _create_group(app, **kwargs)

    log.debug(f"returning resp: {post_group_rsp}")
    # group creation successful
    resp = await jsonResponse(request, post_group_rsp, status=201)
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

    await deleteObject(app, group_id, bucket=bucket)

    resp = await jsonResponse(request, {})
    log.response(request, resp=resp)
    return resp
