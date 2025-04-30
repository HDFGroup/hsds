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
# handles datatypes requests
#

from aiohttp.web_exceptions import HTTPBadRequest, HTTPGone
from json import JSONDecodeError

from h5json.hdf5dtype import validateTypeItem, getBaseTypeJson
from h5json.objid import isValidUuid

from .util.httpUtil import getHref, respJsonAssemble, getBooleanParam
from .util.httpUtil import jsonResponse
from .util.linkUtil import validateLinkName
from .util.authUtil import getUserPasswordFromRequest, aclCheck
from .util.authUtil import validateUserPassword
from .util.domainUtil import getDomainFromRequest, getPathForDomain, isValidDomain
from .util.domainUtil import getBucketForDomain, verifyRoot
from .servicenode_lib import getDomainJson, getObjectJson, validateAction
from .servicenode_lib import getObjectIdByPath, getPathForObjectId
from .servicenode_lib import createObject, createObjectByPath, deleteObject
from . import hsds_logger as log


async def GET_Datatype(request):
    """HTTP method to return JSON for committed datatype"""
    log.request(request)
    app = request.app
    params = request.rel_url.query
    include_attrs = False

    h5path = None
    getAlias = False
    ctype_id = request.match_info.get("id")
    if not ctype_id and "h5path" not in params:
        msg = "Missing type id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if "include_attrs" in params and params["include_attrs"]:
        include_attrs = True

    if ctype_id:
        if not isValidUuid(ctype_id, "datatypes"):
            msg = f"Invalid type id: {ctype_id}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if "getalias" in params:
            if params["getalias"]:
                getAlias = True
    else:
        group_id = None
        if "grpid" in params:
            group_id = params["grpid"]
            if not isValidUuid(group_id, "groups"):
                msg = f"Invalid parent group id: {group_id}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
        if "h5path" not in params:
            msg = "Expecting either ctype id or h5path url param"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        h5path = params["h5path"]
        if h5path[0] != "/" and group_id is None:
            msg = "h5paths must be absolute"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        msg = f"GET_Datatype, h5path: {h5path}"
        if group_id:
            msg += f" group_id: {group_id}"
        log.info(msg)

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

    if h5path:
        domain_json = await getDomainJson(app, domain)
        verifyRoot(domain_json)

        if group_id is None:
            group_id = domain_json["root"]
        # throws 404 if not found
        kwargs = {"bucket": bucket, "domain": domain}
        ctype_id, domain, _ = await getObjectIdByPath(app, group_id, h5path, **kwargs)
        if not isValidUuid(ctype_id, "datatypes"):
            msg = f"No datatype exist with the path: {h5path}"
            log.warn(msg)
            raise HTTPGone()
        log.info(f"got ctype_id: {ctype_id} from h5path: {h5path}")

    await validateAction(app, domain, ctype_id, username, "read")

    # get authoritative state for ctype from DN
    #   (even if it's in the meta_cache)
    kwargs = {"bucket": bucket, "refresh": True, "include_attrs": include_attrs}

    type_json = await getObjectJson(app, ctype_id, **kwargs)
    type_json = respJsonAssemble(type_json, params, ctype_id)
    type_json["domain"] = getPathForDomain(domain)

    if getAlias:
        root_id = type_json["root"]
        alias = []
        idpath_map = {root_id: "/"}
        kwargs = {"bucket": bucket, "tgt_id": ctype_id}
        h5path = await getPathForObjectId(app, root_id, idpath_map, **kwargs)
        if h5path:
            alias.append(h5path)
        type_json["alias"] = alias

    hrefs = []
    ctype_uri = "/datatypes/" + ctype_id
    hrefs.append({"rel": "self", "href": getHref(request, ctype_uri)})
    root_uri = "/groups/" + type_json["root"]
    hrefs.append({"rel": "root", "href": getHref(request, root_uri)})
    hrefs.append({"rel": "home", "href": getHref(request, "/")})
    href = getHref(request, ctype_uri + "/attributes")
    hrefs.append({"rel": "attributes", "href": href})
    type_json["hrefs"] = hrefs

    resp = await jsonResponse(request, type_json)
    log.response(request, resp=resp)
    return resp


async def POST_Datatype(request):
    """HTTP method to create new committed datatype object"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    await validateUserPassword(app, username, pswd)

    if not request.has_body:
        msg = "POST Datatype with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    try:
        body = await request.json()
    except JSONDecodeError:
        msg = "Unable to load JSON body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if "type" not in body:
        msg = "POST Datatype has no type key in body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    datatype = body["type"]
    if isinstance(datatype, str):
        try:
            # convert predefined type string (e.g. "H5T_STD_I32LE") to
            # corresponding json representation
            datatype = getBaseTypeJson(datatype)
            log.debug(f"got datatype: {datatype}")
        except TypeError:
            msg = "POST Dataset with invalid predefined type"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    try:
        validateTypeItem(datatype)
    except KeyError as ke:
        msg = f"KeyError creating type: {ke}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    except TypeError as te:
        msg = f"TypeError creating type: {te}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    except ValueError as ve:
        msg = f"ValueError creating type: {ve}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

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

    parent_id = None
    link_title = None
    obj_id = None
    h5path = None
    if "id" in body:
        obj_id = body["id"]
        log.debug(f"POST datatype using client id: {obj_id}")

    if "link" in body:
        if "h5path" in body:
            msg = "link can't be used with h5path"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        link_body = body["link"]
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

    # setup args to createObject
    kwargs = {"bucket": bucket, "obj_type": datatype}
    if obj_id:
        kwargs["obj_id"] = obj_id

    # TBD: creation props for datatype obj?
    if parent_id:
        kwargs["parent_id"] = parent_id
        kwargs["h5path"] = h5path
        # allow parent group creation or not
        implicit = getBooleanParam(params, "implicit")
        if implicit:
            kwargs["implicit"] = True
        ctype_json = await createObjectByPath(app, **kwargs)
    else:
        # create an anonymous datatype
        kwargs["root_id"] = root_id
        ctype_json = await createObject(app, **kwargs)

    # datatype creation successful
    resp = await jsonResponse(request, ctype_json, status=201)
    log.response(request, resp=resp)

    return resp


async def DELETE_Datatype(request):
    """HTTP method to delete a committed type resource"""
    log.request(request)
    app = request.app
    ctype_id = request.match_info.get("id")
    if not ctype_id:
        msg = "Missing committed type id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(ctype_id, "datatypes"):
        msg = f"Invalid committed type id: {ctype_id}"
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
    params = {}
    if bucket:
        params["bucket"] = bucket

    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    verifyRoot(domain_json)

    await validateAction(app, domain, ctype_id, username, "delete")

    await deleteObject(app, ctype_id, bucket=bucket)

    resp = await jsonResponse(request, {})
    log.response(request, resp=resp)
    return resp
