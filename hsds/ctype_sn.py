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
 
import json
from aiohttp.web_exceptions import HTTPBadRequest, HTTPGone
 
from util.httpUtil import http_post, http_put, http_delete, getHref, jsonResponse
from util.idUtil import   isValidUuid, getDataNodeUrl, createObjId
from util.authUtil import getUserPasswordFromRequest, aclCheck, validateUserPassword
from util.domainUtil import  getDomainFromRequest, isValidDomain
from util.hdf5dtype import validateTypeItem, getBaseTypeJson
from servicenode_lib import getDomainJson, getObjectJson, validateAction, getObjectIdByPath, getPathForObjectId
import hsds_logger as log


async def GET_Datatype(request):
    """HTTP method to return JSON for committed datatype"""
    log.request(request)
    app = request.app 
    params = request.rel_url.query
    include_attrs = False

    h5path = None
    getAlias = False
    ctype_id = request.match_info.get('id')
    if not ctype_id and "h5path" not in params:
        msg = "Missing type id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if "include_attrs" in params and params["include_attrs"]:
        include_attrs = True

    if ctype_id:
        if not isValidUuid(ctype_id, "Type"):
            msg = "Invalid type id: {}".format(ctype_id)
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if "getalias" in params:
            if params["getalias"]:
                getAlias = True 
    else:
        group_id = None
        if "grpid" in params:
            group_id = params["grpid"]
            if not isValidUuid(group_id, "Group"):
                msg = "Invalid parent group id: {}".format(group_id)
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
        if "h5path" not in params:
            msg = "Expecting either ctype id or h5path url param"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        h5path = params["h5path"]
        if h5path[0] != '/' and group_id is None:
            msg = "h5paths must be absolute"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.info("GET_Datatype, h5path: {}".format(h5path))

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if h5path:
        domain_json = await getDomainJson(app, domain)
        if "root" not in domain_json:
            msg = "Expected root key for domain: {}".format(domain)
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if group_id is None:
            group_id = domain_json["root"]
        ctype_id = await getObjectIdByPath(app, group_id, h5path)  # throws 404 if not found
        if not isValidUuid(ctype_id, "Datatype"):
            msg = "No datatype exist with the path: {}".format(h5path)
            log.warn(msg)
            raise HTTPGone()
        log.info("got ctype_id: {} from h5path: {}".format(ctype_id, h5path))

    await validateAction(app, domain, ctype_id, username, "read")

    # get authoritative state for group from DN (even if it's in the meta_cache).
    type_json = await getObjectJson(app, ctype_id, refresh=True, include_attrs=include_attrs)  
    type_json["domain"] = domain

    if getAlias:
        root_id = type_json["root"]
        alias = []
        idpath_map = {root_id: '/'}
        h5path = await getPathForObjectId(app, root_id, idpath_map, tgt_id=ctype_id)
        if h5path:
            alias.append(h5path)
        type_json["alias"] = alias

    hrefs = []
    ctype_uri = '/datatypes/'+ctype_id
    hrefs.append({'rel': 'self', 'href': getHref(request, ctype_uri)})
    root_uri = '/groups/' + type_json["root"]    
    hrefs.append({'rel': 'root', 'href': getHref(request, root_uri)})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})
    hrefs.append({'rel': 'attributes', 'href': getHref(request, ctype_uri+'/attributes')})
    type_json["hrefs"] = hrefs

    resp = await jsonResponse(request, type_json)
    log.response(request, resp=resp)
    return resp

async def POST_Datatype(request):
    """HTTP method to create new committed datatype object"""
    log.request(request)
    app = request.app

    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    await validateUserPassword(app, username, pswd)

    if not request.has_body:
        msg = "POST Datatype with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    body = await request.json()
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
            log.debug("got datatype: {}".format(datatype))
        except TypeError:
            msg = "POST Dataset with invalid predefined type"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg) 
    validateTypeItem(datatype)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    
    domain_json = await getDomainJson(app, domain, reload=True)

    aclCheck(domain_json, "create", username)  # throws exception if not allowed

    if "root" not in domain_json:
        msg = "Expected root key for domain: {}".format(domain)
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    link_id = None
    link_title = None
    if "link" in body:
        link_body = body["link"]
        if "id" in link_body:
            link_id = link_body["id"]
        if "name" in link_body:
            link_title = link_body["name"]
        if link_id and link_title:
            log.debug("link id: {}".format(link_id))
            # verify that the referenced id exists and is in this domain
            # and that the requestor has permissions to create a link
            await validateAction(app, domain, link_id, username, "create")

    root_id = domain_json["root"]
    ctype_id = createObjId("datatypes", rootid=root_id) 
    log.debug("new  type id: {}".format(ctype_id))
    ctype_json = {"id": ctype_id, "root": root_id, "type": datatype }
    log.debug("create named type, body: " + json.dumps(ctype_json))
    req = getDataNodeUrl(app, ctype_id) + "/datatypes"
    
    type_json = await http_post(app, req, data=ctype_json)

    # create link if requested
    if link_id and link_title:
        link_json={}
        link_json["id"] = ctype_id
        link_json["class"] = "H5L_TYPE_HARD"
        link_req = getDataNodeUrl(app, link_id)
        link_req += "/groups/" + link_id + "/links/" + link_title
        log.debug("PUT link - : " + link_req)
        put_rsp = await http_put(app, link_req, data=link_json)
        log.debug("PUT Link resp: {}".format(put_rsp))

    # datatype creation successful     
    resp = await jsonResponse(request, type_json, status=201)
    log.response(request, resp=resp)

    return resp

async def DELETE_Datatype(request):
    """HTTP method to delete a committed type resource"""
    log.request(request)
    app = request.app 
    meta_cache = app['meta_cache']

    ctype_id = request.match_info.get('id')
    if not ctype_id:
        msg = "Missing committed type id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(ctype_id, "Type"):
        msg = "Invalid committed type id: {}".format(ctype_id)
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    username, pswd = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)
    
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    
    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    if "root" not in domain_json:
        log.error("Expected root key for domain: {}".format(domain))
        raise HTTPBadRequest(reason="Unexpected Error")

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, ctype_id, username, "delete")

    req = getDataNodeUrl(app, ctype_id) + "/datatypes/" + ctype_id
 
    await http_delete(app, req)

    if ctype_id in meta_cache:
        del meta_cache[ctype_id]  # remove from cache
 
    resp = await jsonResponse(request, {})
    log.response(request, resp=resp)
    return resp
