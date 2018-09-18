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
#import asyncio 
import json
from aiohttp.http_exceptions import HttpBadRequest, HttpProcessingError

from util.httpUtil import  http_post, http_put, http_get_json, http_delete, jsonResponse, getHref
from util.idUtil import  getDataNodeUrl, createObjId
#from util.s3Util import getS3Keys
from util.authUtil import getUserPasswordFromRequest, aclCheck
from util.authUtil import validateUserPassword, getAclKeys
from util.domainUtil import getParentDomain, getDomainFromRequest
from servicenode_lib import getDomainJson, getObjectJson, getObjectIdByPath
from basenode import getAsyncNodeUrl
import hsds_logger as log
import config

async def get_domain_json(app, domain):
    req = getDataNodeUrl(app, domain)
    req += "/domains" 
    params = {"domain": domain}
    log.info("sending dn req: {}".format(req))
    domain_json = await http_get_json(app, req, params=params)
    return domain_json

async def domain_query(app, domain, rsp_dict):
    try :
        domain_json = await get_domain_json(app, domain)
        rsp_dict[domain] = domain_json
    except HttpProcessingError as hpe:
        rsp_dict[domain] = { "status_code": hpe.code}

async def getRootInfo(app, root_id, verbose=False):
    """ Get extra information about the given domain """
    # Gather additional info on the domain
    an_url = getAsyncNodeUrl(app)
    req = an_url + "/root/" + root_id
    log.info("ASync GET: {}".format(root_id))
    params = {}
    if verbose:
        params["verbose"] = 1
    try:
        root_info = await http_get_json(app, req, params=params)
    except HttpProcessingError as hpe:
        if hpe.code == 501:
            log.warn("sqlite db not available")
            return None
        if hpe.code == 404:
            # sqlite db not sync'd?
            log.warn("root id: {} not found in db".format(root_id))
            return None
        else:
            log.error("Async error: {}".format(hpe))
            raise HttpProcessingError(code=500, message="Unexpected Error")
    return root_info

async def get_toplevel_domains(app):
    """ Get list of top level domains """
    an_url = getAsyncNodeUrl(app)
    req = an_url + "/domains"
    params = {"domain": "/"}
    log.info("ASync GET TopLevelDomains")
    try:
        rsp_json = await http_get_json(app, req, params=params)
    except HttpProcessingError as hpe:
        if hpe.code == 501:
            log.warn("sqlite db not available")
            return None
        if hpe.code == 404:
            # sqlite db not sync'd?
            log.warn("404 repsonse for get_toplevel_domains")
            return None
        else:
            log.error("Async error: {}".format(hpe))
            raise HttpProcessingError(code=500, message="Unexpected Error")
    if "domains" not in rsp_json:
        log.error("domains not found in get_toplevel_domain request")
        raise HttpProcessingError(code=500, message="Unexpected Error")

    return rsp_json["domains"]



async def get_collection(app, root_id, collection, marker=None, limit=None):
    """ Return the object ids for given collection.
    """   
    root_info = await getRootInfo(app, root_id, verbose=True)
    if root_info is None:
        return None
    log.info("got root_info: {}".format(root_info))
     
    obj_map = root_info["objects"] 
    if root_id not in obj_map:
        msg = "Expected to get root_id: {} in collection map".formt(root_id)
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")

    root = obj_map[root_id]
    if collection not in root:
        msg = "Expected to find key: {} in obj_map".format(collection)
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    
    obj_col = root[collection]

    obj_ids = []
    for obj_id in obj_col:
        obj_ids.append(obj_id)
    obj_ids.sort()  # sort keys 

    rows = []
    for obj_id in obj_ids:
        object = obj_col[obj_id]
        object["id"] = obj_id
        # expected keys:
        #   id - objectid
        #   etag
        #   size
        #   lastModified
           
        if marker:
            if marker == obj_id:
                # got to the marker, clear it so we will start 
                # return ids on the next iteration
                marker = None
        else:
            # return id, etag, lastModified, and size
            if obj_id == root_id:
                continue  # don't include root obj
            rows.append(object)
            if limit is not None and len(rows) == limit:
                log.info("got to limit of: {}, breaking".format(limit))
                break
    log.debug("get_collection returning: {}".format(rows))
    return rows
 
        
async def get_domains(request):
    """ This method is called by GET_Domains and GET_Domain when no domain is passed in.
    """
    app = request.app
    # if there is no domain passed in, get a list of top level domains
    log.info("get_domains")
    params = {}
    if "domain" not in request.GET:
        params["prefix"] = '/'
    else:
        params["prefix"] = request.GET["domain"]

    # always use "verbose" to pull info from RootTable
    if "verbose" in request.GET and request.GET["verbose"]:
        params["verbose"] = 1
    else:
        params["verbose"] = 0

    if not params["prefix"].startswith('/'):
        msg = "Prefix must start with '/'"
        log.warn(msg)
        raise HttpBadRequest(message=msg)     

    if "Limit" in request.GET:
        try:
            params["Limit"] = int(request.GET["Limit"])
            log.debug("GET_Domains - using Limit: {}".format(params["Limit"]))
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.warn(msg)   
            raise HttpBadRequest(message=msg)
    if "Marker" in request.GET:
        params["Marker"] = request.GET["Marker"]
        log.debug("got Marker request param: {}".format(params["Marker"]))


    an_url = getAsyncNodeUrl(app)
    req = an_url + "/domains"
    log.debug("get /domains: {}".format(params))
    obj_json = await http_get_json(app, req, params=params)
    if "domains" in obj_json:
        domains = obj_json["domains"]
    else:
        log.error("Unexepected response from AN")
        domains = None
    
    return domains


async def GET_Domains(request):
    """HTTP method to return JSON for child domains of given domain"""
    log.request(request)
    app = request.app

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domains = await get_domains(request)

    rsp_json = {"domains": domains}
    rsp_json["hrefs"] = []
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

 
async def GET_Domain(request):
    """HTTP method to return JSON for given domain"""
    log.request(request)
    app = request.app

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = None
    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    if not domain: 
        log.info("no domain passed in, returning all top-level domains")
        # no domain passed in, return top-level domains for this request
        domains = await get_domains(request)
        rsp_json = {"domains": domains}
        rsp_json["hrefs"] = []
        resp = await jsonResponse(request, rsp_json)
        log.response(request, resp=resp)
        return resp

    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    log.info("got domain: {}".format(domain))
   
    domain_json = await get_domain_json(app, domain)

    if domain_json is None:
        log.warn("domain: {} not found".format(domain))
        raise HttpProcessingError(code=404, message="Not Found")
     
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    log.debug("got domain_json: {}".format(domain_json))
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "read", username)  # throws exception if not authorized

    if "h5path" in request.GET:
        # if h5path is passed in, return object info for that path
        #   (if exists)
        h5path = request.GET["h5path"]
        root_id = domain_json["root"]
        obj_id = await getObjectIdByPath(app, root_id, h5path)  # throws 404 if not found
        log.info("get obj_id: {} from h5path: {}".format(obj_id, h5path))
        # get authoritative state for object from DN (even if it's in the meta_cache).
        obj_json = await getObjectJson(app, obj_id, refresh=True)
        obj_json["domain"] = domain
        # Not bothering with hrefs for h5path lookups...
        resp = await jsonResponse(request, obj_json)
        log.response(request, resp=resp)
        return resp
    
    # return just the keys as per the REST API
    rsp_json = { }
    if "root" in domain_json:
        rsp_json["root"] = domain_json["root"]
        rsp_json["class"] = "domain"
    else:
        rsp_json["class"] = "folder"
    if "owner" in domain_json:
        rsp_json["owner"] = domain_json["owner"]
    if "created" in domain_json:
        rsp_json["created"] = domain_json["created"]
    if "lastModified" in domain_json:
        rsp_json["lastModified"] = domain_json["lastModified"]

    if "verbose" in request.GET and request.GET["verbose"] and "root" in domain_json:
        results = await getRootInfo(app, domain_json["root"])
        if results:
            obj_count = 0
            if "lastModified" in results:
                rsp_json["lastModified"] = results["lastModified"]
            if "totalSize" in results:
                rsp_json["allocated_bytes"] = results["totalSize"]
            if "groupCount" in results:
                # don't count the root group
                if results["groupCount"] < 1:
                    log.error("Should see at least one group for root: {}".format(domain_json["root"]))
                    rsp_json["num_groups"] = 0
                else:
                    rsp_json["num_groups"] = results["groupCount"] - 1
                obj_count += results["groupCount"]
            if "typeCount" in results:
                rsp_json["num_datatypes"] = results["typeCount"]
                obj_count += results["typeCount"]
            if "datasetCount" in results:
                rsp_json["num_datasets"] = results["datasetCount"]
                obj_count += results["datasetCount"]
            if "chunkCount" in results:
                obj_count += results["chunkCount"]
            rsp_json["num_objects"] = obj_count   
    
     
    hrefs = []
    hrefs.append({'rel': 'self', 'href': getHref(request, '/')})
    if "root" in domain_json:
        root_uuid = domain_json["root"]
        hrefs.append({'rel': 'database', 'href': getHref(request, '/datasets')})
        hrefs.append({'rel': 'groupbase', 'href': getHref(request, '/groups')})
        hrefs.append({'rel': 'typebase', 'href': getHref(request, '/datatypes')})
        hrefs.append({'rel': 'root', 'href': getHref(request, '/groups/' + root_uuid)})
    
    hrefs.append({'rel': 'acls', 'href': getHref(request, '/acls')})
    parent_domain = getParentDomain(domain)
    log.debug("href parent domain: {}".format(parent_domain))
    if parent_domain:
        hrefs.append({'rel': 'parent', 'href': getHref(request, '/', domain=parent_domain)})

    
         
    rsp_json["hrefs"] = hrefs
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

async def PUT_Domain(request):
    """HTTP method to create a new domain"""
    log.request(request)
    app = request.app
    # yet exist
    username, pswd = getUserPasswordFromRequest(request) # throws exception if user/password is not valid
    await validateUserPassword(app, username, pswd)

    # inital perms for owner and default
    owner_perm = {'create': True, 'read': True, 'update': True, 'delete': True, 'readACL': True, 'updateACL': True } 
    default_perm = {'create': False, 'read': True, 'update': False, 'delete': False, 'readACL': False, 'updateACL': False } 
    
    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
 
    log.info("PUT domain: {}, username: {}".format(domain, username))

    body = None
    is_folder = False
    owner = username
    if request.has_body:
        body = await request.json()   
        log.debug("PUT domain with body: {}".format(body))
        if body and "folder" in body:
            if body["folder"]:
                is_folder = True
        if body and "owner" in body:
            owner = body["owner"]

    if owner != username and username != "admin":
        log.warn("Only admin users are allowed to set owner for new domains");   
        raise HttpProcessingError(code=403, message="Forbidden")


    parent_domain = getParentDomain(domain)
    log.debug("Parent domain: [{}]".format(parent_domain))
    
    if (not parent_domain or parent_domain == '/') and not is_folder:
        msg = "Only folder domains can be created at the top-level"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    if (not parent_domain or parent_domain == '/') and username != "admin":
        msg = "creation of top-level domains is only supported by admin users"
        log.warn(msg)
        raise HttpProcessingError(code=403, message="Forbidden")
    

    parent_json = None
    if parent_domain and parent_domain != '/':
        try:
            parent_json = await getDomainJson(app, parent_domain, reload=True)
        except HttpProcessingError as hpe:
            msg = "Parent domain: {} not found".format(parent_domain)
            log.warn(msg)
            raise HttpProcessingError(code=404, message=msg)

        log.debug("parent_json {}: {}".format(parent_domain, parent_json))
        if "root" in parent_json and parent_json["root"]:
            msg = "Parent domain must be a folder"
            log.warn(msg)
            raise HttpProcessingError(code=400, message=msg)

    if parent_json:
        aclCheck(parent_json, "create", username)  # throws exception if not allowed
    
    if not is_folder:
        # create a root group for the new domain
        root_id = createObjId("groups") 
        log.debug("new root group id: {}".format(root_id))
        group_json = {"id": root_id, "root": root_id, "domain": domain }
        log.debug("create group for domain, body: " + json.dumps(group_json))
    
        # create root group
        req = getDataNodeUrl(app, root_id) + "/groups"
        try:
            group_json = await http_post(app, req, data=group_json)
        except HttpProcessingError as ce:
            msg="Error creating root group for domain -- " + str(ce)
            log.error(msg)
            raise ce
    else:
        log.debug("no root group, creating folder")
 
    domain_json = { }
    
    domain_acls = {}
    # owner gets full control
    domain_acls[owner] = owner_perm
    if config.get("default_public"):
        # this will make the domain public readable
        log.debug("adding default perm for domain: {}".format(domain))
        domain_acls["default"] =  default_perm

    # construct dn request to create new domain
    req = getDataNodeUrl(app, domain)
    req += "/domains" 
    body = { "owner": owner, "domain": domain }
    body["acls"] = domain_acls

    if not is_folder:
        body["root"] = root_id

    log.debug("creating domain: {} with body: {}".format(domain, body))
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

    domain = None
    meta_only = False  # if True, just delete the meta cache value
    if request.has_body:
        body = await request.json() 
        if "domain" in body:
            domain = body["domain"]
        else:
            msg = "No domain in request body"
            log.warn(msg)
            raise HttpBadRequest(message=msg)

        if "meta_only" in body:
            meta_only = body["meta_only"]
    else:
        # get domain from request uri
        try:
            domain = getDomainFromRequest(request)
        except ValueError:
            msg = "Invalid domain"
            log.warn(msg)
            raise HttpBadRequest(message=msg)

    log.info("meta_only domain delete: {}".format(meta_only))
    if meta_only:
        # remove from domain cache if present
        domain_cache = app["domain_cache"]
        if domain in domain_cache:
            log.info("deleting {} from domain_cache".format(domain))
            del domain_cache[domain]
        resp = await jsonResponse(request, {})
        return resp

    username, pswd = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)

    parent_domain = getParentDomain(domain)
    if (not parent_domain or parent_domain == '/') and username != "admin":
        msg = "Deletion of top-level domains is only supported by admin users"
        log.warn(msg)
        raise HttpProcessingError(code=403, message="Forbidden")

    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except HttpProcessingError as hpe:
        msg = "domain not found"
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)

    aclCheck(domain_json, "delete", username)  # throws exception if not allowed

    req = getDataNodeUrl(app, domain)
    req += "/domains" 
    body = { "domain": domain }
    
    rsp_json = await http_delete(app, req, data=body)
 
    resp = await jsonResponse(request, rsp_json)

    if "root" in domain_json:
        # delete the root group
        root_id = domain_json["root"]
        req = getDataNodeUrl(app, root_id)
        req += "/groups/" + root_id
        await http_delete(app, req)

    # remove from domain cache if present
    domain_cache = app["domain_cache"]
    if domain in domain_cache:
        del domain_cache[domain]

    # delete domain cache from other sn_urls
    sn_urls = app["sn_urls"]
    body["meta_only"] = True 
    for node_no in sn_urls:
        if node_no == app["node_number"]:
            continue # don't send to ourselves
        sn_url = sn_urls[node_no]
        req = sn_url + "/"
        log.info("sending sn request: {}".format(req))
        try: 
            sn_rsp = await http_delete(app, req, data=body)
            log.info("{} response: {}".format(req, sn_rsp))
        except HttpProcessingError as hpe:
            log.warn("got hpe for sn_delete: {}".format(hpe))

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
        await validateUserPassword(app, username, pswd)

    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    # use reload to get authoritative domain json
    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except HttpProcessingError as hpe:
        msg = "domain not found"
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)
     
    # validate that the requesting user has permission to read ACLs in this domain
    if acl_username in (username, "default"):
        # allow read access for a users on ACL, or default
        aclCheck(domain_json, "read", username)  # throws exception if not authorized
    else:
        aclCheck(domain_json, "readACL", username)  # throws exception if not authorized
     
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    acls = domain_json["acls"]

    log.debug("got domain_json: {}".format(domain_json))

    if acl_username not in acls:
        msg = "acl for username: [{}] not found".format(acl_username)
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)

    acl = acls[acl_username]
    acl_rsp = {}
    for k in acl.keys():
        acl_rsp[k] = acl[k]
    acl_rsp["userName"] = acl_username

    # return just the keys as per the REST API
    rsp_json = { }
    rsp_json["acl"] = acl_rsp
    hrefs = []
    hrefs.append({'rel': 'self', 'href': getHref(request, '/acls')})
    if "root" in domain_json:
        hrefs.append({'rel': 'root', 'href': getHref(request, '/groups/' + domain_json["root"])})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})
    hrefs.append({'rel': 'owner', 'href': getHref(request, '/')})
    rsp_json["hrefs"] = hrefs

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
        await validateUserPassword(app, username, pswd)

    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # use reload to get authoritative domain json
    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except HttpProcessingError as hpe:
        msg = "domain not found"
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)
     
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    acls = domain_json["acls"]

    log.debug("got domain_json: {}".format(domain_json))
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

    hrefs = []
    hrefs.append({'rel': 'self', 'href': getHref(request, '/acls')})
    if "root" in domain_json:
        hrefs.append({'rel': 'root', 'href': getHref(request, '/groups/' + domain_json["root"])})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})
    hrefs.append({'rel': 'owner', 'href': getHref(request, '/')})
    rsp_json["hrefs"] = hrefs

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
    await validateUserPassword(app, username, pswd)

    if not request.has_body:
        msg = "PUT ACL with no body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    body = await request.json()   
    acl_keys = getAclKeys()

    for k in body.keys():
        if k not in acl_keys:
            msg = "Unexpected key in request body: {}".format(k)
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        if body[k] not in (True, False):
            msg = "Unexpected value for key in request body: {}".format(k)
            log.warn(k)
            raise HttpBadRequest(message=msg)

    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

     
    # don't use app["domain_cache"]  if a direct domain request is made 
    # as opposed to an implicit request as with other operations, query
    # the domain from the authoritative source (the dn node)
    req = getDataNodeUrl(app, domain)
    req += "/acls/" + acl_username
    log.info("sending dn req: {}".format(req))    
    body["domain"] = domain

    put_rsp = await http_put(app, req, data=body)
    log.info("PUT ACL resp: " + str(put_rsp))
    
    # ACL update successful     
    resp = await jsonResponse(request, put_rsp, status=201)
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
        await validateUserPassword(app, username, pswd)

    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    # verify the domain 
    try:
        domain_json = await getDomainJson(app, domain)
    except HttpProcessingError as hpe:
        msg = "domain not found"
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)
     
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    log.debug("got domain_json: {}".format(domain_json))
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "read", username)  # throws exception if not authorized

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

    obj_ids = []
    if "root" in domain_json or domain_json["root"]:
        # get the dataset collection list
        objects = await get_collection(app, domain_json["root"], "datasets", marker=marker, limit=limit)
        for object in objects:
            obj_ids.append(object["id"])
    log.debug("returning obj_ids: {}".format(obj_ids))
     
    # create hrefs 
    hrefs = []
    hrefs.append({'rel': 'self', 'href': getHref(request, '/datasets')})
    if "root" in domain_json:
        root_uuid = domain_json["root"]
        hrefs.append({'rel': 'root', 'href': getHref(request, '/groups/' + root_uuid)})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})

    # return obj ids and hrefs
    rsp_json = { }
    rsp_json["datasets"] = obj_ids
    rsp_json["hrefs"] = hrefs
     
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
        await validateUserPassword(app, username, pswd)
    
    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # use reload to get authoritative domain json
    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except HttpProcessingError as hpe:
        msg = "domain not found"
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)
     
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    log.debug("got domain_json: {}".format(domain_json))
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "read", username)  # throws exception if not authorized

    # get the groups collection list
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

    obj_ids = []
    if "root" in domain_json or domain_json["root"]:
        # get the dataset collection list
        objects = await get_collection(app, domain_json["root"], "groups", marker=marker, limit=limit)
        for object in objects:
            obj_ids.append(object["id"])
 
    # create hrefs 
    hrefs = []
    hrefs.append({'rel': 'self', 'href': getHref(request, '/groups')})
    if "root" in domain_json:
        root_uuid = domain_json["root"]
        hrefs.append({'rel': 'root', 'href': getHref(request, '/groups/' + root_uuid)})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})

    # return obj ids and hrefs
    rsp_json = { }
    rsp_json["groups"] = obj_ids
    rsp_json["hrefs"] = hrefs
     
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
        await validateUserPassword(app, username, pswd)
    
    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # use reload to get authoritative domain json
    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except HttpProcessingError as hpe:
        msg = "domain not found"
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)
     
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    log.debug("got domain_json: {}".format(domain_json))
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "read", username)  # throws exception if not authorized

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

    # get the datatype collection list
    obj_ids = []
    if "root" in domain_json or domain_json["root"]:
        # get the dataset collection list
        objects = await get_collection(app, domain_json["root"], "datatypes", marker=marker, limit=limit)
        for object in objects:
            obj_ids.append(object["id"])
 
    # create hrefs 
    hrefs = []
    hrefs.append({'rel': 'self', 'href': getHref(request, '/datatypes')})
    if "root" in domain_json:
        root_uuid = domain_json["root"]
        hrefs.append({'rel': 'root', 'href': getHref(request, '/groups/' + root_uuid)})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})

    # return obj ids and hrefs
    rsp_json = { }
    rsp_json["datatypes"] = obj_ids
    rsp_json["hrefs"] = hrefs
     
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp
    


 