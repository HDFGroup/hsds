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
from aiohttp.web_exceptions import HTTPBadRequest, HTTPForbidden, HTTPNotFound, HTTPGone, HTTPInternalServerError
from aiohttp import ClientResponseError
from aiohttp.web import json_response


from util.httpUtil import  http_post, http_put, http_get, http_delete, getHref
from util.idUtil import  getDataNodeUrl, createObjId, getCollectionForId
from util.authUtil import getUserPasswordFromRequest, aclCheck
from util.authUtil import validateUserPassword, getAclKeys
from util.domainUtil import getParentDomain, getDomainFromRequest
from util.s3Util import getS3Keys
from servicenode_lib import getDomainJson, getObjectJson, getObjectIdByPath, getRootInfo
from basenode import getAsyncNodeUrl
import hsds_logger as log
import config


async def get_toplevel_domains(app):
    """ Get list of top level domains """
    an_url = getAsyncNodeUrl(app)
    req = an_url + "/domains"
    params = {"domain": "/"}
    log.info("ASync GET TopLevelDomains")
    try:
        rsp_json = await http_get(app, req, params=params)
    except ClientResponseError as ce:
        if ce.code == 501:
            log.warn("sqlite db not available")
            return None
        if ce.code == 404:
            # sqlite db not sync'd?
            log.warn("404 repsonse for get_toplevel_domains")
            return None
        else:
            log.error("Async error: {}".format(ce))
            raise HTTPInternalServerError()
    if "domains" not in rsp_json:
        log.error("domains not found in get_toplevel_domain request")
        raise HTTPInternalServerError()

    return rsp_json["domains"]



async def get_collections(app, root_id):
    """ Return the object ids for given root.
    """   

    groups = {}
    datasets = {}
    datatypes = {}
    lookup_ids = set()
    lookup_ids.add(root_id)

    while lookup_ids:
        grp_id = lookup_ids.pop()
        req = getDataNodeUrl(app, grp_id)
        req += '/groups/' + grp_id + "/links"
        log.debug("collection get LINKS: " + req)
        try:
            links_json = await http_get(app, req)  # throws 404 if doesn't exist
        except HTTPNotFound:
            log.warn(f"get_collection, group {grp_id} not found")
            continue
        
        log.debug(f"got links json from dn for group_id: {grp_id}") 
        links = links_json["links"]
        log.debug(f"get_collection: got links: {links}")
        for link in links:
            if link["class"] != 'H5L_TYPE_HARD':
                continue
            link_id = link["id"]
            obj_type = getCollectionForId(link_id)
            if obj_type == "groups":
                if link_id in groups:
                    continue  # been here before
                groups[link_id] = {}
                lookup_ids.add(link_id)
            elif obj_type == "datasets":
                if link_id in datasets:
                    continue
                datasets[link_id] = {}
            elif obj_type == "datatypes":
                if link_id in datatypes:
                    continue
                datatypes[link_id] = {}
            else:
                log.error(f"get_collection: unexpected link object type: {obj_type}")
                HTTPInternalServerError()

    result = {}
    result["groups"] = groups
    result["datasets"] = datasets
    result["datatypes"] = datatypes
    return result
    
def getIdList(objs, marker=None, limit=None):
    """ takes a map of ids to objs and returns ordered list
        of ids, optionally reduced by marker and limit """

    ids = []
    for k in objs:
        ids.append(k)
    ids.sort()
    if not marker and not limit:
        return ids  # just return ids
    ret_ids = []
    for id in ids:
        if marker:
            if id == marker:
                marker = None  # clear so we will start adding items
            continue
        ret_ids.append(id)
        if limit and len(ret_ids) == limit:
            break
    return ret_ids

async def get_domain_response(app, domain_json, verbose=False):
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

    lastModified = 0
    if "lastModified" in domain_json:
        lastModified = domain_json["lastModified"]
    totalSize = len(json.dumps(domain_json))
    allocated_bytes = 0

    if verbose and "root" in domain_json:
        root_info = await getRootInfo(app, domain_json["root"])
        if root_info:
            allocated_bytes = root_info["allocated_bytes"]  
            totalSize += allocated_bytes
            if "metadata_bytes" in root_info:
                # this key was added for schema v2
                totalSize += root_info["metadata_bytes"]
            if root_info["lastModified"] > lastModified:
                lastModified = root_info["lastModified"]
            num_groups = root_info["num_groups"]
            num_datatypes = root_info["num_datatypes"]
            num_datasets = len(root_info["datasets"])
            num_chunks = root_info["num_chunks"]
        else:
            # no info json (either v1 schema or not created yet)
            # get collection sizes by interating over graph
            collections = await get_collections(app, domain_json["root"])
            num_groups = len(collections["groups"]) + 1  # add 1 for root group
            num_datasets = len(collections["datasets"])
            num_datatypes = len(collections["datatypes"])
            num_chunks = 0

        num_objects = num_groups + num_datasets + num_datatypes + num_chunks
        rsp_json["num_groups"] = num_groups
        rsp_json["num_datasets"] = num_datasets
        rsp_json["num_datatypes"] = num_datatypes
        rsp_json["num_objects"] = num_objects
        rsp_json["total_size"] = totalSize
        rsp_json["allocated_bytes"] = allocated_bytes
        rsp_json["num_objects"] =  num_objects

    rsp_json["lastModified"] = lastModified
    return rsp_json

 
        
async def get_domains(request):
    """ This method is called by GET_Domains and GET_Domain """
    app = request.app
    
    # if there is no domain passed in, get a list of top level domains
    if "domain" not in request.rel_url.query:
        prefix = '/'
    else:
        prefix = request.rel_url.query["domain"]
    log.info(f"get_domains for: {prefix}")

    if not prefix.startswith('/'):
        msg = "Prefix must start with '/'"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)  

    # always use "verbose" to pull info from RootTable
    if "verbose" in request.rel_url.query and request.rel_url.query["verbose"]:
        verbose = True
    else:
        verbose = False

    limit = None
    if "Limit" in request.rel_url.query:
        try:
            limit = int(request.rel_url.query["Limit"])
            log.debug(f"GET_Domains - using Limit: {limit}")
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.warn(msg)   
            raise HTTPBadRequest(reason=msg)

    marker = None        
    if "Marker" in request.rel_url.query:
        marker = request.rel_url.query["Marker"]
        log.debug(f"got Marker request param: {marker}")

    
    # list the S3 keys for this prefix
    domainNames = []
    if prefix == "/":
        # search for domains from the top_level domains config
        domainNames = config.get("top_level_domains")
    else:
        s3prefix = prefix[1:]
        log.debug(f"listing S3 keys for {s3prefix}")
        s3keys = await getS3Keys(app, include_stats=False, prefix=s3prefix, deliminator='/')  
        log.debug(f"getS3Keys returned: {len(s3keys)} keys")
        log.debug(f"s3keys {s3keys}")
        
        for s3key in s3keys:
            if s3key[-1] != '/':
                log.debug(f"ignoring key: {s3key}")
                continue
            log.debug(f"got s3key: {s3key}")
            domain = "/" + s3key[:-1]
            if marker:
                if marker == domain:
                    marker = None
                    continue
            
            log.debug(f"adding domain: {domain} to domain list")
            domainNames.append(domain)

            if limit and len(domainNames) == limit:
                # got to requested limit
                break


    # get domain info for each domain
    domains = []
    for domain in domainNames:
        # query DN's for domain json
        # TBD - multicast to DN nodes
        log.debug(f"getDomainJson for {domain}")
        domain_json = await getDomainJson(app, domain, reload=True)
        if domain_json:
            domain_rsp = await get_domain_response(app, domain_json, verbose=verbose)
            # mixin domain anme
            domain_rsp["name"] = domain
            domains.append(domain_rsp)
        
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
    resp = json_response(rsp_json)
    log.response(request, resp=resp)
    return resp

 
async def GET_Domain(request):
    """HTTP method to return JSON for given domain"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = None
    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        log.warn("Invalid domain")
        raise HTTPBadRequest(reason="Invalid domain name")

    verbose = False
    if "verbose" in params and params["verbose"]:
        verbose = True

    if not domain: 
        log.info("no domain passed in, returning all top-level domains")
        # no domain passed in, return top-level domains for this request
        domains = await get_domains(request)
        rsp_json = {"domains": domains}
        rsp_json["hrefs"] = []
        resp = json_response(rsp_json)
        log.response(request, resp=resp)
        return resp


    log.info("got domain: {}".format(domain))
   
    domain_json = await getDomainJson(app, domain, reload=True)
     
    if domain_json is None:
        log.warn("domain: {} not found".format(domain))
        raise HTTPNotFound()
     
    if 'owner' not in domain_json:
        log.error("No owner key found in domain")
        raise HTTPInternalServerError()

    if 'acls' not in domain_json:
        log.error("No acls key found in domain")
        raise HTTPInternalServerError()

    log.debug("got domain_json: {}".format(domain_json))
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "read", username)  # throws exception if not authorized

    if "h5path" in params:
        # if h5path is passed in, return object info for that path
        #   (if exists)
        h5path = params["h5path"]
        root_id = domain_json["root"]
        obj_id = await getObjectIdByPath(app, root_id, h5path)  # throws 404 if not found
        log.info("get obj_id: {} from h5path: {}".format(obj_id, h5path))
        # get authoritative state for object from DN (even if it's in the meta_cache).
        obj_json = await getObjectJson(app, obj_id, refresh=True)
        obj_json["domain"] = domain
        # Not bothering with hrefs for h5path lookups...
        resp = json_response(obj_json)
        log.response(request, resp=resp)
        return resp
    
    # return just the keys as per the REST API
    rsp_json = await get_domain_response(app, domain_json, verbose=verbose)
     
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
    resp = json_response(rsp_json)
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
        raise HTTPBadRequest(reason=msg)
 
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
        raise HTTPForbidden()


    parent_domain = getParentDomain(domain)
    log.debug("Parent domain: [{}]".format(parent_domain))
    
    if (not parent_domain or parent_domain == '/') and not is_folder:
        msg = "Only folder domains can be created at the top-level"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if (not parent_domain or parent_domain == '/') and username != "admin":
        msg = "creation of top-level domains is only supported by admin users"
        log.warn(msg)
        raise HTTPForbidden()
    

    parent_json = None
    if parent_domain and parent_domain != '/':
        try:
            parent_json = await getDomainJson(app, parent_domain, reload=True)
        except ClientResponseError as ce:
            if ce.code == 404:
                msg = "Parent domain: {} not found".format(parent_domain)
                log.warn(msg)
                raise HTTPNotFound()
            elif ce.code == 410:
                msg = "Parent domain: {} removed".format(parent_domain)
                log.warn(msg)
                raise HTTPGone()
            else:
                log.error(f"Unexpected error: {ce.code}")
                raise HTTPInternalServerError()

        log.debug("parent_json {}: {}".format(parent_domain, parent_json))
        if "root" in parent_json and parent_json["root"]:
            msg = "Parent domain must be a folder"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    if parent_json:
        aclCheck(parent_json, "create", username)  # throws exception if not allowed
    
    if not is_folder:
        # create a root group for the new domain
        root_id = createObjId("roots") 
        log.debug("new root group id: {}".format(root_id))
        group_json = {"id": root_id, "root": root_id, "domain": domain }
        log.debug("create group for domain, body: " + json.dumps(group_json))
    
        # create root group
        req = getDataNodeUrl(app, root_id) + "/groups"
        try:
            group_json = await http_post(app, req, data=group_json)
        except ClientResponseError as ce:
            msg="Error creating root group for domain -- " + str(ce)
            log.error(msg)
            raise HTTPInternalServerError()
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
    except ClientResponseError as ce:
        msg="Error creating domain state -- " + str(ce)
        log.error(msg)
        raise HTTPInternalServerError()

    # domain creation successful     
    resp = json_response(domain_json, status=201)
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
            raise HTTPBadRequest(reason=msg)

        if "meta_only" in body:
            meta_only = body["meta_only"]
    else:
        # get domain from request uri
        try:
            domain = getDomainFromRequest(request)
        except ValueError:
            msg = "Invalid domain"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    log.info("meta_only domain delete: {}".format(meta_only))
    if meta_only:
        # remove from domain cache if present
        domain_cache = app["domain_cache"]
        if domain in domain_cache:
            log.info("deleting {} from domain_cache".format(domain))
            del domain_cache[domain]
        resp = json_response({})
        return resp

    username, pswd = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)

    parent_domain = getParentDomain(domain)
    if (not parent_domain or parent_domain == '/') and username != "admin":
        msg = "Deletion of top-level domains is only supported by admin users"
        log.warn(msg)
        raise HTTPForbidden()

    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except ClientResponseError as ce:
        if ce.code == 404:
            log.warn("domain not found")
            raise HTTPNotFound()
        elif ce.code == 410:
            log.warn("domain has been removed")
            raise HTTPGone()
        else:
            log.error(f"unexpected error: {ce.code}")
            raise HTTPInternalServerError()

    aclCheck(domain_json, "delete", username)  # throws exception if not allowed

    req = getDataNodeUrl(app, domain)
    req += "/domains" 
    body = { "domain": domain }
    
    rsp_json = await http_delete(app, req, data=body)
 
    
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
        except ClientResponseError as ce:
            log.warn("got error for sn_delete: {}".format(ce))
    
    resp = json_response(rsp_json)
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
        raise HTTPBadRequest(reason=msg)

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
        raise HTTPBadRequest(reason=msg)

    # use reload to get authoritative domain json
    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except ClientResponseError as ce:
        if ce.code in (404,410):
            msg = "domain not found"
            log.warn(msg)
            raise HTTPNotFound()
        else:
            log.error(f"unexpected error: {ce.code}")
            raise HTTPInternalServerError()
     
    # validate that the requesting user has permission to read ACLs in this domain
    if acl_username in (username, "default"):
        # allow read access for a users on ACL, or default
        aclCheck(domain_json, "read", username)  # throws exception if not authorized
    else:
        aclCheck(domain_json, "readACL", username)  # throws exception if not authorized
     
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HTTPInternalServerError()

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HTTPInternalServerError()

    acls = domain_json["acls"]

    log.debug("got domain_json: {}".format(domain_json))

    if acl_username not in acls:
        msg = "acl for username: [{}] not found".format(acl_username)
        log.warn(msg)
        raise HTTPNotFound()

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

    resp = json_response(rsp_json)
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
        raise HTTPBadRequest(message=msg)
    
    # use reload to get authoritative domain json
    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except ClientResponseError as ce:
        msg = "domain not found"
        log.warn(msg)
        raise HTTPNotFound()
     
    if 'owner' not in domain_json:
        log.error("No owner key found in domain")
        raise HTTPInternalServerError()

    if 'acls' not in domain_json:
        log.error("No acls key found in domain")
        raise HTTPInternalServerError()

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

    resp = json_response(rsp_json)
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
        raise HTTPBadRequest(reason=msg)

    (username, pswd) = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)

    if not request.has_body:
        msg = "PUT ACL with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    body = await request.json()   
    acl_keys = getAclKeys()

    for k in body.keys():
        if k not in acl_keys:
            msg = "Unexpected key in request body: {}".format(k)
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if body[k] not in (True, False):
            msg = "Unexpected value for key in request body: {}".format(k)
            log.warn(k)
            raise HTTPBadRequest(reason=msg)

    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

     
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
    resp = json_response(put_rsp, status=201)
    log.response(request, resp=resp)
    return resp


async def GET_Datasets(request):
    """HTTP method to return dataset collection for given domain"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

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
        raise HTTPBadRequest(reason=msg)

    # verify the domain 
    try:
        domain_json = await getDomainJson(app, domain)
    except ClientResponseError as ce:
        if ce.code == 404:
            msg = "Domain: {} not found".format(domain)
            log.warn(msg)
            raise HTTPNotFound()
        elif ce.code == 410:
            msg = "Domain: {} removed".format(domain)
            log.warn(msg)
            raise HTTPGone()
        else:
            log.error(f"Unexpected error: {ce.code}")
            raise HTTPInternalServerError()
        msg = "domain not found"
        log.warn(msg)
        raise HTTPNotFound()
     
    if 'owner' not in domain_json:
        log.error("No owner key found in domain")
        raise HTTPInternalServerError()

    if 'acls' not in domain_json:
        log.error("No acls key found in domain")
        raise HTTPInternalServerError()

    log.debug("got domain_json: {}".format(domain_json))
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "read", username)  # throws exception if not authorized

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

    obj_ids = []
    if "root" in domain_json or domain_json["root"]:
        # get the dataset collection list
        collections = await get_collections(app, domain_json["root"])  
        objs = collections["datasets"]
        obj_ids = getIdList(objs, marker=marker, limit=limit)
        
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
     
    resp = json_response(rsp_json)
    log.response(request, resp=resp)
    return resp

async def GET_Groups(request):
    """HTTP method to return groups collection for given domain"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

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
        raise HTTPBadRequest(reason=msg)
    
    # use reload to get authoritative domain json
    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except ClientResponseError as ce:
        if ce.code == 404:
            msg = "domain not found"
            log.warn(msg)
            raise HTTPNotFound()
        else:
            log.error(f"Unexpected error: {ce.code}")
            raise HTTPInternalServerError()
     
    if 'owner' not in domain_json:
        log.error("No owner key found in domain")
        raise HTTPInternalServerError()

    if 'acls' not in domain_json:
        log.error("No acls key found in domain")
        raise HTTPInternalServerError()

    log.debug("got domain_json: {}".format(domain_json))
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "read", username)  # throws exception if not authorized

    # get the groups collection list
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

    obj_ids = []
    if "root" in domain_json or domain_json["root"]:
        # get the groups collection list
        collections = await get_collections(app, domain_json["root"])  
        objs = collections["groups"]
        obj_ids = getIdList(objs, marker=marker, limit=limit)
 
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
     
    resp = json_response(rsp_json)
    log.response(request, resp=resp)
    return resp

async def GET_Datatypes(request):
    """HTTP method to return datatype collection for given domain"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

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
        raise HTTPBadRequest(reason=msg)
    
    # use reload to get authoritative domain json
    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except ClientResponseError as ce:
        if ce.code in (404, 410):
            msg = "domain not found"
            log.warn(msg)
            raise HTTPNotFound()
        else:
            log.error(f"Unexpected Error: {ce.code})")
            raise HTTPInternalServerError()
     
    if 'owner' not in domain_json:
        log.error("No owner key found in domain")
        raise HTTPInternalServerError()

    if 'acls' not in domain_json:
        log.error("No acls key found in domain")
        raise HTTPInternalServerError()

    log.debug("got domain_json: {}".format(domain_json))
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "read", username)  # throws exception if not authorized

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

    # get the datatype collection list
    obj_ids = []
    if "root" in domain_json or domain_json["root"]:
        # get the groups collection list
        collections = await get_collections(app, domain_json["root"])  
        objs = collections["datatypes"]
        obj_ids = getIdList(objs, marker=marker, limit=limit)
 
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
     
    resp = json_response(rsp_json)
    log.response(request, resp=resp)
    return resp
