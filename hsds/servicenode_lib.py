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
import os.path as op
from aiohttp.web_exceptions import HTTPBadRequest, HTTPForbidden, HTTPNotFound, HTTPInternalServerError


from util.idUtil import getDataNodeUrl, getCollectionForId, isSchema2Id, getS3Key
from util.s3Util import getS3JSONObj
from util.authUtil import aclCheck
from util.httpUtil import http_get

import hsds_logger as log


async def getDomainJson(app, domain, reload=False):
    """ Return domain JSON from cache or fetch from DN if not found
        Note: only call from sn!
    """
    # TBD - default reload to True because some h5pyd tests fail due to
    # cached values being picked up (test case deletes/re-creates domain)
    # It would be desirable to use default of False to avoid extra
    # round-trips to DN node
    log.info("getDomainJson({})".format(domain))
    if app["node_type"] != "sn":
        log.error("wrong node_type")
        raise HTTPInternalServerError()

    domain_cache = app["domain_cache"]

    if domain in domain_cache:
        if reload:
            del domain_cache[domain]
        else:
            log.debug("returning domain_cache value")
            return domain_cache[domain]

    req = getDataNodeUrl(app, domain)
    req += "/domains"
    params = { "domain": domain } 
    log.debug("sending dn req: {}".format(req))
    
    domain_json = await http_get(app, req, params=params)
    
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HTTPInternalServerError()

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HTTPInternalServerError()

    domain_cache[domain] = domain_json  # add to cache
    return domain_json

async def validateAction(app, domain, obj_id, username, action):
    """ check that the given object belongs in the domain and that the 
        requested action (create, read, update, delete, readACL, udpateACL) 
        is permitted for the requesting user.  
    """
    meta_cache = app['meta_cache']
    log.info("validateAction(domain={}, obj_id={}, username={}, action={})".format(domain, obj_id, username, action))
    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    if "root" not in domain_json:
        msg = "Expected root key for domain: {}".format(domain)
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    obj_json = None
    if obj_id in meta_cache:
        obj_json = meta_cache[obj_id]
    else:
        # fetch from DN
        collection = getCollectionForId(obj_id)
        req = getDataNodeUrl(app, obj_id)
        req += '/' + collection + '/' + obj_id
        obj_json = await http_get(app, req) 
        meta_cache[obj_id] = obj_json

    log.debug("obj_json[root]: {} domain_json[root]: {}".format(obj_json["root"], domain_json["root"]))
    if obj_json["root"] != domain_json["root"]:
        log.info("unexpected root, reloading domain")
        domain_json = await getDomainJson(app, domain, reload=True)
        if "root" not in domain_json or obj_json["root"] != domain_json["root"]:
            msg = "Object id is not a member of the given domain"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    if action not in ("create", "read", "update", "delete", "readACL", "updateACL"):
        log.error("unexpected action: {}".format(action))
        raise HTTPInternalServerError()

    reload = False
    try:
        aclCheck(domain_json, action, username)  # throws exception if not allowed
    except HTTPForbidden:
        log.info("got HttpProcessing error on validate action for domain: {}, reloading...".format(domain))
        # just in case the ACL was recently updated, refetch the domain
        reload = True
    if reload:
        domain_json = await getDomainJson(app, domain, reload=True)
        aclCheck(domain_json, action, username) 


async def getObjectJson(app, obj_id, refresh=False):
    """ Return top-level json (i.e. excluding attributes or links) for a given obj_id.
    If refresh is False, any data present in the meta_cache will be returned.  If not
    the DN will be queries, and any resultant data added to the meta_cache.  
    Note: meta_cache values may be stale, but use of immutable data (e.g. type of a dataset)
    is always valid
    """
    meta_cache = app['meta_cache']
    obj_json = None
    log.info("getObjectJson {}".format(obj_id))
    if obj_id in meta_cache and not refresh:
        log.debug("found {} in meta_cache".format(obj_id))
        obj_json = meta_cache[obj_id]
    else:
        req = getDataNodeUrl(app, obj_id)
        collection =  getCollectionForId(obj_id) 
        req += '/' + collection + '/' + obj_id
        obj_json = await http_get(app, req)  # throws 404 if doesn't exist
        meta_cache[obj_id] = obj_json
    if obj_json is None:
        msg = "Object: {} not found".format(obj_id)
        log.warn(msg)
        raise HTTPNotFound()
    return obj_json

async def getObjectIdByPath(app, obj_id, h5path, refresh=False):
    """ Find the object at the provided h5path location.
    If not found raise 404 error.
    """
    log.info("getObjectIdByPath obj_id: {} h5path: {} refresh: {}".format(obj_id, h5path, refresh))
    if h5path.startswith("./"):
        h5path = h5path[2:]  # treat as relative path
    links = h5path.split('/')
    for link in links:
        if not link:
            continue  # skip empty link
        log.debug("getObjectIdByPath for objid: {} got link: {}".format(obj_id, link))
        if getCollectionForId(obj_id) != "groups":
            # not a group, so won't have links
            msg = "h5path: {} not found".format(h5path)
            log.warn(msg)
            raise HTTPNotFound()
        req = getDataNodeUrl(app, obj_id)
        req += "/groups/" + obj_id + "/links/" + link
        log.debug("get LINK: " + req)
        link_json = await http_get(app, req)
        log.debug("got link_json: " + str(link_json)) 
        if link_json["class"] != 'H5L_TYPE_HARD':
            # don't follow soft/external links
            msg = "h5path: {} not found".format(h5path)
            log.warn(msg)
            raise HTTPInternalServerError()
        obj_id = link_json["id"]
    # if we get here, we've traveresed the entire path and found the object
    return obj_id

async def getPathForObjectId(app, parent_id, idpath_map, tgt_id=None):
    """ Search the object starting with the given parent_id.
    idpath should be a dict with at minimum the key: parent_id: <parent_path>.
    If tgt_id is not None, returns first path that matches the tgt_id or None if not found.
    If Tgt_id is no, returns the idpath_map.
    """

    if not parent_id:
        log.error("No parent_id passed to getPathForObjectId")
        raise HTTPInternalServerError()

    if parent_id not in idpath_map:
        msg = "Obj {} expected to be found in idpath_map".format(parent_id)
        log.error(msg)
        raise HTTPInternalServerError()
    
    parent_path = idpath_map[parent_id]
    if parent_id == tgt_id:
        return parent_path

    req = getDataNodeUrl(app, parent_id)
    req += "/groups/" + parent_id + "/links" 
        
    log.debug("getPathForObjectId LINKS: " + req)
    links_json = await http_get(app, req)
    log.debug("getPathForObjectId got links json from dn for parent_id: {}".format(parent_id)) 
    links = links_json["links"]

    h5path = None
    for link in links:
        if link["class"] != "H5L_TYPE_HARD":
            continue  # ignore everything except hard links
        link_id = link["id"]
        if link_id in idpath_map:
            continue  # this node has already been visited
        title = link["title"]
        if tgt_id is not None and link_id == tgt_id:
            # found it!
            h5path = op.join(parent_path, title)
            break
        idpath_map[link_id] = op.join(parent_path, title)
        if getCollectionForId(link_id) != "groups":
            continue
        h5path = await getPathForObjectId(app, link_id, idpath_map, tgt_id) # recursive call
        if tgt_id is not None and h5path:
            break
    
    return h5path

async def getRootInfo(app, root_id):  
    """ Get extra information the root collection. """
    # Gather additional info on the domain
    log.debug(f"getRootInfo {root_id}")
    
    if not isSchema2Id(root_id):
        log.info(f"no dataset details not available for schema v1 id: {root_id} returning null results")
        return None

    s3_key = getS3Key(root_id)

    parts = s3_key.split('/')
    # dset_key is in the format  db/<root>/d/<dset>/.dataset.json
    # get the key for the root info object as: db/<root>/.info.json
    if len(parts) != 3:
        log.error(f"Unexpected s3key format: {s3_key}")
        return None

    info_key = f"db/{parts[1]}/.info.json"

    try:
        info_json = await getS3JSONObj(app, info_key)
    except HTTPNotFound:
        log.warn(f"info.json not found for key: {info_key}")
        return None

    return info_json
