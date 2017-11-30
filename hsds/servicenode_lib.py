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
from aiohttp.errors import HttpBadRequest, HttpProcessingError

from util.idUtil import getDataNodeUrl, getCollectionForId
from util.authUtil import aclCheck
from util.httpUtil import http_get_json

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
        raise HttpProcessingError(code=500, message="Unexpected Error")

    domain_cache = app["domain_cache"]
    #domain = getDomainFromRequest(request)

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
    
    domain_json = await http_get_json(app, req, params=params)
    
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError(code=500, message="Unexpected error")

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
        raise HttpBadRequest(message=msg)

    obj_json = None
    if obj_id in meta_cache:
        obj_json = meta_cache[obj_id]
    else:
        # fetch from DN
        collection = getCollectionForId(obj_id)
        req = getDataNodeUrl(app, obj_id)
        req += '/' + collection + '/' + obj_id
        obj_json = await http_get_json(app, req) 
        meta_cache[obj_id] = obj_json

    if obj_json["root"] != domain_json["root"]:
        log.info("unexpected root, reloading domain")
        domain_json = await getDomainJson(app, domain, reload=True)
        if "root" not in domain_json or obj_json["root"] != domain_json["root"]:
            msg = "Object id is not a member of the given domain"
            log.warn(msg)
            raise HttpBadRequest(message=msg)

    if action not in ("create", "read", "update", "delete", "readACL", "updateACL"):
        log.error("unexpected action: {}".format(action))
        raise HttpProcessingError(code=500, message="Unexpected error")

    aclCheck(domain_json, action, username)  # throws exception if not allowed

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
        obj_json = await http_get_json(app, req)  # throws 404 if doesn't exist'
        meta_cache[obj_id] = obj_json
    if obj_json is None:
        msg = "Object: {} not found".format(obj_id)
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)
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
            raise HttpProcessingError(code=404, message=msg)
        req = getDataNodeUrl(app, obj_id)
        req += "/groups/" + obj_id + "/links/" + link
        log.debug("get LINK: " + req)
        link_json = await http_get_json(app, req)
        log.debug("got link_json: " + str(link_json)) 
        if link_json["class"] != 'H5L_TYPE_HARD':
            # don't follow soft/external links
            msg = "h5path: {} not found".format(h5path)
            log.warn(msg)
            raise HttpProcessingError(code=404, message=msg)
        obj_id = link_json["id"]
    # if we get here, we've traveresed the entire path and found the object
    return obj_id

    

