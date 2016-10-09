
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
from aiohttp import HttpProcessingError 
from aiohttp.errors import HttpBadRequest

from util.idUtil import getDataNodeUrl, getCollectionForId
from util.authUtil import aclCheck
from util.httpUtil import http_get_json

import hsds_logger as log


async def getDomainJson(app, domain):
    """ Return domain JSON from cache or fetch from DN if not found
        Note: only call from sn!
    """
    log.info("getDomainJson({})".format(domain))
    if app["node_type"] != "sn":
        log.error("wrong node_type")
        raise HttpProcessingError(code=500, message="Unexpected Error")

    domain_cache = app["domain_cache"]
    #domain = getDomainFromRequest(request)

    if domain in domain_cache:
        log.info("returning domain_cache value")
        return domain_cache[domain]

    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain 
    log.info("sending dn req: {}".format(req))
    
    domain_json = await http_get_json(app, req)
    
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
    is always validateAction
    """
    meta_cache = app['meta_cache']
    obj_json = None
    if obj_id in meta_cache and not refresh:
        log.info("found {} in meta_cache".format(obj_id))
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
    

