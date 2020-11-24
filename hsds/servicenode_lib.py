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
from aiohttp.client_exceptions import ClientOSError

from .util.idUtil import getDataNodeUrl, getCollectionForId, isSchema2Id, getS3Key
from .util.storUtil import getStorJSONObj, isStorObj
from .util.authUtil import aclCheck
from .util.httpUtil import http_get
from .util.domainUtil import getBucketForDomain, verifyRoot

from . import hsds_logger as log


async def getDomainJson(app, domain, reload=False):
    """ Return domain JSON from cache or fetch from DN if not found
        Note: only call from sn!
    """
    # TBD - default reload to True because some h5pyd tests fail due to
    # cached values being picked up (test case deletes/re-creates domain)
    # It would be desirable to use default of False to avoid extra
    # round-trips to DN node
    log.info(f"getDomainJson({domain}, reload={reload})")
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

    log.debug(f"sending dn req: {req} params: {params}")

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
    log.info(f"validateAction(domain={domain}, obj_id={obj_id}, username={username}, action={action})")
    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    verifyRoot(domain_json)

    obj_json = None
    if obj_id in meta_cache:
        log.debug(f"validdateAction - found {obj_id} in meta_cache")
        obj_json = meta_cache[obj_id]
    else:
        # fetch from DN
        log.debug(f"validateAction - fetch {obj_id}")
        collection = getCollectionForId(obj_id)
        req = getDataNodeUrl(app, obj_id)
        req += '/' + collection + '/' + obj_id
        bucket = getBucketForDomain(domain)
        params = {}
        if bucket:
            params["bucket"] = bucket
        obj_json = await http_get(app, req, params=params)
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
        log.error(f"unexpected action: {action}")
        raise HTTPInternalServerError()

    reload = False
    try:
        aclCheck(app, domain_json, action, username)  # throws exception if not allowed
    except HTTPForbidden:
        log.info(f"got HttpProcessing error on validate action for domain: {domain}, reloading...")
        # just in case the ACL was recently updated, refetch the domain
        reload = True
    if reload:
        domain_json = await getDomainJson(app, domain, reload=True)
        aclCheck(app, domain_json, action, username)


async def getObjectJson(app, obj_id, bucket=None, refresh=False, include_links=False, include_attrs=False):
    """ Return top-level json (i.e. excluding attributes or links by default) for a given obj_id.
    If refresh is False, any data present in the meta_cache will be returned.  If not
    the DN will be queries, and any resultant data added to the meta_cache.
    Note: meta_cache values may be stale, but use of immutable data (e.g. type of a dataset)
    is always valid
    """
    meta_cache = app['meta_cache']
    obj_json = None
    if include_links or include_attrs:
        # links and attributes are subject to change, so always refresh
        refresh = True
    log.info(f"getObjectJson {obj_id}")
    if obj_id in meta_cache and not refresh:
        log.debug(f"found {obj_id} in meta_cache")
        obj_json = meta_cache[obj_id]
    elif "dn_urls" not in app:
        # no DN containers, fetch the JSON directly from storage
        log.debug("No dn_urls, doing direct read")
        try:
            s3_key = getS3Key(obj_id)
            obj_exists = await isStorObj(app, s3_key)
            if not obj_exists:
                log.warn(f"key: {s3_key} not found")
                raise HTTPNotFound()
            obj_json = await getStorJSONObj(app, s3_key)
        except ValueError as ve:
            log.error(f"Got ValueError exception: {ve}")
            raise HTTPInternalServerError()
        except ClientOSError as coe:
            log.error(f"Got ClientOSError: {coe}")
            raise HTTPInternalServerError()
    else:
        req = getDataNodeUrl(app, obj_id)
        collection =  getCollectionForId(obj_id)
        params = {}
        if include_links:
            params["include_links"] = 1
        if include_attrs:
            params["include_attrs"] = 1
        if bucket:
            params["bucket"] = bucket
        req += '/' + collection + '/' + obj_id
        obj_json = await http_get(app, req, params=params)  # throws 404 if doesn't exist
        meta_cache[obj_id] = obj_json
    if obj_json is None:
        msg = f"Object: {obj_id} not found"
        log.warn(msg)
        raise HTTPNotFound()
    return obj_json

async def getObjectIdByPath(app, obj_id, h5path, bucket=None, refresh=False):
    """ Find the object at the provided h5path location.
    If not found raise 404 error.
    """
    log.info(f"getObjectIdByPath obj_id: {obj_id} h5path: {h5path} refresh: {refresh}")
    if h5path.startswith("./"):
        h5path = h5path[2:]  # treat as relative path
    links = h5path.split('/')
    for link in links:
        if not link:
            continue  # skip empty link
        log.debug(f"getObjectIdByPath for objid: {obj_id} got link: {link}")
        if getCollectionForId(obj_id) != "groups":
            # not a group, so won't have links
            msg = f"h5path: {h5path} not found"
            log.warn(msg)
            raise HTTPNotFound()
        req = getDataNodeUrl(app, obj_id)
        req += "/groups/" + obj_id + "/links/" + link
        log.debug("get LINK: " + req)
        params = {}
        if bucket:
            params["bucket"] = bucket
        link_json = await http_get(app, req, params=params)
        log.debug("got link_json: " + str(link_json))
        if link_json["class"] != 'H5L_TYPE_HARD':
            # don't follow soft/external links
            msg = f"h5path: {h5path} not found"
            log.warn(msg)
            raise HTTPInternalServerError()
        obj_id = link_json["id"]
    # if we get here, we've traveresed the entire path and found the object
    return obj_id

async def getPathForObjectId(app, parent_id, idpath_map, tgt_id=None, bucket=None):
    """ Search the object starting with the given parent_id.
    idpath should be a dict with at minimum the key: parent_id: <parent_path>.
    If tgt_id is not None, returns first path that matches the tgt_id or None if not found.
    If Tgt_id is no, returns the idpath_map.
    """

    if not parent_id:
        log.error("No parent_id passed to getPathForObjectId")
        raise HTTPInternalServerError()

    if parent_id not in idpath_map:
        msg = f"Obj {parent_id} expected to be found in idpath_map"
        log.error(msg)
        raise HTTPInternalServerError()

    parent_path = idpath_map[parent_id]
    if parent_id == tgt_id:
        return parent_path

    req = getDataNodeUrl(app, parent_id)
    req += "/groups/" + parent_id + "/links"
    params = {}
    if bucket:
        params["bucket"] = bucket

    log.debug("getPathForObjectId LINKS: " + req)
    links_json = await http_get(app, req, params=params)
    log.debug(f"getPathForObjectId got links json from dn for parent_id: {parent_id}")
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
        h5path = await getPathForObjectId(app, link_id, idpath_map, tgt_id=tgt_id, bucket=bucket) # recursive call
        if tgt_id is not None and h5path:
            break

    return h5path

async def getRootInfo(app, root_id, bucket=None):
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
        info_json = await getStorJSONObj(app, info_key, bucket=bucket)
    except HTTPNotFound:
        log.warn(f"info.json not found for key: {info_key}")
        return None

    return info_json
