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
# utility methods for service node handlers
#

import asyncio

from aiohttp.web_exceptions import HTTPBadRequest, HTTPForbidden
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError
from aiohttp.client_exceptions import ClientOSError, ClientError

from .util.authUtil import getAclKeys
from .util.idUtil import getDataNodeUrl, getCollectionForId, isSchema2Id
from .util.idUtil import getS3Key
from .util.linkUtil import h5Join
from .util.storUtil import getStorJSONObj, isStorObj
from .util.authUtil import aclCheck
from .util.httpUtil import http_get, http_delete
from .util.domainUtil import getBucketForDomain, verifyRoot

from . import hsds_logger as log


async def getDomainJson(app, domain, reload=False):
    """Return domain JSON from cache or fetch from DN if not found
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
    params = {"domain": domain}

    log.debug(f"sending dn req: {req} params: {params}")

    domain_json = await http_get(app, req, params=params)

    if "owner" not in domain_json:
        log.warn("No owner key found in domain")
        raise HTTPInternalServerError()

    if "acls" not in domain_json:
        log.warn("No acls key found in domain")
        raise HTTPInternalServerError()

    domain_cache[domain] = domain_json  # add to cache
    return domain_json


def checkBucketAccess(app, bucket, action="read"):
    """ if the given bucket is not the default bucket, check
    that non-default bucket access is enabled.
    Throw 403 error if not allowed """
    if bucket and bucket != app["bucket_name"]:
        # check that we are allowed to access non-default buckets
        if action == "read":
            if not app["allow_any_bucket_read"]:
                log.warn(f"read access to non-default bucket: {bucket} not enabled")
                raise HTTPForbidden()
        else:
            # write acction
            if not app["allow_any_bucket_write"]:
                log.warn(f"write access to non-default bucket: {bucket} not enabled")
                raise HTTPForbidden()


async def validateAction(app, domain, obj_id, username, action):
    """check that the given object belongs in the domain and that the
    requested action (create, read, update, delete, readACL, udpateACL)
    is permitted for the requesting user.
    """
    meta_cache = app["meta_cache"]
    msg = f"validateAction(domain={domain}, obj_id={obj_id}, "
    msg += f"username={username}, action={action})"
    log.info(msg)
    bucket = getBucketForDomain(domain)
    if bucket:
        checkBucketAccess(app, bucket, action=action)

    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    verifyRoot(domain_json)

    obj_json = None
    if obj_id in meta_cache:
        log.debug(f"validateAction - found {obj_id} in meta_cache")
        obj_json = meta_cache[obj_id]
    else:
        # fetch from DN
        log.debug(f"validateAction - fetch {obj_id}")
        collection = getCollectionForId(obj_id)
        req = getDataNodeUrl(app, obj_id)
        req += "/" + collection + "/" + obj_id
        bucket = getBucketForDomain(domain)
        params = {}
        if bucket:
            params["bucket"] = bucket
        obj_json = await http_get(app, req, params=params)
        meta_cache[obj_id] = obj_json

    s1 = obj_json["root"]
    s2 = domain_json["root"]
    msg = f"obj_json[root]: {s1} domain_json[root]: {s2}"
    log.debug(msg)
    if obj_json["root"] != domain_json["root"]:
        log.info("unexpected root, reloading domain")
        domain_json = await getDomainJson(app, domain, reload=True)
        if "root" not in domain_json or s1 != domain_json["root"]:
            msg = "Object id is not a member of the given domain"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    acl_keys = getAclKeys()
    if action not in acl_keys:
        log.error(f"unexpected action: {action}")
        raise HTTPInternalServerError()

    reload = False
    try:
        # throws exception if not allowed
        aclCheck(app, domain_json, action, username)
    except HTTPForbidden:
        msg = "got HttpProcessing error on validate action for domain: "
        msg += f"{domain}, reloading..."
        log.info(msg)
        # just in case the ACL was recently updated, refetch the domain
        reload = True
    if reload:
        domain_json = await getDomainJson(app, domain, reload=True)
        aclCheck(app, domain_json, action, username)


async def getObjectJson(
    app, obj_id, bucket=None, refresh=False, include_links=False, include_attrs=False
):
    """Return top-level json (i.e. excluding attributes or links by default)
    for a given obj_id.
    If refresh is False, any data present in the meta_cache will be
    returned.  If not the DN will be queried, and any resultant data
    added to the meta_cache.
    Note: meta_cache values may be stale, but use of immutable data
      (e.g. type of a dataset) is always valid
    """
    meta_cache = app["meta_cache"]
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
        collection = getCollectionForId(obj_id)

        params = {}
        if include_links:
            params["include_links"] = 1
        if include_attrs:
            params["include_attrs"] = 1
        if bucket:
            params["bucket"] = bucket
        req += "/" + collection + "/" + obj_id
        log.debug(f"getObjectJson - fetching {obj_id} from {req}")
        # throws 404 if doesn't exist
        obj_json = await http_get(app, req, params=params)
        meta_cache[obj_id] = obj_json
    if obj_json is None:
        msg = f"Object: {obj_id} not found, req: {req}, params: {params}"
        log.warn(msg)
        raise HTTPNotFound()

    return obj_json


async def getDsetJson(app, dset_id,
                      bucket=None,
                      refresh=False,
                      include_links=False,
                      include_attrs=False):
    kwargs = {}
    kwargs["bucket"] = bucket
    kwargs["refresh"] = refresh
    kwargs["include_links"] = include_links
    kwargs["include_attrs"] = include_attrs
    dset_json = await getObjectJson(app, dset_id, **kwargs)
    if refresh:
        # can just return the json
        return dset_json

    # check to see if the dataspace is mutable
    # if so, refresh if necessary
    datashape = dset_json["shape"]
    if "maxdims" in datashape:
        log.debug("getDsetJson - refreshing json for mutable shape")
        kwargs["refresh"] = True
        dset_json = await getObjectJson(app, dset_id, **kwargs)
    return dset_json


async def getObjectIdByPath(app, obj_id, h5path, bucket=None, refresh=False, domain=None,
                            follow_soft_links=False, follow_external_links=False):
    """Find the object at the provided h5path location.
    If not found raise 404 error.
    Returns a tuple of the object's id, the domain it is under,
    and the json for the link to the object.
    """

    msg = f"getObjectIdByPath obj_id: {obj_id} h5path: {h5path} in domain: {domain} "
    msg += f"refresh: {refresh}"
    log.info(msg)

    if getCollectionForId(obj_id) != "groups":
        # not a group, so won't have links
        msg = f"h5path: {h5path} not found"
        msg += f"getCollectionForId({obj_id}) returned {getCollectionForId(obj_id)}"
        log.warn(msg)
        raise HTTPNotFound()

    if h5path.startswith("./"):
        h5path = h5path[2:]  # treat as relative path

    links = h5path.split("/")
    link_json = None

    if h5path == "/":
        log.debug("Root group requested by path")
        ext_domain_json = await getDomainJson(app, domain)
        return ext_domain_json["root"], domain, None

    if h5path == ".":
        log.debug("Group requested self by path")
        return obj_id, domain, None

    for link in links:
        if not link:
            continue  # skip empty link

        req = getDataNodeUrl(app, obj_id)
        req += "/groups/" + obj_id + "/links/" + link
        log.debug("get LINK: " + req)
        params = {}
        if bucket:
            params["bucket"] = bucket
        link_json = await http_get(app, req, params=params)

        if link_json["class"] == "H5L_TYPE_EXTERNAL":
            if not follow_external_links:
                msg = "Query found unexpected external link"
                log.warn(msg)
                raise HTTPBadRequest()

            # find domain object is stored under
            domain = link_json["h5domain"]

            if domain.startswith("hdf5:/"):
                # strip off prefix
                domain = domain[6:]

            if bucket:
                domain = bucket + domain

            ext_domain_json = await getDomainJson(app, domain)

            verifyRoot(ext_domain_json)

            msg = f"external domain response = {ext_domain_json}"
            log.debug(msg)

            if link_json["h5path"][0] == '/':
                msg = "External link by absolute path"
                log.debug(msg)
                obj_id, domain, link_json = await getObjectIdByPath(
                    app, ext_domain_json["root"], link_json["h5path"],
                    bucket=bucket, refresh=refresh, domain=domain,
                    follow_soft_links=follow_soft_links,
                    follow_external_links=follow_external_links)
            else:
                msg = "Cannot follow external link by relative path"
                log.warn(msg)
                raise HTTPInternalServerError()

        elif link_json["class"] == "H5L_TYPE_SOFT":
            if not follow_soft_links:
                msg = "Query found unexpected soft link"
                log.warn(msg)
                raise HTTPBadRequest()

            path_from_link = link_json["h5path"]

            if path_from_link[0] != "/":
                # If relative path, keep parent object the same
                obj_id, domain, link_json = await getObjectIdByPath(
                    app, obj_id, path_from_link, bucket=bucket,
                    refresh=refresh, domain=domain,
                    follow_soft_links=follow_soft_links,
                    follow_external_links=follow_external_links)
            else:
                if not domain:
                    msg = "Soft link with absolute path used with no domain given"
                    log.warn(msg)
                    raise HTTPInternalServerError()

                # If absolute path, replace parent object with root group
                domain_json = await getDomainJson(app, domain)
                verifyRoot(domain_json)

                obj_id, domain, link_json = await getObjectIdByPath(
                    app, domain_json["root"], path_from_link,
                    bucket=bucket, refresh=refresh, domain=domain,
                    follow_soft_links=follow_soft_links,
                    follow_external_links=follow_external_links)

        elif link_json["class"] == "H5L_TYPE_HARD":
            obj_id = link_json["id"]

        else:
            log.warn("Link has invalid type!")
            raise HTTPInternalServerError()

    # If object at the end of the path was a symbolic link, search again under that link
    if link_json and (link_json["class"] != "H5L_TYPE_HARD"):
        log.debug("Recursing under symbolic link")
        parent_id = None

        if link_json["class"] == "H5L_TYPE_SOFT":

            if link_json["h5path"][0] == '/':
                domain_json = await getDomainJson(app, domain)
                parent_id = domain_json["root"]
            else:
                parent_id = obj_id

        elif link_json["class"] == "H5L_TYPE_EXTERNAL":
            domain = link_json["h5domain"]

            ext_domain_json = await getDomainJson(app, domain)
            verifyRoot(ext_domain_json)

            msg = f"external domain response = {ext_domain_json}"
            log.debug(msg)

            parent_id = ext_domain_json["root"]

            if link_json["h5path"][0] != '/':
                msg = "External link by relative path is unsupported"
                log.warn(msg)
                raise HTTPInternalServerError()

        obj_id, domain, link_json = await getObjectIdByPath(
            app, parent_id, link_json["h5path"],
            bucket=bucket, refresh=refresh, domain=domain,
            follow_soft_links=follow_soft_links,
            follow_external_links=follow_external_links)

    return obj_id, domain, link_json


async def getPathForObjectId(app, parent_id, idpath_map, tgt_id=None, bucket=None):
    """Search the object starting with the given parent_id.
    idpath should be a dict with at minimum the key: parent_id: <parent_path>.
    If tgt_id is not None, returns first path that matches the tgt_id or
    None if not found.
    If Tgt_id is None, returns the idpath_map.
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
    msg = "getPathForObjectId got links json from dn for "
    msg += f"parent_id: {parent_id}"
    log.debug(msg)
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
            h5path = h5Join(parent_path, title)
            break
        idpath_map[link_id] = h5Join(parent_path, title)
        if getCollectionForId(link_id) != "groups":
            continue
        # recursive call
        kwargs = {"tgt_id": tgt_id, "bucket": bucket}
        h5path = await getPathForObjectId(app, link_id, idpath_map, **kwargs)
        if tgt_id is not None and h5path:
            break

    return h5path


async def getRootInfo(app, root_id, bucket=None):
    """Get extra information the root collection."""
    # Gather additional info on the domain
    log.debug(f"getRootInfo {root_id}")

    if not isSchema2Id(root_id):
        msg = f"no dataset details not available for schema v1 id: {root_id} "
        msg += "returning null results"
        log.info(msg)
        return None

    s3_key = getS3Key(root_id)

    parts = s3_key.split("/")
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


async def removeChunks(app, chunk_ids, bucket=None):
    """ Remove chunks with the given ids """

    log.info(f"removeChunks, {len(chunk_ids)} chunks")
    log.debug(f"removeChunks for: {chunk_ids}")

    dn_urls = app["dn_urls"]
    if not dn_urls:
        log.error("removeChunks request, but no dn_urls")
        raise HTTPInternalServerError()

    log.debug(f"doFlush - dn_urls: {dn_urls}")
    params = {}
    if bucket:
        params["bucket"] = bucket
    failed_count = 0

    try:
        tasks = []
        for chunk_id in chunk_ids:
            dn_url = getDataNodeUrl(app, chunk_id)
            req = dn_url + "/chunks/" + chunk_id
            task = asyncio.ensure_future(http_delete(app, req, params=params))
            tasks.append(task)
        done, pending = await asyncio.wait(tasks)
        if pending:
            # should be empty since we didn't use return_when parameter
            log.error("removeChunks - got pending tasks")
            raise HTTPInternalServerError()
        for task in done:
            if task.exception():
                exception_type = type(task.exception())
                msg = f"removeChunks - task had exception: {exception_type}"
                log.warn(msg)
                failed_count += 1

    except ClientError as ce:
        msg = f"removeChunks - ClientError: {ce}"
        log.error(msg)
        raise HTTPInternalServerError()
    except asyncio.CancelledError as cle:
        log.error(f"removeChunks - CancelledError: {cle}")
        raise HTTPInternalServerError()

    if failed_count:
        msg = f"removeChunks, failed count: {failed_count}"
        log.error(msg)
    else:
        log.info(f"removeChunks complete for {len(chunk_ids)} chunks - no errors")
