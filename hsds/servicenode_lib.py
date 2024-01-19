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
import json

from aiohttp.web_exceptions import HTTPBadRequest, HTTPForbidden
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError
from aiohttp.client_exceptions import ClientOSError, ClientError

from .util.authUtil import getAclKeys
from .util.arrayUtil import encodeData
from .util.idUtil import getDataNodeUrl, getCollectionForId
from .util.idUtil import isSchema2Id, getS3Key, isValidUuid
from .util.linkUtil import h5Join, validateLinkName, getLinkClass
from .util.storUtil import getStorJSONObj, isStorObj
from .util.authUtil import aclCheck
from .util.httpUtil import http_get, http_put, http_post, http_delete
from .util.domainUtil import getBucketForDomain, verifyRoot, getLimits
from .util.storUtil import getCompressors
from .basenode import getVersion

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


async def getDomainResponse(app, domain_json, bucket=None, verbose=False):
    """ construct JSON response for domain request """
    rsp_json = {}
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
    metadata_bytes = 0
    allocated_bytes = 0
    linked_bytes = 0
    num_chunks = 0
    num_linked_chunks = 0
    md5_sum = ""

    if verbose and "root" in domain_json:
        root_id = domain_json["root"]
        root_info = await getRootInfo(app, root_id, bucket=bucket)
        if root_info:
            allocated_bytes = root_info["allocated_bytes"]
            totalSize += allocated_bytes
            if "linked_bytes" in root_info:
                linked_bytes += root_info["linked_bytes"]
                totalSize += linked_bytes
            if "num_linked_chunks" in root_info:
                num_linked_chunks = root_info["num_linked_chunks"]
            if "metadata_bytes" in root_info:
                # this key was added for schema v2
                metadata_bytes = root_info["metadata_bytes"]
                totalSize += metadata_bytes
            if root_info["lastModified"] > lastModified:
                lastModified = root_info["lastModified"]
            if "md5_sum" in root_info:
                md5_sum = root_info["md5_sum"]

            num_groups = root_info["num_groups"]
            num_datatypes = root_info["num_datatypes"]
            num_datasets = len(root_info["datasets"])
            num_chunks = root_info["num_chunks"]
            rsp_json["scan_info"] = root_info  # return verbose info here

        else:
            # root info not available - just return 0 for these values
            allocated_bytes = 0
            totalSize = 0
            num_groups = 0
            num_datasets = 0
            num_datatypes = 0
            num_chunks = 0

        num_objects = num_groups + num_datasets + num_datatypes + num_chunks
        rsp_json["num_groups"] = num_groups
        rsp_json["num_datasets"] = num_datasets
        rsp_json["num_datatypes"] = num_datatypes
        rsp_json["num_objects"] = num_objects
        rsp_json["total_size"] = totalSize
        rsp_json["allocated_bytes"] = allocated_bytes
        rsp_json["num_objects"] = num_objects
        rsp_json["metadata_bytes"] = metadata_bytes
        rsp_json["linked_bytes"] = linked_bytes
        rsp_json["num_chunks"] = num_chunks
        rsp_json["num_linked_chunks"] = num_linked_chunks
        rsp_json["md5_sum"] = md5_sum

    # pass back config parameters the client may care about

    rsp_json["limits"] = getLimits()
    rsp_json["compressors"] = getCompressors()
    rsp_json["version"] = getVersion()
    rsp_json["lastModified"] = lastModified
    return rsp_json


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

    msg = f"GetObjectJson - obj_id: {obj_id} refresh: {refresh} "
    msg += f"include_links: {include_links} include_attrs: {include_attrs}"
    log.debug(msg)

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

    if obj_json is None:
        msg = f"Object: {obj_id} not found, req: {req}, params: {params}"
        log.warn(msg)
        raise HTTPNotFound()

    # store object in meta_cache (but don't include links or attributes,
    #   since they are volatile)
    cache_obj = {}
    for k in obj_json:
        if k in ("links", "attributes"):
            continue
        cache_obj[k] = obj_json[k]
    meta_cache[obj_id] = cache_obj
    log.debug(f"stored {cache_obj} in meta_cache")

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


async def getLinks(app, group_id,
                   titles=None,
                   create_order=False,
                   limit=None,
                   marker=None,
                   pattern=None,
                   bucket=None):

    """ Get the link jsons for the given titles """

    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links"
    params = {"bucket": bucket}
    log.debug(f"getLinks {group_id}")

    if titles:
        # do a post request with the given title list
        log.debug(f"getLinks for {group_id} - {len(titles)} titles")
        data = {"titles": titles}
        post_rsp = await http_post(app, req, data=data, params=params)
        log.debug(f"got link_json: {post_rsp}")
        if "links" not in post_rsp:
            log.error("unexpected response from post links")
            raise HTTPInternalServerError()
        links = post_rsp["links"]
    else:
        # do a get for all links
        log.debug(f"getLinks, all links for {group_id}")
        if create_order:
            params["CreateOrder"] = 1
        if limit is not None:
            params["Limit"] = str(limit)
        if marker is not None:
            params["Marker"] = marker
        if pattern is not None:
            params["pattern"] = pattern

        get_rsp = await http_get(app, req, params=params)
        log.debug(f"got link_json: {get_rsp}")
        if "links" not in get_rsp:
            log.error("unexpected response from get links")
            raise HTTPInternalServerError()
        links = get_rsp["links"]

    return links


async def getLink(app, group_id, title, bucket=None):
    """ Get the link json for the given title """

    titles = [title, ]
    links = await getLinks(app, group_id, titles=titles, bucket=bucket)

    if len(links) != 1:
        log.error(f"expected 1 link but got: {len(links)}")
        raise HTTPInternalServerError()
    link_json = links[0]

    return link_json


async def putLink(app, group_id, title, tgt_id=None, h5path=None, h5domain=None, bucket=None):
    """ create a new link.  Return 201 if this is a new link,
    or 200 if it's a duplicate of an existing link. """

    try:
        validateLinkName(title)
    except ValueError:
        raise HTTPBadRequest(reason="invalid link name")

    if h5path and tgt_id:
        msg = "putLink - provide tgt_id or h5path, but not both"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    link_json = {}
    if tgt_id:
        link_json["id"] = tgt_id
    if h5path:
        link_json["h5path"] = h5path
    if h5domain:
        link_json["h5domain"] = h5domain

    try:
        link_class = getLinkClass(link_json)
    except ValueError:
        raise HTTPBadRequest(reason="invalid link")

    link_json["class"] = link_class

    # for hard links, verify that the referenced id exists and is in
    # this domain
    if link_class == "H5L_TYPE_HARD":
        tgt_id = link_json["id"]
        ref_json = await getObjectJson(app, tgt_id, bucket=bucket)
        group_json = await getObjectJson(app, group_id, bucket=bucket)
        if ref_json["root"] != group_json["root"]:
            msg = "Hard link must reference an object in the same domain"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    # ready to add link now
    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links"
    log.debug(f"PUT links - PUT request: {req}")
    params = {"bucket": bucket}

    data = {"links": {title: link_json}}

    put_rsp = await http_put(app, req, data=data, params=params)
    log.debug(f"PUT Link resp: {put_rsp}")
    if "status" in put_rsp:
        status = put_rsp["status"]
    else:
        status = 201
    return status


async def putHardLink(app, group_id, title, tgt_id=None, bucket=None):
    """ create a new hard link.  Return 201 if this is a new link,
      or 200 if it's a duplicate of an existing link """

    status = await putLink(app, group_id, title, tgt_id=tgt_id, bucket=bucket)
    return status


async def putSoftLink(app, group_id, title, h5path=None, bucket=None):
    """ create a new soft link.  Return 201 if this is a new link,
      or 200 if it's a duplicate of an existing link """

    status = await putLink(app, group_id, title, h5path=h5path, bucket=bucket)
    return status


async def putExternalLink(app, group_id, title, h5path=None, h5domain=None, bucket=None):
    """ create a new external link.  Return 201 if this is a new link,
      or 200 if it's a duplicate of an existing link """

    status = await putLink(app, group_id, title, h5path=h5path, h5domain=h5domain, bucket=bucket)
    return status


async def putLinks(app, group_id, items, bucket=None):
    """ create a new links.  Return 201 if any item is a new link,
    or 200 if it's a duplicate of an existing link. """

    isValidUuid(group_id, obj_class="group")
    group_json = None

    # validate input
    for title in items:
        try:
            validateLinkName(title)
            item = items[title]
            link_class = getLinkClass(item)
        except ValueError:
            # invalid link
            raise HTTPBadRequest(reason="invalid link")

        if link_class == "H5L_TYPE_HARD":
            tgt_id = item["id"]
            isValidUuid(tgt_id)
            # for hard links, verify that the referenced id exists and is in
            # this domain
            ref_json = await getObjectJson(app, tgt_id, bucket=bucket)
            if not group_json:
                # just need to fetch this once
                group_json = await getObjectJson(app, group_id, bucket=bucket)
            if ref_json["root"] != group_json["root"]:
                msg = "Hard link must reference an object in the same domain"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

    # ready to add links now
    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links"
    log.debug(f"PUT links - PUT request: {req}")
    params = {"bucket": bucket}

    data = {"links": items}

    put_rsp = await http_put(app, req, data=data, params=params)
    log.debug(f"PUT Link resp: {put_rsp}")
    if "status" in put_rsp:
        status = put_rsp["status"]
    else:
        status = 201
    return status


async def deleteLinks(app, group_id, titles=None, separator="/", bucket=None):
    """ delete the requested set of links from the given object """

    if titles is None or len(titles) == 0:
        msg = "provide a list of link names for deletion"
        log.debug(msg)
        raise HTTPBadRequest(reason=msg)

    node_url = getDataNodeUrl(app, group_id)
    req = f"{node_url}/groups/{group_id}/links"
    log.debug(f"deleteLinks: {req}")
    params = {"separator": separator, "bucket": bucket}

    # stringify the list of link_names
    titles_param = separator.join(titles)
    params["titles"] = titles_param
    log.debug(f"using params: {params}")
    await http_delete(app, req, params=params)


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

        link_json = await getLink(app, obj_id, link, bucket=bucket)

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
                kwargs = {}
                kwargs["bucket"] = bucket
                kwargs["refresh"] = refresh
                kwargs["domain"] = domain
                kwargs["follow_soft_links"] = follow_soft_links
                kwargs["follow_external_links"] = follow_external_links
                obj_id, domain, link_json = await getObjectIdByPath(
                    app, ext_domain_json["root"], link_json["h5path"], **kwargs)
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


async def doFlush(app, root_id, bucket=None):
    """return wnen all DN nodes have wrote any pending changes to S3"""
    log.info(f"doFlush {root_id}")
    params = {"flush": 1}
    if bucket:
        params["bucket"] = bucket
    dn_urls = app["dn_urls"]
    dn_ids = []
    log.debug(f"doFlush - dn_urls: {dn_urls}")
    failed_count = 0

    try:
        tasks = []
        for dn_url in dn_urls:
            req = dn_url + "/groups/" + root_id
            task = asyncio.ensure_future(http_put(app, req, params=params))
            tasks.append(task)
        done, pending = await asyncio.wait(tasks)
        if pending:
            # should be empty since we didn't use return_when parameter
            log.error("doFlush - got pending tasks")
            raise HTTPInternalServerError()
        for task in done:
            if task.exception():
                exception_type = type(task.exception())
                msg = f"doFlush - task had exception: {exception_type}"
                log.warn(msg)
                failed_count += 1
            else:
                json_rsp = task.result()
                log.debug(f"PUT /groups rsp: {json_rsp}")
                if json_rsp and "id" in json_rsp:
                    dn_ids.append(json_rsp["id"])
                else:
                    log.error("expected dn_id in flush response from DN")
    except ClientError as ce:
        msg = f"doFlush - ClientError for http_put('/groups/{root_id}'): {ce}"
        log.error(msg)
        raise HTTPInternalServerError()
    except asyncio.CancelledError as cle:
        log.error(f"doFlush - CancelledError '/groups/{root_id}'): {cle}")
        raise HTTPInternalServerError()
    msg = f"doFlush for {root_id} complete, failed: {failed_count} "
    msg += f"out of {len(dn_urls)}"
    log.info(msg)
    if failed_count > 0:
        log.error(f"doFlush fail count: {failed_count} returning 500")
        raise HTTPInternalServerError()
    else:
        log.info("doFlush no fails, returning dn ids")
        return dn_ids


async def getAttributes(app, obj_id,
                        attr_names=None,
                        include_data=True,
                        max_data_size=0,
                        ignore_nan=False,
                        create_order=False,
                        pattern=None,
                        encoding=None,
                        limit=0,
                        marker=None,
                        bucket=None
                        ):
    """ get the requested set of attributes from the given object """
    if attr_names is None:
        msg = "attr_names is None, do a GET for all attributes"
        log.debug(msg)

    collection = getCollectionForId(obj_id)
    node_url = getDataNodeUrl(app, obj_id)
    req = f"{node_url}/{collection}/{obj_id}/attributes"
    log.debug(f"getAttributes: {req}")
    params = {}
    if include_data:
        params["IncludeData"] = 1
    if ignore_nan:
        params["ignore_nan"] = 1
    if bucket:
        params["bucket"] = bucket
    if create_order:
        params["CreateOrder"] = 1
    if encoding:
        params["encoding"] = encoding
    if max_data_size > 0:
        params["max_data_size"] = max_data_size

    if attr_names:
        # send names via a POST request
        data = {"attributes": attr_names}
        log.debug(f"using params: {params}")
        dn_json = await http_post(app, req, data=data, params=params)
        log.debug(f"attributes POST response for obj_id {obj_id} got: {dn_json}")
    else:
        # some additonal query params for get attributes
        if limit:
            params["Limit"] = limit
        if marker:
            params["Marker"] = marker
        if pattern:
            params["pattern"] = pattern

        log.debug(f"using params: {params}")
        # do a get to fetch all the attributes
        dn_json = await http_get(app, req, params=params)
        log.debug(f"attribute GET response for obj_id {obj_id} got: {dn_json}")

    log.debug(f"got attributes json from dn for obj_id: {obj_id}")
    if "attributes" not in dn_json:
        msg = f"expected attributes key from dn, but got: {dn_json}"
        log.error(msg)
        raise HTTPInternalServerError()

    attributes = dn_json["attributes"]
    if not isinstance(attributes, list):
        msg = f"was expecting list of attributes, but got: {type(attributes)}"
        log.error(msg)
        raise HTTPInternalServerError()

    if attr_names and len(attributes) < len(attr_names):
        msg = f"POST attributes requested {len(attr_names)}, "
        msg += f"but only {len(attributes)} were returned"
        log.warn(msg)

    log.debug(f"getAttributes returning {len(attributes)} attributes")
    return attributes


async def putAttributes(app,
                        obj_id,
                        attr_json=None,
                        replace=False,
                        bucket=None
                        ):

    """ write the given attributes to the appropriate DN """
    req = getDataNodeUrl(app, obj_id)
    collection = getCollectionForId(obj_id)
    req += f"/{collection}/{obj_id}/attributes"
    log.info(f"putAttribute: {req}")

    params = {}
    if replace:
        # allow attribute to be overwritten
        log.debug("setting replace for putAtttributes")
        params["replace"] = 1
    else:
        log.debug("replace is not set for putAttributes")

    if bucket:
        params["bucket"] = bucket

    data = {"attributes": attr_json}
    log.debug(f"put attributes params: {params}")
    log.debug(f"put attributes: {attr_json}")
    put_rsp = await http_put(app, req, data=data, params=params)

    if "status" in put_rsp:
        status = put_rsp["status"]
    else:
        status = 201

    log.info(f"putAttributes status: {status}")

    return status


async def deleteAttributes(app, obj_id, attr_names=None, separator="/", bucket=None):
    """ delete the requested set of attributes from the given object """

    if attr_names is None or len(attr_names) == 0:
        msg = "provide a list of attribute names for deletion"
        log.debug(msg)
        raise HTTPBadRequest(reason=msg)

    collection = getCollectionForId(obj_id)
    node_url = getDataNodeUrl(app, obj_id)
    req = f"{node_url}/{collection}/{obj_id}/attributes"
    log.debug(f"deleteAttributes: {req}")
    # always use base64 to avoid any issues with url encoding
    params = {"encoding": "base64", "separator": separator}
    if bucket:
        params["bucket"] = bucket

    # stringify the list of attr_names
    attr_name_param = separator.join(attr_names)
    attr_name_param = encodeData(attr_name_param).decode("ascii")
    params["attr_names"] = attr_name_param
    log.debug(f"using params: {params}")
    await http_delete(app, req, params=params)


async def deleteObj(app, obj_id, bucket=None):
    """ send delete request for group, datatype, or dataset obj """
    log.debug(f"deleteObj {obj_id}")
    req = getDataNodeUrl(app, obj_id)
    collection = getCollectionForId(obj_id)
    req += f"/{collection}/{obj_id}"
    params = {}
    if bucket:
        params["bucket"] = bucket
    log.debug(f"http_delete req: {req} params: {params}")

    await http_delete(app, req, params=params)

    meta_cache = app["meta_cache"]
    if obj_id in meta_cache:
        del meta_cache[obj_id]  # remove from cache
