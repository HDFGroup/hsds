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
import math
import time
import numpy as np

from aiohttp.web_exceptions import HTTPBadRequest, HTTPForbidden, HTTPGone, HTTPConflict
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError

from aiohttp.client_exceptions import ClientOSError, ClientError
from aiohttp import ClientResponseError

from h5json.array_util import encodeData, decodeData, bytesToArray, bytesArrayToList
from h5json.array_util import jsonToArray, getNumpyValue
from h5json.objid import getCollectionForId, createObjId, getRootObjId
from h5json.objid import isSchema2Id, getS3Key, isValidUuid
from h5json.hdf5dtype import getBaseTypeJson, validateTypeItem, createDataType, getItemSize
from h5json.filters import getFiltersJson
from h5json.shape_util import getShapeDims, getShapeClass
from h5json.dset_util import guessChunk, getChunkSize
from h5json.dset_util import validateChunkLayout, getDataSize, getDsetMaxDims
from h5json.dset_util import LAYOUT_CLASSES

from .util.nodeUtil import getDataNodeUrl
from .util.authUtil import getAclKeys
from .util.linkUtil import h5Join, validateLinkName, getLinkClass, getRequestLinks
from .util.storUtil import getStorJSONObj, isStorObj, getSupportedFilters
from .util.authUtil import aclCheck
from .util.httpUtil import http_get, http_put, http_post, http_delete
from .util.domainUtil import getBucketForDomain, verifyRoot, getLimits
from .util.dsetUtil import getShapeJson
from .util.storUtil import getCompressors

from .basenode import getVersion
from . import hsds_logger as log
from . import config


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

    try:
        domain_json = await http_get(app, req, params=params)
    except HTTPNotFound:
        log.warn(f"domain: {domain} not found")
        raise
    except HTTPGone:
        log.warn(f"domain: {domain} has been removed")
        raise
    except ClientResponseError as ce:
        # shouldn't get this if we are catching relevant exceptions
        # in http_get...
        log.error(f"Unexpected ClientResponseError: {ce}")

        if ce.code == 404:
            log.warn("domain not found")
            raise HTTPNotFound()
        elif ce.code == 410:
            log.warn("domain has been removed")
            raise HTTPGone()
        else:
            log.error(f"unexpected error: {ce.code}")
            raise HTTPInternalServerError()

    if not domain_json:
        msg = f"nothing returned (and no exceptionraised) for domain: {domain}"
        log.error(msg)
        raise HTTPInternalServerError()

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


async def getObjectJson(app,
                        obj_id,
                        bucket=None,
                        refresh=False,
                        include_links=False,
                        include_attrs=False
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
    log.debug(f"stored {obj_id} in meta_cache")

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
    if pattern is not None:
        params["pattern"] = pattern
    log.debug(f"getLinks {group_id}")

    if titles:
        # do a post request with the given title list
        log.debug(f"getLinks for {group_id} - {len(titles)} titles")
        log.debug(f"  params: {params}")
        data = {"titles": titles}
        post_rsp = await http_post(app, req, data=data, params=params)
        log.debug(f"got link_json: {post_rsp}")
        if "links" not in post_rsp:
            log.error("unexpected response from post links")
            raise HTTPInternalServerError()
        links = post_rsp["links"]
    else:
        # do a get for all links
        if create_order:
            params["CreateOrder"] = 1
        if limit is not None:
            params["Limit"] = str(limit)
        if marker is not None:
            params["Marker"] = marker
        log.debug(f"getLinks, all links for {group_id}, params: {params}")

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


async def putLink(app, group_id, title,
                  tgt_id=None,
                  h5path=None,
                  h5domain=None,
                  bucket=None,
                  created=None):
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
    if created:
        link_json["created"] = created

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

    log.debug(f"putHardLink for group {group_id}, tgt: {tgt_id} title: {title}")
    status = await putLink(app, group_id, title, tgt_id=tgt_id, bucket=bucket)
    return status


async def putSoftLink(app, group_id, title, h5path=None, bucket=None):
    """ create a new soft link.  Return 201 if this is a new link,
      or 200 if it's a duplicate of an existing link """

    log.debug(f"putSoftLink for group {group_id}, h5path: {h5path} title: {title}")
    status = await putLink(app, group_id, title, h5path=h5path, bucket=bucket)
    return status


async def putExternalLink(app, group_id, title, h5path=None, h5domain=None, bucket=None):
    """ create a new external link.  Return 201 if this is a new link,
      or 200 if it's a duplicate of an existing link """

    msg = f"putExternalLink for group {group_id}, "
    msg += f"h5path: {h5path}, h5domain: {h5domain}"
    log.debug(msg)
    status = await putLink(app, group_id, title, h5path=h5path, h5domain=h5domain, bucket=bucket)
    return status


async def putLinks(app, group_id, items, bucket=None):
    """ create a new links.  Return 201 if any item is a new link,
    or 200 if it's a duplicate of an existing link. """

    isValidUuid(group_id, obj_class="groups")

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
            if "id" not in item:
                msg = "expected id key for hard link class"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            tgt_id = item["id"]
            try:
                isValidUuid(tgt_id)
            except ValueError:
                msg = f"invalid object id: {tgt_id}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

    # ready to add links now
    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links"
    log.debug(f"PUT links {len(items)} items - PUT request: {req}")
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
                raise HTTPBadRequest(reason=msg)

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
                raise HTTPBadRequest(reason=msg)

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


async def getTypeFromRequest(app, body, obj_id=None, bucket=None):
    """ return a type json from the request body """
    if "type" not in body:
        msg = "expected type in body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    datatype = body["type"]

    if isinstance(datatype, str) and datatype.startswith("t-"):
        # Committed type - fetch type json from DN
        ctype_id = datatype
        log.debug(f"got ctypeid: {ctype_id}")
        ctype_json = await getObjectJson(app, ctype_id, bucket=bucket)
        log.debug(f"ctype {ctype_id}: {ctype_json}")
        root_id = getRootObjId(obj_id)
        if ctype_json["root"] != root_id:
            msg = "Referenced committed datatype must belong in same domain"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        datatype = ctype_json["type"]
        # add the ctype_id to the type
        datatype["id"] = ctype_id
    elif isinstance(datatype, str):
        try:
            # convert predefined type string (e.g. "H5T_STD_I32LE") to
            # corresponding json representation
            datatype = getBaseTypeJson(datatype)
        except TypeError:
            msg = "PUT attribute with invalid predefined type"
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

    return datatype


def getShapeFromRequest(body):
    """ get shape json from request body """
    shape_json = {}
    if "shape" in body:
        shape_body = body["shape"]
        shape_class = None
        if isinstance(shape_body, dict) and "class" in shape_body:
            shape_class = shape_body["class"]
        elif isinstance(shape_body, str):
            shape_class = shape_body
        if shape_class:
            if shape_class == "H5S_NULL":
                shape_json["class"] = "H5S_NULL"
                if isinstance(shape_body, dict) and "dims" in shape_body:
                    msg = "can't include dims with null shape"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
                if isinstance(shape_body, dict) and "value" in body:
                    msg = "can't have H5S_NULL shape with value"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
            elif shape_class == "H5S_SCALAR":
                shape_json["class"] = "H5S_SCALAR"
                if "dims" in shape_body:
                    msg = "dimensions aren't valid for scalar attribute"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
            elif shape_class == "H5S_SIMPLE":
                shape_json["class"] = "H5S_SIMPLE"
                dims = getShapeDims(shape_body)
                shape_json["dims"] = dims
            else:
                msg = f"Unknown shape class: {shape_class}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
        else:
            # no class, interpret shape value as dimensions and
            # use H5S_SIMPLE as class
            if isinstance(shape_body, list) and len(shape_body) == 0:
                shape_json["class"] = "H5S_SCALAR"
            else:
                shape_json["class"] = "H5S_SIMPLE"
                dims = getShapeDims(shape_body)
                shape_json["dims"] = dims
    else:
        shape_json["class"] = "H5S_SCALAR"

    return shape_json


async def getAttributeFromRequest(app, req_json, obj_id=None, bucket=None):
    """ return attribute from given request json """
    attr_item = {}
    log.debug(f"getAttributeFromRequest req_json: {req_json} obj_id: {obj_id}")
    attr_type = await getTypeFromRequest(app, req_json, obj_id=obj_id, bucket=bucket)
    attr_shape = getShapeFromRequest(req_json)
    attr_item = {"type": attr_type, "shape": attr_shape}
    attr_value = getValueFromRequest(req_json, attr_type, attr_shape)
    if attr_value is not None:
        if isinstance(attr_value, bytes):
            attr_value = encodeData(attr_value)  # store as base64
            attr_item["encoding"] = "base64"
        else:
            # just store the JSON dict or primitive value
            attr_item["value"] = attr_value
    else:
        attr_item["value"] = None

    now = time.time()
    if "created" in req_json:
        created = req_json["created"]
        # allow "pre-dated" attributes if the timestamp is within the last 10 seconds
        predate_max_time = config.get("predate_max_time", default=10.0)
        if now - created < predate_max_time:
            attr_item["created"] = created
        else:
            msg = "stale created timestamp for attribute, ignoring "
            msg += f"predate config: {predate_max_time:6.2f} "
            msg += f"age: {(now - created):6.2f}"
            log.warn(msg)
    if "created" not in attr_item:
        attr_item["created"] = now

    return attr_item


async def getAttributesFromRequest(app, req_json, obj_id=None, bucket=None):
    """ read the given JSON dictionary and return dict of attribute json """

    attr_items = {}
    kwargs = {"obj_id": obj_id}
    if bucket:
        kwargs["bucket"] = bucket
    if "attributes" in req_json:
        attributes = req_json["attributes"]
        if not isinstance(attributes, dict):
            msg = f"expected list for attributes but got: {type(attributes)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        # read each attr_item and canonicalize the shape, type, verify value
        for attr_name in attributes:
            attr_json = attributes[attr_name]
            attr_item = await getAttributeFromRequest(app, attr_json, **kwargs)
            attr_items[attr_name] = attr_item
    else:
        log.debug(f"getAttributesFromRequest - no attribute defined in {req_json}")

    return attr_items


def getValueFromRequest(body, data_type, data_shape):
    """ Get attribute value from request json """
    dims = getShapeDims(data_shape)
    if "value" in body:
        if dims is None:
            msg = "Bad Request: data can not be included with H5S_NULL space"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        value = body["value"]
        # validate that the value agrees with type/shape
        arr_dtype = createDataType(data_type)  # np datatype
        if len(dims) == 0:
            np_dims = [1, ]
        else:
            np_dims = dims

        if "encoding" in body:
            encoding = body["encoding"]
            log.debug(f"using encoding: {encoding}")
            item_size = getItemSize(data_type)
            if item_size == "H5T_VARIABLE":
                msg = "base64 encoding is not support for variable length attributes"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            try:
                data = decodeData(value)
            except ValueError:
                msg = "unable to decode data"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

            expected_byte_count = arr_dtype.itemsize * np.prod(dims)
            if len(data) != expected_byte_count:
                msg = f"expected: {expected_byte_count} but got: {len(data)}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

            # check to see if this works with our shape and type
            try:
                arr = bytesToArray(data, arr_dtype, np_dims)
            except ValueError as e:
                log.debug(f"data: {data}")
                log.debug(f"type: {arr_dtype}")
                log.debug(f"np_dims: {np_dims}")
                msg = f"Bad Request: encoded input data doesn't match shape and type: {e}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

            value_json = None
            # now try converting to JSON
            list_data = arr.tolist()
            try:
                value_json = bytesArrayToList(list_data)
            except ValueError as err:
                msg = f"Cannot decode bytes to list: {err}, will store as encoded bytes"
                log.warn(msg)
            if value_json:
                log.debug("will store base64 input as json")
                if data_shape["class"] == "H5S_SCALAR":
                    # just use the scalar value
                    value = value_json[0]
                else:
                    value = value_json  # return this
            else:
                value = data  # return bytes to signal that this needs to be encoded
        else:
            # verify that the input data matches the array shape and type
            try:
                jsonToArray(np_dims, arr_dtype, value)
            except ValueError as e:
                msg = f"Bad Request: input data doesn't match selection: {e}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
    else:
        value = None

    return value


async def getAttributes(app, obj_id,
                        attr_names=None,
                        include_data=False,
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
    log.info(f"putAttributes: {req}")

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


async def deleteObject(app, obj_id, bucket=None):
    """ send delete request for group, datatype, or dataset obj """
    log.debug(f"deleteObject {obj_id}")
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


def validateDatasetCreationProps(creation_props, type_json=None, shape=None):
    """ validate creation props """

    log.debug(f"validateDatasetCreationProps: {creation_props}")
    if "fillValue" in creation_props:
        if not type_json or not shape:
            msg = "shape and type must be set to use fillValue"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        # validate fill value compatible with type
        dt = createDataType(type_json)
        fill_value = creation_props["fillValue"]
        log.debug(f"got fill_value: {fill_value}")
        if "fillValue_encoding" in creation_props:
            fill_value_encoding = creation_props["fillValue_encoding"]
            if fill_value_encoding not in ("None", "base64"):
                msg = f"unexpected value for fill_value_encoding: {fill_value_encoding}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            else:
                # should see a string in this case
                if not isinstance(fill_value, str):
                    msg = f"unexpected fill value: {fill_value} "
                    msg += f"for encoding: {fill_value_encoding}"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
        else:
            fill_value_encoding = None

            try:
                getNumpyValue(fill_value, dt=dt, encoding=fill_value_encoding)
            except ValueError:
                msg = f"invalid fill value: {fill_value}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

    if "filters" in creation_props:
        if not type_json or not shape:
            msg = "shape and type must be set to use filters"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        supported_filters = getSupportedFilters(include_compressors=True)
        log.debug(f"supported_filters: {supported_filters}")
        try:
            filters_out = getFiltersJson(creation_props, supported_filters=supported_filters)
        except (KeyError, ValueError):
            # raise bad request exception if not valid
            msg = "invalid filter provided"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        # replace filters with our standardized list
        log.debug(f"setting filters to: {filters_out}")
        creation_props["filters"] = filters_out


def getCreateArgs(body,
                  root_id=None,
                  bucket=None,
                  type=None,
                  implicit=False,
                  chunk_table=None,
                  ignore_link=False):
    """ get args for createObject from request body """

    log.debug(f"getCreateArgs with body keys: {list(body.keys())}")
    if ignore_link:
        log.debug("getCreateArgs, ignore_link is set")

    kwargs = {"bucket": bucket}
    predate_max_time = config.get("predate_max_time", default=10.0)

    parent_id = None
    obj_id = None
    h5path = None

    if "parent_id" not in body:
        parent_id = root_id
    else:
        parent_id = body["parent_id"]

    if "h5path" in body:
        h5path = body["h5path"]
        # normalize the h5path
        if h5path.startswith("/"):
            if parent_id == root_id:
                # just adjust the path to be relative
                h5path = h5path[1:]
            else:
                msg = f"PostCrawler expecting relative h5path, but got: {h5path}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

        if h5path.endswith("/"):
            h5path = h5path[:-1]  # makes iterating through the links a bit easier

    if "link" in body:
        if "h5path" in body:
            msg = "'link' key in body can't be used with h5path"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        # if ignore_link is set, parent_links will be created post object creation
        link_body = body["link"]
        log.debug(f"link_body: {link_body}")
        if "id" in link_body and not ignore_link:
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
            if not ignore_link:
                h5path = link_title  # just use the link name as the h5path
                log.debug(f"set h5path to {link_title}")

    if parent_id and h5path:
        # these are used by createObjectByPath
        kwargs["parent_id"] = parent_id
        kwargs["implicit"] = implicit
        kwargs["h5path"] = h5path
    else:
        kwargs["root_id"] = root_id

    if "id" in body:
        obj_id = body["id"]
        if not isValidUuid(obj_id):
            msg = f"Invalid id: {obj_id}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        kwargs["obj_id"] = obj_id
        log.debug(f"createObject will use client id: {obj_id}")

    if "creationProperties" in body:
        creation_props = body["creationProperties"]
        # validate after we've checked for shape and type
    else:
        creation_props = {}
    kwargs["creation_props"] = creation_props

    if "attributes" in body:
        attrs = body["attributes"]
        if not isinstance(attrs, dict):
            msg = f"expected dict for for attributes, but got: {type(attrs)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.debug(f"createObject attributes: {attrs}")

        # tbd: validate attributes
        kwargs["attrs"] = attrs

    if "links" in body:
        body_links = body["links"]
        log.debug(f"got links for new group: {body_links}")
        try:
            links = getRequestLinks(body["links"], predate_max_time=predate_max_time)
        except ValueError:
            msg = "invalid link item sent in request"
            raise HTTPBadRequest(reason=msg)
        log.debug(f"adding links to createObject request: {links}")
        kwargs["links"] = links

    if type:
        kwargs["type"] = type
        type_json = type
    elif "type" in body:
        type_json = body["type"]
        if isinstance(type_json, str):
            try:
                # convert predefined type string (e.g. "H5T_STD_I32LE") to
                # corresponding json representation
                type_json = getBaseTypeJson(type_json)
                log.debug(f"got type: {type_json}")
            except TypeError:
                msg = f"POST with invalid predefined type: {type_json}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
    else:
        type_json = None

    if type_json:
        try:
            validateTypeItem(type_json)
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
        kwargs["type"] = type_json
    else:
        pass  # no type
    return kwargs


def getDatasetCreateArgs(body,
                         root_id=None,
                         bucket=None,
                         type=None,
                         implicit=False,
                         chunk_table=None,
                         ignore_link=False):

    """ get args for createDataset from request body """

    # call getCreateArgs for group, datatype objects, then fill in for dataset specific options
    kwargs = getCreateArgs(body,
                           root_id=root_id,
                           bucket=bucket,
                           type=type,
                           implicit=implicit,
                           ignore_link=ignore_link)

    if "type" not in kwargs:
        msg = "no type specified for create dataset"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    type_json = kwargs["type"]
    #
    # Validate shape if present
    #

    # will return scalar shape if no shape key in body
    shape_json = getShapeJson(body)
    try:
        shape_class = getShapeClass(shape_json)
        shape_dims = getShapeDims(shape_json)
    except (KeyError, TypeError, ValueError):
        msg = f"Invalid shape: {shape_json}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    log.debug(f"got createArgs: {list(kwargs.keys())}")

    kwargs["shape"] = shape_json

    # get layout for dataset creation
    log.debug("getting dataset creation settings")
    layout_props = None
    min_chunk_size = int(config.get("min_chunk_size"))
    max_chunk_size = int(config.get("max_chunk_size"))
    type_json = kwargs["type"]
    item_size = getItemSize(type_json)
    if item_size == "H5T_VARIABLE":
        item_size = config.get("default_vlen_type_size", default=128)
    creation_props = kwargs["creation_props"]
    layout_props = None

    if creation_props:
        validateDatasetCreationProps(creation_props, type_json=type_json, shape=shape_json)
        if "layout" in creation_props:
            layout_props = creation_props["layout"]
            try:
                validateChunkLayout(shape_json, item_size, layout_props, chunk_table=chunk_table)
            except ValueError:
                msg = f"invalid chunk layout: {layout_props}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
    else:
        creation_props = {}

    layout_class = None
    chunk_dims = None
    if layout_props:
        layout_class = layout_props.get("class")

    if layout_class:
        if layout_class not in LAYOUT_CLASSES:
            msg = f"unknown layout_class: {layout_class}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        # check dims is defined for any chunked layout
        if layout_class.startswith("H5D_CHUNKED"):
            if "dims" not in layout_props:
                msg = "chunked layout specified without dims"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            chunk_dims = layout_props["dims"]
            if len(chunk_dims) != len(shape_dims):
                msg = "chunk dimensions have different rank than dataset"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
        elif layout_class == "H5D_CONTIGUOUS_REF" and getItemSize(type_json) == "H5T_VARIABLE":
            # ref dataset does not work with vlen type
            msg = "H5D_CONTIGUOUS_REF cannot be used with variable length types"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        else:
            pass

    elif shape_class == "H5S_NULL":
        layout_class = None
        log.debug("using None layout for H5S_NULL dataset")
    elif shape_class == "H5S_SCALAR":
        layout_class = "H5D_CONTIGUOUS"
        log.debug("Using H5D_CONTIGUOUS for H5S_SCALAR dataset")
    elif shape_class == "H5S_SIMPLE":
        dset_size = getDataSize(shape_dims, item_size)
        if dset_size <= min_chunk_size:
            # default to contiguous
            layout_class = "H5D_CONTIGUOUS"
            log.debug(f"Using H5D_CONTIGUOUS for small (<{min_chunk_size}) dataset")
        else:
            layout_class = "H5D_CHUNKED"
            log.debug(f"shape_json: {shape_json}")
            log.debug(f"item_size: {item_size}")
            log.debug(f"chunk_min: {min_chunk_size}")
            log.debug(f"chunk_max: {max_chunk_size}")
            kwargs = {"chunk_min": min_chunk_size, "chunk_max": max_chunk_size}
            chunk_dims = guessChunk(shape_json, item_size, **kwargs)
            log.debug(f"initial autochunk layout: {chunk_dims}")
            chunk_size = getChunkSize(chunk_dims, item_size)

            # log warning if the chunk shape if chunk size is too small or too big
            if chunk_size < min_chunk_size:
                msg = f"chunk size: {chunk_size} less than recommended min size: {min_chunk_size}"
                log.warn(msg)
            elif chunk_size > max_chunk_size:
                msg = f"chunk size: {chunk_size} greater than recommended "
                msg += f"max size: {max_chunk_size}"
                log.debug(msg)
    else:
        msg = f"unexpected shape_class: {shape_class}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if not layout_props:
        layout_props = {"class": layout_class}
    if chunk_dims:
        layout_props["dims"] = chunk_dims
    log.debug(f"using dataset layout: {layout_props}")
    creation_props["layout"] = layout_props

    # set partition_count if needed:
    max_chunks_per_folder = int(config.get("max_chunks_per_folder"))
    set_partition = False
    if max_chunks_per_folder > 0:
        if "dims" in layout_props:
            set_partition = True

    if set_partition:
        log.debug(f"updating layout for partition constraint: {max_chunks_per_folder}")
        shape_dims = getShapeDims(shape_json)
        max_dims = getDsetMaxDims(shape_json)

        num_chunks = 1
        rank = len(shape_dims)
        unlimited_count = 0
        if max_dims:
            for i in range(rank):
                if max_dims[i] == 0:
                    unlimited_count += 1
            msg = f"number of unlimited dimensions: {unlimited_count}"
            log.debug(msg)

        for i in range(rank):
            max_dim = 1
            if max_dims:
                max_dim = max_dims[i]
                if max_dim == 0:
                    # don't really know what the ultimate extent
                    # could be, but assume 10^6 for total number of
                    # elements and square-shaped array...
                    MAX_ELEMENT_GUESS = 10.0 ** 6
                    exp = 1 / unlimited_count
                    max_dim = int(math.pow(MAX_ELEMENT_GUESS, exp))
            else:
                max_dim = shape_dims[i]
            num_chunks *= math.ceil(max_dim / chunk_dims[i])

        if num_chunks > max_chunks_per_folder:
            partition_count = math.ceil(num_chunks / max_chunks_per_folder)
            msg = f"set partition count to: {partition_count}, "
            msg += f"num_chunks: {num_chunks}"
            log.info(msg)
            layout_props["partition_count"] = partition_count
        else:
            msg = "do not need chunk partitions, num_chunks: "
            msg += f"{num_chunks} max_chunks_per_folder: "
            msg += f"{max_chunks_per_folder}"
            log.info(msg)

    if layout_class in ("H5D_CHUNKED_REF", "H5D_CHUNKED_REF_INDIRECT"):
        chunk_size = getChunkSize(chunk_dims, item_size)

        msg = f"chunk_size: {chunk_size}, min: {min_chunk_size}, "
        msg += f"max: {max_chunk_size}"
        log.debug(msg)
        # nothing to do about inefficiently small chunks, but large chunks
        # can be subdivided
        if chunk_size < min_chunk_size:
            msg = f"chunk size: {chunk_size} less than min size: "
            msg += f"{min_chunk_size} for {layout_class} dataset"
            log.warn(msg)
        elif chunk_size > max_chunk_size:
            msg = f"chunk size: {chunk_size} greater than max size: "
            msg += f"{max_chunk_size}, for {layout_class} dataset"
            log.warn(msg)
        layout_props["dims"] = chunk_dims

    creation_props["layout"] = layout_props
    kwargs["creation_props"] = creation_props

    #
    # get input data if present
    #
    if "value" in body and body["value"]:
        # data to initialize dataset included in request
        if shape_json["class"] == "H5S_NULL":
            msg = "null shape datasets can not have initial values"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        input_data = body["value"]
        msg = "input data doesn't match request type and shape"
        dims = getShapeDims(shape_json)
        if not dims:
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        arr_dtype = createDataType(type_json)

        try:
            input_arr = jsonToArray(dims, arr_dtype, input_data)
        except ValueError:
            log.warn(f"ValueError: {msg}")
            raise HTTPBadRequest(reason=msg)
        except TypeError:
            log.warn(f"TypeError: {msg}")
            raise HTTPBadRequest(reason=msg)
        except IndexError:
            log.warn(f"IndexError: {msg}")
            raise HTTPBadRequest(reason=msg)
        log.debug(f"got json arr: {input_arr.shape}")
        kwargs["value"] = input_data

    return kwargs


async def createLinkFromParent(app, parent_id, h5path, tgt_id=None, bucket=None, implicit=False):
    """ create link or links from parentId to tgt_id.
        If implicit is True, create any intermediate group objects needed """

    if not h5path:
        log.warn("createLinkFromParent with null h5path")
        return
    log.info(f"createLinkFromParent, parent_id: {parent_id} h5path: {h5path} tgt_id={tgt_id}")
    if implicit:
        log.debug("createLinkFromParent - using implicit creation")
    link_titles = h5path.split("/")
    log.debug(f"link_titles: {link_titles}")
    for i in range(len(link_titles)):
        if i == len(link_titles) - 1:
            last_link = True
        else:
            last_link = False
        link_title = link_titles[i]
        log.debug(f"createLinkFromParent - processing link: {link_title}")
        link_json = None
        try:
            link_json = await getLink(app, parent_id, link_title, bucket=bucket)
        except (HTTPNotFound, HTTPGone):
            pass  # link doesn't exist

        if link_json:
            log.debug(f"link for link_title {link_title} found: {link_json}")
            # if this is the last link, that's a problem
            if last_link:
                msg = f"object at {h5path} already exists"
                log.warn(msg)
                raise HTTPConflict()
            # otherwise, verify that this is a hardlink
            if link_json.get("class") != "H5L_TYPE_HARD":
                msg = "createLinkFromParent - h5path must contain only hard links"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            parent_id = link_json["id"]
            if getCollectionForId(parent_id) != "groups":
                # parent objects must be groups!
                msg = f"{link_title} is not a group"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            else:
                log.debug(f"link: {link_title} to sub-group found")
        else:
            log.debug(f"link for link_title {link_title} not found")
            if last_link:
                # create a link to the new object
                await putHardLink(app, parent_id, link_title, tgt_id=tgt_id, bucket=bucket)
                parent_id = tgt_id  # new parent
            elif implicit:
                # create a new group object
                log.info(f"creating intermediate group object for: {link_title}")
                kwargs = {"parent_id": parent_id, "bucket": bucket}
                grp_id = createObjId("groups", root_id=getRootObjId(parent_id))
                kwargs["obj_id"] = grp_id
                # createObject won't call back to this function since we haven't set the h5path
                await createObject(app, **kwargs)
                # create a link to the subgroup
                await putHardLink(app, parent_id, link_title, tgt_id=grp_id, bucket=bucket)
                parent_id = grp_id  # new parent
            else:
                if len(link_titles) > 1:
                    msg = f"createLinkFromParent failed: not all groups in {h5path} exist"
                else:
                    msg = f"createLinkFromParent failed: {h5path} does not exist"
                log.warn(msg)
                raise HTTPNotFound(reason=msg)


async def createObject(app,
                       parent_id=None,
                       root_id=None,
                       h5path=None,
                       obj_id=None,
                       type=None,
                       shape=None,
                       creation_props=None,
                       attrs=None,
                       links=None,
                       implicit=None,
                       bucket=None):
    """ create a group, ctype, or dataset object and return object json
        Determination on whether a group, ctype, or dataset is created is based on:
            1) if type and shape are set, a dataset object will be created
            2) if type is set but not shape, a  datatype object will be created
            3) otherwise (type and shape are both None), a group object will be created
        The layout parameter only applies to dataset creation
    """
    if type and shape:
        collection = "datasets"
    elif type:
        collection = "datatypes"
    else:
        collection = "groups"

    if not root_id:
        root_id = getRootObjId(parent_id)
    log.info(f"createObject for {collection} collection, root_id: {root_id}, bucket: {bucket}")
    if root_id != parent_id:
        log.debug(f"    parent_id: {parent_id}")
    if obj_id:
        log.debug(f"    obj_id: {obj_id}")
    if h5path:
        log.debug(f"    h5path: {h5path}")
    if type:
        log.debug(f"    type: {type}")
    if shape:
        log.debug(f"    shape: {shape}")
    if creation_props:
        log.debug(f"    cprops: {creation_props}")
    if attrs:
        log.debug(f"    attrs: {attrs}")
    if links:
        log.debug(f"    links: {links}")

    if h5path:
        if h5path.startswith("/"):
            if parent_id == root_id:
                # just adjust the path to be relative
                h5path = h5path[1:]
            else:
                msg = f"createObject expecting relative h5path, but got: {h5path}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

        if h5path.endswith("/"):
            h5path = h5path[:-1]  # makes iterating through the links a bit easier

    if obj_id:
        log.debug(f"using client supplied id: {obj_id}")
        if not isValidUuid(obj_id, obj_class=collection):
            msg = f"invalid id: {obj_id}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if getRootObjId(obj_id) != root_id:
            msg = f"id: {obj_id} is not valid for root: {root_id}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        obj_id = createObjId(collection, root_id=root_id)
        log.info(f"new obj id: {obj_id}")
    obj_json = {"id": obj_id, "root": root_id}
    if type:
        obj_json["type"] = type
    if shape:
        obj_json["shape"] = shape
    if creation_props:
        obj_json["creationProperties"] = creation_props
    else:
        obj_json["creationProperties"] = {}
    if attrs:
        kwargs = {"obj_id": obj_id, "bucket": bucket}
        attrs_json = {"attributes": attrs}
        attr_items = await getAttributesFromRequest(app, attrs_json, **kwargs)
        log.debug(f"got attr_items: {attr_items}")
        obj_json["attributes"] = attr_items
    if links:
        if collection != "groups":
            msg = "links can only be used with groups"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        obj_json["links"] = links
    log.debug(f"create {collection} obj, body: {obj_json}")
    dn_url = getDataNodeUrl(app, obj_id)
    req = f"{dn_url}/{collection}"
    params = {"bucket": bucket}
    rsp_json = await http_post(app, req, data=obj_json, params=params)

    log.debug(f"createObject: {req} got rsp_json: {rsp_json}")

    # object creation successful, create link from parent if requested
    if h5path:
        kwargs = {"tgt_id": obj_id, "bucket": bucket, "implicit": implicit}
        await createLinkFromParent(app, parent_id, h5path, **kwargs)

    return rsp_json


async def createGroup(app,
                      parent_id=None,
                      root_id=None,
                      h5path=None,
                      obj_id=None,
                      creation_props=None,
                      attrs=None,
                      links=None,
                      implicit=None,
                      bucket=None):

    """ create a new group object """

    kwargs = {}
    kwargs["parent_id"] = parent_id
    kwargs["root_id"] = root_id
    kwargs["h5path"] = h5path
    kwargs["obj_id"] = obj_id
    kwargs["creation_props"] = creation_props
    kwargs["attrs"] = attrs
    kwargs["links"] = links
    kwargs["implicit"] = implicit
    kwargs["bucket"] = bucket
    rsp_json = await createObject(app, **kwargs)
    return rsp_json


async def createDatatypeObj(app,
                            parent_id=None,
                            root_id=None,
                            type=None,
                            h5path=None,
                            obj_id=None,
                            creation_props=None,
                            attrs=None,
                            links=None,
                            implicit=None,
                            bucket=None):

    """ create a new committed type object"""

    if not type:
        msg = "type not set for committed type creation"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    kwargs = {}
    kwargs["parent_id"] = parent_id
    kwargs["root_id"] = root_id
    kwargs["type"] = type
    kwargs["h5path"] = h5path
    kwargs["obj_id"] = obj_id
    kwargs["creation_props"] = creation_props
    kwargs["attrs"] = attrs
    kwargs["links"] = links
    kwargs["implicit"] = implicit
    kwargs["bucket"] = bucket
    rsp_json = await createObject(app, **kwargs)
    return rsp_json


async def createDataset(app,
                        parent_id=None,
                        root_id=None,
                        type=None,
                        shape=None,
                        h5path=None,
                        obj_id=None,
                        creation_props=None,
                        attrs=None,
                        links=None,
                        implicit=None,
                        bucket=None):

    """ create a new dataset object"""

    if not type:
        msg = "type not set for dataset creation"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if not shape:
        # default to a scalar dataset
        shape = {"class": "H5S_SCALAR"}

    kwargs = {}
    kwargs["parent_id"] = parent_id
    kwargs["root_id"] = root_id
    kwargs["type"] = type
    kwargs["shape"] = shape
    kwargs["h5path"] = h5path
    kwargs["obj_id"] = obj_id
    kwargs["creation_props"] = creation_props
    kwargs["attrs"] = attrs
    kwargs["links"] = links
    kwargs["implicit"] = implicit
    kwargs["bucket"] = bucket
    dset_json = await createObject(app, **kwargs)

    return dset_json
