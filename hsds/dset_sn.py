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
# handles dataset requests
#

from json import JSONDecodeError
from aiohttp.web_exceptions import HTTPBadRequest, HTTPNotFound, HTTPInternalServerError

from h5json.hdf5dtype import createDataType
from h5json.array_util import getNumElements, jsonToArray
from h5json.objid import isValidUuid, isSchema2Id

from .util.httpUtil import getHref, respJsonAssemble
from .util.httpUtil import jsonResponse, getBooleanParam
from .util.dsetUtil import getPreviewQuery, getShapeDims
from .util.authUtil import getUserPasswordFromRequest, aclCheck
from .util.authUtil import validateUserPassword
from .util.domainUtil import getDomainFromRequest, getPathForDomain, isValidDomain
from .util.domainUtil import getBucketForDomain, verifyRoot
from .servicenode_lib import getDomainJson, getObjectJson, getDsetJson, getPathForObjectId
from .servicenode_lib import getObjectIdByPath, validateAction, getRootInfo
from .servicenode_lib import getDatasetCreateArgs, createDataset, deleteObject
from .dset_lib import updateShape, deleteAllChunks, doHyperslabWrite
from .post_crawl import createDatasets
from .domain_crawl import DomainCrawler
from . import hsds_logger as log


async def getDatasetDetails(app, dset_id, root_id, bucket=None):
    """Get extra information about the given dataset"""
    # Gather additional info on the domain
    log.debug(f"getDatasetDetails {dset_id}")

    if not isSchema2Id(root_id):
        msg = f"no dataset details not available for schema v1 id: {root_id}"
        msg += "returning null result"
        log.info(msg)
        return None

    root_info = await getRootInfo(app, root_id, bucket=bucket)
    if not root_info:
        log.warn(f"info.json not found for root: {root_id}")
        return None

    if "datasets" not in root_info:
        log.error("datasets key not found in root_info")
        return None
    datasets = root_info["datasets"]
    if dset_id not in datasets:
        log.warn(f"dataset id: {dset_id} not found in root_info")
        return None

    log.debug(f"returning datasetDetails: {datasets[dset_id]}")

    return datasets[dset_id]


async def GET_Dataset(request):
    """HTTP method to return JSON description of a dataset"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    include_attrs = False

    h5path = None
    getAlias = False
    dset_id = request.match_info.get("id")
    if not dset_id and "h5path" not in params:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if "include_attrs" in params and params["include_attrs"]:
        include_attrs = True

    if dset_id:
        if not isValidUuid(dset_id, "Dataset"):
            msg = f"Invalid dataset id: {dset_id}"
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
                msg = f"Invalid parent group id: {group_id}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
        if "h5path" not in params:
            msg = "Expecting either ctype id or h5path url param"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        h5path = params["h5path"]
        if not group_id and h5path[0] != "/":
            msg = "h5paths must be absolute"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        msg = f"GET_Dataset, h5path: {h5path}"
        if group_id:
            msg += f" group_id: {group_id}"
        log.info(msg)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app["allow_noauth"]:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    verbose = False
    if "verbose" in params and params["verbose"]:
        verbose = True

    if h5path:
        if group_id is None:
            domain_json = await getDomainJson(app, domain)
            verifyRoot(domain_json)
            group_id = domain_json["root"]
        # throws 404 if not found
        kwargs = {"bucket": bucket, "domain": domain}
        dset_id, domain, _ = await getObjectIdByPath(app, group_id, h5path, **kwargs)
        if not isValidUuid(dset_id, "Dataset"):
            msg = f"No dataset exist with the path: {h5path}"
            log.warn(msg)
            raise HTTPNotFound()
        log.info(f"get dataset_id: {dset_id} from h5path: {h5path}")

    # get authoritative state for dataset from DN (even if it's
    # in the meta_cache).
    kwargs = {"refresh": True, "include_attrs": include_attrs, "bucket": bucket}
    dset_json = await getDsetJson(app, dset_id, **kwargs)

    # check that we have permissions to read the object
    await validateAction(app, domain, dset_id, username, "read")

    dset_json = respJsonAssemble(dset_json, params, dset_id)

    dset_json["domain"] = getPathForDomain(domain)

    if getAlias:
        root_id = dset_json["root"]
        alias = []
        idpath_map = {root_id: "/"}
        h5path = await getPathForObjectId(
            app, root_id, idpath_map, tgt_id=dset_id, bucket=bucket
        )
        if h5path:
            alias.append(h5path)
        dset_json["alias"] = alias

    hrefs = []
    dset_uri = "/datasets/" + dset_id
    hrefs.append({"rel": "self", "href": getHref(request, dset_uri)})
    root_uri = "/groups/" + dset_json["root"]
    hrefs.append({"rel": "root", "href": getHref(request, root_uri)})
    hrefs.append({"rel": "home", "href": getHref(request, "/")})
    href = getHref(request, dset_uri + "/attributes")
    hrefs.append({"rel": "attributes", "href": href})

    # provide a value link if the dataset is relatively small,
    # otherwise create a preview link that shows a limited number of
    # data values
    dset_shape = dset_json["shape"]
    if dset_shape["class"] != "H5S_NULL":
        count = 1
        if dset_shape["class"] == "H5S_SIMPLE":
            dims = dset_shape["dims"]
            count = getNumElements(dims)
        if count <= 100:
            # small number of values, provide link to entire dataset
            href = getHref(request, dset_uri + "/value")
            hrefs.append({"rel": "data", "href": href})
        else:
            # large number of values, create preview link
            previewQuery = getPreviewQuery(dset_shape["dims"])
            kwargs = {"query": previewQuery}
            href = getHref(request, dset_uri + "/value", **kwargs)
            hrefs.append({"rel": "preview", "href": href})

    dset_json["hrefs"] = hrefs

    if verbose:
        # get allocated size and num_chunks for the dataset if available
        dset_detail = await getDatasetDetails(
            app, dset_id, dset_json["root"], bucket=bucket
        )
        if dset_detail is not None:
            if "num_chunks" in dset_detail:
                dset_json["num_chunks"] = dset_detail["num_chunks"]
            if "allocated_bytes" in dset_detail:
                dset_json["allocated_size"] = dset_detail["allocated_bytes"]
            if "lastModified" in dset_detail:
                dset_json["lastModified"] = dset_detail["lastModified"]

    resp = await jsonResponse(request, dset_json)
    log.response(request, resp=resp)
    return resp


async def GET_DatasetType(request):
    """HTTP method to return JSON for dataset's type"""
    log.request(request)
    app = request.app

    dset_id = request.match_info.get("id")
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = f"Invalid dataset id: {dset_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app["allow_noauth"]:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    # get authoritative state for group from DN (even if it's in
    # the meta_cache).
    dset_json = await getDsetJson(app, dset_id, refresh=True, bucket=bucket)

    await validateAction(app, domain, dset_id, username, "read")

    hrefs = []
    dset_uri = "/datasets/" + dset_id
    self_uri = dset_uri + "/type"
    hrefs.append({"rel": "self", "href": getHref(request, self_uri)})
    dset_uri = "/datasets/" + dset_id
    hrefs.append({"rel": "owner", "href": getHref(request, dset_uri)})
    root_uri = "/groups/" + dset_json["root"]
    hrefs.append({"rel": "root", "href": getHref(request, root_uri)})

    resp_json = {}
    resp_json["type"] = dset_json["type"]
    resp_json["hrefs"] = hrefs

    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp


async def GET_DatasetShape(request):
    """HTTP method to return JSON for dataset's shape"""
    log.request(request)
    app = request.app

    dset_id = request.match_info.get("id")
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = f"Invalid dataset id: {dset_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app["allow_noauth"]:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    # get authoritative state for dataset from DN (even if it's in
    # the meta_cache).
    dset_json = await getDsetJson(app, dset_id, refresh=True, bucket=bucket)

    await validateAction(app, domain, dset_id, username, "read")

    hrefs = []
    dset_uri = "/datasets/" + dset_id
    self_uri = dset_uri + "/shape"
    hrefs.append({"rel": "self", "href": getHref(request, self_uri)})
    dset_uri = "/datasets/" + dset_id
    hrefs.append({"rel": "owner", "href": getHref(request, dset_uri)})
    root_uri = "/groups/" + dset_json["root"]
    hrefs.append({"rel": "root", "href": getHref(request, root_uri)})

    resp_json = {}
    resp_json["shape"] = dset_json["shape"]
    resp_json["hrefs"] = hrefs
    resp_json["created"] = dset_json["created"]
    resp_json["lastModified"] = dset_json["lastModified"]

    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp


async def PUT_DatasetShape(request):
    """HTTP method to update dataset's shape"""
    log.request(request)
    app = request.app
    shape_update = None
    extend = 0
    extend_dim = 0

    dset_id = request.match_info.get("id")
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = f"Invalid dataset id: {dset_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    username, pswd = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)

    # validate request
    if not request.has_body:
        msg = "PUT shape with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    try:
        data = await request.json()
    except JSONDecodeError:
        msg = "Unable to load JSON body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if "shape" not in data and "extend" not in data:
        msg = "PUT shape has no shape or extend key in body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if "shape" in data and "extend" in data:
        msg = "PUT shape must have shape or extend key in body but not both"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if "shape" in data:
        shape_update = data["shape"]
        if isinstance(shape_update, int):
            # convert to a list
            shape_update = [shape_update, ]
        log.debug(f"shape_update: {shape_update}")

    if "extend" in data:
        try:
            extend = int(data["extend"])
        except ValueError:
            msg = "extend value must be integer"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if extend <= 0:
            msg = "extend value must be positive"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if "extend_dim" in data:
            try:
                extend_dim = int(data["extend_dim"])
            except ValueError:
                msg = "extend_dim value must be integer"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            if extend_dim < 0:
                msg = "extend_dim value must be non-negative"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    # verify the user has permission to update shape
    await validateAction(app, domain, dset_id, username, "update")

    dset_json = await getDsetJson(app, dset_id, bucket=bucket)

    shape_orig = dset_json["shape"]
    log.debug(f"shape_orig: {shape_orig}")

    # verify that the extend request is valid
    if shape_orig["class"] != "H5S_SIMPLE":
        msg = "Unable to extend shape of datasets who are not H5S_SIMPLE"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if "maxdims" not in shape_orig:
        msg = "Dataset is not extensible"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    dims = shape_orig["dims"]
    rank = len(dims)

    if shape_update and len(shape_update) != rank:
        msg = "Extent of update shape request does not match dataset sahpe"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if extend_dim < 0 or extend_dim >= rank:
        msg = "Extension dimension must be less than rank and non-negative"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if shape_update is None:
        # construct a shape update using original dims and extend dim and value
        shape_update = dims.copy()
        shape_update[extend_dim] += extend

    selection = await updateShape(app, dset_json, shape_update, bucket=bucket)

    json_resp = {}
    if selection:
        json_resp["selection"] = selection

    resp = await jsonResponse(request, json_resp, status=201)
    log.response(request, resp=resp)
    return resp


async def POST_Dataset(request):
    """HTTP method to create a new dataset object"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    await validateUserPassword(app, username, pswd)

    if not request.has_body:
        msg = "POST Datasets with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    try:
        body = await request.json()
    except JSONDecodeError:
        msg = "Unable to load JSON body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    log.debug(f"got body: {body}")
    # get domain, check authorization
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    domain_json = await getDomainJson(app, domain, reload=True)
    log.debug(f"got domain_json: {domain_json}")
    root_id = domain_json["root"]

    # throws exception if not allowed
    aclCheck(app, domain_json, "create", username)

    verifyRoot(domain_json)

    # allow parent group creation or not
    implicit = getBooleanParam(params, "implicit")

    post_rsp = None

    datatype_json = None
    init_values = []    # value initializer for each object

    def _updateInitValuesList(kwargs):
        # remove value key from kwargs and append
        # to init_values list
        if "value" in kwargs:
            init_values.append(kwargs["value"])
            del kwargs["value"]
        else:
            # add a placeholder
            init_values.append(None)

    #
    # handle case of committed type input
    #
    if isinstance(body, dict) and "type" in body:

        body_type = body["type"]
        log.debug(f"got datatype: {body_type}")
        if isinstance(body_type, str) and body_type.startswith("t-"):
            ctype_id = body_type
            # Committed type - fetch type json from DN
            log.debug(f"got ctype_id: {ctype_id}")
            ctype_json = await getObjectJson(app, ctype_id, bucket=bucket)
            log.debug(f"ctype: {ctype_json}")
            if ctype_json["root"] != root_id:
                msg = "Referenced committed datatype must belong in same domain"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            datatype_json = ctype_json["type"]
            # add the ctype_id to type type
            datatype_json["id"] = ctype_id
        else:
            pass  # we'll fetch type in getDatasetCreateArgs

    if isinstance(body, list):
        count = len(body)
        log.debug(f"multiple dataset create: {count} items")
        if count == 0:
            # equivalent to no body
            msg = "POST Dataset with no body"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        elif count == 1:
            # just create one object in typical way
            kwargs = getDatasetCreateArgs(body[0],
                                          root_id=root_id,
                                          type=datatype_json,
                                          bucket=bucket,
                                          implicit=implicit)
            _updateInitValuesList(kwargs)
        else:
            # create multiple dataset objects
            kwarg_list = []  # list of kwargs for each object

            for item in body:
                log.debug(f"item: {item}")
                if not isinstance(item, dict):
                    msg = f"Post_Dataset - invalid item type: {type(item)}"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
                kwargs = getDatasetCreateArgs(item,
                                              root_id=root_id,
                                              type=datatype_json,
                                              bucket=bucket)
                _updateInitValuesList(kwargs)
                kwargs["ignore_link"] = True
                kwarg_list.append(kwargs)
            kwargs = {"bucket": bucket, "root_id": root_id}
            if datatype_json:
                kwargs["type"] = datatype_json
            log.debug(f"createDatasetObjects, items: {kwarg_list}")
            post_rsp = await createDatasets(app, kwarg_list, **kwargs)
    else:
        # single object create
        kwargs = getDatasetCreateArgs(body,
                                      root_id=root_id,
                                      type=datatype_json,
                                      bucket=bucket,
                                      implicit=implicit)
        _updateInitValuesList(kwargs)
        log.debug(f"kwargs for dataset create: {kwargs}")

    if post_rsp is None:
        # Handle cases other than multi ctype create here
        post_rsp = await createDataset(app, **kwargs)

    log.debug(f"returning resp: {post_rsp}")

    if "objects" in post_rsp:
        # add any links in multi request
        objects = post_rsp["objects"]
        obj_count = len(objects)
        log.debug(f"Post dataset multi create: {obj_count} objects")
        if len(body) != obj_count:
            msg = f"Expected {obj_count} objects but got {len(body)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        obj_count = 1  # single object create
        objects = [post_rsp, ]  # treat as an array to make the following code more consistent

    if len(init_values) != obj_count:
        msg = f"Expected {obj_count} init values"
        log.error(msg)
        raise HTTPInternalServerError()

    # write any init data values
    for index in range(obj_count):
        init_data = init_values[index]
        if init_data is None:
            continue
        dset_json = objects[index]
        log.debug(f"init value, post_rsp: {dset_json}")
        shape_json = dset_json["shape"]
        type_json = dset_json["type"]
        arr_dtype = createDataType(type_json)
        dims = getShapeDims(shape_json)
        try:
            input_arr = jsonToArray(dims, arr_dtype, init_data)
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

        # write data if provided
        log.debug(f"write input_arr: {input_arr}")
        # make selection for entire dataspace
        dims = getShapeDims(shape_json)
        slices = []
        for dim in dims:
            s = slice(0, dim, 1)
            slices.append(s)
        # make a one page list to handle the write in one chunk crawler run
        # (larger write request should user binary streaming)
        kwargs = {"page_number": 0, "page": slices}
        kwargs["dset_json"] = dset_json
        kwargs["bucket"] = bucket
        kwargs["select_dtype"] = input_arr.dtype
        kwargs["data"] = input_arr
        # do write
        await doHyperslabWrite(app, request, **kwargs)

    if "objects" in post_rsp:
        # add any links in multi request
        objects = post_rsp["objects"]
        obj_count = len(objects)
        log.debug(f"Post datatype multi create: {obj_count} objects")
        if len(body) != obj_count:
            msg = f"Expected {obj_count} objects but got {len(body)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        parent_ids = {}
        for index in range(obj_count):
            item = body[index]
            if "link" in item:
                link_item = item["link"]
                parent_id = link_item.get("id")
                title = link_item.get("name")
                if parent_id and title:
                    # add a hard link
                    object = objects[index]
                    obj_id = object["id"]
                    if parent_id not in parent_ids:
                        parent_ids[parent_id] = {}
                    links = parent_ids[parent_id]
                    links[title] = {"id": obj_id}
        if parent_ids:
            log.debug(f"POST dataset multi - adding links: {parent_ids}")
            kwargs = {"action": "put_link", "bucket": bucket}
            kwargs["replace"] = True

            crawler = DomainCrawler(app, parent_ids, **kwargs)

            # will raise exception on not found, server busy, etc.
            await crawler.crawl()
            status = crawler.get_status()

            log.info(f"DomainCrawler done for put_links action, status: {status}")

    # dataset creation successful
    resp = await jsonResponse(request, post_rsp, status=201)
    log.response(request, resp=resp)

    return resp


async def DELETE_Dataset(request):
    """HTTP method to delete a dataset resource"""
    log.request(request)
    app = request.app

    dset_id = request.match_info.get("id")
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = f"Invalid dataset id: {dset_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    username, pswd = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid oomain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    verifyRoot(domain_json)

    # check authority to do a delete
    await validateAction(app, domain, dset_id, username, "delete")

    # free any allocated chunks
    await deleteAllChunks(app, dset_id, bucket=bucket)

    # delete the dataset object
    await deleteObject(app, dset_id, bucket=bucket)

    resp = await jsonResponse(request, {})
    log.response(request, resp=resp)
    return resp
