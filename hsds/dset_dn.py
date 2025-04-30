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
# data node of hsds cluster
#

from aiohttp.web_exceptions import HTTPBadRequest, HTTPNotFound, HTTPConflict
from aiohttp.web_exceptions import HTTPInternalServerError
from aiohttp.web import json_response

from h5json.objid import isValidUuid, validateUuid

from .util.domainUtil import isValidBucketName
from .util.timeUtil import getNow
from .datanode_lib import get_obj_id, check_metadata_obj, get_metadata_obj
from .datanode_lib import save_metadata_obj, delete_metadata_obj
from . import hsds_logger as log


async def GET_Dataset(request):
    """HTTP GET method to return JSON for /groups/"""
    log.request(request)
    app = request.app
    params = request.rel_url.query
    dset_id = get_obj_id(request)

    if not isValidUuid(dset_id, obj_class="datasets"):
        log.error(f"Unexpected dataset_id: {dset_id}")
        raise HTTPInternalServerError()
    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = None

    if not isValidBucketName(bucket):
        msg = f"Invalid bucket name: {bucket}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    dset_json = await get_metadata_obj(app, dset_id, bucket=bucket)

    resp_json = {}
    resp_json["id"] = dset_json["id"]
    resp_json["root"] = dset_json["root"]
    resp_json["created"] = dset_json["created"]
    resp_json["lastModified"] = dset_json["lastModified"]
    resp_json["type"] = dset_json["type"]
    resp_json["shape"] = dset_json["shape"]
    resp_json["attributeCount"] = len(dset_json["attributes"])
    if "creationProperties" in dset_json:
        resp_json["creationProperties"] = dset_json["creationProperties"]
    if "layout" in dset_json:
        resp_json["layout"] = dset_json["layout"]
    if "include_attrs" in params and params["include_attrs"]:
        resp_json["attributes"] = dset_json["attributes"]

    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp


async def POST_Dataset(request):
    """Handler for POST /datasets"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    if not request.has_body:
        msg = "POST_Dataset with no body"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    body = await request.json()
    log.info(f"POST_Dataset, body: {body}")
    if "bucket" in params:
        bucket = params["bucket"]
    elif "bucket" in body:
        bucket = params["bucket"]
    else:
        bucket = None

    if not isValidBucketName(bucket):
        msg = f"Invalid bucket name: {bucket}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    dset_id = get_obj_id(request, body=body)
    if not isValidUuid(dset_id, obj_class="datasets"):
        log.error(f"Unexpected dataset_id: {dset_id}")
        raise HTTPInternalServerError()

    # verify the id doesn't already exist
    obj_found = await check_metadata_obj(app, dset_id, bucket=bucket)
    if obj_found:
        msg = f"Post with existing dset_id: {dset_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if "root" not in body:
        msg = "POST_Dataset with no root"
        log.error(msg)
        raise HTTPInternalServerError()
    root_id = body["root"]
    try:
        validateUuid(root_id, "group")
    except ValueError:
        msg = "Invalid root_id: " + root_id
        log.error(msg)
        raise HTTPInternalServerError()

    if "type" not in body:
        msg = "POST_Dataset with no type"
        log.error(msg)
        raise HTTPInternalServerError()
    type_json = body["type"]
    if "shape" not in body:
        msg = "POST_Dataset with no shape"
        log.error(msg)
        raise HTTPInternalServerError()
    shape_json = body["shape"]

    layout = None
    if "layout" in body:
        layout = body["layout"]  # client specified chunk layout

    # ok - all set, create committed type obj
    now = getNow(app)

    log.debug(f"POST_dataset typejson: {type_json}, shapejson: {shape_json}")

    dset_json = {
        "id": dset_id,
        "root": root_id,
        "created": now,
        "lastModified": now,
        "type": type_json,
        "shape": shape_json,
        "attributes": {},
    }

    if "creationProperties" in body:
        dset_json["creationProperties"] = body["creationProperties"]
    if layout is not None:
        dset_json["layout"] = layout

    kwargs = {"bucket": bucket, "notify": True, "flush": True}
    await save_metadata_obj(app, dset_id, dset_json, **kwargs)

    resp_json = {}
    resp_json["id"] = dset_id
    resp_json["root"] = root_id
    resp_json["created"] = dset_json["created"]
    resp_json["type"] = type_json
    resp_json["shape"] = shape_json
    resp_json["lastModified"] = dset_json["lastModified"]
    resp_json["attributeCount"] = 0

    resp = json_response(resp_json, status=201)
    log.response(request, resp=resp)
    return resp


async def DELETE_Dataset(request):
    """HTTP DELETE method for dataset"""
    log.request(request)
    app = request.app
    params = request.rel_url.query
    dset_id = request.match_info.get("id")
    log.info(f"DELETE dataset: {dset_id}")

    if not isValidUuid(dset_id, obj_class="datasets"):
        log.error(f"Unexpected dataset id: {dset_id}")
        raise HTTPInternalServerError()

    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = app["bucket_name"]

    if not bucket:
        msg = "DELETE_Dataset - bucket not set"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    elif not isValidBucketName(bucket):
        msg = f"Invalid bucket name: {bucket}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    # verify the id  exist
    obj_found = await check_metadata_obj(app, dset_id, bucket=bucket)
    if not obj_found:
        raise HTTPNotFound()

    log.debug(f"deleting dataset: {dset_id}")

    notify = True
    if "Notify" in params and not params["Notify"]:
        notify = False
    await delete_metadata_obj(app, dset_id, bucket=bucket, notify=notify)

    resp_json = {}

    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp


async def PUT_DatasetShape(request):
    """HTTP method to update dataset's shape"""
    log.request(request)
    app = request.app
    params = request.rel_url.query
    dset_id = request.match_info.get("id")

    if not isValidUuid(dset_id, obj_class="datasets"):
        log.error(f"Unexpected dset_id: {dset_id}")
        raise HTTPInternalServerError()

    body = await request.json()

    log.info(f"PUT datasetshape: {dset_id}, body: {body}")

    if "shape" not in body and "extend" not in body:
        log.error("Expected shape or extend keys")
        raise HTTPInternalServerError()

    if "bucket" in params:
        bucket = params["bucket"]
    elif "bucket" in body:
        bucket = params["bucket"]
    else:
        bucket = None

    if not isValidBucketName(bucket):
        msg = f"Invalid bucket name: {bucket}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    dset_json = await get_metadata_obj(app, dset_id, bucket=bucket)

    shape_orig = dset_json["shape"]
    log.debug(f"shape_orig: {shape_orig}")

    if "maxdims" not in shape_orig:
        log.error("expected maxdims in dataset json")
        raise HTTPInternalServerError()

    dims = shape_orig["dims"]
    maxdims = shape_orig["maxdims"]

    resp_json = {}

    if "extend" in body:
        # extend the shape by the give value and return the
        # newly extended area
        extension = body["extend"]
        extend_dim = 0

        if "extend_dim" in body:
            extend_dim = body["extend_dim"]
        log.info(f"datashape extend: {extension} dim: {extend_dim}")

        selection = "["
        for i in range(len(dims)):
            if i == extend_dim:
                lb = dims[i]
                ub = lb + extension
                if maxdims[extend_dim] != 0 and ub > maxdims[extend_dim]:
                    msg = "maximum extent exceeded"
                    log.warn(msg)
                    raise HTTPConflict()

                selection += f"{lb}:{ub}"
                dims[i] = ub
            else:
                selection += ":"
            if i < len(dims) - 1:
                selection += ","
        selection += "]"
        resp_json["selection"] = selection
    else:
        # verify that the extend request is still valid
        # e.g. another client has already extended the shape since the SN
        # verified it
        shape_update = body["shape"]
        log.debug(f"shape_update: {shape_update}")

        # Update the shape!
        for i in range(len(dims)):
            dims[i] = shape_update[i]

    # write back to S3, save to metadata cache
    log.info(f"Updated dimensions: {dims}")
    await save_metadata_obj(app, dset_id, dset_json, bucket=bucket)

    resp = json_response(resp_json, status=201)
    log.response(request, resp=resp)
    return resp
