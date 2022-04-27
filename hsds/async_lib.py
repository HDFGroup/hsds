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

import time
import hashlib
import numpy as np
from aiohttp.client_exceptions import ClientError
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError
from aiohttp.web_exceptions import HTTPForbidden
from .util.idUtil import isValidUuid, isSchema2Id, getS3Key, isS3ObjKey
from .util.idUtil import getObjId, isValidChunkId, getCollectionForId
from .util.chunkUtil import getDatasetId, getNumChunks, ChunkIterator
from .util.hdf5dtype import getItemSize, createDataType
from .util.arrayUtil import getShapeDims, getNumElements, bytesToArray
from .util.dsetUtil import getHyperslabSelection, getFilterOps

from .util.storUtil import getStorKeys, putStorJSONObj, getStorJSONObj
from .util.storUtil import deleteStorObj, getStorBytes, isStorObj
from . import hsds_logger as log
from . import config

# List all keys under given root and optionally update info.json
# Note: only works with schema v2 domains!


async def getDatasetJson(app, dsetid, bucket=None):
    # try to read the dataset json from s3
    s3_key = getS3Key(dsetid)
    try:
        dset_json = await getStorJSONObj(app, s3_key, bucket=bucket)
    except HTTPNotFound:
        log.warn(f"HTTPNotFound for {s3_key} bucket:{bucket}")
        return None
    except HTTPForbidden:
        log.warn(f"HTTPForbidden error for {s3_key} bucket:{bucket}")
        return None
    except HTTPInternalServerError:
        msg = f"HTTPInternalServerError error for {s3_key} bucket:{bucket}"
        log.warn(msg)
        return None
    return dset_json


async def updateDatasetInfo(app, dset_id, dataset_info, bucket=None):
    # get dataset metadata and deteermine number logical)_bytes,
    # linked_bytes, and num_linked_chunks

    dset_json = await getDatasetJson(app, dset_id, bucket=bucket)
    msg = f"updateDatasetInfo - id: {dset_id} dataset_info: {dataset_info}"
    log.debug(msg)
    if "shape" not in dset_json:
        msg = f"updateDatasetInfo - no shape dataset_json for {dset_id} "
        msg += "- skipping"
        log.debug(msg)
        return   # null dataspace
    shape_json = dset_json["shape"]
    if "class" in shape_json and shape_json["class"] == 'H5S_NULL':
        log.debug(f"updatedDatasetInfo - null space for {dset_id} - skipping")
        return
    if "type" not in dset_json:
        msg = "updateDatasetInfo - expected to find type in dataset_json for "
        msg += f"{dset_id}"
        log.warn(msg)
        return
    type_json = dset_json["type"]
    item_size = getItemSize(type_json)
    if "layout" not in dset_json:
        msg = "updateDatasetInfo - expected to find layout in dataset_json "
        msg += f"for {dset_id}"
        log.warn(msg)
        return
    layout = dset_json["layout"]
    msg = f"updateDatasetInfo - shape: {shape_json} type: {type_json} "
    msg += f"item size: {item_size} layout: {layout}"
    log.info(msg)

    dims = getShapeDims(shape_json)  # returns None for HS_NULL dsets

    if dims is None:
        return  # null dataspace

    if item_size == 'H5T_VARIABLE':
        # arbitrary lgoical size for vaariable, so just set to allocated size
        logical_bytes = dataset_info['allocated_bytes']
    else:
        num_elements = getNumElements(dims)
        logical_bytes = num_elements * item_size
    dataset_info["logical_bytes"] = logical_bytes
    log.debug(f"dims: {dims}")
    rank = len(dims)
    layout_class = layout["class"]
    msg = f"updateDatasetInfo - {dset_id} has layout_class: {layout_class}"
    log.debug(msg)
    selection = getHyperslabSelection(dims)  # select entire datashape
    linked_bytes = 0
    num_linked_chunks = 0

    if layout_class == 'H5D_CONTIGUOUS_REF':
        # In H5D_CONTIGUOUS_REF a non-compressed part of the HDF5 is divided
        # into equal size chunks, so we can just compute link bytes and num
        # chunks based on the size of the coniguous dataset
        layout_dims = layout["dims"]
        num_chunks = getNumChunks(selection, layout_dims)
        chunk_size = item_size
        for dim in layout_dims:
            chunk_size *= dim
        msg = "updateDatasetInfo, H5D_CONTIGUOUS_REF, num_chunks: "
        msg += f"{num_chunks} chunk_size: {chunk_size}"
        log.debug(msg)
        linked_bytes = chunk_size * num_chunks
        num_linked_chunks = num_chunks
    elif layout_class == 'H5D_CHUNKED_REF':
        chunks = layout["chunks"]
        # chunks is a dict with tuples (offset, size)
        for chunk_id in chunks:
            chunk_info = chunks[chunk_id]
            linked_bytes += chunk_info[1]
        num_linked_chunks = len(chunks)
    elif layout_class == 'H5D_CHUNKED_REF_INDIRECT':
        log.debug("chunk ref indirect")
        if "chunk_table" not in layout:
            msg = "Expected to find chunk_table in dataset layout for "
            msg += f"{dset_id}"
            log.error()
            return
        chunktable_id = layout["chunk_table"]
        # get  state for dataset from DN.
        kwargs = {"bucket": bucket}
        chunktable_json = await getDatasetJson(app, chunktable_id, **kwargs)
        log.debug(f"chunktable_json: {chunktable_json}")
        chunktable_dims = getShapeDims(chunktable_json["shape"])
        if len(chunktable_dims) != rank:
            msg = "Expected rank of chunktable to be same as the dataset "
            msg += f"for {dset_id}"
            log.warn(msg)
            return
        chunktable_layout = chunktable_json["layout"]
        log.debug(f"chunktable_layout: {chunktable_layout}")
        if not isinstance(chunktable_layout, dict):
            log.warn(f"unexpected chunktable_layout: {chunktable_id}")
            return
        if "class" not in chunktable_layout:
            log.warn(f"expected class in chunktable_layout: {chunktable_id}")
            return
        if chunktable_layout["class"] != 'H5D_CHUNKED':
            log.warn("expected chunktable layout class to be chunked")
            return
        if "dims" not in chunktable_layout:
            log.warn("expected chunktable layout to have dims key")
            return
        dims = chunktable_layout["dims"]
        chunktable_type_json = chunktable_json["type"]
        chunktable_item_size = getItemSize(chunktable_type_json)
        chunktable_dt = createDataType(chunktable_type_json)
        chunktable_filter_ops = \
            getFilterOps(app, chunktable_json, chunktable_item_size)

        # read chunktable one chunk at a time - this can be slow if there
        # are a lot of chunks, but this is only used by the async bucket
        # scan task
        sel = getHyperslabSelection(chunktable_dims)
        it = ChunkIterator(chunktable_id, sel, dims)
        msg = f"updateDatasetInfo - iterating over chunks in {chunktable_id}"
        log.debug(msg)

        while True:
            try:
                chunktable_chunk_id = it.next()
                msg = "updateDatasetInfo - gotchunktable chunk id: "
                msg += f"{chunktable_chunk_id}"
                log.debug(msg)
                s3key = getS3Key(chunktable_chunk_id)
                # read the chunk
                try:
                    is_stor_obj = await isStorObj(app,
                                                  s3key,
                                                  bucket=bucket)
                except HTTPInternalServerError as hse:
                    msg = "updateDatasetInfo - got error checking for key: "
                    msg += f"{s3key}: {hse}"
                    log.warning(msg)
                    continue
                if not is_stor_obj:
                    msg = "updateDatasetInfo - no chunk found for chunktable "
                    msg += f"id: {chunktable_chunk_id}"
                    log.debug(msg)
                else:
                    kwargs = {"filter_ops": chunktable_filter_ops,
                              "bucket": bucket}
                    try:
                        chunk_bytes = await getStorBytes(app, s3key, **kwargs)
                    except HTTPInternalServerError as hse:
                        msg = "updateDatasetInfo - got error reading "
                        msg += f"chunktable for key: {s3key}: {hse}"
                        log.warning(msg)
                        continue
                    chunk_arr = bytesToArray(chunk_bytes, chunktable_dt, dims)
                    if chunk_arr is None:
                        msg = "updateDatasetInfo - expected to find chunk "
                        msg += f"found for: {s3key}"
                        log.warn(msg)
                    else:
                        # convert to 1-d list
                        try:
                            nelements = getNumElements(chunk_arr.shape)
                            chunk_arr = chunk_arr.reshape((nelements,))
                            for i in range(nelements):
                                e = chunk_arr[i]
                                # elements should have 2 (if it is offset and
                                # size) or 3 (if it is offset, size, and path)
                                chunk_size = int(e[1])
                                if chunk_size > 0:
                                    linked_bytes += chunk_size
                                    num_linked_chunks += 1
                        except Exception as e:
                            msg = "updateDatasetInfo - got exception "
                            msg += "parsing chunktable array "
                            msg += f"{chunktable_chunk_id}: {e}"
                            log.error(msg)
            except StopIteration:
                break
        msg = "updateDatasetInfo - done with chunktable iteration "
        msg += f"for {chunktable_id}"
        log.debug(msg)
    elif layout_class == 'H5D_CHUNKED':
        msg = "updateDatasetInfo - no linked bytes/chunks for "
        msg += "H5D_CHUNKED layout"
        log.debug(msg)
    else:
        log.error(f"unexpected chunk layout: {layout_class}")

    msg = f"updateDatasetInfo - {dset_id} setting linked_bytes to "
    msg += f"{linked_bytes}, num_linked_chunks to {num_linked_chunks}"
    log.debug(msg)
    dataset_info["linked_bytes"] = linked_bytes
    dataset_info["num_linked_chunks"] = num_linked_chunks


def scanRootCallback(app, s3keys):
    log.debug(f"scanRoot - callback, {len(s3keys)} items")
    if isinstance(s3keys, list):
        log.error("got list result for s3keys callback")
        raise ValueError("unexpected callback format")

    results = app["scanRoot_results"]
    scanRoot_keyset = app["scanRoot_keyset"]
    checksums = results["checksums"]
    for s3key in s3keys.keys():

        if not isS3ObjKey(s3key):
            log.info(f"not s3obj key, ignoring: {s3key}")
            continue
        if s3key in scanRoot_keyset:
            log.warn(f"scanRoot - dejavu for key: {s3key}")
            continue
        scanRoot_keyset.add(s3key)
        msg = f"scanRoot adding key: {s3key} to keyset, "
        msg += f"{len(scanRoot_keyset)} keys"
        log.debug(msg)

        objid = getObjId(s3key)
        etag = None
        obj_size = None
        lastModified = None
        item = s3keys[s3key]
        if "ETag" in item:
            etag = item["ETag"]
            checksums[objid] = etag
        if "Size" in item:
            obj_size = item["Size"]
        if "LastModified" in item:
            lastModified = item["LastModified"]
        msg = f"scanRoot - got key {objid}: {etag} {obj_size} {lastModified}"
        log.debug(msg)

        if lastModified > results["lastModified"]:
            msg = "scanRoot: changing lastModified from: "
            msg += f"{results['lastModified']} to {lastModified}"
            log.debug(msg)
            results["lastModified"] = lastModified
        is_chunk = False
        if isValidChunkId(objid):
            is_chunk = True
            results["num_chunks"] += 1
            results["allocated_bytes"] += obj_size
        else:
            results["metadata_bytes"] += obj_size

        if is_chunk or getCollectionForId(objid) == "datasets":
            if is_chunk:
                dsetid = getDatasetId(objid)
            else:
                dsetid = objid
            datasets = results["datasets"]
            if dsetid not in datasets:
                log.debug(f"scanRoot - adding dataset id: {dsetid}")
                dataset_info = {}
                dataset_info["lastModified"] = 0
                dataset_info["num_chunks"] = 0
                dataset_info["allocated_bytes"] = 0
                dataset_info["logical_bytes"] = 0
                dataset_info["linked_bytes"] = 0
                dataset_info["num_linked_chunks"] = 0
                dataset_info["logical_bytes"] = 0
                datasets[dsetid] = dataset_info
            dataset_info = datasets[dsetid]
            if lastModified > dataset_info["lastModified"]:
                dataset_info["lastModified"] = lastModified
            if is_chunk:
                dataset_info["num_chunks"] += 1
                dataset_info["allocated_bytes"] += obj_size
                msg = f"scanRoot - updating dataset {dsetid} - num_chunks: "
                msg += f"{dataset_info['num_chunks']}, allocated_bytes: "
                msg += f"{dataset_info['allocated_bytes']}"
                log.debug(msg)
        elif getCollectionForId(objid) == "groups":
            results["num_groups"] += 1
        elif getCollectionForId(objid) == "datatypes":
            results["num_datatypes"] += 1
        else:
            msg = f"scanRoot - Unexpected collection type for id: {objid}"
            log.error(msg)


async def scanRoot(app, rootid, update=False, bucket=None):

    # iterate through all s3 keys under the given root.
    # Return dict with stats for the root.
    #
    # Note: not re-entrant!  Only one scanRoot an be run at a time per app.
    log.info(f"scanRoot for rootid: {rootid} bucket: {bucket}")

    if not isValidUuid(rootid):
        raise ValueError("Invalid root id")

    if not isSchema2Id(rootid):
        msg = f"no tabulation for schema v1 id: {rootid} returning "
        msg += "null results"
        log.warn(msg)
        return {}

    if not bucket:
        bucket = config.get("bucket_name")
    if not bucket:
        raise ValueError(f"no bucket defined for scan of {rootid}")

    root_key = getS3Key(rootid)

    if not root_key.endswith("/.group.json"):
        raise ValueError("unexpected root key")
    root_prefix = root_key[:-(len(".group.json"))]

    log.debug(f"scanRoot - using prefix: {root_prefix}")

    results = {}
    results["lastModified"] = 0
    results["num_groups"] = 0
    results["num_datatypes"] = 0
    results["datasets"] = {}  # since we need per dataset info
    results["num_chunks"] = 0
    results["allocated_bytes"] = 0
    results["metadata_bytes"] = 0
    results["num_linked_chunks"] = 0
    results["linked_bytes"] = 0
    results["logical_bytes"] = 0
    results["checksums"] = {}  # map of objid to checksums
    results["bucket"] = bucket
    results["scan_start"] = time.time()

    app["scanRoot_results"] = results
    app["scanRoot_keyset"] = set()

    kwargs = {"prefix": root_prefix, "include_stats": True,
              "bucket": bucket, "callback": scanRootCallback}
    await getStorKeys(app, **kwargs)
    num_objects = results["num_groups"]
    num_objects += results["num_datatypes"]
    num_objects += len(results["datasets"])
    num_objects += results["num_chunks"]
    log.info(f"scanRoot - got {num_objects} keys for rootid: {rootid}")

    dataset_results = results["datasets"]
    for dsetid in dataset_results:
        dataset_info = dataset_results[dsetid]
        log.info(f"got dataset: {dsetid}: {dataset_info}")
        await updateDatasetInfo(app, dsetid, dataset_info, bucket=bucket)
        if dataset_info["logical_bytes"] != "variable":
            results["logical_bytes"] += dataset_info["logical_bytes"]
            results["linked_bytes"] += dataset_info["linked_bytes"]
            results["num_linked_chunks"] += dataset_info["num_linked_chunks"]

    log.info(f"scanRoot - scan complete for rootid: {rootid}")

    # compute overall checksum
    checksums = results["checksums"]

    if len(checksums) != num_objects:
        msg = f"skipping domain checksum calculation - {len(checksums)} found "
        msg += f"but {num_objects} hdf objects"
        log.warn(msg)
    else:
        # create a numpy array to store checksums
        msg = f"creating numpy checksum array for {num_objects} checksums"
        log.debug(msg)
        checksum_arr = np.zeros((num_objects,), dtype='S16')
        objids = list(checksums.keys())
        objids.sort()
        for i in range(num_objects):
            objid = objids[i]
            checksum_arr[i] = checksums[objid]
        log.debug("numpy array created")
        hash_object = hashlib.md5(checksum_arr.tobytes())
        md5_sum = hash_object.hexdigest()
        log.debug(f"got domain_checksum: {md5_sum}")
        results["md5_sum"] = md5_sum
    # free up memory used by the checksums
    del results["checksums"]

    results["scan_complete"] = time.time()

    if update:
        # write .info object back to S3
        info_key = root_prefix + ".info.json"
        msg = f"scanRoot - updating info key: {info_key} with results: "
        msg += f"{results}"
        log.info(msg)
        await putStorJSONObj(app, info_key, results, bucket=bucket)
    return results


async def objDeleteCallback(app, s3keys):
    log.info(f"objDeleteCallback, {len(s3keys)} items")

    if not isinstance(s3keys, list):
        log.error("expected list result for objDeleteCallback")
        raise ValueError("unexpected callback format")

    if "objDelete_prefix" not in app or not app["objDelete_prefix"]:
        log.error("Unexpected objDeleteCallback")
        raise ValueError("Invalid objDeleteCallback")

    prefix = app["objDelete_prefix"]
    prefix_len = len(prefix)
    for s3key in s3keys:
        if not s3key.startswith(prefix):
            log.error(f"Unexpected key {s3key} for prefix: {prefix}")
            raise ValueError("invalid s3key for objDeleteCallback")
        full_key = prefix + s3key[prefix_len:]
        log.info(f"removeKeys - objDeleteCallback deleting key: {full_key}")
        await deleteStorObj(app, full_key)

    log.info("objDeleteCallback complete")


async def removeKeys(app, objid):
    """ delete all keys for given root or dataset id"""
    # iterate through all s3 keys under the given root or dataset id
    #  and delete them
    #
    # Note: not re-entrant!  Only one removeKeys an be run at a time
    # per app.
    log.info(f"removeKeys: {objid}")
    if not isSchema2Id(objid):
        log.warn("ignoring non-schema2 id")
        raise KeyError("Invalid key")
    s3key = getS3Key(objid)
    log.debug(f"removeKeys - got s3key: {s3key}")
    expected_suffixes = (".dataset.json", ".group.json")
    s3prefix = None

    for suffix in expected_suffixes:
        if s3key.endswith(suffix):
            s3prefix = s3key[:-len(suffix)]
    if not s3prefix:
        log.error("removeKeys - unexpected s3key for delete_set")
        raise KeyError("unexpected key suffix")
    msg = f"removeKeys - delete for {objid} searching for s3prefix: {s3prefix}"
    log.info(msg)
    if app["objDelete_prefix"]:
        msg = "removeKeys - objDelete_prefix is already set - improper use of "
        msg += "non-reentrant call?"
        log.error(msg)
        # just continue and reset
    app["objDelete_prefix"] = s3prefix
    try:
        kwargs = {"prefix": s3prefix,
                  "include_stats": False,
                  "callback": objDeleteCallback}
        await getStorKeys(app, **kwargs)
    except ClientError as ce:
        log.error(f"removeKeys - getS3Keys faiiled: {ce}")
    except HTTPNotFound:
        msg = "removeKeys - HTTPNotFound error for getStorKeys with prefix: "
        msg += f"{s3prefix}"
        log.warn(msg)
    except HTTPInternalServerError:
        msg = "removeKeys - HTTPInternalServerError for getStorKeys with "
        msg += f"prefix: {s3prefix}"
        log.error(msg)
    except Exception as e:
        msg = "removeKeys - Unexpected Exception for getStorKeys with "
        msg += f"prefix: {s3prefix}: {e}"
        log.error(msg)

    # reset the prefix
    app["objDelete_prefix"] = None
