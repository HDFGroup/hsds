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
import asyncio
import copy
import json
import logging
import sys
import unittest
from unittest.mock import patch

from aiohttp.test_utils import make_mocked_request

sys.path.append("../..")
from hsds import dset_dn

DSET_ID = "d-b4b3b3d6-94343adc-1727-28bebf-12caac"
ROOT_ID = "g-b4b3b3d6-94343adc-1727-28bebf-12caaf"
BUCKET = "hsdstest"


def makeDnApp():
    """ minimal app state for a single-DN cluster, enough for the request
        gating in hsds_logger and the partition check in get_obj_id """
    app = {}
    app["id"] = "dn-1"
    app["node_type"] = "dn"
    app["node_state"] = "READY"
    app["dn_ids"] = ["dn-1", ]
    app["dn_urls"] = ["http://dn1", ]
    app["max_task_count"] = 0
    return app


def makeDsetJson(cpl_layout=None, layout=None):
    """ dataset json as stored in the DN meta_cache """
    dset_json = {
        "id": DSET_ID,
        "root": ROOT_ID,
        "created": 1600000000,
        "lastModified": 1600000000,
        "type": {"class": "H5T_INTEGER", "base": "H5T_STD_I32LE"},
        "shape": {"class": "H5S_SIMPLE", "dims": [2000, 500]},
        "attributes": {},
    }
    if cpl_layout is not None:
        dset_json["creationProperties"] = {"layout": cpl_layout}
    if layout is not None:
        dset_json["layout"] = layout
    return dset_json


def getDataset(dset_json):
    """ run the DN GET_Dataset handler against the given stored json and
        return the parsed response body """
    app = makeDnApp()
    path = f"/datasets/{DSET_ID}"
    req = make_mocked_request(
        "GET", f"{path}?bucket={BUCKET}", app=app, match_info={"id": DSET_ID}
    )

    async def fake_get_metadata_obj(app, obj_id, bucket=None):
        return dset_json

    with patch.object(dset_dn, "get_metadata_obj", fake_get_metadata_obj):
        resp = asyncio.run(dset_dn.GET_Dataset(req))

    return json.loads(resp.body)


class DsetDnTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DsetDnTest, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.WARNING)

    def testGetDatasetFoldsInLegacyLayout(self):
        # a dataset written by an older version stores the resolved chunk
        # shape under a top-level "layout" key - that is the shape actually
        # in use, so it is what gets reported
        dset_json = makeDsetJson(
            cpl_layout={"class": "H5D_CHUNKED", "dims": [1000, 1000]},
            layout={"class": "H5D_CHUNKED", "dims": [500, 500]},
        )
        rsp_json = getDataset(dset_json)
        cpl_layout = rsp_json["creationProperties"]["layout"]
        self.assertEqual(cpl_layout, {"class": "H5D_CHUNKED", "dims": [500, 500]})

    def testGetDatasetPreservesRefLayout(self):
        # ... but for a reference layout the top-level layout is just a plain
        # chunk shape.  Folding it in drops the file_uri/chunk_table that
        # locate the data, so the SN can no longer resolve any chunk and reads
        # come back as fill values.
        ref_layout = {
            "class": "H5D_CHUNKED_REF_INDIRECT",
            "file_uri": "s3://a-storage-bucket/some-file.h5",
            "dims": [2000, 500],
            "chunk_table": "d-b4b3b3d6-94343adc-1727-28bebf-12cabc",
        }
        dset_json = makeDsetJson(
            cpl_layout=copy.deepcopy(ref_layout),
            layout={"class": "H5D_CHUNKED", "dims": [2000, 500]},
        )
        rsp_json = getDataset(dset_json)
        cpl_layout = rsp_json["creationProperties"]["layout"]
        self.assertEqual(cpl_layout, ref_layout)

    def testGetDatasetPreservesContiguousRefLayout(self):
        # H5D_CONTIGUOUS_REF locates its data with file_uri/offset/size and
        # carries no dims of its own
        ref_layout = {
            "class": "H5D_CONTIGUOUS_REF",
            "file_uri": "s3://a-storage-bucket/some-file.h5",
            "offset": 1234,
            "size": 2000 * 500 * 4,
        }
        dset_json = makeDsetJson(
            cpl_layout=copy.deepcopy(ref_layout),
            layout={"class": "H5D_CHUNKED", "dims": [500, 500]},
        )
        rsp_json = getDataset(dset_json)
        cpl_layout = rsp_json["creationProperties"]["layout"]
        self.assertEqual(cpl_layout, ref_layout)

    def testGetDatasetDoesNotMutateMetaCache(self):
        # dset_json is the meta_cache entry.  Rewriting its creationProperties
        # in place corrupts the cached metadata for the life of the process -
        # including the DN's own view of the layout class in get_chunk.
        ref_layout = {
            "class": "H5D_CHUNKED_REF",
            "file_uri": "s3://a-storage-bucket/some-file.h5",
            "dims": [2000, 500],
            "chunks": {"0_0": [1234, 5678]},
        }
        dset_json = makeDsetJson(
            cpl_layout=copy.deepcopy(ref_layout),
            layout={"class": "H5D_CHUNKED", "dims": [2000, 500]},
        )
        getDataset(dset_json)
        self.assertEqual(dset_json["creationProperties"]["layout"], ref_layout)

        # and a second GET reports the same thing as the first
        rsp_json = getDataset(dset_json)
        self.assertEqual(rsp_json["creationProperties"]["layout"], ref_layout)

    def testGetDatasetDoesNotMutateMetaCacheNonRef(self):
        # the same holds on the fold-in path, where the cached
        # creationProperties layout was being overwritten outright
        cpl_layout = {"class": "H5D_CHUNKED", "dims": [1000, 1000]}
        dset_json = makeDsetJson(
            cpl_layout=copy.deepcopy(cpl_layout),
            layout={"class": "H5D_CHUNKED", "dims": [500, 500]},
        )
        getDataset(dset_json)
        self.assertEqual(dset_json["creationProperties"]["layout"], cpl_layout)


if __name__ == "__main__":
    unittest.main()
