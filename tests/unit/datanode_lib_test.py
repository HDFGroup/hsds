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
import logging
import sys
import unittest

import numpy as np
from aiohttp.web_exceptions import HTTPServiceUnavailable
from h5json.objid import createObjId

sys.path.append("../..")
from hsds.datanode_lib import save_chunk
from hsds.util.lruCache import LruCache

CACHE_SIZE = 1024 * 1024  # 1 MB


def makeApp():
    """ the parts of a single-DN app that save_chunk uses """
    app = {
        "node_type": "dn",
        "id": "dn-1",
        "dn_ids": ["dn-1"],
        "dn_urls": ["http://localhost:6101"],
        "chunk_cache": LruCache(mem_target=CACHE_SIZE, name="ChunkCache"),
        "filter_map": {},
        "dirty_ids": {},
    }
    return app


def makeDset(num_elements):
    """ json for a 1-d float64 dataset stored as one chunk """
    dset_id = createObjId("datasets", root_id=createObjId("groups"))
    dset_json = {
        "id": dset_id,
        "type": {"class": "H5T_FLOAT", "base": "H5T_IEEE_F64LE"},
        "shape": {"class": "H5S_SIMPLE", "dims": [num_elements]},
        "creationProperties": {"layout": {"class": "H5D_CHUNKED", "dims": [num_elements]}},
    }
    chunk_id = "c" + dset_id[1:] + "_0"
    return dset_json, chunk_id


class DatanodeLibTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DatanodeLibTest, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.WARNING)

    def testSaveChunkLargerThanCache(self):
        # 200K float64 elements is 1.6 MB, more than the 1 MB cache. The check used
        # to compare the element count (200K) against the free bytes, so it let
        # this through.
        app = makeApp()
        dset_json, chunk_id = makeDset(200_000)
        chunk_arr = np.zeros((200_000,), dtype="<f8")
        with self.assertRaises(HTTPServiceUnavailable):
            save_chunk(app, chunk_id, dset_json, chunk_arr)
        self.assertFalse(chunk_id in app["chunk_cache"])
        self.assertEqual(app["dirty_ids"], {})


if __name__ == "__main__":
    unittest.main()
