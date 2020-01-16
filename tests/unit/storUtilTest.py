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
import time
import numpy as np
from aiobotocore import get_session
import unittest
import sys

sys.path.append('../../hsds/util')
sys.path.append('../../hsds')
from util.storUtil import getStorJSONObj, putStorJSONObj, putStorBytes, isStorObj
from util.storUtil import deleteStorObj, getStorObjStats, getStorKeys, releaseStorageClient
import config


class StorUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(StorUtilTest, self).__init__(*args, **kwargs)
        # main

    async def stor_util_test(self, app):

        obj_json_1 = {"a": 1, "b": 2, "c": 3}
        obj_json_2 = {"d": 4, "e": 5, "f": 6}
        obj_json_3 = {"g": 7, "h": 8, "i": 9}
        np_arr_1 = np.arange(10)
        np_arr_2 = np.array([2,3,5,7,11,13,17,19])
        key_folder = "stor_util_test"

        # try writing some objects
        await putStorJSONObj(app, f"{key_folder}/obj_json_1", obj_json_1)
        await putStorJSONObj(app, f"{key_folder}/obj_json_2", obj_json_2)
        await putStorJSONObj(app, f"{key_folder}/obj_json_3", obj_json_3)
        await putStorBytes(app, f"{key_folder}/np_arr_1", np_arr_1.tobytes())
        await putStorBytes(app, f"{key_folder}/np_arr_2", np_arr_2.tobytes())

        # check the keys exists
        """
        self.assertTrue(await isStorObj(app, f"{key_folder}/obj_json_1"))
        self.assertTrue(await isStorObj(app, f"{key_folder}/obj_json_2"))
        self.assertTrue(await isStorObj(app, f"{key_folder}/obj_json_3"))
        self.assertTrue(await isStorObj(app, f"{key_folder}/np_arr_1"))
        self.assertTrue(await isStorObj(app, f"{key_folder}/np_arr_2"))

        # check non-existent key returns false
        self.assertFalse(await isStorObj(app, f"{key_folder}/bogus"))
        """
        # read back objects and compare results
        obj_json_1_copy = await getStorJSONObj(app, f"{key_folder}/obj_json_1")
        self.assertEqual(obj_json_1, obj_json_1_copy)
        obj_json_2_copy = await getStorJSONObj(app, f"{key_folder}/obj_json_2")
        self.assertEqual(obj_json_2, obj_json_2_copy)
        obj_json_3_copy = await getStorJSONObj(app, f"{key_folder}/obj_json_3")
        self.assertEqual(obj_json_3, obj_json_3_copy)
         

        obj_stats = await getStorObjStats(app, f"{key_folder}/np_arr_1")
        self.assertTrue("ETag" in obj_stats)
        self.assertTrue("LastModified" in obj_stats)
        # check modified time account for possible time skew
        now = time.time()
        self.assertTrue(abs(now - obj_stats["LastModified"]) < 10)

        self.assertTrue("Size" in obj_stats)
        self.assertEqual(obj_stats["Size"], 80)  # 10 element array of 64bit ints

        # list keys
        key_list = await getStorKeys(app, prefix=key_folder)
        print("key_list:", key_list)
        self.assertEqual(len(key_list), 5)
        self.assertTrue(f"{key_folder}/obj_json_1" in key_list)
        self.assertTrue(f"{key_folder}/obj_json_2" in key_list)
        self.assertTrue(f"{key_folder}/obj_json_3" in key_list)
        self.assertTrue(f"{key_folder}/np_arr_1" in key_list)
        self.assertTrue(f"{key_folder}/np_arr_2" in key_list)

        # delete keys
        for key in key_list:
            await deleteStorObj(app, key)

        key_list = await getStorKeys(app, prefix=key_folder)
        self.assertFalse(key_list)

        await releaseStorageClient(app)



    def testStorUtil(self):
        
        
        bucket = config.get("hsds_unit_test_bucket")
        if not bucket:
            print("No bucket configued, create bucket and export HSDS_UNIT_TEST_BUCKET=<bucket_name> to enable test")
            return
        print("using bucket:", bucket)

        # we need to setup a asyncio loop to query s3
        loop = asyncio.get_event_loop()
        session = get_session(loop=loop)

        app = {}
        app["session"] = session
        app["bucket_name"] = bucket

        loop.run_until_complete(self.stor_util_test(app))

        loop.close()




if __name__ == '__main__':
    #setup test files

    unittest.main()
