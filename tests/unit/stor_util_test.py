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
import random
import time
import numpy as np
from aiobotocore.session import get_session
import unittest
import sys
from aiohttp.web_exceptions import HTTPNotFound

sys.path.append("../..")
import hsds.config as config
from hsds.util.storUtil import getStorJSONObj, putStorJSONObj, putStorBytes
from hsds.util.storUtil import getStorBytes, isStorObj
from hsds.util.storUtil import getStorObjStats, getStorKeys, releaseStorageClient
from hsds.util.storUtil import getStorageDriverName


class StorUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(StorUtilTest, self).__init__(*args, **kwargs)
        # main

    async def stor_util_test(self, app):

        storage_driver = getStorageDriverName(app)
        print(f"Using storage driver: {storage_driver}")
        try:
            await getStorKeys(app)
        except HTTPNotFound:
            bucket = app["bucket_name"]
            msg = f"WARNING: Failed to find bucket: {bucket}. Create this bucket or "
            msg += "specify a different bucket using HSDS_UNIT_TEST_BUCKET "
            msg += " environment variable to enable this test"

            print(msg)
            return

        obj_json_1 = {"a": 1, "b": 2, "c": 3}
        obj_json_2 = {"d": 4, "e": 5, "f": 6}
        obj_json_3 = {"g": 7, "h": 8, "i": 9}
        np_arr_1 = np.arange(10)
        np_arr_2 = np.array([2, 3, 5, 7, 11, 13, 17, 19])
        key_folder = "stor_util_test"
        subkey_folder = f"{key_folder}/subkey_folder"

        # try writing some objects
        await putStorJSONObj(app, f"{key_folder}/obj_json_1", obj_json_1)
        await putStorJSONObj(app, f"{key_folder}/obj_json_2", obj_json_2)
        await putStorJSONObj(app, f"{key_folder}/obj_json_3", obj_json_3)
        await putStorBytes(app, f"{key_folder}/np_arr_1", np_arr_1.tobytes())
        await putStorBytes(app, f"{key_folder}/np_arr_2", np_arr_2.tobytes())

        # write two objects to nested folder
        await putStorJSONObj(app, f"{subkey_folder}/obj_json_1", obj_json_1)
        await putStorBytes(app, f"{subkey_folder}/np_arr_1", np_arr_1.tobytes())

        # check the keys exists
        self.assertTrue(await isStorObj(app, f"{key_folder}/obj_json_1"))
        self.assertTrue(await isStorObj(app, f"{key_folder}/obj_json_2"))
        self.assertTrue(await isStorObj(app, f"{key_folder}/obj_json_3"))
        self.assertTrue(await isStorObj(app, f"{key_folder}/np_arr_1"))
        self.assertTrue(await isStorObj(app, f"{key_folder}/np_arr_2"))

        # check non-existent key returns false
        self.assertFalse(await isStorObj(app, f"{key_folder}/bogus"))

        # read back objects and compare results
        obj_json_1_copy = await getStorJSONObj(app, f"{key_folder}/obj_json_1")
        self.assertEqual(obj_json_1, obj_json_1_copy)
        obj_json_2_copy = await getStorJSONObj(app, f"{key_folder}/obj_json_2")
        self.assertEqual(obj_json_2, obj_json_2_copy)
        obj_json_3_copy = await getStorJSONObj(app, f"{key_folder}/obj_json_3")
        self.assertEqual(obj_json_3, obj_json_3_copy)

        # read binary objects
        np_arr_1_bytes = await getStorBytes(app, f"{key_folder}/np_arr_1")
        self.assertEqual(np_arr_1_bytes, np_arr_1.tobytes())

        np_arr_2_bytes = await getStorBytes(app, f"{key_folder}/np_arr_2")
        self.assertEqual(np_arr_2_bytes, np_arr_2.tobytes())

        # try to read non-existent object
        try:
            await getStorBytes(app, f"{key_folder}/bogus")
            self.assertTrue(False)
        except HTTPNotFound:
            pass  # return expected

        # try reading non-existent bucket
        # make up a random bucket name
        nchars = 25
        bucket_name = bytearray(nchars)
        for i in range(nchars):
            bucket_name[i] = ord("a") + random.randint(0, 25)
        bucket_name = bucket_name.decode("ascii")
        print("bucket name:", bucket_name)

        try:
            await getStorBytes(app, f"{key_folder}/bogus", bucket=bucket_name)
            self.assertTrue(False)
        except HTTPNotFound:
            pass  # return expected

        # Try getSorObjStats

        obj_stats = await getStorObjStats(app, f"{key_folder}/np_arr_1")
        self.assertTrue("ETag" in obj_stats)
        self.assertTrue("LastModified" in obj_stats)
        # check modified time account for possible time skew
        now = time.time()
        self.assertTrue(abs(now - obj_stats["LastModified"]) < 10)

        self.assertTrue("Size" in obj_stats)
        self.assertEqual(obj_stats["Size"], 80)  # 10 element array of 64bit ints

        obj_stats = await getStorObjStats(app, f"{key_folder}/np_arr_2")
        self.assertTrue("ETag" in obj_stats)
        self.assertTrue("LastModified" in obj_stats)
        # check modified time account for possible time skew
        now = time.time()
        self.assertTrue(abs(now - obj_stats["LastModified"]) < 10)

        self.assertTrue("Size" in obj_stats)
        self.assertEqual(obj_stats["Size"], 64)  # 8 element array of 64bit ints

        # try reading a non-existent key
        # await getStorO

        # list keys in top folder
        key_list = await getStorKeys(app, prefix="", deliminator="/")

        self.assertEqual(len(key_list), 1)
        self.assertEqual(key_list[0], "stor_util_test/")

        # list keys in folder - get all subkeys
        key_list = await getStorKeys(app, prefix=key_folder + "/", deliminator="")
        for key in key_list:
            print("got key:", key)

        self.assertEqual(len(key_list), 7)
        self.assertTrue(f"{key_folder}/obj_json_1" in key_list)
        self.assertTrue(f"{key_folder}/obj_json_2" in key_list)
        self.assertTrue(f"{key_folder}/obj_json_3" in key_list)
        self.assertTrue(f"{key_folder}/np_arr_1" in key_list)
        self.assertTrue(f"{key_folder}/np_arr_2" in key_list)
        self.assertTrue(f"{subkey_folder}/obj_json_1" in key_list)
        self.assertTrue(f"{subkey_folder}/np_arr_1" in key_list)

        # get just sub-folders
        key_list = await getStorKeys(app, prefix=key_folder + "/", deliminator="/")
        for key in key_list:
            print("got delim key:", key)
        self.assertEqual(len(key_list), 1)
        self.assertTrue(f"{subkey_folder}/" in key_list)

        # get keys from subkey folder
        key_list = await getStorKeys(app, prefix=subkey_folder)
        self.assertTrue(f"{subkey_folder}/obj_json_1" in key_list)
        self.assertTrue(f"{subkey_folder}/np_arr_1" in key_list)

        # get keys with obj etag, size, last modified
        key_dict = await getStorKeys(app, prefix=key_folder + "/", include_stats=True)

        for k in key_dict:
            v = key_dict[k]
            print(f"{k}: {v}")

        now = time.time()
        for k in key_dict:
            v = key_dict[k]
            self.assertTrue(isinstance(v, dict))
            self.assertTrue("ETag" in v)
            self.assertTrue("Size" in v)
            self.assertTrue(v["Size"] > 0)
            self.assertTrue(v["Size"] <= 160)
            self.assertTrue("LastModified" in v)
            self.assertTrue(v["LastModified"] < now)
            self.assertTrue(v["LastModified"] > 0.0)
        self.assertEqual(len(key_dict), 7)
        self.assertTrue(f"{key_folder}/obj_json_1" in key_dict)
        self.assertTrue(f"{key_folder}/obj_json_2" in key_dict)
        self.assertTrue(f"{key_folder}/obj_json_3" in key_dict)
        self.assertTrue(f"{key_folder}/np_arr_1" in key_dict)
        self.assertTrue(f"{key_folder}/np_arr_2" in key_dict)
        self.assertTrue(f"{subkey_folder}/obj_json_1" in key_dict)
        self.assertTrue(f"{subkey_folder}/np_arr_1" in key_dict)

        # delete keys
        """
        await deleteStorObj(app, f"{key_folder}/obj_json_1")
        await deleteStorObj(app, f"{key_folder}/obj_json_2")
        await deleteStorObj(app, f"{key_folder}/obj_json_3")
        await deleteStorObj(app, f"{key_folder}/np_arr_1")
        await deleteStorObj(app, f"{key_folder}/np_arr_2")
        await deleteStorObj(app, f"{subkey_folder}/obj_json_1")
        await deleteStorObj(app, f"{subkey_folder}/np_arr_1")

        key_list = await getStorKeys(app, prefix=key_folder)
        if key_list:
            print("unexpected keys:", key_list)
        self.assertFalse(key_list)
        """
        await releaseStorageClient(app)

    def testStorUtil(self):

        cors_domain = config.get("cors_domain")
        print(f"cors_domain: [{cors_domain}]")
        bucket = config.get("hsds_unit_test_bucket")
        if not bucket:
            msg = "No bucket configured, create bucket and export "
            msg += " HSDS_UNIT_TEST_BUCKET=<bucket_name> to enable test"
            print(msg)

            return

        # we need to setup a asyncio loop to query s3
        loop = asyncio.get_event_loop()
        session = get_session(loop=loop)

        app = {}
        app["session"] = session
        app["bucket_name"] = bucket
        app["loop"] = loop

        loop.run_until_complete(self.stor_util_test(app))

        loop.close()


if __name__ == "__main__":
    # setup test files

    unittest.main()
