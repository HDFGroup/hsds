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
import unittest
import time
import requests
import json
import helper


class UpTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(UpTest, self).__init__(*args, **kwargs)

    def testCorsGetAbout(self):
        endpoint = helper.getEndpoint()
        print("endpoint:", endpoint)
        req = endpoint + "/about"
        rsp = requests.options(
            req,
            headers={
                "Access-Control-Request-Method": "GET",
                "Origin": "*",
                "Access-Control-Request-Headers": "Authorization",
            },
        )
        self.assertEqual(rsp.status_code, 200)

        # cross origin allowed by default
        self.assertEqual(rsp.headers["Access-Control-Allow-Origin"], "*")
        self.assertEqual(
            rsp.headers["Access-Control-Allow-Methods"], "GET",
        )

    def testGetAbout(self):
        endpoint = helper.getEndpoint()
        print("endpoint:", endpoint)
        s = helper.getSession(endpoint)
        req = endpoint + "/about"
        rsp = s.get(req)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["Content-Type"], "application/json; charset=utf-8")

        rspJson = json.loads(rsp.text)
        self.assertTrue("state") in rspJson
        self.assertEqual(rspJson["state"], "READY")
        self.assertTrue("about" in rspJson)
        self.assertTrue("hsds_version" in rspJson)
        self.assertTrue("greeting" in rspJson)
        self.assertTrue("name" in rspJson)
        self.assertTrue("start_time" in rspJson)
        self.assertTrue("node_count") in rspJson
        self.assertTrue(rspJson["node_count"] > 0)
        start_time = rspJson["start_time"]
        now = int(time.time())
        self.assertTrue(now > start_time)

    def testGetInfo(self):
        req = helper.getEndpoint() + "/info"
        rsp = requests.get(req)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["Content-Type"], "application/json; charset=utf-8")
        rspJson = json.loads(rsp.text)
        self.assertTrue("node" in rspJson)
        node = rspJson["node"]
        self.assertTrue("id" in node)
        self.assertTrue(node["id"].startswith("sn-"))
        self.assertTrue("node_number" not in node)  # only for dn nodes
        self.assertTrue("node_count" in node)
        self.assertTrue(node["node_count"] >= 0)
        self.assertTrue("type" in node)
        self.assertEqual(node["type"], "sn")
        self.assertTrue("state" in node)
        self.assertEqual(node["state"], "READY")


if __name__ == "__main__":
    # setup test files

    unittest.main()
