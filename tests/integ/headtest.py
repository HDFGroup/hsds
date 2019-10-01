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
import requests
import config
import json
import uuid
import unittest
import helper

class HeadTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(HeadTest, self).__init__(*args, **kwargs)
        self.endpoint =  config.get('head_endpoint')

    def testGetInfo(self):
        req = self.endpoint + "/info"
        print("req", req)
        rsp = requests.get(req)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json')
        rsp_json = json.loads(rsp.text)
        helper.validateId(rsp_json["id"])
        self.assertEqual(rsp_json["active_dn_count"], 0)
        self.assertEqual(rsp_json["active_sn_count"], 0)
        self.assertEqual(rsp_json["target_dn_count"], 4)
        self.assertEqual(rsp_json["target_sn_count"], 4)
        self.assertEqual(rsp_json["cluster_state"], "INITIALIZING")
        #print(rsp_json)

    def testRegister(self):
        info_req = self.endpoint + "/info"
        register_req = self.endpoint + "/register"
        active_sn, active_dn = helper.getActiveNodeCount()
        self.assertEqual(active_sn, 0)
        self.assertEqual(active_dn, 0)

        reg_call_count = 9
        sn_node_numbers = set()
        dn_node_numbers = set()
        sn_node_count = 0
        dn_node_count = 0
        node_type = 'dn'

        # node_types = ("dn", "sn") * type_count
        for i in range(reg_call_count):
            # register some fake nodes
            node_id = str(uuid.uuid1())
            node_port = 6000 + i
            body = {"id": node_id, "port": node_port, "node_type": node_type}
            print("body", body)
            rsp = requests.post(register_req, data=json.dumps(body))
            self.assertEqual(rsp.status_code, 200)
            rsp_json = json.loads(rsp.text)
            print(rsp_json)

            self.assertTrue("node_number" in rsp_json)
            if rsp_json["node_number"] >= 0:
                if node_type == "dn":
                    dn_node_count += 1
                    dn_node_numbers.add(rsp_json["node_number"])
                else:
                    sn_node_count += 1
                    sn_node_numbers.add(rsp_json["node_number"])
            else:
                # we should have all the dn nodes allocated now
                self.assertEqual(len(dn_node_numbers), dn_node_count)
                self.assertEqual(node_type, "dn")
                node_type = "sn" # switch to sn


        self.assertEqual(sn_node_count, 4)
        self.assertEqual(dn_node_count, 4)
        self.assertEqual(len(sn_node_numbers), sn_node_count)
        self.assertEqual(len(dn_node_numbers), dn_node_count)

        rsp = requests.get(info_req)
        self.assertEqual(rsp.status_code, 200)
        rsp_json = json.loads(rsp.text)

        self.assertEqual(rsp_json["active_dn_count"], 4)
        self.assertEqual(rsp_json["active_sn_count"], 4)
        self.assertEqual(rsp_json["target_dn_count"], 4)
        self.assertEqual(rsp_json["target_sn_count"], 4)
        self.assertEqual(rsp_json["cluster_state"], "READY")

        for nodestate in ("/nodestate", "/nodestate/dn", "/nodestate/sn"):
            nodes_req = self.endpoint + nodestate
            rsp = requests.get(nodes_req)
            self.assertEqual(rsp.status_code, 200)
            self.assertEqual(rsp.headers['content-type'], 'application/json')
            rsp_json = json.loads(rsp.text)
            self.assertTrue("nodes" in rsp_json)
            nodes = rsp_json["nodes"]
            node_urls = set()  # we should have disting host + port for each node

            for node in nodes:
                self.assertTrue("id" in node)
                self.assertTrue("node_type" in node)
                self.assertTrue(node["node_type"] in ("sn", "dn"))
                self.assertTrue("port" in node)
                self.assertTrue("node_number" in node)
                self.assertTrue(node["node_number"] >= 0)
                self.assertTrue("host" in node)
                url = "{}:{}".format(node["host"], node["port"])
                node_urls.add(url)

            if nodestate == "/nodestate":
                self.assertEqual(len(nodes), 8)
                self.assertEqual(len(node_urls), 8)

            else:
                self.assertEqual(len(nodes), 4)
                self.assertEqual(len(node_urls), 4)

if __name__ == '__main__':
    unittest.main()
