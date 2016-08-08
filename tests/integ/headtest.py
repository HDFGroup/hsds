import requests
import config
import json
import uuid
import unittest
import helper

class HeadTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(HeadTest, self).__init__(*args, **kwargs)
        self.endpoint = 'http://' + config.get('head_host') + ':' + str(config.get('head_port'))    
       
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
        
        # node_types = ("dn", "sn") * type_count
        for i in range(reg_call_count):
            # register some fake nodes
            node_id = str(uuid.uuid1())
            node_port = 6000 + i
            body = {"id": node_id, "port": node_port}
            rsp = requests.post(register_req, data=json.dumps(body))
            self.assertEqual(rsp.status_code, 200)
            rsp_json = json.loads(rsp.text)
            print(rsp_json)         
            
            self.assertTrue("node_type" in rsp_json)
             
            if rsp_json["node_type"] == "sn":
                sn_node_count += 1
                self.assertTrue("node_number" in rsp_json)
                sn_node_numbers.add(rsp_json["node_number"])
            elif rsp_json["node_type"] == "dn":
                dn_node_count += 1
                self.assertTrue("node_number" in rsp_json)
                dn_node_numbers.add(rsp_json["node_number"])
            elif rsp_json["node_type"] == "reserve":
                # reserve type should come after others are registered
                self.assertEqual(sn_node_count, 4)
                self.assertEqual(dn_node_count, 4)
            else:
                self.assertTrue(False)  # unexpected node type

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

        nodes_req = self.endpoint + "/nodestate"
        rsp = requests.get(nodes_req)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json')
        rsp_json = json.loads(rsp.text)
        print(rsp_json)



             
            


         


if __name__ == '__main__':
    unittest.main()
