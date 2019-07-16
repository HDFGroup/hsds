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
import requests
import json
import helper
 

class QueryTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(QueryTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()
        
        # main
     
    def testSimpleQuery(self):
        # Test query value for 1d dataset
        print("testSimpleQuery", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        #    
        #create 1d dataset
        #
        fixed_str4_type = {"charSet": "H5T_CSET_ASCII", 
                "class": "H5T_STRING", 
                "length": 4, 
                "strPad": "H5T_STR_NULLPAD" }
        fixed_str6_type = {"charSet": "H5T_CSET_ASCII", 
                "class": "H5T_STRING", 
                "length": 6, 
                "strPad": "H5T_STR_NULLPAD" }
        fields = (  {'name': 'symbol', 'type': fixed_str4_type}, 
                    {'name': 'date', 'type': fixed_str6_type},
                    {'name': 'open', 'type': 'H5T_STD_I32LE'},
                    {'name': 'close', 'type': 'H5T_STD_I32LE'} ) 
        datatype = {'class': 'H5T_COMPOUND', 'fields': fields }

        num_elements = 12
        payload = {'type': datatype, 'shape': num_elements}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))
         
        # link new dataset as 'dset1'
        name = 'dset'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name 
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        
        # write entire array
        value = [
            ("EBAY", "20170102", 3023, 3088),
            ("AAPL", "20170102", 3054, 2933),
            ("AMZN", "20170102", 2973, 3011),
            ("EBAY", "20170103", 3042, 3128),
            ("AAPL", "20170103", 3182, 3034),
            ("AMZN", "20170103", 3021, 2788),
            ("EBAY", "20170104", 2798, 2876),
            ("AAPL", "20170104", 2834, 2867),
            ("AMZN", "20170104", 2891, 2978),
            ("EBAY", "20170105", 2973, 2962),
            ("AAPL", "20170105", 2934, 3010),
            ("AMZN", "20170105", 3018, 3086)
        ] 
         
        payload = {'value': value}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value
        
        # get back rows for AAPL
        params = {'query': "symbol == b'AAPL'" }
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue("index" in rspJson)
        readData = rspJson["value"]
        self.assertEqual(len(readData), 4)
        for item in readData:
            self.assertEqual(item[0], "AAPL")
        indices = rspJson["index"]
        self.assertEqual(indices, [1,4,7,10])

        # combine with a selection
        params["select"] = "[2:12]"
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        #self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue("index" in rspJson)
        readData = rspJson["value"]
        self.assertEqual(len(readData), 3)
        for item in readData:
            self.assertEqual(item[0], "AAPL")
        indices = rspJson["index"]
        self.assertEqual(indices, [4,7,10])

        # combine with Limit
        params["Limit"] = 2
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        #self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue("index" in rspJson)
        readData = rspJson["value"]
        self.assertEqual(len(readData), 2)
        for item in readData:
            self.assertEqual(item[0], "AAPL")
        indices = rspJson["index"]
        self.assertEqual(indices, [4,7])

        # try bad Limit
        params["Limit"] = "abc"
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        # try invalid query string
        params = {'query': "foobar" }
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 400)

    def testPutQuery(self):
        # Test PUT query for 1d dataset
        print("testPutQuery", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        #    
        #create 1d dataset
        #
        fixed_str4_type = {"charSet": "H5T_CSET_ASCII", 
                "class": "H5T_STRING", 
                "length": 4, 
                "strPad": "H5T_STR_NULLPAD" }
        fixed_str8_type = {"charSet": "H5T_CSET_ASCII", 
                "class": "H5T_STRING", 
                "length": 8, 
                "strPad": "H5T_STR_NULLPAD" }
        fields = (  {'name': 'symbol', 'type': fixed_str4_type}, 
                    {'name': 'date', 'type': fixed_str8_type},
                    {'name': 'open', 'type': 'H5T_STD_I32LE'},
                    {'name': 'close', 'type': 'H5T_STD_I32LE'} ) 
        datatype = {'class': 'H5T_COMPOUND', 'fields': fields }

        num_elements = 12
        payload = {'type': datatype, 'shape': num_elements}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))
         
        # link new dataset as 'dset1'
        name = 'dset'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name 
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        
        # write entire array
        value = [
            ("EBAY", "20170102", 3023, 3088),
            ("AAPL", "20170102", 3054, 2933),
            ("AMZN", "20170102", 2973, 3011),
            ("EBAY", "20170103", 3042, 3128),
            ("AAPL", "20170103", 3182, 3034),
            ("AMZN", "20170103", 3021, 2788),
            ("EBAY", "20170104", 2798, 2876),
            ("AAPL", "20170104", 2834, 2867),
            ("AMZN", "20170104", 2891, 2978),
            ("EBAY", "20170105", 2973, 2962),
            ("AAPL", "20170105", 2934, 3010),
            ("AMZN", "20170105", 3018, 3086)
        ] 
         
        payload = {'value': value}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value
        
        # set any rows with AAPL to have open of 999
        params = {'query': "symbol == b'AAPL'" }
        update_value = {"open": 999}
        payload = {'value': update_value}
        rsp = requests.put(req, params=params, data=json.dumps(update_value), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue("index" in rspJson)
        readData = rspJson["value"]
        self.assertEqual(len(readData), 4)
        for item in readData:
            self.assertEqual(item[0], "AAPL")
        indices = rspJson["index"]
        self.assertEqual(indices, [1,4,7,10])

        # read values and verify the expected changes where made
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        read_values = rspJson["value"]
        self.assertEqual(len(read_values), len(value))
        for i in range(len(value)):
            orig_item = value[i]
            mod_item = read_values[i]
            self.assertEqual(orig_item[0], mod_item[0])
            self.assertEqual(orig_item[1], mod_item[1])
            self.assertEqual(orig_item[3], mod_item[3])

            if orig_item[0] == "AAPL":
                self.assertEqual(mod_item[2], 999)
            else:
                self.assertEqual(orig_item[2], mod_item[2])

        # re-write values
        payload = {'value': value}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # set just one row with AAPL to have open of 42
        params = {'query': "symbol == b'AAPL'" }
        params["Limit"] = 1
        update_value = {"open": 999}
        payload = {'value': update_value}
        rsp = requests.put(req, params=params, data=json.dumps(update_value), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue("index" in rspJson)
        readData = rspJson["value"]
        self.assertEqual(len(readData), 1)
        for item in readData:
            self.assertEqual(item[0], "AAPL")
        indices = rspJson["index"]
        self.assertEqual(indices, [1])

        # read values and verify the expected changes where made
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        read_values = rspJson["value"]
        self.assertEqual(len(read_values), len(value))
        for i in range(len(value)):
            orig_item = value[i]
            mod_item = read_values[i]
            self.assertEqual(orig_item[0], mod_item[0])
            self.assertEqual(orig_item[1], mod_item[1])
            self.assertEqual(orig_item[3], mod_item[3])

            if orig_item[0] == "AAPL" and i == 1:
                self.assertEqual(mod_item[2], 999)
            else:
                self.assertEqual(orig_item[2], mod_item[2])




       

if __name__ == '__main__':
    #setup test files
    
    unittest.main()
