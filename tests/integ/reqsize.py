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


class ReqSizeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ReqSizeTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()

    def testMaxWriteRequest(self):
        # Test to see how many bytes we can write at once to a datset.
        # Note: not meant to be put into testall.py script
        print("testPut1DDatasetBinary", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"
        headers_bin_rsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp["accept"] = "application/octet-stream"


        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset with one byte ints
        num_elements = 1024*1024*1024  # 1GB dataset
        data = { "type": "H5T_STD_I8LE", "shape": num_elements }
        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset1d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # get the dataset chunk layout
        req = helper.getEndpoint() + "/datasets/" + dset_id
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        print(layout_json)

        # setup request to write to dataset
        req = self.endpoint + "/datasets/" + dset_id + "/value"

        num_bytes = 1024
        # write selection to dataset, each time increasing the size by a factor of 16
        # This will likely fail prior to getting to the full size of the dataset
        while num_bytes < num_elements:
            num_bytes *= 16
            print("creating {} byte np array".format(num_bytes))
            data = bytearray(num_bytes)
            for i in range(num_bytes):
                data[i] = i%256
            params = {"select": "[0:{}]".format(num_bytes)}
            print("params:", params)
            print("writing data")
            # write to the dset
            rsp = requests.put(req, data=data, headers=headers_bin_req, params=params)
            self.assertEqual(rsp.status_code, 200)
        print("done!")


if __name__ == '__main__':
    #setup test files

    unittest.main()

