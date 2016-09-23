##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of H5Serv (HDF5 REST Server) Service, Libraries and      #
# Utilities.  The full HDF5 REST Server copyright notice, including          #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################
import unittest
import requests
import json
import helper
 
class UpTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(UpTest, self).__init__(*args, **kwargs)
        
        # main
    def testGetInfo(self):
        req = helper.getEndpoint() + '/info'
        rsp = requests.get(req)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json')
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertTrue(rspJson["id"].startswith("sn-"))
        self.assertTrue("node_number" in rspJson)
        self.assertTrue(rspJson["node_number"] >= 0)
        self.assertTrue("node_type" in rspJson)
        self.assertEqual(rspJson["node_type"], "sn")
        self.assertTrue("node_state" in rspJson)
        self.assertEqual(rspJson["node_state"], "READY")  
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
