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
import config
import helper
 

class LinkTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(LinkTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        print(self.base_domain)
        helper.setupDomain(self.base_domain)
        
        # main
     
    def testHardLink(self):
        print("testHardLink", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + '/'

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]

        # get root group and check it has no links
        req = helper.getEndpoint() + "/groups/" + root_id 
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)  # no links

        # create a new group
        req = helper.getEndpoint() + '/groups'
        rsp = requests.post(req, headers=headers)
        self.assertEqual(rsp.status_code, 201) 
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)   
        self.assertEqual(rspJson["attributeCount"], 0)   
        grp1_id = rspJson["id"]
        self.assertTrue(helper.validateId(grp1_id))

        # try to get "/g1"  (doesn't exist yet)
        link_title = "g1"
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + link_title 
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 404)  # link doesn't exist yet
        
        # try creating a link with a different user (should fail)
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        payload = {"id": grp1_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 403)  # forbidden

        # create "/g1" with original user
        headers = helper.getRequestHeaders(domain=self.base_domain)
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # created

        # now gettting the link should succeed
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # should get link now
        rspJson = json.loads(rsp.text)
        print(rspJson)
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("link" in rspJson)
        rspLink = rspJson["link"]
        self.assertEqual(rspLink["class"], "H5L_TYPE_HARD")
        self.assertEqual(rspLink["id"], grp1_id)
        self.assertEqual(rspLink["title"], "g1")
        self.assertEqual(rspLink["collection"], "groups")
        
        # try creating the link again  (should fail with conflict)
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 409)  # conflict
        
        # get the root group and verify the link count is one
        req = helper.getEndpoint() + "/groups/" + root_id 
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 1)  # link count is 1
        
        # try deleting link with a different user (should fail)
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + link_title 
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 403)   # forbidden

        # delete the link with original user
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + link_title 
        headers = helper.getRequestHeaders(domain=self.base_domain)
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200) 

        # try creating a link with a bogus id
        import uuid
        fake_id = "g-" + str(uuid.uuid1())
        payload = {"id": fake_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 404)  # not found

        # got a real id, but outside this domain
        another_domain = helper.getParentDomain(self.base_domain)
        another_id = helper.getRootUUID(another_domain)
        payload = {"id": another_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # Invalid request

        # get the root group and verify the link count is zero
        req = helper.getEndpoint() + "/groups/" + root_id 
        headers = helper.getRequestHeaders(domain=self.base_domain)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)  # link count is zero

    def testSoftLink(self):
        print("testSoftLink", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + '/'

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]

        # get root group and check it has no links
        req = helper.getEndpoint() + "/groups/" + root_id 
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)  # no links

        # create softlink
        target_domain = 'external_target.' + helper.getParentDomain(self.base_domain)
        target_path = '/dset1'
        link_title = 'softlink'
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + link_title 
        payload = {"h5path": "somewhere"}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # created

        # get root group and check it has one link
        req = helper.getEndpoint() + "/groups/" + root_id 
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 1)  # no links

        # get the link
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + link_title 
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # should get the softlink  
        rspJson = json.loads(rsp.text)
        print(rspJson)
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("link" in rspJson)
        rspLink = rspJson["link"]
        self.assertEqual(rspLink["class"], "H5L_TYPE_SOFT")
        self.assertEqual(rspLink["h5path"], "somewhere")
        

         
    
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
