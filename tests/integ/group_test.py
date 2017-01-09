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
 

class GroupTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(GroupTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        print(self.base_domain)
        helper.setupDomain(self.base_domain)
        
        # main
     
    def testGetRootGroup(self):
        print("testGetRootGroup", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + '/'

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)
        req = helper.getEndpoint() + '/groups/' + root_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        group_id = rspJson["id"]
        helper.validateId(group_id)
        self.assertTrue("root" in rspJson)
        root_id = rspJson["root"]
        self.assertEqual(group_id, root_id)
        self.assertTrue("domain" in rspJson)
        self.assertEqual(rspJson["domain"], self.base_domain)
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("linkCount" in rspJson)
        self.assertTrue("attributeCount" in rspJson)

        # try get with a different user (who has read permission)
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["root"], root_uuid)

        # try to do a GET with a different domain (should fail)
        another_domain = helper.getParentDomain(self.base_domain)
        print("another_domain:", another_domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        req = helper.getEndpoint() + '/groups/' + root_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)
         

    def testGetInvalidUUID(self):
        print("testGetRootGroup", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + '/'  
        invalid_uuid = "foobar"  
        req = helper.getEndpoint() + "/groups/" + invalid_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        import uuid
        bad_uuid = "g-" + str(uuid.uuid1())    
        req = helper.getEndpoint() + "/groups/" + bad_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 404)

    def testPost(self):
        # test POST group
        print("testPost", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + '/groups'  

        # create a new group
        rsp = requests.post(req, headers=headers)
        self.assertEqual(rsp.status_code, 201) 
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)   
        self.assertEqual(rspJson["attributeCount"], 0)   
        group_id = rspJson["id"]
        self.assertTrue(helper.validateId(group_id))

        # verify we can do a get on the new group
        req = helper.getEndpoint() + '/groups/' + group_id
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertEqual(rspJson["id"], group_id)
        self.assertTrue("root" in rspJson)
        self.assertTrue(rspJson["root"] != group_id)
        self.assertTrue("domain" in rspJson)
        self.assertEqual(rspJson["domain"], self.base_domain)

        # try POST with user who doesn't have create permission on this domain
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        req = helper.getEndpoint() + '/groups'
        rsp = requests.post(req, headers=headers)
        self.assertEqual(rsp.status_code, 403) # forbidden
    
    def testPostWithLink(self):
        # test PUT_root
        print("testPostWithLink", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get root id
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)
        
        # delete the domain
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # try getting the domain
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 410)

        # try re-creating a domain
        rsp = requests.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        new_root_id = rspJson["root"]
        self.assertTrue(new_root_id != root_uuid)

        root_uuid = new_root_id
        

        # get root group and verify link count is 0
        req = helper.getEndpoint() + '/groups/' + root_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        
        # create new group  
        payload = { 'link': { 'id': root_uuid, 'name': 'linked_group' } }
        req = helper.getEndpoint() + "/groups"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201) 
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], 0)
        self.assertTrue(helper.validateId(rspJson["id"]) ) 

        # get root group and verify link count is 1
        req = helper.getEndpoint() + '/groups/' + root_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 1)

    def testDelete(self):
        # test Delete
        print("testDelete", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_id = rspJson["root"]

        req = helper.getEndpoint() + '/groups'  
        
        # create a new group
        rsp = requests.post(req, headers=headers)
        self.assertEqual(rsp.status_code, 201) 
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        group_id = rspJson["id"]
        self.assertTrue(helper.validateId(group_id))

        # verify we can do a get on the new group
        req = helper.getEndpoint() + '/groups/' + group_id
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertEqual(rspJson["id"], group_id)
        self.assertTrue("root" in rspJson)
        self.assertTrue(rspJson["root"] != group_id)
        self.assertTrue("domain" in rspJson)
        self.assertEqual(rspJson["domain"], self.base_domain)

        # try DELETE with user who doesn't have create permission on this domain
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 403) # forbidden

        # try to do a DELETE with a different domain (should fail)
        another_domain = helper.getParentDomain(self.base_domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        req = helper.getEndpoint() + '/groups/' + group_id
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)   
        
        # delete the new group
        headers = helper.getRequestHeaders(domain=self.base_domain)
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue(rspJson is not None)

        # a get for the group should now return 410 (GONE)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 410)

        # try deleting the root group
        req = helper.getEndpoint() + '/groups/' + root_id
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 403)  # Forbidden    
    
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
