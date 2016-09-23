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
from copy import copy
import unittest
import requests
import json
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
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("link" in rspJson)
        rspLink = rspJson["link"]
        self.assertEqual(rspLink["title"], "g1")
        self.assertEqual(rspLink["class"], "H5L_TYPE_HARD")
        self.assertEqual(rspLink["id"], grp1_id)
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
        link_title = 'softlink'
        target_path = 'somewhere'
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + link_title 
        payload = {"h5path": target_path}
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
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("link" in rspJson)
        rspLink = rspJson["link"]
        self.assertEqual(rspLink["title"], link_title)
        self.assertEqual(rspLink["class"], "H5L_TYPE_SOFT")
        self.assertEqual(rspLink["h5path"], target_path)

    def testExternalLink(self):
        print("testExternalLink", self.base_domain)
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

        # create external link
        target_domain = 'external_target.' + helper.getParentDomain(self.base_domain)
        target_path = 'somewhere'
        link_title = 'external_link'
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + link_title 
        payload = {"h5path": target_path, "h5domain": target_domain}
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
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("link" in rspJson)
        rspLink = rspJson["link"]
        self.assertEqual(rspLink["title"], link_title)
        self.assertEqual(rspLink["class"], "H5L_TYPE_EXTERNAL")
        self.assertEqual(rspLink["h5path"], target_path)
        self.assertEqual(rspLink["h5domain"], target_domain)
        

    def testGetLinks(self):
        print("testGetLinks", self.base_domain)
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

        link_names = ["first", "second", "third", "fourth", "fifth", "sixth",
            "seventh", "eighth", "ninth", "tenth", "eleventh", "twelfth"]

        # create subgroups and link them to root using the above names
        for link_name in link_names:
            req = helper.getEndpoint() + '/groups'
            rsp = requests.post(req, headers=headers)
            self.assertEqual(rsp.status_code, 201) 
            rspJson = json.loads(rsp.text)
            grp_id = rspJson["id"]
            # link the new group
            req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + link_name
            payload = {"id": grp_id} 
            rsp = requests.put(req, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)  # created

        # get the root group and verify the link count is correct
        req = helper.getEndpoint() + "/groups/" + root_id 
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], len(link_names))   

        # get all the links for the root group
        req = helper.getEndpoint() + "/groups/" + root_id + "/links"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("links" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        links = rspJson["links"]
        self.assertEqual(len(links), len(link_names))
        ret_names = []
        for link in links:
            self.assertTrue("title" in link)
            self.assertTrue("class" in link)
            self.assertEqual(link["class"], "H5L_TYPE_HARD")
            self.assertTrue("collection" in link)
            self.assertEqual(link["collection"], "groups")
            self.assertTrue("created" in link)
            ret_names.append(link["title"])
             

        # result should come back in sorted order
        sorted_names = copy(link_names)
        sorted_names.sort()
        # sorted list should be:
        # ['eighth', 'eleventh', 'fifth', 'first', 'fourth', 'ninth',
        #  'second', 'seventh', 'sixth', 'tenth', 'third', 'twelfth']
        #
        
        self.assertEqual(ret_names, sorted_names)

        # get links with a result limit of 4
        limit=4
        req = helper.getEndpoint() + "/groups/" + root_id + "/links?Limit=" + str(limit)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("links" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        links = rspJson["links"]
        self.assertEqual(len(links), limit)
        last_link = links[-1]
        self.assertEqual(last_link["title"], sorted_names[limit-1])

        # get links after the one with name: "seventh"
        marker = "seventh"
        req = helper.getEndpoint() + "/groups/" + root_id + "/links?Marker=" + marker
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("links" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        links = rspJson["links"]
        self.assertEqual(len(links), 4)  #   "sixth", "tenth", "third", "twelfth"
        last_link = links[-1]
        self.assertEqual(last_link["title"], "twelfth")

        # get links starting with name: "seventh", and limit to 3 results
        marker = "seventh"
        limit = 3
        req = helper.getEndpoint() + "/groups/" + root_id + "/links"
        req += "?Marker=" + marker + "&Limit=" + str(limit)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("links" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        links = rspJson["links"]
        self.assertEqual(len(links), 3)  #  "sixth", "tenth", "third" 
        last_link = links[-1]
        self.assertEqual(last_link["title"], "third")    
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
