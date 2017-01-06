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
import config
import helper
 

class DomainTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DomainTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        
        # main
     
    def testBaseDomain(self):
        print("testBaseDomain", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json')
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # verify that passing domain as query string works as well
        del headers["host"]
        req += "?host=" + self.base_domain
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json')
        rspJson = json.loads(rsp.text)
        root_uuid_2 = rspJson["root"]
        self.assertEqual(root_uuid, root_uuid_2)



    def testGetTopLevelDomain(self):
        domain = "home"
        print("testGetTopLevelDomain", domain)
        headers = helper.getRequestHeaders(domain=domain)
        
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 404)
        domain = "test_user1.home"
        headers = helper.getRequestHeaders(domain=domain)
        
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
         
        

    def testCreateDomain(self):
        print("testCreateDomain", self.base_domain)
        domain = "newdomain." + self.base_domain
        
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + '/'

        rsp = requests.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        for k in ("root", "owner", "acls", "created", "lastModified"):
             self.assertTrue(k in rspJson)

        root_id = rspJson["root"]

        # verify that putting the same domain again fails with a 409 error
        rsp = requests.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 409)

        # do a get on the new domain
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for k in ("root", "owner"):
             self.assertTrue(k in rspJson)
        # we should get the same value for root id
        self.assertEqual(root_id, rspJson["root"])

        # try doing a GET with a host query args
        headers = helper.getRequestHeaders()
        req = helper.getEndpoint() + "/?host=" + domain
        # do a get on the domain with a query arg for host
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for k in ("root", "owner"):
             self.assertTrue(k in rspJson)
        # we should get the same value for root id
        self.assertEqual(root_id, rspJson["root"])

        # verify we can access root groups
        root_req =  helper.getEndpoint() + "/groups/" + root_id
        headers = helper.getRequestHeaders(domain=domain)
        rsp = requests.get(root_req, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # try doing a un-authenticated request
        if config.get("test_noauth"):
            headers = helper.getRequestHeaders()
            req = helper.getEndpoint() + "/?host=" + domain
            # do a get on the domain with a query arg for host
            rsp = requests.get(req)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            for k in ("root", "owner"):
                self.assertTrue(k in rspJson)
            # we should get the same value for root id
            self.assertEqual(root_id, rspJson["root"])


    def testGetNotFound(self):
        domain = 'doesnotexist.' + self.base_domain  
        headers = helper.getRequestHeaders(domain=domain) 
        req = helper.getEndpoint() + '/'
        
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 404)

    def testInvalidDomain(self):
        # can't have two consecutive dots'       
        domain = 'two.dots..are.bad.' + self.base_domain   
        req = helper.getEndpoint() + '/'
        headers = helper.getRequestHeaders(domain=domain)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)  # 400 == bad syntax
        
        # can't have a slash
        domain = 'no/slash.' + self.base_domain    
        req = helper.getEndpoint() + '/'
        headers = helper.getRequestHeaders(domain=domain)
        rsp = requests.get(req, headers=headers)
        # somehow this is showing up as a 400 in ceph and 404 in S3
        self.assertTrue(rsp.status_code in (400, 404))  # 400 == bad syntax
        
        # just a dot is no good
        domain = '.'  + self.base_domain  
        req = helper.getEndpoint() + '/'
        headers = helper.getRequestHeaders(domain=domain)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)  # 400 == bad syntax
        
        # dot in the front is bad
        domain =  '.dot.in.front.is.bad.' + self.base_domain    
        req = helper.getEndpoint() + '/'
        headers = helper.getRequestHeaders(domain=domain)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)  # 400 == bad syntax

    def testDeleteDomain(self):
        print("testDeleteDomain", self.base_domain)
        domain = "deleteme." + self.base_domain
        
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + '/'

        # create a domain
        rsp = requests.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]

        # do a get on the domain
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(root_id, rspJson["root"])
        
        # try deleting the domain with a user who doesn't have permissions'
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 403) # forbidden

        # delete the domain (with the orginal user)
        headers = helper.getRequestHeaders(domain=domain)
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
        self.assertTrue(new_root_id != root_id)

        # verify we can access root groups
        root_req =  helper.getEndpoint() + "/groups/" + new_root_id
        headers = helper.getRequestHeaders(domain=domain)
        rsp = requests.get(root_req, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # TBD - try deleting a top-level domain

        # TBD - try deleting a domain that has child-domains

    def testDomainCollections(self):
        print("testDomainCollections", self.base_domain)
        domain = "domain_collections." + self.base_domain
        
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + '/'

        rsp = requests.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        for k in ("root", "owner", "acls", "created", "lastModified"):
             self.assertTrue(k in rspJson)

        root_id = rspJson["root"]

        # get the datasets collection
        req = helper.getEndpoint() + '/datasets'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("datasets in rspJson")
        datasets = rspJson["datasets"]
        self.assertEqual(len(datasets), 0)

        # get the groups collection
        req = helper.getEndpoint() + '/groups'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("groups in rspJson")
        groups = rspJson["groups"]
        self.assertEqual(len(groups), 0)

        # get the datatypes collection
        req = helper.getEndpoint() + '/datatypes'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("datatypes in rspJson")
        datatypes = rspJson["datatypes"]
        self.assertEqual(len(datatypes), 0)

         
    
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
