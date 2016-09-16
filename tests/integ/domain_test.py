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

    def testCreateDomain(self):
        print("testCreateDomain", self.base_domain)
        domain = "newdomain." + self.base_domain
        
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + '/'

        rsp = requests.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        for k in ("root", "owner", "acls"):
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

        # try doing a un-authenticated request
        if config.get("test_noauth"):
            print("test_noauth")
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
        # somehow this is showing up as a 400
        self.assertEqual(rsp.status_code, 404)  # 400 == bad syntax
        
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
        
         
    
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
