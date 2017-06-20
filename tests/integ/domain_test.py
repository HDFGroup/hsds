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
import time
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

        # try using DNS-style domain name  
        domain = helper.getDNSDomain(self.base_domain)
        params = { "host": domain }
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json')
        rspJson = json.loads(rsp.text)
        root_uuid_3 = rspJson["root"]
        self.assertEqual(root_uuid, root_uuid_3)


    def testGetDomain(self):
        domain = helper.getTestDomain("tall.h5")
        print("testGetDomain", domain)
        headers = helper.getRequestHeaders(domain=domain)
        
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        if rsp.status_code != 200:
            print("WARNING: Failed to get domain: {}. Is test data setup?".format(domain))
            return  # abort rest of test
        self.assertEqual(rsp.headers['content-type'], 'application/json')
        rspJson = json.loads(rsp.text)
         
        for name in ("lastModified", "created", "hrefs", "root", "owner", "class"):
            self.assertTrue(name in rspJson)
        now = time.time()
        self.assertTrue(rspJson["created"] < now - 60 * 5)
        self.assertTrue(rspJson["lastModified"] < now - 60 * 5)
        self.assertEqual(len(rspJson["hrefs"]), 7)
        self.assertTrue(rspJson["root"].startswith("g-"))
        self.assertTrue(rspJson["owner"])
        self.assertEqual(rspJson["class"], "domain")
        self.assertFalse("num_groups" in rspJson)  # should only show up with the verbose param
        
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # verify that passing domain as query string works as well
        del headers["host"]
        params = {"host": domain}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json')
        rspJson = json.loads(rsp.text)
        root_uuid_2 = rspJson["root"]
        self.assertEqual(root_uuid, root_uuid_2)

        # same deal using the "domain" param
        params = {"domain": domain}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json')
        rspJson = json.loads(rsp.text)
        root_uuid_3 = rspJson["root"]
        self.assertEqual(root_uuid, root_uuid_3)

        # verify that invalid domain fails
        domain = domain[1:]  # strip off the '/'
        params = {"domain": domain}

        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 400)

    def testGetDomainVerbose(self):
        domain = helper.getTestDomain("tall.h5")
        print("testGetDomainVerbose", domain)
        headers = helper.getRequestHeaders(domain=domain)
        
        req = helper.getEndpoint() + '/'
        params = {"verbose": 1}
        rsp = requests.get(req, params=params, headers=headers)
        if rsp.status_code != 200:
            print("WARNING: Failed to get domain: {}. Is test data setup?".format(domain))
            return  # abort rest of test
        self.assertEqual(rsp.headers['content-type'], 'application/json')
        rspJson = json.loads(rsp.text)
         
        for name in ("lastModified", "created", "hrefs", "root", "owner", "class"):
            self.assertTrue(name in rspJson)
        now = time.time()
        self.assertTrue(rspJson["created"] < now - 60 * 5)
        self.assertTrue(rspJson["lastModified"] < now - 60 * 5)
        self.assertEqual(len(rspJson["hrefs"]), 7)
        self.assertTrue(rspJson["root"].startswith("g-"))
        self.assertTrue(rspJson["owner"])
        self.assertEqual(rspJson["class"], "domain")
        
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        self.assertTrue("num_groups" in rspJson)
        self.assertEqual(rspJson["num_groups"], 5)
        self.assertTrue("num_datasets" in rspJson)
        self.assertEqual(rspJson["num_datasets"], 4)
        self.assertTrue("num_datatypes" in rspJson)
        self.assertEqual(rspJson["num_datatypes"], 0)
        self.assertTrue("allocated_bytes" in rspJson)
        # test that allocated_bytes falls in a given range
        self.assertTrue(rspJson["allocated_bytes"] > 5500)  
        self.assertTrue(rspJson["allocated_bytes"] < 6000)  
        self.assertTrue("num_chunks" in rspJson)
        self.assertTrue(rspJson["num_chunks"], 4)
 
        

    def testGetTopLevelDomain(self):
        domain = "/home"
        print("testGetTopLevelDomain", domain)
        headers = helper.getRequestHeaders(domain=domain)
        
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertFalse("root" in rspJson)  # no root group for folder domain
        self.assertTrue("owner" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("class" in rspJson)
        self.assertEqual(rspJson["class"], "folder")
        domain = "test_user1.home"
        headers = helper.getRequestHeaders(domain=domain)
        
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
         
        

    def testCreateDomain(self):
        domain = self.base_domain + "/newdomain.h6"
        print("testCreateDomain", domain)        
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

    def testCreateFolder(self):
        domain = self.base_domain + "/newfolder"
        print("testCreateFolder", domain)        
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + '/'
        body = {"folder": True}
        rsp = requests.put(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        for k in ("owner", "acls", "created", "lastModified"):
             self.assertTrue(k in rspJson)
        self.assertFalse("root" in rspJson)  # no root -> folder
 
        # verify that putting the same domain again fails with a 409 error
        rsp = requests.put(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 409)

        # do a get on the new folder
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
         
        self.assertTrue("owner" in rspJson)
        self.assertTrue("class" in rspJson)
        self.assertEqual(rspJson["class"], "folder")
         

        # try doing a un-authenticated request
        if config.get("test_noauth"):
            headers = helper.getRequestHeaders()
            req = helper.getEndpoint() + "/?host=" + domain
            # do a get on the folder with a query arg for host
            rsp = requests.get(req)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            for k in ("class", "owner"):
                self.assertTrue(k in rspJson)
            self.assertFalse("root" in rspJson)   

    

    def testInvalidChildDomain(self):
        domain = self.base_domain + "/notafolder/newdomain.h5"
        # should fail assuming "notafolder" doesn't exist
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + '/'

        rsp = requests.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 404)
         

    def testGetNotFound(self):
        domain =  self.base_domain + "/doesnotexist.h6" 
        print("testGetNotFound", domain)
        headers = helper.getRequestHeaders(domain=domain) 
        req = helper.getEndpoint() + '/'
        
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 404)

    def testDNSDomain(self):
        # DNS domain names are in reverse order with dots as seperators...
         
        dns_domain = helper.getDNSDomain(self.base_domain)
        print("testDNSDomain", dns_domain)
        # verify we can access base domain as via dns name
        headers = helper.getRequestHeaders(domain=dns_domain)
        
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json')
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # can't have two consecutive dots'       
        domain = 'two.dots..are.bad.' + dns_domain 
        req = helper.getEndpoint() + '/'
        headers = helper.getRequestHeaders(domain=domain)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)  # 400 == bad syntax
        
        # can't have a slash
        domain = 'no/slash.' + dns_domain    
        req = helper.getEndpoint() + '/'
        headers = helper.getRequestHeaders(domain=domain)
        rsp = requests.get(req, headers=headers)
        # somehow this is showing up as a 400 in ceph and 404 in S3
        self.assertTrue(rsp.status_code in (400, 404))  # 400 == bad syntax
        
        # just a dot is no good
        domain = '.'  + dns_domain  
        req = helper.getEndpoint() + '/'
        headers = helper.getRequestHeaders(domain=domain)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)  # 400 == bad syntax
        
        # dot in the front is bad
        domain =  '.dot.in.front.is.bad.' + dns_domain    
        req = helper.getEndpoint() + '/'
        headers = helper.getRequestHeaders(domain=domain)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)  # 400 == bad syntax

    def testDeleteDomain(self):
        domain = self.base_domain + "/deleteme.h6"
        print("testDeleteDomain", domain)
        
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
        domain = helper.getTestDomain("tall.h5")
        print("testDomainCollections", domain)
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + '/'

        rsp = requests.get(req, headers=headers)
        if rsp.status_code != 200:
            print("WARNING: Failed to get domain: {}. Is test data setup?".format(domain))
            return  # abort rest of test

        rspJson = json.loads(rsp.text)
        for k in ("root", "owner", "created", "lastModified"):
             self.assertTrue(k in rspJson)

        root_id = rspJson["root"]
        helper.validateId(root_id)

        # get the datasets collection
        req = helper.getEndpoint() + '/datasets'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("datasets in rspJson")
        datasets = rspJson["datasets"]
        for objid in datasets:
            helper.validateId(objid)
        self.assertEqual(len(datasets), 4)

        # get the first 2 datasets
        params = {"Limit": 2}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("datasets in rspJson")
        batch = rspJson["datasets"]
        self.assertEqual(len(batch), 2)
        helper.validateId(batch[0])
        self.assertEqual(batch[0], datasets[0])
        helper.validateId(batch[1])
        self.assertEqual(batch[1], datasets[1])
        # next batch
        params["Marker"] = batch[1]
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("datasets in rspJson")
        batch = rspJson["datasets"]
        self.assertEqual(len(batch), 2)
        helper.validateId(batch[0])
        self.assertEqual(batch[0], datasets[2])
        helper.validateId(batch[1])
        self.assertEqual(batch[1], datasets[3])

        # get the groups collection
        req = helper.getEndpoint() + '/groups'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("groups in rspJson")
        groups = rspJson["groups"]
        self.assertEqual(len(groups), 5)
        # get the first 2 groups
        params = {"Limit": 2}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("groups in rspJson")
        batch = rspJson["groups"]
        self.assertEqual(len(batch), 2)
        helper.validateId(batch[0])
        self.assertEqual(batch[0], groups[0])
        helper.validateId(batch[1])
        self.assertEqual(batch[1], groups[1])
        # next batch
        params["Marker"] = batch[1]
        params["Limit"] = 100
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("groups in rspJson")
        batch = rspJson["groups"]
        self.assertEqual(len(batch), 3)
        for i in range(3):
            helper.validateId(batch[i])
            self.assertEqual(batch[i], groups[2+i])
         
        # get the datatypes collection
        req = helper.getEndpoint() + '/datatypes'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("datatypes in rspJson")
        datatypes = rspJson["datatypes"]
        self.assertEqual(len(datatypes), 0)  # no datatypes in this domain

    def testGetDomains(self):
        print("testGetDomains", self.base_domain)
    
        import os.path as op
        # back up two levels
        domain = op.dirname(self.base_domain)
        domain = op.dirname(domain) + '/'
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + '/domains'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json')
        rspJson = json.loads(rsp.text)
        self.assertTrue("domains" in rspJson)
        domains = rspJson["domains"]
        
        domain_count = len(domains)
        if domain_count < 9:
            # this should only happen in the very first test run
            print("Expected to find more domains!")
            return

        for item in domains:
            self.assertTrue("name" in item)
            name = item["name"]
            self.assertEqual(name[0], '/')
            self.assertTrue(name[-1] != '/')
            self.assertTrue("owner" in item)
            self.assertTrue("created" in item)
            self.assertTrue("lastModified" in item)
            self.assertTrue("class") in item
            self.assertTrue(item["class"] in ("domain", "folder"))
       
        # try getting the first 4 domains
        params = {"domain": domain, "Limit": 4}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("domains" in rspJson)
        part1 = rspJson["domains"]
        
        self.assertEqual(len(part1), 4)
        for item in part1:
            self.assertTrue("name" in item)
            name = item["name"]
            self.assertEqual(name[0], '/')
            self.assertTrue(name[-1] != '/')
             
        # get next batch of 4
        params = {"domain": domain, "Marker": name, "Limit": 4}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("domains" in rspJson)
        part2 = rspJson["domains"]
        self.assertEqual(len(part2), 4)
        for item in part2:
            self.assertTrue("name" in item)
            name = item["name"]
            self.assertTrue(name != params["Marker"])

        # empty sub-domains
        domain = helper.getTestDomain("tall.h5") + '/'
        params = {"domain": domain}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("domains" in rspJson)
        domains = rspJson["domains"]
        self.assertEqual(len(domains), 0)

    def testGetTopLevelDomains(self):
        print("testGetDomains", self.base_domain)
    
        import os.path as op
        # Either '/' or no domain should get same result
        for domain in (None, '/'):
            headers = helper.getRequestHeaders(domain=domain)
            print("headers:", headers)
            req = helper.getEndpoint() + '/domains'
            rsp = requests.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)
            self.assertEqual(rsp.headers['content-type'], 'application/json')
            rspJson = json.loads(rsp.text)
            self.assertTrue("domains" in rspJson)
            domains = rspJson["domains"]
        
            domain_count = len(domains)
            if domain_count == 0:
                # this should only happen in the very first test run
                print("Expected to find more domains!")
                self.assertTrue(False)
                return

            for item in domains:
                self.assertTrue("name" in item)
                name = item["name"]
                self.assertEqual(name[0], '/')
                self.assertTrue(name[-1] != '/')
                self.assertTrue("owner" in item)
                self.assertTrue("created" in item)
                self.assertTrue("lastModified" in item)
                self.assertTrue("class") in item
                self.assertTrue(item["class"] in ("domain", "folder"))
          
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
