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
from copy import copy
import unittest
import time
import requests
import json
import config
import helper


class HardLinkTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(HardLinkTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.base_endpoint = helper.getEndpoint() + "/"

    def assertLooksLikeUUID(self, s):
        self.assertTrue(
                helper.validateId(s),
                f"Helper thinks `{s}` does not look like a valid UUID")

    def testHardLink(self):
        headers = helper.getRequestHeaders(domain=self.base_domain)
        root_id = helper.getRootUUID(domain=self.base_domain)

        # get root group and check it has no links
        req = helper.getEndpoint() + "/groups/" + root_id
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rsp.json()["linkCount"], 0, "should have no links")

        # create a new group
        req = helper.getEndpoint() + '/groups'
        rsp = requests.post(req, headers=headers)
        self.assertEqual(rsp.status_code, 201) 
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)   
        self.assertEqual(rspJson["attributeCount"], 0)   
        grp1_id = rspJson["id"]
        self.assertLooksLikeUUID(grp1_id)

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

        # try creating a link without a link name
        payload = {"id": grp1_id}
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" 
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 404)  # Not Found

        # try creating a link with a forward slash in link name
        link_title = "one/two"
        payload = {"id": grp1_id}
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + link_title
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 404)  # Not Found

        # try creating a link with a backward slash in link name
        link_title = "two\\one"
        payload = {"id": grp1_id}
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + link_title
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # Created

        # delete the link
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200) 

        # got a real id, but outside this domain
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/another_domain"
        another_domain = helper.getParentDomain(self.base_domain)
        another_id = helper.getRootUUID(another_domain)
        payload = {"id": another_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # Invalid request

        # try creating a link with a space in the title
        link_title = "name with spaces"
        payload = {"id": grp1_id}
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + link_title
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # Created
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # should get link now
        rspJson = json.loads(rsp.text)
        self.assertTrue("link" in rspJson)
        rspLink = rspJson["link"]
        self.assertEqual(rspLink["title"], link_title)
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200) 

        # get the root group and verify the link count is zero
        req = helper.getEndpoint() + "/groups/" + root_id 
        headers = helper.getRequestHeaders(domain=self.base_domain)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)  # link count should zero


class LinkTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(LinkTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)

    def testSoftLink(self):
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

        # Use a marker that is not present (should return 404)
        marker = "foobar"
        req = helper.getEndpoint() + "/groups/" + root_id + "/links?Marker=" + marker
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 404)

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

    @unittest.skipUnless(
            config.get("test_on_uploaded_file"),
            "don't test without uploaded file")
    def testGet(self):
        # test getting links from an existing domain
        domain = helper.getTestDomain("tall.h5")
        headers = helper.getRequestHeaders(domain=domain)

        # verify domain exists
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(
                rsp.status_code,
                200, 
                f"Failed to get test file domain: {domain}")

        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        self.assertTrue(root_uuid.startswith("g-"))

        # get the "/g1" group
        g1_2_uuid = helper.getUUIDByPath(domain, "/g1/g1.2")

        now = time.time()

        # get links for /g1/g1.2:
        req = helper.getEndpoint() + '/groups/' + g1_2_uuid + '/links'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        hrefs = rspJson["hrefs"]
        self.assertEqual(len(hrefs), 3)
        self.assertTrue("links" in rspJson)
        links = rspJson["links"]
        self.assertEqual(len(links), 2)
        g1_2_1_uuid = None
        extlink_file = None
        for link in links:
            self.assertTrue("class" in link)
            link_class = link["class"]
            if link_class == 'H5L_TYPE_HARD':
                for name in ("target", "created", "collection", "class", "id", 
                    "title", "href"):
                    self.assertTrue(name in link)
                g1_2_1_uuid = link["id"]
                self.assertTrue(g1_2_1_uuid.startswith("g-"))
                self.assertEqual(link["title"], "g1.2.1")
                self.assertTrue(link["created"] < now - 60 * 5)
            else:
                self.assertEqual(link_class, 'H5L_TYPE_EXTERNAL')
                for name in ("created", "class", "h5domain", "h5path", 
                    "title", "href"):
                    self.assertTrue(name in link)
                self.assertEqual(link["title"], "extlink")
                extlink_file = link["h5domain"]
                self.assertEqual(extlink_file, "somefile")
                self.assertEqual(link["h5path"], "somepath")
                self.assertTrue(link["created"] < now - 60 * 5)

        self.assertTrue(g1_2_1_uuid is not None)
        self.assertTrue(extlink_file is not None)
        self.assertEqual(helper.getUUIDByPath(domain, "/g1/g1.2/g1.2.1"), g1_2_1_uuid)

        # get link by title
        req = helper.getEndpoint() + '/groups/' + g1_2_1_uuid + '/links/slink'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for name in ("created", "lastModified", "link", "hrefs"):
            self.assertTrue(name in rspJson)
        # created should be same as lastModified for links 
        self.assertEqual(rspJson["created"], rspJson["lastModified"])
        self.assertTrue(rspJson["created"] < now - 60 * 5)
        hrefs = rspJson["hrefs"]
        self.assertEqual(len(hrefs), 3)

        link = rspJson["link"]
        for name in ("title", "h5path", "class"):
            self.assertTrue(name in link)

        self.assertEqual(link["class"], 'H5L_TYPE_SOFT')
        self.assertFalse("h5domain" in link)  # only for external links
        self.assertEqual(link["title"], "slink")
        self.assertEqual(link["h5path"], "somevalue")



if __name__ == '__main__':
    #setup test files

    unittest.main()
    
