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
        self.common_headers = helper.getRequestHeaders(domain=self.base_domain)
        self.domain_root = helper.getRootUUID(domain=self.base_domain)

    def assertLooksLikeUUID(self, s):
        self.assertTrue(
                helper.validateId(s),
                f"Helper thinks `{s}` does not look like a valid UUID")

    def testRootGroupHasNoLinksAtStart(self):
        rsp = requests.get(
                self.base_endpoint + "groups/" + self.domain_root,
                headers=self.common_headers)
        self.assertEqual(rsp.status_code, 200, "could not get groups")  
        self.assertEqual(rsp.json()["linkCount"], 0, "should have no links")

    def testUnlinkedGroupIsUnlinked(self):
        # SETUP - create new group
        rsp = requests.post(
                self.base_endpoint + "groups",
                headers=self.common_headers)
        self.assertEqual(rsp.status_code, 201, "unable to create new group")
        rspJson = rsp.json()
        self.assertEqual(rspJson["linkCount"], 0)   
        self.assertEqual(rspJson["attributeCount"], 0)   
        grp1_id = rspJson["id"]
        self.assertLooksLikeUUID(grp1_id)

        # TEST
        rsp = requests.get(
                self.base_endpoint + f"groups",
                headers=self.common_headers)
        self.assertEqual(
                rsp.status_code,
                200,
                "unable to get root groups listing")
        self.assertEqual(
                rsp.json()["groups"],
                [],
                "unlinked group should not be present in domain groups list")

        # TEARDOWN - delete group
        self.assertEqual(
                requests.delete(
                        self.base_endpoint + f"groups/{grp1_id}",
                        headers=self.common_headers
                ).status_code,
                200,
                "unable to delete temporary group")

    def testGetMissingLink(self):
        linkname = "g1"
        self.assertEqual(
                requests.get(
                        self.base_endpoint + \
                        f"groups/{self.domain_root}/links/{linkname}",
                        headers=self.common_headers
                ).status_code,
                404,
                "Absent link should result in expected error code")

    def testCannotCreateLinkWithouPermission(self):
        linkname = "g1"
        wrong_username = "test_user2"

        # SETUP - create new group
        rsp = requests.post(
                self.base_endpoint + "groups",
                headers=self.common_headers)
        self.assertEqual(rsp.status_code, 201, "unable to create new group")
        rspJson = rsp.json()
        self.assertEqual(rspJson["linkCount"], 0)   
        self.assertEqual(rspJson["attributeCount"], 0)   
        grp1_id = rspJson["id"]
        self.assertLooksLikeUUID(grp1_id)
        
        req = f"{self.base_endpoint}groups/{self.domain_root}/links/{linkname}"
        headers = helper.getRequestHeaders(
                domain=self.base_domain,
                username=wrong_username)
        payload = {"id": grp1_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 403, "unauthorized put is forbidded")

        # TEARDOWN - delete group
        self.assertEqual(
                requests.delete(
                        self.base_endpoint + f"groups/{grp1_id}",
                        headers=self.common_headers
                ).status_code,
                200,
                "unable to delete temporary group")

    def testHardLink(self):
        headers = helper.getRequestHeaders(domain=self.base_domain)
        root_id = helper.getRootUUID(domain=self.base_domain)

        # create a new group
        req = helper.getEndpoint() + '/groups'
        rsp = requests.post(req, headers=headers)
        self.assertEqual(rsp.status_code, 201, "problem creating new group") 
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)   
        self.assertEqual(rspJson["attributeCount"], 0)   
        grp1_id = rspJson["id"]
        self.assertLooksLikeUUID(grp1_id)

        linkname = "g1"
        req = f"{self.base_endpoint}groups/{self.domain_root}/links/{linkname}"

        # create "/g1" with original user
        headers = helper.getRequestHeaders(domain=self.base_domain)
        payload = {"id": grp1_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual( rsp.status_code, 201, f"problem creating {linkname}")

        # now gettting the link should succeed
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200, f"problem getting {linkname}")
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
        self.assertEqual(rsp.status_code, 409, "expected conflict")

        # get the root group and verify the link count is one
        req = helper.getEndpoint() + "/groups/" + root_id 
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 1)

        # try deleting link with a different user (should fail)
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + linkname 
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(
                rsp.status_code,
                403,
                "other user deleting link is forbidden")

        # delete the link with original user
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + linkname 
        headers = helper.getRequestHeaders(domain=self.base_domain)
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200, "problem deleting link") 

        # try creating a link with a bogus id
        import uuid
        fake_id = "g-" + str(uuid.uuid1())
        payload = {"id": fake_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(
                rsp.status_code,
                404,
                "linking to bogus id should fail")

        # try creating a link without a link name
        payload = {"id": grp1_id}
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" 
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(
                rsp.status_code,
                404,
                "`PUT /groups/{id}/links/` should 404")

        # try creating a link with a forward slash in link name
        link_title = "one/two"
        payload = {"id": grp1_id}
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + link_title
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 404, "name cannot contain slashes")

        # try creating a link with a backward slash in link name
        link_title = "two\\one"
        payload = {"id": grp1_id}
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + link_title
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(
                rsp.status_code,
                201,
                f"problem making link {link_title}")

        # delete the link
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200, f"problem deleting link {req}") 

        # got a real id, but outside this domain
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/another_domain"
        another_domain = helper.getParentDomain(self.base_domain)
        another_id = helper.getRootUUID(another_domain)
        payload = {"id": another_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400, "creating 'external' hard link")

        # try creating a link with a space in the title
        link_title = "name with spaces"
        payload = {"id": grp1_id}
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/" + link_title
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201, f"problem linking {link_title}")
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200, f"problem getting {link_title}")
        rspJson = json.loads(rsp.text)
        self.assertTrue("link" in rspJson)
        rspLink = rspJson["link"]
        self.assertEqual(rspLink["title"], link_title)
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200, f"problem deleting {link_title}")

        # get the root group and verify the link count is zero
        req = helper.getEndpoint() + "/groups/" + root_id 
        headers = helper.getRequestHeaders(domain=self.base_domain)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200, "problem getting groups list")
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)

class LinkTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(LinkTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)

    def testSoftLink(self):
        headers = helper.getRequestHeaders(domain=self.base_domain)
        endpoint = helper.getEndpoint()
        link_title = "softlink"
        target_path = "somewhere"
        root_id = helper.getRootUUID(self.base_domain)
        root_req = f"{endpoint}/groups/{root_id}"
        link_req = f"{root_req}/links/{link_title}"

        self.assertEqual(
                requests.get(root_req, headers=headers).json()["linkCount"],
                0,
                "domain should have no links")

        payload = {"h5path": target_path}
        rsp = requests.put(link_req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201, "problem creating soft link")

        self.assertEqual(
                requests.get(root_req, headers=headers).json()["linkCount"],
                1,
                "root should report one and only one link")

        rsp = requests.get(link_req, headers=headers)
        self.assertEqual(rsp.status_code, 200, "problem getting soft link")
        rspJson = rsp.json()
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("link" in rspJson)
        self.assertEqual(len(rspJson), 4, "group links info has only 4 keys")
        self.assertDictEqual(
            rspJson["link"],
            {   "title": link_title,
                "class": "H5L_TYPE_SOFT",
                "h5path": target_path,
            })

    def testExternalLink(self):
        endpoint = helper.getEndpoint()
        headers = helper.getRequestHeaders(domain=self.base_domain)
        root_id = helper.getRootUUID(self.base_domain)

        self.assertEqual(
                requests.get(
                        f"{endpoint}/groups/{root_id}",
                         headers=headers
                ).json()["linkCount"],
                0,
                "domain should have no links")

        # create external link
        target_domain = 'external_target.' + helper.getParentDomain(self.base_domain)
        target_path = 'somewhere'
        link_title = 'external_link'
        req = f"{endpoint}/groups/{root_id}/links/{link_title}"
        payload = {"h5path": target_path, "h5domain": target_domain}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201, "problem creating ext. link")

        # get root group and check it has one link
        req = f"{endpoint}/groups/{root_id}" 
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200, "problem getting root group")  
        self.assertEqual(rsp.json()["linkCount"], 1)

        # get the link
        req = f"{endpoint}/groups/{root_id}/links/{link_title}"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200, "problem getting softlink")
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

        # how to get the object at external link -- should fail as nonexistent
        # continued from above section; uses link info returned
        ext_domain = rspLink["h5domain"]
        ext_path = rspLink["h5path"]
        with self.assertRaises(KeyError):
            helper.getUUIDByPath(ext_domain, ext_path)

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
    
