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
import uuid
import config
import helper


class HardLinkTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(HardLinkTest, self).__init__(*args, **kwargs)
        self.domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.domain)
        self.endpoint = helper.getEndpoint()
        self.headers = helper.getRequestHeaders(domain=self.domain)
        self.root_uuid = helper.getRootUUID(domain=self.domain)

    def assertLooksLikeUUID(self, s):
        self.assertTrue(
                helper.validateId(s),
                f"Helper thinks `{s}` does not look like a valid UUID")

    def assertGroupListCountIs(self, num):
        rsp = requests.get(
                f"{self.endpoint}/groups",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "could not get groups")  
        self.assertEqual(len(rsp.json()["groups"]), num)

    def assertGroupHasNLinks(self, group_uuid, num):
        rsp = requests.get(
                f"{self.endpoint}/groups/{group_uuid}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "could not get group")  
        self.assertEqual(rsp.json()["linkCount"], num)

    def testRootGroupHasNoLinksAtStart(self):
        self.assertGroupHasNLinks(self.root_uuid, 0)
        self.assertGroupListCountIs(0)

    def testUnlinkedGroupIsUnlinked(self):
        group_uuid = helper.postGroup(self.domain)
        self.assertLooksLikeUUID(group_uuid)

        self.assertGroupListCountIs(0)

    def testGetMissingLink_Fails404(self):
        linkname = "g1"
        rsp = requests.get(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}",
                headers=self.headers)
        self.assertEqual(
                rsp.status_code,
                404,
                "Absent link should result in expected error code")

    def testCreateLinkWithouPermission_Fails403(self):
        linkname = "g1"
        wrong_username = "test_user2"

        grp1_id = helper.postGroup(self.domain)
        self.assertLooksLikeUUID(grp1_id)
        self.assertGroupListCountIs(0)

        req = f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}"
        headers = helper.getRequestHeaders(
                domain=self.domain,
                username=wrong_username)
        payload = {"id": grp1_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 403, "unauthorized put is forbidden")

        self.assertGroupHasNLinks(self.root_uuid, 0)
        self.assertGroupListCountIs(0)

    def testLinkFromRoot(self):
        linkname = "g1"
        gid = helper.postGroup(self.domain)
        self.assertGroupListCountIs(0)

        payload = {"id": gid}
        rsp = requests.put(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}",
                headers=self.headers,
                data=json.dumps(payload))
        self.assertEqual(rsp.status_code, 201, "problem making link")
        self.assertGroupHasNLinks(self.root_uuid, 1)
        self.assertGroupHasNLinks(gid, 0)
        self.assertGroupListCountIs(1)

        # get the link and inspect
        expected_keys = (
            "created",
            "hrefs",
            "lastModified",
            "link",
        )
        expected_rels = (
            "home",
            "owner",
            "self",
            "target",
        )
        rsp = requests.get(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, f"problem getting {linkname}")
        rspJson = json.loads(rsp.text)
        for key in expected_keys:
            self.assertTrue(key in rspJson, f"missing '{key}'")
        self.assertEqual(len(rspJson), len(expected_keys), rspJson)
        rspLink = rspJson["link"]
        self.assertDictEqual(
                rspLink,
                {   "title": "g1",
                    "class": "H5L_TYPE_HARD",
                    "id": gid,
                    "collection": "groups",
                })
        rels = [obj["rel"] for obj in rspJson["hrefs"]]
        for rel in rels :
            self.assertTrue(rel in expected_rels, f"extra: '{rel}'")
        self.assertEqual(len(rels), len(expected_rels), rels)

    def testDuplicateLink_Fails409(self):
        linkname = "g1"
        dset_payload = { # arbitrary
            "type": "H5T_STD_U16LE",
            "dims": [4, 4],
        }
        root = self.root_uuid

        dset_id = helper.postDataset(self.domain, dset_payload)
        gid1 = helper.postGroup(self.domain, path=f"/{linkname}")
        gid2 = helper.postGroup(self.domain)
        self.assertGroupListCountIs(1)

        for id, kind in ((dset_id, "dataset"), (gid2, "group")):
            payload = {"id": id}
            rsp = requests.put(
                    f"{self.endpoint}/groups/{root}/links/{linkname}",
                    headers=self.headers,
                    data=json.dumps(payload))
            self.assertEqual(
                    rsp.status_code,
                    409,
                    f"{kind} should have conflict with existing link")
            self.assertGroupHasNLinks(self.root_uuid, 1)
            self.assertGroupListCountIs(1)

    def testDeleteLinkWithouPermission_Fails403(self):
        linkname = "g1"
        wrong_username = "test_user2"

        grp1_id = helper.postGroup(self.domain, path=f"/{linkname}")
        self.assertLooksLikeUUID(grp1_id)
        self.assertGroupHasNLinks(self.root_uuid, 1)
        self.assertGroupListCountIs(1)

        headers = helper.getRequestHeaders(
                domain=self.domain,
                username=wrong_username)
        rsp = requests.delete(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}",
                headers=headers)
        self.assertEqual(rsp.status_code, 403, "unauthorized put is forbidden")

        self.assertGroupHasNLinks(self.root_uuid, 1)
        self.assertGroupListCountIs(1)

    def testDelete(self):
        linkname = "g1"
        grp1_id = helper.postGroup(self.domain, path=f"/{linkname}")
        self.assertLooksLikeUUID(grp1_id)
        self.assertGroupHasNLinks(self.root_uuid, 1)
        self.assertGroupListCountIs(1)

        rsp = requests.delete(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem deleting")

        self.assertGroupHasNLinks(self.root_uuid, 0)
        self.assertGroupListCountIs(0)
        rsp = requests.get(
                f"{self.endpoint}/groups/{grp1_id}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "group should still exist")

    def testLinkToBogusID_Fails404(self):
        fake_id = "g-" + str(uuid.uuid1())
        payload = {"id": fake_id}
        rsp = requests.put(
                f"{self.endpoint}/groups/{self.root_uuid}/links/nojoy",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(
                rsp.status_code,
                404)

    def testEmptyLinkName_Fails404(self):
        gid = helper.postGroup(self.domain)
        payload = {"id": gid}
        rsp = requests.put(
                f"{self.endpoint}/groups/{self.root_uuid}/links/",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(
                rsp.status_code,
                404)

    def testShashesInName_Fails404(self):
        linkname = "with/slashes"
        gid = helper.postGroup(self.domain)
        payload = {"id": gid}
        rsp = requests.put(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(
                rsp.status_code,
                404)

    def testBackslashesInNameOK(self):
        linkname = "with\\backslashes"
        gid = helper.postGroup(self.domain)
        payload = {"id": gid}
        rsp = requests.put(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(
                rsp.status_code,
                201)

    def testSpacesInNameOK(self):
        linkname = "with spaces ok"
        gid = helper.postGroup(self.domain)
        payload = {"id": gid}
        rsp = requests.put(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(
                rsp.status_code,
                201)

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
    unittest.main()

