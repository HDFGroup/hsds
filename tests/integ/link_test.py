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
from requests import delete as DELETE, get as GET, post as POST, put as PUT
import json
import uuid
import config
import helper

GET_LINK_KEYS = [
    "created",
    "hrefs",
    "lastModified",
    "link",
]
GET_LINK_HREFS_RELS = [
    "home",
    "owner",
    "self",
    "target",
]
GET_SOFTLINK_HREFS_RELS = [
    "home",
    "owner",
    "self",
]

# ----------------------------------------------------------------------

class HardLinkTest(helper.TestCase):
    def __init__(self, *args, **kwargs):
        super(HardLinkTest, self).__init__(*args, **kwargs)

    def testRootGroupHasNoLinksAtStart(self):
        self.assertGroupHasNLinks(self.root_uuid, 0)
        self.assertGroupsListLenIs(0)

    def testUnlinkedGroupIsUnlinked(self):
        group_uuid = helper.postGroup(self.domain)
        self.assertLooksLikeUUID(group_uuid)
        self.assertGroupsListLenIs(0)

    def testGetMissingLink_Fails404(self):
        linkname = "g1"
        rsp = GET(
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
        self.assertGroupsListLenIs(0)

        req = f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}"
        headers = helper.getRequestHeaders(
                domain=self.domain,
                username=wrong_username)
        payload = {"id": grp1_id}
        rsp = PUT(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 403, "unauthorized put is forbidden")

        self.assertGroupHasNLinks(self.root_uuid, 0)
        self.assertGroupsListLenIs(0)

    def testLinkFromRoot(self):
        linkname = "g1"
        gid = helper.postGroup(self.domain)
        self.assertGroupsListLenIs(0)

        payload = {"id": gid}
        rsp = PUT(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}",
                headers=self.headers,
                data=json.dumps(payload))
        self.assertEqual(rsp.status_code, 201, "problem making link")
        self.assertGroupHasNLinks(self.root_uuid, 1)
        self.assertGroupHasNLinks(gid, 0)
        self.assertGroupsListLenIs(1)

        # get the link and inspect
        expected_keys = GET_LINK_KEYS
        expected_rels = GET_LINK_HREFS_RELS
        rsp = GET(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, f"problem getting {linkname}")
        rspJson = rsp.json()
        self.assertListMembershipEqual(rspJson, GET_LINK_KEYS)
        self.assertHrefsHasOnlyRels(rspJson, GET_LINK_HREFS_RELS)
        rspLink = rspJson["link"]
        self.assertDictEqual(
                rspLink,
                {   "title": linkname,
                    "class": "H5L_TYPE_HARD",
                    "id": gid,
                    "collection": "groups",
                })

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
        self.assertGroupsListLenIs(1)

        for id, kind in ((dset_id, "dataset"), (gid2, "group")):
            payload = {"id": id}
            rsp = PUT(
                    f"{self.endpoint}/groups/{root}/links/{linkname}",
                    headers=self.headers,
                    data=json.dumps(payload))
            self.assertEqual(
                    rsp.status_code,
                    409,
                    f"{kind} should have conflict with existing link")
            self.assertGroupHasNLinks(self.root_uuid, 1)
            self.assertGroupsListLenIs(1)

    def testDeleteLinkWithouPermission_Fails403(self):
        linkname = "g1"
        wrong_username = "test_user2"

        grp1_id = helper.postGroup(self.domain, path=f"/{linkname}")
        self.assertLooksLikeUUID(grp1_id)
        self.assertGroupHasNLinks(self.root_uuid, 1)
        self.assertGroupsListLenIs(1)

        headers = helper.getRequestHeaders(
                domain=self.domain,
                username=wrong_username)
        rsp = DELETE(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}",
                headers=headers)
        self.assertEqual(rsp.status_code, 403, "unauthorized put is forbidden")

        self.assertGroupHasNLinks(self.root_uuid, 1)
        self.assertGroupsListLenIs(1)

    def testDelete(self):
        linkname = "g1"
        grp1_id = helper.postGroup(self.domain, path=f"/{linkname}")
        self.assertLooksLikeUUID(grp1_id)
        self.assertGroupHasNLinks(self.root_uuid, 1)
        self.assertGroupsListLenIs(1)

        rsp = DELETE(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem deleting")

        self.assertGroupHasNLinks(self.root_uuid, 0)
        self.assertGroupsListLenIs(0)
        rsp = GET(
                f"{self.endpoint}/groups/{grp1_id}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "group should still exist")

    def testLinkToBogusID_Fails404(self):
        fake_id = "g-" + str(uuid.uuid1())
        payload = {"id": fake_id}
        rsp = PUT(
                f"{self.endpoint}/groups/{self.root_uuid}/links/nojoy",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(
                rsp.status_code,
                404)

    def testEmptyLinkName_Fails404(self):
        gid = helper.postGroup(self.domain)
        payload = {"id": gid}
        rsp = PUT(
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
        rsp = PUT(
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
        rsp = PUT(
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
        rsp = PUT(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(
                rsp.status_code,
                201)

    def testLinkTwoDatasetsOnCreate_Fails500(self):
        d1 = "d1"
        d11 = "d1.1"
        dset_payload = { # arbitrary
            "type": "H5T_STD_U16LE",
            "dims": [4, 4],
        }
        d1id = helper.postDataset(
                self.domain,
                copy(dset_payload),
                linkpath="/"+d1)

        # link-on-create from dataset
        payload = copy(dset_payload)
        payload["link"] = {
            "id": d1id,
            "name": d11,
        }
        rsp = POST(
                f"{self.endpoint}/datasets",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 500, "should not succeed")

    def testGetLinks_Many_Hard(self):
        headers = self.headers
        endpoint = self.endpoint
        root_id = self.root_uuid
        link_names = [
            "first",
            "second",
            "third",
            "fourth",
            "fifth",
            "sixth",
            "seventh",
            "eighth",
            "ninth",
            "tenth",
            "eleventh",
            "twelfth"
        ]
        sorted_names = sorted(link_names)

        self.assertGroupHasNLinks(self.root_uuid, 0)
        self.assertGroupsListLenIs(0)

        # create subgroups and link them to root using the above names
        for link_name in link_names:
            gid = helper.postGroup(self.domain, path="/"+link_name)
            self.assertTrue(helper.validateId(gid))

        expCount = len(link_names)
        self.assertGroupHasNLinks(self.root_uuid, expCount)
        self.assertGroupsListLenIs(expCount)

        # get all the links for the root group
        rsp = GET(
                f"{endpoint}/groups/{root_id}/links",
                headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = rsp.json()
        links = rspJson["links"]
        for link in links: # verify link integrity
            self.assertEqual(link["class"], "H5L_TYPE_HARD")
            self.assertEqual(link["collection"], "groups")
            self.assertTrue("title" in link)
            #ret_names.append(link["title"])

        # result should come back in sorted order
        ret_names = [link["title"] for link in links]
        self.assertEqual(ret_names, sorted_names)

        # get links with a result limit of 4
        limit=4
        params = { "Limit": limit }
        rsp = GET(
                f"{endpoint}/groups/{root_id}/links",
                params=params,
                headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = rsp.json()
        self.assertTrue("links" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        links = rspJson["links"]
        self.assertEqual(len(links), limit)
        last_link = links[-1]
        self.assertEqual(last_link["title"], sorted_names[limit-1])
        linknames = [link["title"] for link in links]
        self.assertListEqual(linknames, sorted_names[:limit])

        # get links after the one with name: "seventh"
        marker = "seventh"
        params = { "Marker": marker }
        rsp = GET(
                f"{endpoint}/groups/{root_id}/links",
                params=params,
                headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = rsp.json()
        self.assertTrue("links" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        links = rspJson["links"]
        expected_names = [n for n in sorted_names if n > marker]
        names = [link["title"] for link in links]
        self.assertListEqual(names, expected_names)
        last_link = links[-1]
        self.assertEqual(last_link["title"], "twelfth")

        # Use a marker that is not present (should return 404)
        marker = "foobar"
        self.assertTrue(marker not in sorted_names)
        params = { "Marker": marker }
        rsp = GET(
                f"{endpoint}/groups/{root_id}/links",
                params=params,
                headers=headers)
        self.assertEqual(rsp.status_code, 404)

        # get links starting with name: "seventh", and limit to 3 results
        marker = "seventh"
        limit = 3
        params = { "Limit" : 3, "Marker" : "seventh" }
        rsp = GET(
                f"{endpoint}/groups/{root_id}/links",
                params=params,
                headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = rsp.json()
        links = rspJson["links"]
        expected_names = [n for n in sorted_names if n > marker][:limit]
        names = [link["title"] for link in links]
        self.assertListEqual(names, expected_names)
        last_link = links[-1]
        self.assertEqual(last_link["title"], "third")

# ----------------------------------------------------------------------

class LinkTest(helper.TestCase):
    def __init__(self, *args, **kwargs):
        super(LinkTest, self).__init__(*args, **kwargs)

    def testSoftLink(self):
        link_title = "softlink"
        target_path = "somewhere"
        root_id = self.root_uuid
        link_req = f"{self.endpoint}/groups/{root_id}/links/{link_title}"

        self.assertGroupsListLenIs(0)
        self.assertGroupHasNLinks(root_id, 0)

        payload = {"h5path": target_path}
        rsp = PUT(
                link_req,
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 201, "problem creating soft link")

        self.assertGroupsListLenIs(0)
        self.assertGroupHasNLinks(root_id, 1)

        rsp = GET(link_req, headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting soft link")
        rspJson = rsp.json()
        self.assertListMembershipEqual(rspJson, GET_LINK_KEYS)
        self.assertHrefsHasOnlyRels(rspJson, GET_SOFTLINK_HREFS_RELS)
        self.assertDictEqual(
                rspJson["link"],
                {   "title": link_title,
                    "class": "H5L_TYPE_SOFT",
                    "h5path": target_path,
                })

    def testSoftLinkToGroupAddsNoAlias(self):
        hardname = "g1"
        softname = "g1_soft"
        root = helper.getRootUUID(self.domain)
        g1id = helper.postGroup(self.domain, f"/{hardname}")

        self.assertGroupsListLenIs(1)
        self.assertGroupHasNLinks(root, 1)
        rsp = GET(
                f"{self.endpoint}/groups/{g1id}",
                params={"getalias": 1},
                headers=self.headers)
        rspJson = rsp.json()
        self.assertListMembershipEqual(rspJson["alias"], [f"/{hardname}"])

        payload = {"h5path": hardname}
        rsp = PUT(
                f"{self.endpoint}/groups/{root}/links/{softname}",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 201, "problem creating soft link")

        self.assertGroupsListLenIs(1)
        self.assertGroupHasNLinks(root, 2)
        rsp = GET(
                f"{self.endpoint}/groups/{g1id}",
                params={"getalias": 1},
                headers=self.headers)
        rspJson = rsp.json()
        self.assertListMembershipEqual(
                rspJson["alias"],
                [f"/{hardname}"])

    def testExternalLink(self):
        root_id = self.root_uuid
        parent_domain = helper.getParentDomain(self.domain)
        target_domain = f"external_target.{parent_domain}"
        target_path = 'somewhere'
        link_title = 'external_link'

        self.assertGroupsListLenIs(0)
        self.assertGroupHasNLinks(root_id, 0)

        # create external link
        req = f"{self.endpoint}/groups/{root_id}/links/{link_title}"
        payload = {"h5path": target_path, "h5domain": target_domain}
        rsp = PUT(req, data=json.dumps(payload), headers=self.headers)
        self.assertEqual(rsp.status_code, 201, "problem creating ext. link")

        self.assertGroupsListLenIs(0)
        self.assertGroupHasNLinks(root_id, 1)

        # get the link
        req = f"{self.endpoint}/groups/{root_id}/links/{link_title}"
        rsp = GET(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting softlink")
        rspJson = rsp.json()
        self.assertListMembershipEqual(rspJson, GET_LINK_KEYS)
        self.assertHrefsHasOnlyRels(rspJson, GET_SOFTLINK_HREFS_RELS)
        rspLink = rspJson["link"]
        self.assertDictEqual(
                rspLink,
                {   "title": link_title,
                    "class": "H5L_TYPE_EXTERNAL",
                    "h5path": target_path,
                    "h5domain": target_domain,
                })

# ----------------------------------------------------------------------

@unittest.skipUnless(
        config.get("test_on_uploaded_file"),
        "don't test without uploaded file")
class LinksInFileTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(LinksInFileTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)

    def testGet(self):
        # test getting links from an existing domain
        domain = helper.getTestDomain("tall.h5")
        headers = helper.getRequestHeaders(domain=domain)

        # verify domain exists
        req = helper.getEndpoint() + '/'
        rsp = GET(req, headers=headers)
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
        rsp = GET(req, headers=headers)
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
        rsp = GET(req, headers=headers)
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

# ----------------------------------------------------------------------

if __name__ == '__main__':
    unittest.main()

