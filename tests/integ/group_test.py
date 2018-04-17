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
import json
import time
import unittest
from requests import delete as DELETE, get as GET, post as POST, put as PUT
import config
import helper

GET_GROUP_BY_ID_KEYS = [
    "attributeCount",
    "created",
    "domain",
    "hrefs",
    "id",
    "lastModified",
    "linkCount",
    "root",
]
GET_GROUP_BY_ID_RELS = [
    "attributes",
    "home",
    "links",
    "root",
    "self"
]

POST_GROUP_KEYS = [
    "attributeCount",
    "created",
    "id",
    "lastModified",
    "linkCount",
    "root",
]

# ----------------------------------------------------------------------

class GroupTest(helper.TestCase):
    def __init__(self, *args, **kwargs):
        super(GroupTest, self).__init__(*args, **kwargs)

    def testGetRootGroup(self):
        req = f"{self.endpoint}/groups/{self.root_uuid}"

        # get domain
        rsp = GET(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = rsp.json()
        self.assertJSONHasOnlyKeys(rspJson, GET_GROUP_BY_ID_KEYS)
        self.assertHrefsHasOnlyRels(rspJson, GET_GROUP_BY_ID_RELS)
        self.assertEqual(rspJson["domain"], self.domain)

        # try get with alias
        params = {"getalias": 1}
        rsp = GET(req, params=params, headers=self.headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = rsp.json()
        get_keys = GET_GROUP_BY_ID_KEYS[:]
        get_keys.append("alias")
        self.assertJSONHasOnlyKeys(rspJson, get_keys)
        self.assertHrefsHasOnlyRels(rspJson, GET_GROUP_BY_ID_RELS)
        self.assertListEqual(rspJson["alias"], ['/'], "root should have alias")

        # try get with a different user (who has read permission)
        otheruser_headers = helper.getRequestHeaders(
                domain=self.domain,
                username="test_user2") # TODO: programmatic selection?
        rsp = GET(req, headers=otheruser_headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = rsp.json()
        self.assertEqual(rspJson["root"], self.root_uuid)

        # try to do a GET with a different domain (should fail)
        another_domain = helper.getParentDomain(self.domain)
        otherdomain_headers = helper.getRequestHeaders(domain=another_domain)
        rsp = GET(req, headers=otherdomain_headers)
        self.assertEqual(rsp.status_code, 400)

    def testGetInvalidUUID(self):
        invalid_uuid = "foobar"  
        req = f"{self.endpoint}/groups/{invalid_uuid}"
        rsp = GET(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 400)

        import uuid
        bad_uuid = "g-" + str(uuid.uuid1())    
        req = f"{self.endpoint}/groups/{bad_uuid}"
        rsp = GET(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 404)

    def testPost(self):
        req = f"{self.endpoint}/groups"

        # create a new group
        rsp = POST(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 201) 
        rspJson = rsp.json()
        self.assertJSONHasOnlyKeys(rspJson, POST_GROUP_KEYS)
        self.assertEqual(rspJson["linkCount"], 0)   
        self.assertEqual(rspJson["attributeCount"], 0)   
        group_id = rspJson["id"]
        self.assertTrue(helper.validateId(group_id))

        # verify we can do a get on the new group
        req = f"{self.endpoint}/groups/{group_id}"
        rsp = GET(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = rsp.json()
        self.assertJSONHasOnlyKeys(rspJson, GET_GROUP_BY_ID_KEYS)
        self.assertHrefsHasOnlyRels(rspJson, GET_GROUP_BY_ID_RELS)
        self.assertEqual(rspJson["id"], group_id)
        self.assertNotEqual(rspJson["root"], group_id)
        self.assertEqual(rspJson["domain"], self.domain)

        # try getting the path of the group 
        params = {"getalias": 1}
        rsp = GET(req, params=params, headers=self.headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = rsp.json()
        self.assertTrue("alias" in rspJson)
        self.assertEqual(rspJson["alias"], [])

        # try POST with user who doesn't have create permission on this domain
        headers = helper.getRequestHeaders(
                domain=self.domain,
                username="test_user2")
        req = f"{self.endpoint}/groups"
        rsp = POST(req, headers=headers)
        self.assertEqual(rsp.status_code, 403) # forbidden

    def testPostWithLink(self):
        linkname = "linked_group"

        # get root group and verify link count is 0
        rsp = GET(
                f"{self.endpoint}/groups/{self.root_uuid}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = rsp.json()
        self.assertEqual(rspJson["linkCount"], 0)

        # create new group  
        payload = {
            "link": {
                "id": self.root_uuid,
                "name": linkname 
            }
        }
        rsp = POST(
                f"{self.endpoint}/groups",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 201) 
        rspJson = rsp.json()
        self.assertJSONHasOnlyKeys(rspJson, POST_GROUP_KEYS)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], 0)
        new_group_id = rspJson["id"]
        self.assertTrue(helper.validateId(rspJson["id"]) )
        self.assertTrue(new_group_id != self.root_uuid)

        # get root group and verify link count is 1
        rsp = GET(
                f"{self.endpoint}/groups/{self.root_uuid}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = rsp.json()
        self.assertEqual(rspJson["linkCount"], 1)

        # read the link back and verify
        rsp = GET(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{linkname}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = rsp.json()
        self.assertDictEqual(
                rspJson["link"],
                {   "collection": "groups",
                    "class": "H5L_TYPE_HARD",
                    "title": linkname,
                    "id": new_group_id,
                })

        # try getting the path of the group 
        params = {"getalias": 1}
        rsp = GET(
                f"{self.endpoint}/groups/{new_group_id}",
                params=params,
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = rsp.json()
        self.assertTrue("alias" in rspJson)
        aliasname = "/" + linkname
        self.assertEqual(rspJson["alias"], [aliasname,])

    def testDelete(self):
        other_user = "test_user2" # TODO: configurable? tied to installation?

        # create a new group
        group_id = helper.postGroup(self.domain)

        # verify we can do a get on the new group
        req = f"{self.endpoint}/groups/{group_id}"
        rsp = GET(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting new group")

        # try DELETE with user who lacks create permission on this domain
        headers = helper.getRequestHeaders(
                domain=self.domain,
                username=other_user)
        rsp = DELETE(req, headers=headers)
        self.assertEqual(
                rsp.status_code,
                403,
                "unauthorized delete is forbidden")

        # try to do a DELETE with a different domain (should fail)
        another_domain = helper.getParentDomain(self.domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        req = f"{self.endpoint}/groups/{group_id}"
        rsp = DELETE(req, headers=headers)
        self.assertEqual(
                rsp.status_code,
                400,
                "can't delete group in nonexistent domain")

        # delete the new group
        rsp = DELETE(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem while deleting")
        rspJson = rsp.json()
        self.assertDictEqual(rsp.json(), {})

        # a get for the group should now return 410 (GONE)
        rsp = GET(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 410, "group should be gone")

        # try deleting the root group
        root_id = helper.getRootUUID(self.domain)
        req = f"{self.endpoint}/groups/{root_id}"
        rsp = DELETE(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 403, "delete root not allowed")

    def getLinkIDsFromGetGroupLinks(self, _json):
        return [l["id"] for l in _json["links"]]

    def testDeleteGroupWithChildGroup(self):
        g1 = "g1"
        g11 = "g1.1"
        g1id = helper.postGroup(self.domain, path="/"+g1)
        g11id = helper.postGroup(self.domain, path=f"/{g1}/{g11}")

        # groups list has g1 and g11
        rsp = GET(
                f"{self.endpoint}/groups",
                headers=self.headers)
        self.assertListMembershipEqual(rsp.json()["groups"], [g1id, g11id])

        # root has one link (to g1)
        rsp = GET(
                f"{self.endpoint}/groups/{self.root_uuid}/links",
                headers=self.headers)
        self.assertListMembershipEqual(
                self.getLinkIDsFromGetGroupLinks(rsp.json()),
                [g1id])

        # g1 has one link (to g1.1)
        rsp = GET(
                f"{self.endpoint}/groups/{g1id}/links",
                headers=self.headers)
        self.assertListMembershipEqual(
                self.getLinkIDsFromGetGroupLinks(rsp.json()),
                [g11id])

        # g11 has no links
        rsp = GET(
                f"{self.endpoint}/groups/{g11id}",
                headers=self.headers)
        self.assertEqual(rsp.json()["linkCount"], 0, "g11 should have 0 links")

        # DELETE Group g1
        rsp = DELETE(
                f"{self.endpoint}/groups/{g1id}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem while deleting")

        # root's link to g1 should persist
        rsp = GET(
                f"{self.endpoint}/groups/{self.root_uuid}",
                headers=self.headers)
        self.assertEqual(
                rsp.json()["linkCount"],
                1,
                "link to deleted group should persist")
        rsp = GET(
                f"{self.endpoint}/groups/{self.root_uuid}/links",
                headers=self.headers)
        self.assertListMembershipEqual(
                self.getLinkIDsFromGetGroupLinks(rsp.json()),
                [g1id])

        # g1 is GONE
        rsp = GET(
                f"{self.endpoint}/groups/{g1id}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 410, "g1 should be GONE")

        # g11 has no links
        rsp = GET(
                f"{self.endpoint}/groups/{g11id}",
                headers=self.headers)
        self.assertEqual(rsp.json()["linkCount"], 0)

        # cannot get groups list with defunct link
        rsp = GET(
                f"{self.endpoint}/groups",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 410, "problem expected (410)")

        # cannot get child group (g11) by path, as parent is GONE
        with self.assertRaises(KeyError):
            _ = helper.getUUIDByPath(self.domain, f"/{g1}/{g11}")

        # DELETE dead Link to g1
        rsp = DELETE(
                f"{self.endpoint}/groups/{self.root_uuid}/links/{g1}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem deleting link")

        # groups list is empty
        rsp = GET(
                f"{self.endpoint}/groups",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting groups list")
        self.assertListMembershipEqual(rsp.json()["groups"], [])

        # still cannot get child group (g1.1) by path
        with self.assertRaises(KeyError):
            _ = helper.getUUIDByPath(self.domain, f"/{g1}/{g11}")

# ----------------------------------------------------------------------

@unittest.skipUnless(config.get("test_on_uploaded_file"), "requires file")
class GroupsInFileTest(unittest.TestCase):
    def testGet(self):
        domain = helper.getTestDomain("tall.h5")

        headers = helper.getRequestHeaders(domain=domain)

        # verify domain exists
        req = helper.getEndpoint() + '/'
        rsp = GET(req, headers=headers)
        self.assertEqual(rsp.status_code, 200, "Unable to get testfile domain")

        rspJson = json.loads(rsp.text)
        grp_uuid = root_uuid = rspJson["root"]
        self.assertTrue(grp_uuid.startswith("g-"))

        # get the group json
        req = helper.getEndpoint() + '/groups/' + grp_uuid
        rsp = GET(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for name in ("id", "hrefs", "attributeCount", "linkCount", 
            "domain", "root", "created", "lastModified"):
            self.assertTrue(name in rspJson)

        self.assertEqual(rspJson["id"], grp_uuid) 

        hrefs = rspJson["hrefs"]
        self.assertEqual(len(hrefs), 5)
        self.assertEqual(rspJson["id"], grp_uuid)
        self.assertEqual(rspJson["attributeCount"], 2)
        self.assertEqual(rspJson["linkCount"], 2)
        self.assertEqual(rspJson["root"], root_uuid)
        self.assertEqual(rspJson["domain"], domain)
        now = time.time()
        # the object shouldn't have been just created or updated
        self.assertTrue(rspJson["created"] < now - 60 * 5)
        self.assertTrue(rspJson["lastModified"] < now - 60 * 5)

        # request the group path
        req = helper.getEndpoint() + '/groups/' + grp_uuid
        params = {"getalias": 1}
        rsp = GET(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("alias" in rspJson)
        self.assertEqual(rspJson["alias"], ['/'])

        # verify trying to read this group from a different domain fails
        other_domain = helper.getTestDomainName(self.__class__.__name__)
        headers = helper.getRequestHeaders(domain=other_domain)
        req = helper.getEndpoint() + '/groups/' + grp_uuid
        rsp = GET(req, headers=headers)
        self.assertEqual(rsp.status_code, 400) 

    def testGetByPath(self):
        domain = helper.getTestDomain("tall.h5")
        headers = helper.getRequestHeaders(domain=domain)

        # verify domain exists
        req = helper.getEndpoint() + '/'
        rsp = GET(req, headers=headers)
        self.assertEqual(rsp.status_code, 200, "Unable to get testfile domain")

        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]

        # get the group at "/g1/g1.1"
        h5path = "/g1/g1.1"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": h5path}
        rsp = GET(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)

        rspJson = json.loads(rsp.text)
        for name in ("id", "hrefs", "attributeCount", "linkCount", 
            "domain", "root", "created", "lastModified"):
            self.assertTrue(name in rspJson)

        # verify we get the same id when following the path via service calls
        g11id = helper.getUUIDByPath(domain, "/g1/g1.1")
        self.assertEqual(g11id, rspJson["id"])

        # Try with a trailing slash
        h5path = "/g1/g1.1/"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": h5path}
        rsp = GET(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)

        rspJson = json.loads(rsp.text)
        self.assertEqual(g11id, rspJson["id"])

        # try relative h5path
        g1id = helper.getUUIDByPath(domain, "/g1/")
        h5path = "./g1.1"
        req = helper.getEndpoint() + "/groups/" + g1id
        params = {"h5path": h5path}
        rsp = GET(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(g11id, rspJson["id"])

        # try a invalid link and verify a 404 is returened
        h5path = "/g1/foobar"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": h5path}
        rsp = GET(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 404)

        # try passing a path to a dataset and verify we get 404
        h5path = "/g1/g1.1/dset1.1.1"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": h5path}
        rsp = GET(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 404)

        # try getting the path of the group 
        req = helper.getEndpoint() + "/groups/" + g11id 
        params = {"getalias": 1}
        rsp = GET(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("alias" in rspJson)
        self.assertEqual(rspJson["alias"], ['/g1/g1.1',])

# ----------------------------------------------------------------------

if __name__ == '__main__':
    unittest.main()

