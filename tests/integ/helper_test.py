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
"""Quality-assurance for helper module."""
import unittest
import requests
import helper

class TestGetDNSDomain(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(helper.getDNSDomain(""), "")

    def test_root(self):
        self.assertEqual(helper.getDNSDomain("/"), "")

    def test_None(self):
        with self.assertRaises(AttributeError):
            helper.getDNSDomain(None)

    def test_nominal(self):
        self.assertEqual(
                helper.getDNSDomain("/path/to/a/file"),
                "file.a.to.path")

# ----------------------------------------------------------------------

class GetUUIDByPathTest(helper.TestCase):
    def __init__(self, *args, **kwargs):
        super(GetUUIDByPathTest, self).__init__(*args, **kwargs)

    def testGetByPath(self):
        payload = {
            "type": "H5T_STD_U32LE",
            "shape": [8],
        }

        # create and link all groups and datasets
        # /
        # /dset0
        # /g1/dset10
        # /g2/g21/dset210
        # /dset210_soft   (soft link to /g2/g21/dset210)
        g1id = helper.postGroup(self.domain, "/g1")
        g2id = helper.postGroup(self.domain, "/g2")
        g21id = helper.postGroup(self.domain, "/g2/g21")
        d0id = helper.postDataset(self.domain, payload, "/dset0")
        d10id = helper.postDataset(self.domain, payload, "/g1/dset10")
        d210id = helper.postDataset(self.domain, payload, "/g2/g21/dset210")
        rsp = requests.put(
                f"{self.endpoint}/groups/{self.root_uuid}/links/dset210_soft", 
                data='{"h5path": "/g2/g21/dset210"}',
                headers=self.headers)
        self.assertEqual(rsp.status_code, 201, "unable to link dset210_soft")

        # TESTS

        self.assertEqual(
                helper.getUUIDByPath(self.domain, "/"),
                self.root_uuid,
                "root group")

        self.assertEqual(
                helper.getUUIDByPath(self.domain, "/g1"),
                g1id,
                "group linked from root")

        self.assertEqual(
                helper.getUUIDByPath(self.domain, "/dset0"),
                d0id,
                "dataset linked from root")

        self.assertEqual(
                helper.getUUIDByPath(self.domain, "/g2/g21"),
                g21id,
                "group linked away from root")

        self.assertEqual(
                helper.getUUIDByPath(self.domain, "/g2/g21/dset210"),
                d210id,
                "dataset linked away from root")

        # unable to get softlink to dataset from root
        with self.assertRaises(KeyError):
            helper.getUUIDByPath(self.domain, "/dset210_soft")

        # object does not exist at target path
        with self.assertRaises(KeyError):
            helper.getUUIDByPath(self.domain, "/nonexistent/thing")

        # object does not exist at target path (partially viable)
        with self.assertRaises(KeyError):
            helper.getUUIDByPath(self.domain, "/g1/dset_missing")

        # path must begin with a slash
        with self.assertRaises(KeyError):
            helper.getUUIDByPath(self.domain, "g2/g21")

# ----------------------------------------------------------------------

class PostDatasetTest(helper.TestCase):
    datatype = {"type": "H5T_STD_U32LE"}

    def __init__(self, *args, **kwargs):
        super(PostDatasetTest, self).__init__(*args, **kwargs)

    def testPostAndGetViaUUID(self):
        did = helper.postDataset(
                self.domain,
                self.datatype)
        self.assertLooksLikeUUID(did)

        get_rsp = requests.get(
                f"{self.endpoint}/datasets/{did}",
                headers=self.headers)
        self.assertEqual(get_rsp.status_code, 200, "unable to get dataset")

    def testPutWithLinkFromRoot(self):
        linkpath = "/dset0" # attached to root group
        did = helper.postDataset(
                self.domain,
                self.datatype,
                linkpath=linkpath)
        self.assertLooksLikeUUID(did)

        # can get back by ID
        get_rsp = requests.get(
                f"{self.endpoint}/datasets/{did}",
                headers=self.headers)
        self.assertEqual(get_rsp.status_code, 200, "problem getting via ID")

        # TEST - get via link
        linkname = linkpath[1:] # remove root slash
        root = helper.getRootUUID(self.domain)
        get_rsp = requests.get(
                f"{self.endpoint}/groups/{root}/links/{linkname}",
                headers=self.headers)
        self.assertEqual(get_rsp.status_code, 200, "problem getting via link")

    def testGetBackFullResponse(self):
        rsp = helper.postDataset(self.domain, self.datatype, response=True)
        self.assertEqual(
                rsp.status_code,
                201,
                "should have CREATED status code")
        rspJson = rsp.json()
        self.assertLooksLikeUUID(rspJson["id"])

    # TODO: link conflicts (err)
    # TODO: link to child group (err)
    # TODO: link to non-group parent (err)
    # TODO: invalid datatype (err)
    # TODO: type-checking?
    # TODO: pass-in username and password

# ----------------------------------------------------------------------

class PostGroupTest(helper.TestCase):
    def __init__(self, *args, **kwargs):
        super(PostGroupTest, self).__init__(*args, **kwargs)

    def testCreateAndLink(self):
        path = "/group1"
        gid = helper.postGroup(self.domain, path=path)
        self.assertLooksLikeUUID(gid)
        uuid = helper.getUUIDByPath(self.domain, path)
        self.assertEqual(uuid, gid)

    def testJustPost(self):
        # verify starting condition
        rsp = requests.get(
                f"{self.endpoint}/groups/{self.root_uuid}",
                headers=self.headers)
        rspJson = rsp.json()
        self.assertEqual(rspJson["linkCount"], 0, "should have no links")
        rsp = requests.get(
                f"{self.endpoint}/groups",
                headers=self.headers)
        rspJson = rsp.json()
        self.assertEqual(rspJson["groups"], [], "expect no non-root groups")

        # create
        gid = helper.postGroup(self.domain)

        # verify end condition
        rsp = requests.get(
                f"{self.endpoint}/groups/{self.root_uuid}",
                headers=self.headers)
        rspJson = rsp.json()
        self.assertEqual(rspJson["linkCount"], 0, "should have no links")
        rsp = requests.get(
                f"{self.endpoint}/groups",
                headers=self.headers)
        rspJson = rsp.json()
        self.assertEqual(rspJson["groups"], [], "new group not part of tree")
        rsp = requests.get(
                f"{self.endpoint}/groups/{gid}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting by UUID")

    def testGetResponseBack(self):
        response = helper.postGroup(self.domain, response=True)
        self.assertEqual(response.status_code, 201, "should report CREATED")
        rspJson = response.json()
        self.assertEqual(type(rspJson), dict)

# ----------------------------------------------------------------------

class HelperVerificationRoutinesTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(HelperVerificationRoutinesTest, self).__init__(*args, **kwargs)

    def testListMembership(self):
        with self.assertRaises(TypeError):
            helper.verifyListMembership(self, None, [])

        with self.assertRaises(TypeError):
            helper.verifyListMembership(self, [], None)

        # unable to sort heterogeneous types
        with self.assertRaises(TypeError):
            helper.verifyListMembership(self, ["a", 7], [7, "a"])

        with self.assertRaises(AssertionError):
            helper.verifyListMembership(self, [3], [3,1])

        with self.assertRaises(AssertionError):
            helper.verifyListMembership(self, [3,2], [3,1])

        with self.assertRaises(AssertionError):
            helper.verifyListMembership(self, [3,2], [])

        helper.verifyListMembership(self, [], [])
        helper.verifyListMembership(self, [3,2], [3,2])
        helper.verifyListMembership(self, [2,3], [3,2])
        helper.verifyListMembership(self, [1,2,3,4,5], [5,4,3,1,2])

        # tuple sequence intepreted as valid sequence
        helper.verifyListMembership(self, (3,1), [3,1])

        # dictionary's keys interpreted as valid sequence
        helper.verifyListMembership(self, {"a": 5}, ["a"])
        with self.assertRaises(AssertionError):
            helper.verifyListMembership(self, [], {"a": 5})

    def testDictKeys(self):
        helper.verifyDictionaryKeys(
                self,
                {"a":5, "L": 2, "None": []},
                ["a", "L", "None"])

        with self.assertRaises(AssertionError):
            helper.verifyDictionaryKeys(self, {"a":5, "b":7}, ["b"])

        with self.assertRaises(AssertionError):
            helper.verifyDictionaryKeys(self, {"a":5}, ["b", "a"])

        with self.assertRaises(AssertionError):
            helper.verifyDictionaryKeys(self, {"a":5, "b":7}, ["7"])

    def testHrefsInJSON(self):
        helper.verifyRelsInJSONHrefs(self, {"hrefs": []}, [])

        _json = {
            "hrefs": [
                {"rel": "a", "href": "http"},
                {"rel": "b", "href": "http"},
                {"rel": "c", "href": "http"},
            ],
        }

        helper.verifyRelsInJSONHrefs(self, _json, ["a", "b", "c"])

        with self.assertRaises(AssertionError):
            helper.verifyRelsInJSONHrefs(self, _json, ["b", "c"])

        with self.assertRaises(AssertionError):
            helper.verifyRelsInJSONHrefs(self, _json, ["b", "c", "a", "d"])

# ----------------------------------------------------------------------

class HelperTestCaseTest(helper.TestCase):
    def __init__(self, *args, **kwargs):
        super(HelperTestCaseTest, self).__init__(*args, **kwargs)

    def testGetRoot(self):
        expKeys = [
            "attributeCount",
            "created",
            "domain",
            "hrefs",
            "id",
            "lastModified",
            "linkCount",
            "root",
        ]
        expRels = [
            "attributes",
            "home",
            "links",
            "root",
            "self",
        ]
        rsp = requests.get(
                f"{self.endpoint}/groups/{self.root_uuid}",
                headers=self.headers)
        rspJson = rsp.json()
        self.assertJSONHasOnlyKeys(rspJson, expKeys)
        self.assertHrefsHasOnlyRels(rspJson, expRels)
        self.assertEqual(rspJson["domain"], self.domain)

# ----------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main()


