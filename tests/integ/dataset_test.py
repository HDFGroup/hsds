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
import time
import config
import helper

# min/max chunk size - these can be set by config, but 
# practially the min config value should be larger than 
# CHUNK_MIN and the max config value should less than 
# CHUNK_MAX
CHUNK_MIN = 1024                # lower limit  (1024b)
CHUNK_MAX = 50*1024*1024        # upper limit (50M) 

POST_DATASET_KEYS = [
    "id",
    "root",
    "shape",
    "type",
    "attributeCount",
    "created",
    "lastModified",
]

GET_DATASET_KEYS = [
    "id",
    "type",
    "shape",
    "hrefs",
    "layout",
    "creationProperties",
    "attributeCount",
    "created",
    "lastModified",
    "root",
    "domain",
]
GET_DATASET_HREFS_RELS = [
    "attributes",
    "data",
    "home",
    "root",
    "self",
]

LIST_DATASETS_KEYS = [
    "datasets",
    "hrefs",
]
LIST_DATASETS_HREFS_RELS = [
    "home",
    "root",
    "self",
]

GET_SHAPE_KEYS = [
    "created",
    "hrefs",
    "lastModified",
    "shape",
]
GET_SHAPE_RELS = [
    "owner",
    "root",
    "self",
]

def _assertLooksLikeUUID(testcase, s):
    testcase.assertTrue(helper.validateId(s), "probably not UUID: " + s)

def _assertDictHasOnlyKeys(testcase, _d, _keys):
    for key in _keys:
        testcase.assertTrue(key in _d, f"missing key `{key}`")
    testcase.assertEqual(len(_d), len(_keys), "extra keys")

def _assertHrefsHasOnlyRels(testcase, _hrefs, _rels):
    href_rels = [item["rel"] for item in _hrefs]
    for rel in _rels:
        testcase.assertTrue(rel in href_rels, f"missing rel `{rel}`")
    testcase.assertEqual(len(href_rels), len(_rels), "extra rels")

def _verifyShape(testcase, dset_uuid, shapedict):
    # requires that the test case have:
    # + 'endpoint' uri to hsds endpoint
    # + 'headers' request headers targeting the relevant domain
    rsp = requests.get(
            f"{testcase.endpoint}/datasets/{dset_uuid}/shape",
            headers=testcase.headers)
    testcase.assertEqual(rsp.status_code, 200, "unable to get shape")
    rspJson = rsp.json()
    _assertDictHasOnlyKeys(testcase, rspJson, GET_SHAPE_KEYS)
    _assertHrefsHasOnlyRels(testcase, rspJson["hrefs"], GET_SHAPE_RELS)
    testcase.assertDictEqual(rspJson["shape"], shapedict)

class CommonDatasetOperationsTest(unittest.TestCase):
    base_domain = None
    root_uuid = None
    endpoint = None
    headers = None
    assertLooksLikeUUID = _assertLooksLikeUUID
    given_payload = {"type": "H5T_IEEE_F32LE"} # arbitrary scalar datatype
    given_dset_id = None

    @classmethod
    def setUpClass(cls):
        """Do one-time setup prior to any tests.

        Prepare domain and post one scalar dataset.
        Populates class variables.
        """
        cls.base_domain = helper.getTestDomainName(cls.__name__)
        cls.headers = helper.getRequestHeaders(domain=cls.base_domain)
        helper.setupDomain(cls.base_domain)
        cls.root_uuid = helper.getRootUUID(cls.base_domain)
        cls.endpoint = helper.getEndpoint()
        cls.given_dset_id = helper.postDataset(
                cls.base_domain,
                cls.given_payload)

    def setUp(self):
        """Sanity checks before each test."""
        assert helper.validateId(helper.getRootUUID(self.base_domain)) == True
        assert self.headers is not None
        get_given = requests.get(
                f"{self.endpoint}/datasets/{self.given_dset_id}",
                headers = self.headers)
        assert get_given.status_code == 200, "given dataset inexplicably gone"
        assert helper.validateId(self.given_dset_id) == True
        assert get_given.json()["id"] == self.given_dset_id

    def testPost(self):
        data = { "type": "H5T_IEEE_F32LE" } # arbitrary
        req = f"{self.endpoint}/datasets"
        rsp = requests.post(req, data=json.dumps(data), headers=self.headers)
        self.assertEqual(rsp.status_code, 201, "problem creating dataset")
        rspJson = rsp.json()
        self.assertEqual(rspJson["attributeCount"], 0)
        dset_id = rspJson["id"]
        self.assertLooksLikeUUID(dset_id)
        for name in POST_DATASET_KEYS:
            self.assertTrue(name in rspJson, name)
        self.assertEqual(len(rspJson), len(POST_DATASET_KEYS))

    def testGet(self):
        req = f"{self.endpoint}/datasets/{self.given_dset_id}"
        rsp = requests.get(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting dataset")
        rspJson = rsp.json()
        for name in GET_DATASET_KEYS:
            self.assertTrue(name in rspJson, name)
        self.assertEqual(len(rspJson), len(GET_DATASET_KEYS))
        self.assertEqual(rspJson["id"], self.given_dset_id)
        self.assertEqual(rspJson["root"], self.root_uuid) 
        self.assertEqual(rspJson["domain"], self.base_domain) 
        self.assertEqual(rspJson["attributeCount"], 0)
        self.assertEqual(type(rspJson["type"]), dict)
        self.assertEqual(type(rspJson["shape"]), dict)

    def testGetType(self):
        req = f"{self.endpoint}/datasets/{self.given_dset_id}/type"
        rsp = requests.get(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting dset's type")
        rspJson = rsp.json()
        self.assertEqual(len(rspJson), 2)
        self.assertEqual(type(rspJson["type"]), dict)
        self.assertEqual(len(rspJson["hrefs"]), 3) 

    def testGetShape(self):
        req = f"{self.endpoint}/datasets/{self.given_dset_id}/shape"
        rsp = requests.get(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting dset's shape")
        rspJson = rsp.json()
        self.assertEqual(len(rspJson), 4)
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertEqual(type(rspJson["shape"]), dict)
        self.assertEqual(len(rspJson["hrefs"]), 3)

    def testGet_VerboseNotYetImplemented(self):
        req = f"{self.endpoint}/datasets/{self.given_dset_id}"
        params = {"verbose": 1}
        rsp = requests.get(req, headers=self.headers, params=params)
        self.assertEqual(rsp.status_code, 200, "problem getting dataset")
        rspJson = rsp.json()
        self.assertFalse("num_chunks" in rspJson)
        self.assertFalse("allocated_size" in rspJson)
        for name in GET_DATASET_KEYS:
            self.assertTrue(name in rspJson, name)
        self.assertEqual(len(rspJson), len(GET_DATASET_KEYS))
        self.assertEqual(rspJson["id"], self.given_dset_id)
        self.assertEqual(rspJson["root"], self.root_uuid) 
        self.assertEqual(rspJson["domain"], self.base_domain) 
        self.assertEqual(rspJson["attributeCount"], 0)
        self.assertEqual(type(rspJson["type"]), dict)
        self.assertEqual(type(rspJson["shape"]), dict)

    def testGet_OtherUserAuthorizedRead(self):
        other_user = "test_user2"
        self.assertNotEqual(other_user, config.get("user_name"))
        req = f"{self.endpoint}/datasets/{self.given_dset_id}"
        headers = helper.getRequestHeaders(
                domain=self.base_domain,
                username=other_user)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200, "unable to get dataset")
        self.assertEqual(rsp.json()["id"], self.given_dset_id)

    def testGetFromOtherDomain_Fails400(self):
        req = f"{self.endpoint}/datasets/{self.given_dset_id}"
        another_domain = helper.getParentDomain(self.base_domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        response = requests.get(req, headers=headers)
        self.assertEqual(response.status_code, 400, "fail 400 to hide details")

    def testDelete_OtherUserWithoutPermission_Fails403(self):
        other_user = "test_user2" # TODO: THIS DEPENDS ON 'test_user2' BEING A RECOGNZIED USER? HOW TO MAKE PROGRAMMATICALLY VALID?
        self.assertNotEqual(other_user, config.get("user_name"))
        datatype = { "type": "H5T_IEEE_F32LE" }
        dset_id = helper.postDataset(self.base_domain, datatype)

        req = f"{self.endpoint}/datasets/{dset_id}"
        headers = helper.getRequestHeaders(
                domain=self.base_domain,
                username=other_user)
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 403, "should be forbidden")

    def testDelete_UnknownUser_Fails401(self):
        other_user = config.get("user_name")[::-1] # reversed copy
        self.assertNotEqual(other_user, config.get("user_name"))
        datatype = { "type": "H5T_IEEE_F32LE" }
        dset_id = helper.postDataset(self.base_domain, datatype)

        req = f"{self.endpoint}/datasets/{dset_id}"
        headers = helper.getRequestHeaders(
                domain=self.base_domain,
                username=other_user)
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 401, "should be unauthorized")

    def testDeleteInOtherDomain_Fails400(self):
        req = f"{self.endpoint}/datasets/{self.given_dset_id}"
        another_domain = helper.getParentDomain(self.base_domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        response = requests.delete(req, headers=headers)
        self.assertEqual(response.status_code, 400, "fail 400 to hide details")

    def testDelete(self):
        datatype = {"type": "H5T_IEEE_F32LE"} # arbitrary
        dset_id = helper.postDataset(self.base_domain, datatype)
        req = f"{self.endpoint}/datasets/{dset_id}"

        get_rsp = requests.get(req, headers=self.headers)
        self.assertEqual(get_rsp.status_code, 200, "should be OK")

        del_rsp = requests.delete(req, headers=self.headers)
        self.assertEqual(del_rsp.status_code, 200, "problem deleting dataset")
        self.assertDictEqual(del_rsp.json(), {}, "should return empty object")

        get_rsp = requests.get(req, headers=self.headers)
        self.assertEqual(
                get_rsp.status_code,
                410,
                "should be GONE")

    def testDeleteWhileStillLinked(self):
        domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(domain)
        headers = helper.getRequestHeaders(domain=domain)
        root = helper.getRootUUID(domain)

        payload = { "type": "H5T_IEEE_F32LE" }
        did0 = helper.postDataset(domain, payload, linkpath="/d0")
        did1 = helper.postDataset(domain, payload, linkpath="/d1")

        # verify setup
        res = requests.get(
                f"{self.endpoint}/groups/{root}/links",
                headers=headers)
        links = res.json()["links"]
        self.assertEqual(len(links), 2, "only the two links") 
        res = requests.get(f"{self.endpoint}/datasets", headers=headers)
        dsets = res.json()["datasets"]
        for id in (did0, did1) :
            self.assertTrue(id in dsets, f"missing {id}")
        self.assertEqual(len(dsets), 2, "only the two datasets")

        # delete d1 while still linked
        res = requests.delete(
                f"{self.endpoint}/datasets/{did0}",
                headers=headers)
        self.assertEqual(res.status_code, 200, "unable to delete dset")

        # verify domain structure
        res = requests.get(f"{self.endpoint}/datasets/{did0}", headers=headers)
        self.assertEqual(res.status_code, 410, "d0 should be gone")
        res = requests.get(f"{self.endpoint}/datasets/{did1}", headers=headers)
        self.assertEqual(res.status_code, 200, "d1 should be ok")
        res = requests.get(
                f"{self.endpoint}/groups/{root}/links",
                headers=headers)
        links = res.json()["links"]
        self.assertEqual(len(links), 1, "only the one link") 
        res = requests.get(f"{self.endpoint}/datasets", headers=headers)
        dsets = res.json()["datasets"]
        self.assertEqual(dsets, [did1], "only the one dset should remain")

    @unittest.skip("TODO")
    def testPostWithMalformedPayload(self):
        pass

class ListDomainDatasetsTest(unittest.TestCase):
    assertJSONHasOnlyKeys = _assertDictHasOnlyKeys
    assertHrefsHasOnlyRels = _assertHrefsHasOnlyRels

    def __init__(self, *args, **kwargs):
        super(ListDomainDatasetsTest, self).__init__(*args, **kwargs)
        self.domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.domain)
        self.endpoint = helper.getEndpoint()
        self.headers = helper.getRequestHeaders(domain=self.domain)

    def testListDatasetsUnlinked(self):
        dtype = {"type": "H5T_STD_U32LE"} # arbitrary
        dset0 = helper.postDataset(self.domain, dtype)

        root = helper.getRootUUID(self.domain)
        res = requests.get(
                f"{self.endpoint}/groups/{root}",
                headers=self.headers)
        self.assertEqual(res.json()["linkCount"], 0, "should have no links")

        res = requests.get(
                f"{self.endpoint}/datasets",
                headers=self.headers)
        res_json = res.json()
        dset_list = res_json["datasets"]
        self.assertEqual(dset_list, [], "list should be empty")
        self.assertJSONHasOnlyKeys(res_json, LIST_DATASETS_KEYS)
        hrefs_list = res_json["hrefs"]
        self.assertHrefsHasOnlyRels(hrefs_list, LIST_DATASETS_HREFS_RELS)

    def testListDatasetsLinkedToRoot(self):
        dset_names = [
            "dest0",
            "dset1",
            "dset2"
        ]
        dset_ids = {} # dictionary -- "name": "id"
        dtype = {"type": "H5T_STD_U32LE"} # arbitrary
        for name in dset_names:
            path = "/" + name
            id = helper.postDataset(self.domain, dtype, linkpath=path)
            dset_ids[name] = id

        root = helper.getRootUUID(self.domain)
        res = requests.get(
            f"{self.endpoint}/groups/{root}",
            headers=self.headers)
        self.assertEqual(res.json()["linkCount"], 3, "should have 3 links")

        res = requests.get(
                f"{self.endpoint}/datasets",
                headers=self.headers)
        res_json = res.json()
        listing = res_json["datasets"]
        for name, id in dset_ids.items() :
            self.assertTrue(id in listing, f"missing {name}: `{id}`")
        self.assertEqual(len(listing), 3)
        self.assertJSONHasOnlyKeys(res_json, LIST_DATASETS_KEYS)
        hrefs = res_json["hrefs"]
        self.assertHrefsHasOnlyRels(hrefs, LIST_DATASETS_HREFS_RELS)

    def testListDatasetsLinkedAtVariousDepths(self):
        # like above, but linked to places other than root group
        dtype = {"type": "H5T_STD_U32LE"} # arbitrary
        domain = self.domain
        headers = self.headers
        endpoint = self.endpoint
        root = helper.getRootUUID(self.domain)

        g1id = helper.postGroup(domain, path="/g1")
        g12id = helper.postGroup(domain, path="/g1/g2")
        d11id = helper.postDataset(domain, dtype, linkpath="/g1/d1")
        d122id = helper.postDataset(domain, dtype, linkpath="/g1/g2/d2")
        d123id = helper.postDataset(domain, dtype, linkpath="/g1/g2/d3")

        res = requests.get(f"{endpoint}/groups/{root}", headers=headers)
        self.assertEqual(res.json()["linkCount"], 1, "root links to g1")

        res = requests.get(f"{endpoint}/groups/{g1id}", headers=headers)
        self.assertEqual(res.json()["linkCount"], 2, "g1 links to g2 and d1")

        res = requests.get(f"{endpoint}/datasets", headers=headers)
        res_json = res.json()
        listing = res_json["datasets"]
        for path, id in [("d1", d11id), ("d2", d122id), ("d3", d123id)]:
            self.assertTrue(id in listing, f"missing {path}: `{id}`")
        self.assertEqual(len(listing), 3, "should have three datasets")
        self.assertJSONHasOnlyKeys(res_json, LIST_DATASETS_KEYS)
        hrefs = res_json["hrefs"]
        self.assertHrefsHasOnlyRels(hrefs, LIST_DATASETS_HREFS_RELS)

class PostDatasetWithLinkTest(unittest.TestCase):
    # link-upon-creation behavior
    linkname = "linked_dset"

    def __init__(self, *args, **kwargs):
        super(PostDatasetWithLinkTest, self).__init__(*args, **kwargs)
        self.domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.domain)
        self.endpoint = helper.getEndpoint()
        self.headers = helper.getRequestHeaders(domain=self.domain)
        self.root = helper.getRootUUID(domain=self.domain)

    def assertGroupHasNLinks(self, group_uuid, count, msg):
        rsp = requests.get(
                f"{self.endpoint}/groups/{group_uuid}",
                headers=self.headers)
        rsp_json = json.loads(rsp.text)
        self.assertEqual(rsp_json["linkCount"], count, msg)

    def assertLinkIsExpectedDataset(self, group_uuid, linkname, dset_uuid):
        rsp = requests.get(
                f"{self.endpoint}/groups/{group_uuid}/links/{linkname}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting link")
        rsp_json = rsp.json()
        self.assertTrue("link" in rsp_json)
        link = rsp_json["link"]
        self.assertDictEqual(
                link,
                { "id": dset_uuid,
                  "collection": "datasets",
                  "class": "H5L_TYPE_HARD",
                  "title": linkname,
                })

    def assertCanGetDatasetByUUID(self, dset_uuid):
        rsp = requests.get(
                f"{self.endpoint}/datasets/{dset_uuid}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "unable to get dataset")

    def testScalar(self):
        payload = {
            "type": "H5T_STD_U8LE",
        }
        self.assertGroupHasNLinks(self.root, 0, "domain starts empty")

        payload["link"] = {"id": self.root, "name": self.linkname}

        rsp = requests.post(
                f"{self.endpoint}/datasets",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 201, "unable to create dataset")
        dset_uuid = rsp.json()['id']
        self.assertTrue(helper.validateId(dset_uuid), "invalid uuid?")

        self.assertGroupHasNLinks(self.root, 1, "one link to dataset")
        self.assertLinkIsExpectedDataset(self.root, self.linkname, dset_uuid)
        self.assertCanGetDatasetByUUID(dset_uuid)

    def testCompoundVector(self):
        payload = {
            "type": {
                "charSet": "H5T_CSET_ASCII", 
                "class": "H5T_STRING", 
                "strPad": "H5T_STR_NULLTERM", 
                "length": "H5T_VARIABLE",
            },
            "shape": [10],
        }
        self.assertGroupHasNLinks(self.root, 0, "domain starts empty")

        payload["link"] = {"id": self.root, "name": self.linkname}

        rsp = requests.post(
                f"{self.endpoint}/datasets",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 201, "unable to create dataset")
        dset_uuid = rsp.json()['id']
        self.assertTrue(helper.validateId(dset_uuid), "invalid uuid?")

        self.assertGroupHasNLinks(self.root, 1, "one link to dataset")
        self.assertLinkIsExpectedDataset(self.root, self.linkname, dset_uuid)
        self.assertCanGetDatasetByUUID(dset_uuid)

    def testIntegerMultiDimLinkedToNonRoot(self):
        groupname = "g1"
        payload = {
            "type": "H5T_STD_U32LE",
            "shape": [10, 8, 8],
        }
        gid = helper.postGroup(self.domain, path=f"/{groupname}")
        self.assertGroupHasNLinks(gid, 0, "child group should have no links")

        payload["link"] = {"id": gid, "name": self.linkname}

        rsp = requests.post(
                f"{self.endpoint}/datasets",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 201, "unable to create dataset")
        dset_uuid = rsp.json()['id']
        self.assertTrue(helper.validateId(dset_uuid), "invalid uuid?")

        self.assertGroupHasNLinks(gid, 1, "one link to dataset")
        self.assertLinkIsExpectedDataset(gid, self.linkname, dset_uuid)
        self.assertCanGetDatasetByUUID(dset_uuid)

    def testLinkFromDetachedGroup(self):
        root_id = helper.getRootUUID(self.domain)
        gid = helper.postGroup(self.domain)

        # group is valid but unattached; no dataset found
        rsp = requests.get(
                f"{self.endpoint}/groups/{gid}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting group")
        self.assertGroupHasNLinks(root_id, 0, "root has no links")
        self.assertGroupHasNLinks(gid, 0, "new group has no links")
        rsp = requests.get(
                f"{self.endpoint}/groups",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting list")
        self.assertEqual(len(rsp.json()["groups"]), 0, "list should be empty")
        rsp = requests.get(
                f"{self.endpoint}/datasets",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting list")
        self.assertEqual(len(rsp.json()["datasets"]), 0, "list should be empty")

        # create and link new dataset with "unattached" group
        payload = {
            "type": "H5T_STD_U8LE",
            "link": {"id": gid, "name": self.linkname}
        }
        rsp = requests.post(
                f"{self.endpoint}/datasets",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 201, "problem creating dataset")
        dset_uuid = rsp.json()["id"]

        # can get new dset id, but also not part of root tree
        self.assertGroupHasNLinks(root_id, 0, "root still has no links")
        self.assertGroupHasNLinks(gid, 1, "group should have link")
        self.assertLinkIsExpectedDataset(gid, self.linkname, dset_uuid)
        self.assertCanGetDatasetByUUID(dset_uuid)
        rsp = requests.get(
                f"{self.endpoint}/groups",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting list")
        self.assertEqual(len(rsp.json()["groups"]), 0, "list should be empty")
        rsp = requests.get(
                f"{self.endpoint}/datasets",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting list")
        self.assertEqual(len(rsp.json()["datasets"]), 0, "list should be empty")

    def testLinkFromDeletedGroup_Fails400(self):
        gid = helper.postGroup(self.domain)

        res = requests.delete(
                f"{self.endpoint}/groups/{gid}",
                headers=self.headers)
        self.assertEqual(res.status_code, 200, "delete group")
        res = requests.get(
                f"{self.endpoint}/groups/{gid}",
                headers=self.headers)
        self.assertEqual(res.status_code, 410, "group should be GONE")

        # post and link (should fail)
        payload = {
            "type": "H5T_STD_U8LE",
            "link": {"id": gid, "name": self.linkname}
        }
        rsp = requests.post(
                f"{self.endpoint}/datasets",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 400, "should fail")

    def testLinkFromNonGroup_Fails400(self):
        did1 = helper.postDataset(self.domain, {"type": "H5T_STD_U8LE"})
        payload = {
            "type": "H5T_STD_U8LE",
            "link": {"id": did1, "name": self.linkname}
        }
        rsp = requests.post(
                f"{self.endpoint}/datasets",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 400, "should fail")

    def testLinkFromNonsenseUUID_Fails400(self):
        false_uuid = '-' * 38
        payload = {
            "type": "H5T_STD_U8LE",
            "link": {"id": false_uuid, "name": self.linkname}
        }
        rsp = requests.post(
                f"{self.endpoint}/datasets",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 400, "should fail")

    def testDuplicateLink_Fails409(self):
        # put dataset and link to root as link `self.linkname`
        did1 = helper.postDataset(
                self.domain,
                {"type": "H5T_STD_U8LE"},
                linkpath=f"/{self.linkname}")
        root_uuid = helper.getRootUUID(self.domain)
        payload = {
            "type": "H5T_STD_U8LE",
            "link": {"id": root_uuid, "name": self.linkname}
        }
        rsp = requests.post(
                f"{self.endpoint}/datasets",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 409, "should fail with conflict")

class DatasetTest(unittest.TestCase):
    assertJSONHasOnlyKeys = _assertDictHasOnlyKeys
    assertHrefsHasOnlyRels = _assertHrefsHasOnlyRels
    verifyShape = _verifyShape

    def __init__(self, *args, **kwargs):
        super(DatasetTest, self).__init__(*args, **kwargs)
        self.domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.domain)
        self.endpoint = helper.getEndpoint()
        self.headers = helper.getRequestHeaders(domain=self.domain)

    def assertLooksLikeUUID(self, s):
        self.assertTrue(helper.validateId(s), "maybe not UUID: " + s)

    def testScalarShapeEmptyArray(self):
        data = { "type": "H5T_IEEE_F32LE", "shape": [] }
        dset_id = helper.postDataset(self.domain, data)

        rsp = requests.get(
                f"{self.endpoint}/datasets/{dset_id}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "unable to get dataset")
        rspJson = rsp.json()
        self.assertJSONHasOnlyKeys(rspJson, GET_DATASET_KEYS)
        self.assertEqual(rspJson["id"], dset_id)
        self.assertEqual(rspJson["attributeCount"], 0)
        self.assertDictEqual(rspJson["shape"], {"class": "H5S_SCALAR"})
        self.assertDictEqual(
                rspJson["type"], 
               {"class": "H5T_FLOAT", "base": "H5T_IEEE_F32LE"})
        self.assertHrefsHasOnlyRels(rspJson["hrefs"], GET_DATASET_HREFS_RELS)

    def testShapeZero(self):
        data = { "type": "H5T_IEEE_F32LE", "shape": 0 }
        dset_id = helper.postDataset(self.domain, data)

        rsp = requests.get(
                f"{self.endpoint}/datasets/{dset_id}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "unable to get dataset")
        rspJson = rsp.json()
        self.assertJSONHasOnlyKeys(rspJson, GET_DATASET_KEYS)
        self.assertEqual(rspJson["id"], dset_id)
        self.assertEqual(rspJson["attributeCount"], 0)
        self.assertDictEqual(
                rspJson["shape"], 
               {"class": "H5S_SIMPLE", "dims": [0]})
        self.assertDictEqual(
                rspJson["type"], 
               {"class": "H5T_FLOAT", "base": "H5T_IEEE_F32LE"})
        self.assertHrefsHasOnlyRels(rspJson["hrefs"], GET_DATASET_HREFS_RELS)

    def testShapeArrayWithZero(self):
        data = { "type": "H5T_IEEE_F32LE", "shape": [0] }
        dset_id = helper.postDataset(self.domain, data)

        rsp = requests.get(
                f"{self.endpoint}/datasets/{dset_id}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "unable to get dataset")
        rspJson = rsp.json()
        self.assertJSONHasOnlyKeys(rspJson, GET_DATASET_KEYS)
        self.assertEqual(rspJson["id"], dset_id)
        self.assertEqual(rspJson["attributeCount"], 0)
        self.assertDictEqual(
                rspJson["shape"], 
               {"class": "H5S_SIMPLE", "dims": [0]})
        self.assertDictEqual(
                rspJson["type"], 
               {"class": "H5T_FLOAT", "base": "H5T_IEEE_F32LE"})
        self.assertHrefsHasOnlyRels(rspJson["hrefs"], GET_DATASET_HREFS_RELS)

    def testShapeNegativeDim_Fails400(self):
        data = { "type": "H5T_IEEE_F32LE", "shape": [-4] }

        res = requests.post(
                f"{self.endpoint}/datasets",
                headers=self.headers,
                data=json.dumps(data))
        self.assertEqual(res.status_code, 400, "post dataset should fail")
        with self.assertRaises(json.decoder.JSONDecodeError):
            res_json = res.json()

    def testCompoundType(self):
        expected_shape = {"class": "H5S_SIMPLE", "dims": [10]}
        expected_type = {
            "class": "H5T_COMPOUND",
            "fields": [
                {"name": "temp", "type": "H5T_STD_I32LE"},
                {"name": "pressure", "type": "H5T_IEEE_F32LE"}
            ]
        }
        payload = {
            "type": {
                "class": "H5T_COMPOUND",
                "fields": (
                    {"name": "temp", "type": "H5T_STD_I32LE"}, 
                    {"name": "pressure", "type": "H5T_IEEE_F32LE"},
                ),
            },
            "shape": [10],
        }
        rsp = requests.post(
                f"{self.endpoint}/datasets",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 201, "unable to create dataset")
        rspJson = rsp.json()
        self.assertJSONHasOnlyKeys(rspJson, POST_DATASET_KEYS)
        dset_uuid = rspJson['id']
        self.assertLooksLikeUUID(dset_uuid)
        self.assertDictEqual(rspJson["shape"], expected_shape)
        self.assertDictEqual(rspJson["type"], expected_type)

        rsp = requests.get(
                f"{self.endpoint}/datasets/{dset_uuid}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "unable to get dataset")
        rspJson = rsp.json()
        self.assertJSONHasOnlyKeys(rspJson, GET_DATASET_KEYS)
        self.assertDictEqual(rspJson["shape"], expected_shape)
        self.assertDictEqual(rspJson["type"], expected_type)
        self.assertDictEqual(
                rspJson["layout"], 
                {"class": "H5D_CHUNKED", "dims": [10]})
        self.assertHrefsHasOnlyRels(rspJson["hrefs"], GET_DATASET_HREFS_RELS)

    def testCompoundDuplicateMember_Fails400(self):
        field = {"name": "x", "type": "H5T_STD_I32LE"}
        payload = {
            "type": {
                "class": "H5T_COMPOUND",
                "fields": (field, field),
            },
            "shape": [10]
        }
        rsp = requests.post(
               f"{self.endpoint}/datasets",
               data=json.dumps(payload),
               headers=self.headers)
        self.assertEqual(rsp.status_code, 400, "should be a bad request")

    def testTypeFloatShapeH5S_NULL(self):
        data = {
            "type": "H5T_IEEE_F32LE", # arbirary type
            "shape": "H5S_NULL"
        }
        odd_href_rels = [ # has different hrefs from usual dataset-get
            "attributes",
            "home",
            "root",
            "self",
        ]
        dset_id = helper.postDataset(self.domain, data, linkpath="/dset1")

        rsp = requests.get(
                f"{self.endpoint}/datasets/{dset_id}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "unable to get dataset")
        rspJson = rsp.json()
        self.assertDictEqual(
                rspJson["shape"],
                {"class": "H5S_NULL"})
        self.assertDictEqual(
                rspJson["type"],
                {"class": "H5T_FLOAT", "base": "H5T_IEEE_F32LE"},
                "sanity check on datatype")
        self.assertHrefsHasOnlyRels(rspJson["hrefs"], odd_href_rels)

    def testResizableDatasetRank1(self):
        newdims = [15]
        expected_shape_10 = {
            "class": "H5S_SIMPLE",
            "dims": [10],
            "maxdims": [20],
        }
        expected_shape_15 = {
            "class": "H5S_SIMPLE",
            "dims": newdims,
            "maxdims": [20],
        }
        expected_type = {
            "class": "H5T_FLOAT",
            "base": "H5T_IEEE_F32LE",
        }
        payload = {
            "type": "H5T_IEEE_F32LE",
            "shape": [10],
            "maxdims": [20],
        }
        rsp = requests.post(
                f"{self.endpoint}/datasets",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 201, "unable to create dataset")
        rspJson = rsp.json()
        self.assertJSONHasOnlyKeys(rspJson, POST_DATASET_KEYS)
        dset_uuid = rspJson['id']
        self.assertLooksLikeUUID(dset_uuid)
        self.assertDictEqual(rspJson["shape"], expected_shape_10)
        self.assertDictEqual(rspJson["type"], expected_type)

        # verify type and shape with get dataset
        rsp = requests.get(
                f"{self.endpoint}/datasets/{dset_uuid}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "unable to get dataset")
        rspJson = rsp.json()
        self.assertJSONHasOnlyKeys(rspJson, GET_DATASET_KEYS)
        self.assertDictEqual(rspJson["shape"], expected_shape_10)
        self.assertDictEqual(rspJson["type"], expected_type)
        self.assertDictEqual(rspJson["creationProperties"], {})

        # re-verify shape with get shape
        self.verifyShape(dset_uuid, expected_shape_10)

        # resize dataset
        rsp = helper.resizeDataset(
                self.domain,
                dset_uuid,
                newdims,
                response=True)
        self.assertEqual(rsp.status_code, 201, "unable to update shape")
        rspJson = rsp.json()
        self.assertDictEqual(rspJson, {"hrefs": []})

        self.verifyShape(dset_uuid, expected_shape_15)

    def testCommittedType(self):
        # create the datatype
        payload = {"type": "H5T_IEEE_F32LE"}
        rsp = requests.post(
                f"{self.endpoint}/datatypes",
                data=json.dumps(payload),
                headers=self.headers)
        self.assertEqual(rsp.status_code, 201, "unable to create type")
        rspJson = rsp.json()
        dtype_uuid = rspJson["id"]
        self.assertLooksLikeUUID(dtype_uuid)

        # create the dataset
        payload = {"type": dtype_uuid, "shape": [10, 10]}
        dset_uuid = helper.postDataset(self.domain, payload)

        # get dataset type and verify
        rsp = requests.get(
                f"{self.endpoint}/datasets/{dset_uuid}/type",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "unable to get type")
        rspJson = rsp.json()
        expected_type = {
            "base": "H5T_IEEE_F32LE",
            "class": "H5T_FLOAT",
            "id": dtype_uuid,
        }
        self.assertDictEqual(rspJson["type"], expected_type)

class ResizeDatasetTest(unittest.TestCase):
    assertHrefsHasOnlyRels = _assertHrefsHasOnlyRels
    assertJSONHasOnlyKeys = _assertDictHasOnlyKeys
    verifyShape = _verifyShape

    def __init__(self, *args, **kwargs):
        super(ResizeDatasetTest, self).__init__(*args, **kwargs)
        self.domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.domain)
        self.endpoint = helper.getEndpoint()
        self.headers = helper.getRequestHeaders(domain=self.domain)

    def testToMaxdimRank1(self):
        maxdims = [20]
        expected_shape_10 = {
            "class": "H5S_SIMPLE",
            "dims": [10],
            "maxdims": maxdims,
        }
        expected_shape_20 = {
            "class": "H5S_SIMPLE",
            "dims": [20],
            "maxdims": maxdims,
        }
        expected_type = {
            "class": "H5T_FLOAT",
            "base": "H5T_IEEE_F32LE",
        }
        payload = {
            "type": "H5T_IEEE_F32LE",
            "shape": [10],
            "maxdims": maxdims,
        }
        rsp = helper.postDataset(self.domain, payload, response=True)
        self.assertEqual(rsp.status_code, 201, "unable to create dataset")
        rspJson = rsp.json()
        self.assertJSONHasOnlyKeys(rspJson, POST_DATASET_KEYS)
        dset_uuid = rspJson['id']
        self.assertDictEqual(rspJson["shape"], expected_shape_10)
        self.assertDictEqual(rspJson["type"], expected_type)

        rsp = helper.resizeDataset(
                self.domain,
                dset_uuid,
                maxdims,
                response=True)
        self.assertEqual(rsp.status_code, 201, "unable to update shape")
        rspJson = rsp.json()
        self.assertDictEqual(rspJson, {"hrefs": []})

        self.verifyShape(dset_uuid, expected_shape_20)

    def testPastMaxdimsRank1_Fails400(self):
        maxdims = [20]
        expected_shape_10 = {
            "class": "H5S_SIMPLE",
            "dims": [10],
            "maxdims": maxdims,
        }
        payload = {
            "type": "H5T_IEEE_F32LE",
            "shape": [10],
            "maxdims": maxdims,
        }
        dset_uuid = helper.postDataset(self.domain, payload)

        res = helper.resizeDataset(self.domain, dset_uuid, [25], response=True)
        self.assertEqual(res.status_code, 400, "too large should fail")

        # shape should be unchanged
        self.verifyShape(dset_uuid, expected_shape_10)

    def testUnlimitedRank2(self):
        start_shape = [10, 20]
        resized_shape = [10, 500]
        maxdims = [20, 0]
        payload = {
            "type": "H5T_IEEE_F32LE", # arbitrary type
            "shape": start_shape,
            "maxdims": maxdims,
        }
        dset_uuid = helper.postDataset(self.domain, payload)

        expected = {
            "class": "H5S_SIMPLE",
            "dims": start_shape,
            "maxdims": maxdims,
        }
        self.verifyShape(dset_uuid, expected)

        helper.resizeDataset(self.domain, dset_uuid, resized_shape)

        expected = {
            "class": "H5S_SIMPLE",
            "dims": resized_shape,
            "maxdims": maxdims,
        }
        self.verifyShape(dset_uuid, expected)

    def testShrink_Fails400(self):
        maxdims = [20]
        payload = {
            "type": "H5T_IEEE_F32LE",
            "shape": [10],
            "maxdims": maxdims,
        }
        dset_uuid = helper.postDataset(self.domain, payload)

        res = helper.resizeDataset(self.domain, dset_uuid, [5], response=True)
        self.assertEqual(res.status_code, 400, "too large should fail")

        # shape should be unchanged
        expected = {
            "class": "H5S_SIMPLE",
            "dims": [10],
            "maxdims": maxdims,
        }
        self.verifyShape(dset_uuid, expected)

    def testNoMaxdimsOnPost_Fails400(self):
        payload = {
            "type": "H5T_IEEE_F32LE",
            "shape": [10],
        }
        dset_uuid = helper.postDataset(self.domain, payload)

        res = helper.resizeDataset(self.domain, dset_uuid, [20], response=True)
        self.assertEqual(res.status_code, 400, "resize should fail")

        # shape should be unchanged
        expected = {
            "class": "H5S_SIMPLE",
            "dims": [10],
        }
        self.verifyShape(dset_uuid, expected)

    def testPartialResize(self):
        maxdims = [10, 20]
        newdims = [8]
        payload = {
            "type": "H5T_IEEE_F32LE",
            "shape": [5, 10],
            "maxdims": maxdims,
        }
        dset_uuid = helper.postDataset(self.domain, payload)

        res = helper.resizeDataset(
                self.domain,
                dset_uuid,
                newdims,
                response=True)
        self.assertEqual(res.status_code, 400, "should fail")

        expected = {
            "class": "H5S_SIMPLE",
            "dims": [5, 10],
            "maxdims": maxdims,
        }
        self.verifyShape(dset_uuid, expected)

    def testTooManyDimensions_Fails400(self):
        maxdims = [10, 20]
        payload = {
            "type": "H5T_IEEE_F32LE",
            "shape": [5, 10],
            "maxdims": maxdims,
        }
        dset_uuid = helper.postDataset(self.domain, payload)

        newdims = [10, 10, 10]
        res = helper.resizeDataset(
                self.domain,
                dset_uuid,
                newdims,
                response=True)
        self.assertEqual(res.status_code, 400, f"resize should fail")

        # shape should be unchanged
        expected = {
            "class": "H5S_SIMPLE",
            "dims": [5, 10],
            "maxdims": maxdims,
        }
        self.verifyShape(dset_uuid, expected)

class CreationPropertiesTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(CreationPropertiesTest, self).__init__(*args, **kwargs)
        self.domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.domain)
        self.endpoint = helper.getEndpoint()
        self.headers = helper.getRequestHeaders(domain=self.domain)

    def testChunkedAndCompression(self):
        # Create ~1GB dataset
        payload = {
            "type": "H5T_IEEE_F32LE",
            "shape": [365, 780, 1024],
            "maxdims": [0, 780, 1024]
        }
        # define a chunk layout with 4 chunks per 'slice'
        # chunk size is 798720 bytes
        # (TODO: document how these comment details are related to the source)
        gzip_filter = {
            "class": "H5Z_FILTER_DEFLATE",
            "id": 1,
            "level": 9,
            "name": "deflate",
        }
        payload["creationProperties"] = {
            "layout": {
                "class": "H5D_CHUNKED",
                 "dims": [1, 390, 512] 
            },
            "filters": [gzip_filter] 
        }
        dset_uuid = helper.postDataset(self.domain, payload)

        # verify layout
        rsp = requests.get(
                f"{self.endpoint}/datasets/{dset_uuid}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "unable to get dataset")
        rspJson = rsp.json()
        self.assertDictEqual(
                rspJson["layout"],
                {   "class": "H5D_CHUNKED",
                    "dims": [1, 390, 1024]
                })

        # verify compression
        self.assertDictEqual(
                rspJson["creationProperties"],
                payload["creationProperties"])

    def testInvalidFillValue_Fails400(self):
        fill_value = "XXXX" # can't convert to dataset's type (float)
        payload = {
            "type": "H5T_STD_I32LE",
            "shape": 10,
            "creationProperties": { "fillValue": fill_value },
        }
        rsp = helper.postDataset(self.domain, payload, response=True)
        self.assertEqual(rsp.status_code, 400, "post should fail")

    def testAutoChunkRank1(self):
        extent = 1000 * 1000 * 1000
        dims = [extent]
        fields = (
            {"name": "x", "type": "H5T_IEEE_F64LE"}, 
            {"name": "y", "type": "H5T_IEEE_F64LE"},
            {"name": "z", "type": "H5T_IEEE_F64LE"},
        ) 
        datatype = {"class": "H5T_COMPOUND", "fields": fields }
        dcpl = { # TODO: "ignored as too small"??
            "layout": {
                "class": "H5D_CHUNKED",
                "dims": [10],
            }
        }
        payload = {
            "type": datatype,
            "shape": dims,
            "creationProperties": dcpl,
        }
        dset_uuid = helper.postDataset(self.domain, payload)

        # verify layout
        rsp = requests.get(
                f"{self.endpoint}/datasets/{dset_uuid}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "unable to get dataset")
        rspJson = rsp.json()
        self.assertDictEqual(
                rspJson["layout"],
                {"dims": [81920], "class": "H5D_CHUNKED"}) # TODO: 'magic' num
        layout_dims = rspJson["layout"]["dims"]
        self.assertTrue(layout_dims[0] < dims[0])
        chunk_size = layout_dims[0] * 8 * 3  # three 64bit 
        self.assertTrue(chunk_size >= CHUNK_MIN)
        self.assertTrue(chunk_size <= CHUNK_MAX)

    def testAutoChunkRank2(self):
        # 50K x 80K dataset
        dims = [50000, 80000]
        payload = {"type": "H5T_IEEE_F32LE", "shape": dims }
        dset_uuid = helper.postDataset(self.domain, payload)

        # verify layout
        rsp = requests.get(
                f"{self.endpoint}/datasets/{dset_uuid}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "unable to get dataset")
        rspJson = rsp.json()
        layout_json = rspJson["layout"]
        self.assertEqual(layout_json["class"], 'H5D_CHUNKED')
        layout = layout_json["dims"]
        self.assertEqual(len(layout), 2)
        self.assertTrue(layout[0] < dims[0])
        self.assertTrue(layout[1] < dims[1])
        chunk_size = layout[0] * layout[1] * 4 # TODO: explain
        self.assertTrue(chunk_size >= CHUNK_MIN)
        self.assertTrue(chunk_size <= CHUNK_MAX)

    def testMinChunkSize(self):
        # test that chunk layout is adjusted if provided layout is too small
        dims = [50000, 80000]
        dcpl = { # chunk layout with lots of small chunks
            'layout': {
                'class': 'H5D_CHUNKED',
                'dims': [10, 10]
            }
        }
        payload = {
            "type": "H5T_IEEE_F32LE",
            "shape": dims,
            "creationProperties": dcpl,
        }
        dset_uuid = helper.postDataset(self.domain, payload)

        # verify layout
        rsp = requests.get(
                f"{self.endpoint}/datasets/{dset_uuid}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "unable to get dataset")
        rspJson = rsp.json()
        layout_json = rspJson["layout"]
        self.assertEqual(layout_json["class"], 'H5D_CHUNKED')
        layout = layout_json["dims"]
        self.assertEqual(len(layout), 2)
        self.assertTrue(layout[0] < dims[0])
        self.assertTrue(layout[1] < dims[1])
        chunk_size = layout[0] * layout[1] * 4 # TODO: explain
        self.assertTrue(chunk_size >= CHUNK_MIN)
        self.assertTrue(chunk_size <= CHUNK_MAX)

@unittest.skipUnless(config.get("test_on_uploaded_file"), "requires file")
class FileWithDatasetsTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(FileWithDatasetsTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()

    def testGet(self):
        domain = helper.getTestDomain("tall.h5")
        headers = helper.getRequestHeaders(domain=domain)
        
        # verify domain exists
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        if rsp.status_code != 200:
            print("WARNING: Failed to get domain: {}. Is test data setup?".format(domain))
            return  # abort rest of test
        domainJson = json.loads(rsp.text)
        root_uuid = domainJson["root"]
         
        # get the dataset uuid 
        dset_uuid = helper.getUUIDByPath(domain, "/g1/g1.1/dset1.1.1")
        self.assertTrue(dset_uuid.startswith("d-"))

        # get the dataset json
        req = helper.getEndpoint() + '/datasets/' + dset_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for name in ("id", "shape", "hrefs", "layout", "creationProperties", 
            "attributeCount", "created", "lastModified", "root", "domain"):
            self.assertTrue(name in rspJson)
         
        self.assertEqual(rspJson["id"], dset_uuid) 
        self.assertEqual(rspJson["root"], root_uuid) 
        self.assertEqual(rspJson["domain"], domain) 
        hrefs = rspJson["hrefs"]
        self.assertEqual(len(hrefs), 5)
        self.assertEqual(rspJson["id"], dset_uuid)

        shape = rspJson["shape"]
        for name in ("class", "dims", "maxdims"):
            self.assertTrue(name in shape)
        self.assertEqual(shape["class"], 'H5S_SIMPLE')
        self.assertEqual(shape["dims"], [10,10])
        self.assertEqual(shape["maxdims"], [10,10])

        layout = rspJson["layout"]
        self.assertEqual(layout["class"], 'H5D_CHUNKED')
        self.assertEqual(layout["dims"], [10,10])
         
        type = rspJson["type"]
        for name in ("base", "class"):
            self.assertTrue(name in type)
        self.assertEqual(type["class"], 'H5T_INTEGER')
        self.assertEqual(type["base"], 'H5T_STD_I32BE')

        cpl = rspJson["creationProperties"]
        for name in ("layout", "fillTime"):
            self.assertTrue(name in cpl)

        self.assertEqual(rspJson["attributeCount"], 2)

        # these properties should only be available when verbose is used
        self.assertFalse("num_chunks" in rspJson)
        self.assertFalse("allocated_size" in rspJson)

        now = time.time()
        # the object shouldn't have been just created or updated
        self.assertTrue(rspJson["created"] < now - 60 * 5)
        self.assertTrue(rspJson["lastModified"] < now - 60 * 5)

        # request the dataset path
        req = helper.getEndpoint() + '/datasets/' + dset_uuid
        params = {"getalias": 1}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("alias" in rspJson)
        self.assertEqual(rspJson["alias"], ['/g1/g1.1/dset1.1.1'])

    def testGetByPath(self):
        domain = helper.getTestDomain("tall.h5")
        headers = helper.getRequestHeaders(domain=domain)
        
        # verify domain exists
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        if rsp.status_code != 200:
            print("WARNING: Failed to get domain: {}. Is test data setup?".format(domain))
            return  # abort rest of test
        domainJson = json.loads(rsp.text)
        root_uuid = domainJson["root"]
         
        # get the dataset at "/g1/g1.1/dset1.1.1"
        h5path = "/g1/g1.1/dset1.1.1"
        req = helper.getEndpoint() + "/datasets/"
        params = {"h5path": h5path}
        rsp = requests.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)

        rspJson = json.loads(rsp.text)
        for name in ("id", "shape", "hrefs", "layout", "creationProperties", 
            "attributeCount", "created", "lastModified", "root", "domain"):
            self.assertTrue(name in rspJson)

        # get the dataset via a relative apth "g1/g1.1/dset1.1.1"
        h5path = "g1/g1.1/dset1.1.1"
        req = helper.getEndpoint() + "/datasets/"
        params = {"h5path": h5path, "grpid": root_uuid}
        rsp = requests.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)

        rspJson = json.loads(rsp.text)
        for name in ("id", "shape", "hrefs", "layout", "creationProperties", 
            "attributeCount", "created", "lastModified", "root", "domain"):
            self.assertTrue(name in rspJson)


        # get the dataset uuid and verify it matches what we got by h5path
        dset_uuid = helper.getUUIDByPath(domain, "/g1/g1.1/dset1.1.1")
        self.assertTrue(dset_uuid.startswith("d-"))
        self.assertEqual(dset_uuid, rspJson["id"])

        # try a invalid link and verify a 404 is returened
        h5path = "/g1/foobar"
        req = helper.getEndpoint() + "/datasets/"
        params = {"h5path": h5path}
        rsp = requests.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 404)

        # try passing a path to a group and verify we get 404
        h5path = "/g1/g1.1"
        req = helper.getEndpoint() + "/datasets/"
        params = {"h5path": h5path}
        rsp = requests.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 404)

    def testGetVerbose(self):
        domain = helper.getTestDomain("tall.h5")
        headers = helper.getRequestHeaders(domain=domain)
        
        # verify domain exists
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        if rsp.status_code != 200:
            print("WARNING: Failed to get domain: {}. Is test data setup?".format(domain))
            return  # abort rest of test
        domainJson = json.loads(rsp.text)
        root_uuid = domainJson["root"]
        self.assertTrue(helper.validateId(root_uuid))
         
        # get the dataset uuid 
        dset_uuid = helper.getUUIDByPath(domain, "/g1/g1.1/dset1.1.1")
        self.assertTrue(dset_uuid.startswith("d-"))

        # get the dataset json
        req = helper.getEndpoint() + '/datasets/' + dset_uuid
        params = {"verbose": 1}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for name in ("id", "shape", "hrefs", "layout", "creationProperties", 
            "attributeCount", "created", "lastModified", "root", "domain"):
            self.assertTrue(name in rspJson)
         
        # these properties should only be available when verbose is used
        self.assertTrue("num_chunks" in rspJson)
        self.assertTrue("allocated_size" in rspJson)
        self.assertEqual(rspJson["num_chunks"], 1)
        self.assertEqual(rspJson["allocated_size"], 400) # this will likely change once compression is working

if __name__ == '__main__':
    unittest.main()

