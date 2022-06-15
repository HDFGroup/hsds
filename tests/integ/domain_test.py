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
import time
import json
from os import path as pp
import config
import helper

# Note - removed 'snappy'.  See: https://github.com/Blosc/python-blosc/issues/231

EXPECTED_COMPRESSORS = ["blosclz", "lz4", "lz4hc", "gzip", "zstd"]


class DomainTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DomainTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain, folder=True)

    def setUp(self):
        self.session = helper.getSession()

    def tearDown(self):
        if self.session:
            self.session.close()

        # main

    def testBaseDomain(self):
        # make a non-folder domain
        print("testBaseDomain", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["content-type"], "application/json; charset=utf-8")

        # verify that passing domain as query string works as well
        del headers["X-Hdf-domain"]
        req += "?host=" + self.base_domain
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["content-type"], "application/json; charset=utf-8")

        # try using DNS-style domain name
        domain = helper.getDNSDomain(self.base_domain)
        params = {"host": domain}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["content-type"], "application/json; charset=utf-8")

    def testGetDomain(self):
        domain = helper.getTestDomain("tall.h5")
        print("testGetDomain", domain)
        headers = helper.getRequestHeaders(domain=domain)

        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        if rsp.status_code != 200:
            print(
                "WARNING: Failed to get domain: {}. Is test data setup?".format(domain)
            )
            return  # abort rest of test
        self.assertEqual(rsp.headers["content-type"], "application/json; charset=utf-8")
        rspJson = json.loads(rsp.text)

        for name in (
            "lastModified",
            "created",
            "hrefs",
            "root",
            "owner",
            "class",
            "limits",
            "compressors",
            "version",
        ):
            self.assertTrue(name in rspJson)
        now = time.time()
        self.assertTrue(rspJson["created"] < now - 10)
        self.assertTrue(rspJson["lastModified"] < now - 10)
        self.assertEqual(len(rspJson["hrefs"]), 7)
        self.assertTrue(rspJson["root"].startswith("g-"))
        self.assertTrue(rspJson["owner"])
        self.assertEqual(rspJson["class"], "domain")
        self.assertFalse(
            "num_groups" in rspJson
        )  # should only show up with the verbose param
        self.assertFalse(
            "domain_objs" in rspJson
        )  # should only show if getobjs query param is used

        limits = rspJson["limits"]
        limit_keys = ("min_chunk_size", "max_chunk_size", "max_request_size")

        for k in limit_keys:
            self.assertTrue(k in limits)
            limit = limits[k]
            self.assertTrue(isinstance(limit, int))
            self.assertTrue(limit > 0)

        compressors = rspJson["compressors"]
        for compressor in EXPECTED_COMPRESSORS:
            self.assertTrue(compressor in compressors)

        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # get a dict of all objects in the domain
        params = {"getobjs": 1}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("domain_objs" in rspJson)
        domain_objs = rspJson["domain_objs"]
        self.assertEqual(len(domain_objs), 10)
        attr_count = 0
        for objid in domain_objs:
            obj_json = domain_objs[objid]
            self.assertTrue("id" in obj_json)
            self.assertTrue("attributeCount" in obj_json)
            attr_count += obj_json["attributeCount"]
            self.assertFalse("attributes" in obj_json)

        self.assertEqual(attr_count, 4)

        # get a dict of all objects in the domain including any attributes
        params["include_attrs"] = 1
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("domain_objs" in rspJson)
        domain_objs = rspJson["domain_objs"]
        self.assertEqual(len(domain_objs), 10)
        attr_count = 0
        for objid in domain_objs:
            obj_json = domain_objs[objid]
            self.assertFalse("attributeCount" in obj_json)
            self.assertTrue("attributes" in obj_json)
            attributes = obj_json["attributes"]
            for attr_name in attributes:
                # only the names "attr1" and "attr2" are used in this domain
                self.assertTrue(attr_name in ("attr1", "attr2"))
                attr_count += 1
        self.assertEqual(attr_count, 4)

        # verify that passing domain as query string works as well
        del headers["X-Hdf-domain"]
        params = {"host": domain}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["content-type"], "application/json; charset=utf-8")
        rspJson = json.loads(rsp.text)
        root_uuid_2 = rspJson["root"]
        self.assertEqual(root_uuid, root_uuid_2)

        # same deal using the "domain" param
        params = {"domain": domain}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["content-type"], "application/json; charset=utf-8")
        rspJson = json.loads(rsp.text)
        root_uuid_3 = rspJson["root"]
        self.assertEqual(root_uuid, root_uuid_3)

        # verify that request with invalid domain fails
        domain = domain[1:]  # strip off the '/'
        params = {"domain": domain}

        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 400)

    def testGetByPath(self):
        domain = helper.getTestDomain("tall.h5")
        print("testGetByPath", domain)
        headers = helper.getRequestHeaders(domain=domain)

        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        if rsp.status_code != 200:
            print(
                "WARNING: Failed to get domain: {}. Is test data setup?".format(domain)
            )
            return  # abort rest of test
        domainJson = json.loads(rsp.text)
        self.assertTrue("root" in domainJson)
        root_id = domainJson["root"]

        # Get group at /g1/g1.1 by using h5path
        params = {"h5path": "/g1/g1.1"}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        g11id = helper.getUUIDByPath(domain, "/g1/g1.1", session=self.session)
        self.assertEqual(g11id, rspJson["id"])
        self.assertTrue("root" in rspJson)
        self.assertEqual(root_id, rspJson["root"])

        # Get dataset at /g1/g1.1/dset1.1.1 by using relative h5path
        params = {"h5path": "./g1/g1.1/dset1.1.1"}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        d111id = helper.getUUIDByPath(
            domain, "/g1/g1.1/dset1.1.1", session=self.session
        )
        self.assertEqual(d111id, rspJson["id"])
        self.assertTrue("root" in rspJson)
        self.assertEqual(root_id, rspJson["root"])

    def testGetDomainVerbose(self):
        domain = helper.getTestDomain("tall.h5")
        print("testGetDomainVerbose", domain)
        headers = helper.getRequestHeaders(domain=domain)

        req = helper.getEndpoint() + "/"
        params = {"verbose": 1}

        rsp = self.session.get(req, params=params, headers=headers)
        if rsp.status_code == 404:
            print(
                "WARNING: Failed to get domain: {}. Is test data setup?".format(domain)
            )
            return  # abort rest of test
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["content-type"], "application/json; charset=utf-8")
        rspJson = json.loads(rsp.text)

        for name in ("lastModified", "created", "hrefs", "root", "owner", "class"):
            self.assertTrue(name in rspJson)
        now = time.time()
        self.assertTrue(rspJson["created"] < now - 10)
        self.assertTrue(rspJson["lastModified"] < now - 10)
        self.assertEqual(len(rspJson["hrefs"]), 7)
        self.assertTrue(rspJson["root"].startswith("g-"))
        self.assertTrue(rspJson["owner"])
        self.assertEqual(rspJson["class"], "domain")

        root_uuid = rspJson["root"]

        helper.validateId(root_uuid)

        # restore when sqlite changes are complete
        self.assertTrue("num_groups" in rspJson)
        self.assertEqual(rspJson["num_groups"], 6)
        self.assertTrue("num_datasets" in rspJson)
        self.assertEqual(rspJson["num_datasets"], 4)
        self.assertTrue("num_datatypes" in rspJson)
        self.assertEqual(rspJson["num_datatypes"], 0)
        self.assertTrue("allocated_bytes" in rspJson)
        # test that allocated_bytes falls in a given range

        self.assertEqual(rspJson["allocated_bytes"], 580)
        # total_size may change slightly based on specifics of JSON serialization
        self.assertTrue(rspJson["total_size"] > 5000)
        self.assertTrue(rspJson["total_size"] < 7000)
        # TODO - num_chunks should be present
        self.assertTrue("num_objects" in rspJson)
        self.assertTrue(rspJson["num_objects"], 14)

    def testGetTopLevelDomain(self):
        domain = "/home"
        print("testGetTopLevelDomain", domain)
        headers = helper.getRequestHeaders(domain=domain)

        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertFalse("root" in rspJson)  # no root group for folder domain
        self.assertTrue("owner" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("class" in rspJson)
        self.assertEqual(rspJson["class"], "folder")
        domain = "test_user1.home"
        headers = helper.getRequestHeaders(domain=domain)

        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)

    def testCreateDomain(self):
        domain = self.base_domain + "/newdomain.h6"
        print("testCreateDomain", domain)
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + "/"

        rsp = self.session.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        for k in (
            "root",
            "owner",
            "acls",
            "created",
            "lastModified",
            "version",
            "limits",
            "compressors",
        ):
            self.assertTrue(k in rspJson)

        root_id = rspJson["root"]

        # verify that putting the same domain again fails with a 409 error
        rsp = self.session.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 409)

        limit_keys = ("min_chunk_size", "max_chunk_size", "max_request_size")
        limits = rspJson["limits"]
        for k in limit_keys:
            self.assertTrue(k in limits)
            limit = limits[k]
            self.assertTrue(isinstance(limit, int))
            self.assertTrue(limit > 0)
        compressors = rspJson["compressors"]
        for compressor in EXPECTED_COMPRESSORS:
            self.assertTrue(compressor in compressors)

        # do a get on the new domain
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for k in (
            "root",
            "owner",
            "class",
            "created",
            "lastModified",
            "limits",
            "version",
        ):
            self.assertTrue(k in rspJson)
        # we should get the same value for root id
        self.assertEqual(root_id, rspJson["root"])
        # should get limits here too
        limits = rspJson["limits"]
        for k in limit_keys:
            self.assertTrue(k in limits)
            limit = limits[k]
            self.assertTrue(isinstance(limit, int))
            self.assertTrue(limit > 0)

        # try doing a GET with a host query args
        headers = helper.getRequestHeaders()
        req = helper.getEndpoint() + "/?host=" + domain
        # do a get on the domain with a query arg for host
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for k in ("root", "owner"):
            self.assertTrue(k in rspJson)
        # we should get the same value for root id
        self.assertEqual(root_id, rspJson["root"])

        # verify we can access root groups
        root_req = helper.getEndpoint() + "/groups/" + root_id
        headers = helper.getRequestHeaders(domain=domain)
        rsp = self.session.get(root_req, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # try doing a flush on the domain
        req = helper.getEndpoint() + "/"
        params = {"flush": 1}
        rsp = self.session.put(req, params=params, headers=headers)
        # should get a NO_CONTENT code,
        #  c.f. https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#sec9.6
        self.assertEqual(rsp.status_code, 204)

        # same thing using the body
        req = helper.getEndpoint() + "/"
        body = {"flush": 1}
        rsp = self.session.put(req, data=json.dumps(body), headers=headers)
        # should get a NO_CONTENT code,
        # c.f. https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#sec9.6
        self.assertEqual(rsp.status_code, 204)

        # try doing a un-authenticated request
        if config.get("test_noauth") and config.get("default_public"):
            headers = helper.getRequestHeaders()
            req = helper.getEndpoint() + "/?host=" + domain
            # do a get on the domain with a query arg for host
            rsp = self.session.get(req)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            for k in ("root", "owner"):
                self.assertTrue(k in rspJson)
            # we should get the same value for root id
            self.assertEqual(root_id, rspJson["root"])

    def testCreateDomainNodeIds(self):
        domain = self.base_domain + "/newdomain.h6"
        print("testCreateDomainNodeIds", domain)
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + "/"
        params = {"getdnids": 1}

        rsp = self.session.put(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        for k in (
            "root",
            "owner",
            "acls",
            "created",
            "lastModified",
            "version",
            "limits",
            "compressors",
        ):
            self.assertTrue(k in rspJson)

        self.assertTrue("dn_ids" in rspJson)

        dn_ids = rspJson["dn_ids"]
        self.assertTrue(len(dn_ids) >= 1)
        root_id = rspJson["root"]

        # verify we can access root groups
        root_req = helper.getEndpoint() + "/groups/" + root_id
        headers = helper.getRequestHeaders(domain=domain)
        rsp = self.session.get(root_req, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # do a GET on the domain
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)

        for k in (
            "root",
            "owner",
            "created",
            "hrefs",
            "lastModified",
            "version",
            "limits",
            "compressors",
            "dn_ids",
        ):
            self.assertTrue(k in rspJson)

        self.assertTrue("dn_ids" in rspJson)
        self.assertTrue(len(rspJson["dn_ids"]) >= 1)
        self.assertEqual(set(dn_ids), set(rspJson["dn_ids"]))
        self.assertEqual(root_id, rspJson["root"])

        # try doing a flush on the domain
        req = helper.getEndpoint() + "/"
        params = {"flush": 1, "getdnids": 1}
        rsp = self.session.put(req, params=params, headers=headers)
        #  should get content this time
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("dn_ids" in rspJson)
        self.assertTrue(len(rspJson["dn_ids"]) >= 1)
        self.assertEqual(set(dn_ids), set(rspJson["dn_ids"]))

    def testCreateLinkedDomain(self):
        target_domain = self.base_domain + "/target_domain.h5"
        print("testCreateLinkedDomain", target_domain)
        headers = helper.getRequestHeaders(domain=target_domain)
        req = helper.getEndpoint() + "/"

        rsp = self.session.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        for k in ("root", "owner", "acls", "created", "lastModified"):
            self.assertTrue(k in rspJson)

        root_id = rspJson["root"]

        # do a get on the new domain
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for k in ("root", "owner"):
            self.assertTrue(k in rspJson)
        # we should get the same value for root id
        self.assertEqual(root_id, rspJson["root"])

        # create new domain linked with the existing root
        linked_domain = self.base_domain + "/linked_domain.h5"
        print("testCreateLinkedDomain - linked domain", linked_domain)
        headers = helper.getRequestHeaders(domain=linked_domain)
        body = {"linked_domain": target_domain}
        rsp = self.session.put(req, data=json.dumps(body), headers=headers)

        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        for k in ("root", "owner", "acls", "created", "lastModified"):
            self.assertTrue(k in rspJson)
        self.assertEqual(rspJson["root"], root_id)

        # delete the target domain but keep the root
        headers = helper.getRequestHeaders(domain=target_domain)
        body = {"keep_root": 1}
        rsp = self.session.delete(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # verify we can access the root group under the linked domain
        headers = helper.getRequestHeaders(domain=linked_domain)
        root_req = helper.getEndpoint() + "/groups/" + root_id
        rsp = self.session.get(root_req, headers=headers)
        self.assertEqual(rsp.status_code, 200)

    def testCreateFolder(self):
        domain = self.base_domain + "/newfolder"
        print("testCreateFolder", domain)
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + "/"
        body = {"folder": True}
        rsp = self.session.put(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        for k in ("owner", "acls", "created", "lastModified"):
            self.assertTrue(k in rspJson)
        self.assertFalse("root" in rspJson)  # no root -> folder

        # verify that putting the same domain again fails with a 409 error
        rsp = self.session.put(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 409)

        # do a get on the new folder
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)

        self.assertTrue("owner" in rspJson)
        self.assertTrue("class" in rspJson)
        self.assertEqual(rspJson["class"], "folder")

        # try doing a un-authenticated request
        if config.get("test_noauth") and config.get("default_public"):
            headers = helper.getRequestHeaders()
            req = helper.getEndpoint() + "/?host=" + domain
            # do a get on the folder with a query arg for host
            rsp = self.session.get(req)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            for k in ("class", "owner"):
                self.assertTrue(k in rspJson)
            self.assertFalse("root" in rspJson)

    def testAclInheritence(self):
        # this test is here (rather than acl_test.py) since we need to create domains in a folder
        print("testAclInheritence", self.base_domain)
        folder = self.base_domain + "/a_folder"
        headers = helper.getRequestHeaders(domain=folder)
        req = helper.getEndpoint() + "/"
        body = {"folder": True}
        rsp = self.session.put(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        default_public = config.get("default_public")

        # create an ACL for "test_user2" with read and update access
        user2name = config.get("user2_name")
        req = helper.getEndpoint() + "/acls/" + user2name
        perm = {"read": True, "update": True}

        rsp = self.session.put(req, headers=headers, data=json.dumps(perm))
        self.assertEqual(rsp.status_code, 201)

        req = helper.getEndpoint() + "/acls"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rsp_json = json.loads(rsp.text)
        self.assertTrue("acls" in rsp_json)
        acls = rsp_json["acls"]
        parent_acls = {}
        for acl in acls:
            self.assertTrue("userName" in acl)
            user_name = acl["userName"]
            parent_acls[user_name] = acl

        self.assertTrue(user2name in parent_acls)
        # folders are always public read
        self.assertTrue("default" in parent_acls)

        # create a domain
        domain = folder + "/a_domain"
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + "/"
        body = {}
        rsp = self.session.put(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        for k in ("owner", "acls", "created", "lastModified"):
            self.assertTrue(k in rspJson)

        # get ACLs for domain
        req = helper.getEndpoint() + "/acls"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rsp_json = json.loads(rsp.text)
        self.assertTrue("acls" in rsp_json)
        acls = rsp_json["acls"]
        expected = ["test_user1", "test_user2"]
        if default_public:
            expected.append("default")
        child_acls = {}
        for acl in acls:
            self.assertTrue("userName" in acl)
            user_name = acl["userName"]
            child_acls[user_name] = acl

        # verify ACLs match
        for user_name in expected:
            self.assertTrue(user_name in child_acls)
            parent_acl = parent_acls[user_name]
            child_acl = child_acls[user_name]
            for k in ("create", "read", "update", "delete", "readACL", "updateACL"):
                self.assertEqual(parent_acl[k], child_acl[k])

    def testDeleteFolderWithChildren(self):

        folder_name = "testDeleteFolder"
        domain_name = "myfile"
        domain = self.base_domain + "/" + folder_name
        print("testCreateFolder", domain)
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + "/"
        body = {"folder": True}
        rsp = self.session.put(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        for k in ("owner", "acls", "created", "lastModified"):
            self.assertTrue(k in rspJson)
        self.assertFalse("root" in rspJson)  # no root -> folder

        # verify that putting the same domain again fails with a 409 error
        rsp = self.session.put(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 409)

        # do a get on the new folder
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)

        self.assertTrue("owner" in rspJson)
        self.assertTrue("class" in rspJson)
        self.assertEqual(rspJson["class"], "folder")

        # create a child domain
        domain = self.base_domain + "/" + folder_name + "/" + domain_name
        headers = helper.getRequestHeaders(domain=domain)
        rsp = self.session.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # try delete the folder
        domain = self.base_domain + "/" + folder_name
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + "/"
        body = {"folder": True}
        rsp = self.session.delete(req, headers=headers)
        # should get 409
        self.assertEqual(rsp.status_code, 409)

        # delete the child domain
        domain = self.base_domain + "/" + folder_name + "/" + domain_name
        headers = helper.getRequestHeaders(domain=domain)
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # try delete the folder
        domain = self.base_domain + "/" + folder_name
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + "/"
        body = {"folder": True}
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)

    def testInvalidChildDomain(self):
        domain = self.base_domain + "/notafolder/newdomain.h5"
        # should fail assuming "notafolder" doesn't exist
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + "/"

        rsp = self.session.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 404)

    def testGetNotFound(self):
        domain = self.base_domain + "/doesnotexist.h6"
        print("testGetNotFound", domain)
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + "/"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 404)

    def testNullDomain(self):
        headers = helper.getRequestHeaders()
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        self.assertTrue(rsp.status_code in (400, 404))

    def testInvalidDomain(self):
        domain = "bad_domain.h5"
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 404)  # TBD - should this be 400?

    def testWithBucket(self):
        domain = self.base_domain + "/with_bucket.h5"
        headers = helper.getRequestHeaders(domain=domain)
        params = {"bucket": "doesnotexistbucket47839293433"}
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 404)

    def testInvalidBucket(self):
        domain = self.base_domain + "/invalid_bucket.h5"
        headers = helper.getRequestHeaders(domain=domain)
        params = {"bucket": "bad/bucket"}
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 400)

    def testDNSDomain(self):
        # DNS domain names are in reverse order with dots as seperators...

        dns_domain = helper.getDNSDomain(self.base_domain)
        print("testDNSDomain", dns_domain)
        # verify we can access base domain as via dns name
        headers = helper.getRequestHeaders(domain=dns_domain)

        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["content-type"], "application/json; charset=utf-8")

        # can't have two consecutive dots'
        domain = "two.dots..are.bad." + dns_domain
        req = helper.getEndpoint() + "/"
        headers = helper.getRequestHeaders(domain=domain)
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)  # 400 == bad syntax

        # can't have a slash
        domain = "no/slash." + dns_domain
        req = helper.getEndpoint() + "/"
        headers = helper.getRequestHeaders(domain=domain)
        rsp = self.session.get(req, headers=headers)
        # somehow this is showing up as a 400 in ceph and 404 in S3
        self.assertTrue(rsp.status_code in (400, 404))  # 400 == bad syntax

        # just a dot is no good
        domain = "." + dns_domain
        req = helper.getEndpoint() + "/"
        headers = helper.getRequestHeaders(domain=domain)
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)  # 400 == bad syntax

        # dot in the front is bad
        domain = ".dot.in.front.is.bad." + dns_domain
        req = helper.getEndpoint() + "/"
        headers = helper.getRequestHeaders(domain=domain)
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)  # 400 == bad syntax

    def testDeleteDomain(self):
        domain = self.base_domain + "/deleteme.h6"
        print("testDeleteDomain", domain)

        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + "/"

        # create a domain
        rsp = self.session.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]

        # add a sub-group
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        group_id = rspJson["id"]
        self.assertTrue(helper.validateId(group_id))

        # do a get on the domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(root_id, rspJson["root"])

        # try deleting the domain with a user who doesn't have permissions'
        user2_name = config.get("user2_name")
        headers = helper.getRequestHeaders(domain=self.base_domain, username=user2_name)
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 403)  # forbidden

        # delete the domain (with the orginal user)
        headers = helper.getRequestHeaders(domain=domain)
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # try getting the domain
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 410)

        # try re-creating a domain
        rsp = self.session.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        new_root_id = rspJson["root"]
        self.assertTrue(new_root_id != root_id)

        # verify we can access root groups
        root_req = helper.getEndpoint() + "/groups/" + new_root_id
        headers = helper.getRequestHeaders(domain=domain)
        rsp = self.session.get(root_req, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # delete the domain with the admin account
        try:
            admin_username = config.get("admin_username")
            admin_passwd = config.get("admin_password")
            headers = helper.getRequestHeaders(
                domain=domain, username=admin_username, password=admin_passwd
            )
            rsp = self.session.delete(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)
        except KeyError:
            msg = "Skipping admin delete test, set ADMIN_USERNAME and ADMIN_PASSWORD"
            msg += "environment variables to enable"
            print(msg)

        # try creating a folder using the owner flag
        try:
            admin_username = config.get("admin_username")
            admin_passwd = config.get("admin_password")
            username = config.get("user2_name")
            new_domain = f"{self.base_domain}/{username}_folder"
            body = {"folder": True, "owner": username}
            headers = helper.getRequestHeaders(
                domain=new_domain, username=admin_username, password=admin_passwd
            )
            rsp = self.session.put(req, headers=headers, data=json.dumps(body))
            self.assertEqual(rsp.status_code, 201)

            headers = helper.getRequestHeaders(domain=new_domain, username=username)
            rsp = self.session.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
        except KeyError:
            msg = "Skipping domain create with owner test, set ADMIN_USERNAME"
            msg += " and ADMIN_PASSWORD environment variables to enable"
            print(msg)

    def testDomainCollections(self):
        domain = helper.getTestDomain("tall.h5")
        print("testDomainCollections", domain)
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + "/"

        rsp = self.session.get(req, headers=headers)
        if rsp.status_code != 200:
            print(
                "WARNING: Failed to get domain: {}. Is test data setup?".format(domain)
            )
            return  # abort rest of test

        rspJson = json.loads(rsp.text)
        for k in ("root", "owner", "created", "lastModified"):
            self.assertTrue(k in rspJson)

        root_id = rspJson["root"]
        helper.validateId(root_id)

        # get the datasets collection
        req = helper.getEndpoint() + "/datasets"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("datasets" in rspJson)
        datasets = rspJson["datasets"]
        for objid in datasets:
            helper.validateId(objid)
        self.assertEqual(len(datasets), 4)

        # get the first 2 datasets
        params = {"Limit": 2}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("datasets" in rspJson)
        batch = rspJson["datasets"]
        self.assertEqual(len(batch), 2)
        helper.validateId(batch[0])

        self.assertEqual(batch[0], datasets[0])
        helper.validateId(batch[1])
        self.assertEqual(batch[1], datasets[1])

        # next batch
        params["Marker"] = batch[1]
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("datasets" in rspJson)
        batch = rspJson["datasets"]
        self.assertEqual(len(batch), 2)
        helper.validateId(batch[0])
        self.assertEqual(batch[0], datasets[2])
        helper.validateId(batch[1])
        self.assertEqual(batch[1], datasets[3])

        # get the groups collection
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("groups" in rspJson)
        groups = rspJson["groups"]
        self.assertEqual(len(groups), 5)
        # get the first 2 groups
        params = {"Limit": 2}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("groups" in rspJson)
        batch = rspJson["groups"]
        self.assertEqual(len(batch), 2)
        helper.validateId(batch[0])
        self.assertEqual(batch[0], groups[0])
        helper.validateId(batch[1])
        self.assertEqual(batch[1], groups[1])
        # next batch
        params["Marker"] = batch[1]
        params["Limit"] = 100
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("groups" in rspJson)
        batch = rspJson["groups"]
        self.assertEqual(len(batch), 3)
        for i in range(3):
            helper.validateId(batch[i])
            self.assertEqual(batch[i], groups[2 + i])

        # get the datatypes collection
        req = helper.getEndpoint() + "/datatypes"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("datatypes" in rspJson)
        datatypes = rspJson["datatypes"]
        self.assertEqual(len(datatypes), 0)  # no datatypes in this domain

    def testNewDomainCollections(self):
        # verify that newly added groups/datasets show up in the collections
        domain = self.base_domain + "/newDomainCollection.h5"
        helper.setupDomain(domain)
        print("testNewDomainCollections", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get root id
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        def make_group(parent_id, name):
            # create new group
            payload = {"link": {"id": parent_id, "name": name}}
            req = helper.getEndpoint() + "/groups"
            rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)
            rspJson = json.loads(rsp.text)
            new_group_id = rspJson["id"]
            self.assertTrue(helper.validateId(rspJson["id"]))
            return new_group_id

        def make_dset(parent_id, name):
            type_vstr = {
                "charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "strPad": "H5T_STR_NULLTERM",
                "length": "H5T_VARIABLE",
            }
            payload = {
                "type": type_vstr,
                "shape": 10,
                "link": {"id": parent_id, "name": name},
            }
            req = helper.getEndpoint() + "/datasets"
            rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)  # create dataset
            rspJson = json.loads(rsp.text)
            dset_id = rspJson["id"]
            self.assertTrue(helper.validateId(dset_id))
            return dset_id

        def make_ctype(parent_id, name):
            payload = {
                "type": "H5T_IEEE_F64LE",
                "link": {"id": parent_id, "name": name},
            }
            req = helper.getEndpoint() + "/datatypes"
            rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)
            rspJson = json.loads(rsp.text)
            dtype_id = rspJson["id"]
            self.assertTrue(helper.validateId(dtype_id))
            return dtype_id

        group_ids = []
        group_ids.append(make_group(root_uuid, "g1"))
        group_ids.append(make_group(root_uuid, "g2"))
        group_ids.append(make_group(root_uuid, "g3"))
        g3_id = group_ids[2]
        dset_ids = []
        dset_ids.append(make_dset(g3_id, "ds1"))
        dset_ids.append(make_dset(g3_id, "ds2"))
        ctype_ids = []
        ctype_ids.append(make_ctype(g3_id, "ctype1"))

        # get the groups collection
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)

        groups = rspJson["groups"]
        self.assertEqual(len(groups), len(group_ids))
        for objid in groups:
            helper.validateId(objid)
            self.assertTrue(objid in group_ids)

        # get the datasets collection
        req = helper.getEndpoint() + "/datasets"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)

        datasets = rspJson["datasets"]
        self.assertEqual(len(datasets), len(dset_ids))
        for objid in datasets:
            helper.validateId(objid)
            self.assertTrue(objid in dset_ids)

        # get the datatypes collection
        req = helper.getEndpoint() + "/datatypes"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)

        datatypes = rspJson["datatypes"]
        self.assertEqual(len(datatypes), len(ctype_ids))
        for objid in datatypes:
            helper.validateId(objid)
            self.assertTrue(objid in ctype_ids)

    def testGetDomains(self):
        folder = self.base_domain + "/testGetDomains"
        helper.setupDomain(folder, folder=True)
        print("testGetDomains", folder)

        # create some domains in the base_domain folder
        domain_count = 8
        basenames = []
        for i in range(domain_count):
            domain = "domain_" + str(i) + ".h5"
            basenames.append(domain)
            sub_domain = folder + "/" + domain
            helper.setupDomain(sub_domain)
            headers = helper.getRequestHeaders(domain=sub_domain)
            # get root id
            req = helper.getEndpoint() + "/"
            rsp = self.session.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            root_id = rspJson["root"]
            helper.validateId(root_id)
            # create attr1 in root group
            attr_name = "attr1"
            attr_payload = {"type": "H5T_STD_I32LE", "value": i * 2}
            req = (
                helper.getEndpoint() + "/groups/" + root_id + "/attributes/" + attr_name
            )
            rsp = self.session.put(req, data=json.dumps(attr_payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)  # created
            # create attr2 in root group
            attr_name = "attr2"
            fixed_str_type = {
                "charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 8,
                "strPad": "H5T_STR_NULLPAD",
            }
            attr_payload = {"type": fixed_str_type, "value": f"A{i:07}"}
            req = (
                helper.getEndpoint() + "/groups/" + root_id + "/attributes/" + attr_name
            )
            rsp = self.session.put(req, data=json.dumps(attr_payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)  # created

        headers = helper.getRequestHeaders(domain=folder + "/")
        req = helper.getEndpoint() + "/domains"
        rsp = self.session.get(req, headers=headers)  # , params=params)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["content-type"], "application/json; charset=utf-8")
        rspJson = json.loads(rsp.text)
        self.assertTrue("domains" in rspJson)
        domains = rspJson["domains"]

        self.assertEqual(domain_count, len(domains))

        for item in domains:
            self.assertTrue("name" in item)
            name = item["name"]
            self.assertTrue(pp.basename(name) in basenames)
            self.assertEqual(name[0], "/")
            self.assertTrue(name[-1] != "/")
            self.assertTrue("owner" in item)
            self.assertTrue("class" in item)
            self.assertEqual(item["class"], "domain")
            self.assertTrue("lastModified" in item)
            self.assertFalse("size" in item)
            self.assertFalse("compressors" in item)

        # try getting the first 4 domains
        params = {"domain": folder + "/", "Limit": 4}
        if config.get("bucket_name"):
            params["bucket"] = config.get("bucket_name")
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("domains" in rspJson)
        part1 = rspJson["domains"]
        self.assertEqual(len(part1), 4)
        for item in part1:
            self.assertTrue("name" in item)
            name = item["name"]
            self.assertTrue(pp.basename(name) in basenames[0:4])
            self.assertEqual(name[0], "/")
            self.assertTrue(name[-1] != "/")

        # get next batch of 4
        params = {"domain": folder + "/", "Marker": name, "Limit": 4}
        if config.get("bucket_name"):
            params["bucket"] = config.get("bucket_name")
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("domains" in rspJson)
        part2 = rspJson["domains"]
        self.assertEqual(len(part2), 4)
        for item in part2:
            self.assertTrue("name" in item)
            name = item["name"]
            self.assertTrue(pp.basename(name) in basenames[4:8])
            self.assertTrue(name != params["Marker"])

        # try using a glob pattern
        pattern = "domain_[0-3].h5"
        params = {"domain": folder + "/", "pattern": pattern}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("domains" in rspJson)
        domains = rspJson["domains"]
        self.assertEqual(len(domains), 4)
        domain_names = ("domain_0.h5", "domain_1.h5", "domain_2.h5", "domain_3.h5")
        for item in domains:
            self.assertTrue("name" in item)
            name = item["name"]
            self.assertTrue(pp.basename(name) in domain_names)

        # try using a glob pattern with wildcard
        pattern = "domain_*.h5"
        params = {"domain": folder + "/", "pattern": pattern}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("domains" in rspJson)
        domains = rspJson["domains"]
        self.assertEqual(len(domains), 8)
        for item in domains:
            self.assertTrue("name" in item)
            name = item["name"]
            self.assertTrue(pp.basename(name) in basenames)

        # use reg ex with attribute specification
        query = "attr1 > 7"
        params = {"domain": folder + "/", "query": query}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("domains" in rspJson)
        domains = rspJson["domains"]
        self.assertEqual(len(domains), 4)

        # not equal query on attr2
        query = "attr2 != 'A0000004'"
        params = {"domain": folder + "/", "query": query}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("domains" in rspJson)
        domains = rspJson["domains"]
        self.assertEqual(len(domains), 7)

        # combination query on attr1 and attr2
        query = "attr1 > 7 AND attr2 != 'A0000004'"
        params = {"domain": folder + "/", "query": query}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("domains" in rspJson)
        domains = rspJson["domains"]
        self.assertEqual(len(domains), 3)

        # bad query expression
        query = "atttr1 > 7 AND"
        params = {"domain": folder + "/", "query": query}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        # empty sub-domains
        domain = helper.getTestDomain("tall.h5") + "/"
        params = {"domain": domain}
        if config.get("bucket_name"):
            params["bucket"] = config.get("bucket_name")
        rsp = self.session.get(req, params=params, headers=headers)
        if rsp.status_code == 404:
            print(f"WARNING: Failed to get domain: {domain}. Is test data setup?")
        else:
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            self.assertTrue("domains" in rspJson)
            domains = rspJson["domains"]
            self.assertEqual(len(domains), 0)

    def testGetDomainsVerbose(self):
        domain = helper.getTestDomain("tall.h5")
        folder = domain[: -(len("/tall.h5"))]  # get the folder path
        print("testGetDomainsVerbose", domain)
        headers = helper.getRequestHeaders(domain=folder)

        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        if rsp.status_code != 200:
            print(f"WARNING: Failed to get domain: {folder}. Is test data setup?")
            return  # abort rest of test
        self.assertEqual(rsp.headers["content-type"], "application/json; charset=utf-8")
        rspJson = json.loads(rsp.text)

        for name in ("lastModified", "created", "hrefs", "owner", "class"):
            self.assertTrue(name in rspJson)
        now = time.time()
        self.assertTrue(rspJson["created"] < now - 10)
        self.assertTrue(rspJson["lastModified"] < now - 10)
        self.assertEqual(len(rspJson["hrefs"]), 3)
        self.assertTrue(rspJson["owner"])
        self.assertEqual(rspJson["class"], "folder")

        # get dommains in folder
        req = helper.getEndpoint() + "/domains"
        params = {"domain": folder + "/", "verbose": 1}
        if config.get("bucket_name"):
            params["bucket"] = config.get("bucket_name")

        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)

        rspJson = json.loads(rsp.text)
        self.assertTrue("domains" in rspJson)
        domains = rspJson["domains"]

        domain_count = len(domains)
        if domain_count == 0:
            # this should only happen in the very first test run
            print("Expected to find more domains!")
            self.assertTrue(False)
            return
        tall_item = None
        for item in domains:
            self.assertTrue("name" in item)
            name = item["name"]
            self.assertTrue(name.startswith(folder))
            self.assertTrue(name[-1] != "/")
            self.assertTrue("owner" in item)
            self.assertTrue("created" in item)
            self.assertTrue("lastModified" in item)
            self.assertFalse("size" in item)
            self.assertTrue("class") in item
            self.assertTrue(item["class"] in ("domain", "folder"))
            if name.endswith("tall.h5"):
                tall_item = item
        if not tall_item:
            print("WARNING: Failed to get domain. Is test data setup?")
            return  # abort rest of test
        self.assertEqual(tall_item["class"], "domain")
        self.assertTrue("num_objects" in tall_item)
        self.assertEqual(tall_item["num_objects"], 14)
        self.assertTrue("allocated_bytes" in tall_item)
        self.assertEqual(tall_item["allocated_bytes"], 580)
        self.assertTrue("total_size" in tall_item)
        self.assertTrue(tall_item["total_size"] > 5000)

    def testGetTopLevelDomains(self):
        print("testGetTopLevelDomains", self.base_domain)

        # Either '/' or no domain should get same result
        for domain in (None, "/"):
            headers = helper.getRequestHeaders(domain=domain)
            req = helper.getEndpoint() + "/domains"
            rsp = self.session.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)
            self.assertEqual(
                rsp.headers["content-type"], "application/json; charset=utf-8"
            )
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
                self.assertEqual(name[0], "/")
                self.assertTrue(name[-1] != "/")
                self.assertTrue("owner" in item)
                self.assertTrue("created" in item)
                self.assertTrue("lastModified" in item)
                self.assertFalse("size" in item)
                self.assertTrue("class") in item
                self.assertTrue(item["class"] in ("domain", "folder"))


if __name__ == "__main__":
    # setup test files

    unittest.main()
