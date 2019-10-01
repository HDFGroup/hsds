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
import config
import helper

acl_keys = ('create', 'read', 'update', 'delete', 'readACL', 'updateACL')
class AclTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(AclTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)

        # main

    def testGetAcl(self):
        print("testGetAcl", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # there should be an ACL for "test_user1" who has ability to do any action on the domain
        req = helper.getEndpoint() + '/acls/test_user1'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json; charset=utf-8')
        rsp_json = json.loads(rsp.text)
        self.assertTrue("acl" in rsp_json)
        self.assertTrue("hrefs" in rsp_json)
        acl = rsp_json["acl"]
        self.assertEqual(len(acl.keys()), len(acl_keys) + 1)
        self.assertEqual(acl["userName"], "test_user1")
        for k in acl_keys:
            self.assertTrue(k in acl)
            self.assertTrue(acl[k])

        # get the default ACL.  Only 'read' should be true if it exists
        req = helper.getEndpoint() + '/acls/default'
        rsp = requests.get(req, headers=headers)
        if config.get("default_public"):
            self.assertEqual(rsp.status_code, 200)
            self.assertEqual(rsp.headers['content-type'], 'application/json; charset=utf-8')
            rsp_json = json.loads(rsp.text)
            self.assertTrue("acl" in rsp_json)
            self.assertTrue("hrefs" in rsp_json)
            acl = rsp_json["acl"]
            self.assertEqual(len(acl.keys()), len(acl_keys) + 1)
            for k in acl_keys:
                self.assertTrue(k in acl)
                if k == 'read':
                    self.assertEqual(acl[k], True)
                else:
                    self.assertEqual(acl[k], False)
        else:
            self.assertTrue(rsp.status_code == 404)

        # get the root id
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # get the ACL for the Group
        req = helper.getEndpoint() + '/groups/' + root_uuid + "/acls/test_user1"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json; charset=utf-8')
        rsp_json = json.loads(rsp.text)
        self.assertTrue("acl" in rsp_json)
        self.assertTrue("hrefs" in rsp_json)
        acl = rsp_json["acl"]
        self.assertEqual(len(acl.keys()), len(acl_keys) + 1)
        for k in acl_keys:
            self.assertTrue(k in acl)
            self.assertEqual(acl[k], True)


        # try getting the ACL for a random user, should return 404
        req = helper.getEndpoint() + '/acls/joebob'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 404)

        # try fetching an ACL from a user who doesn't have readACL permissions
        req = helper.getEndpoint() + '/acls/test_user1'
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 403) # forbidden

    def testGetAcls(self):
        print("testGetAcls", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        if config.get("default_public"):
            expected_acl_count = 2
        else:
            expected_acl_count = 1

        # there should be an ACL for "default" with read-only access and
        #  "test_user1" who has ability to do any action on the domain
        req = helper.getEndpoint() + '/acls'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json; charset=utf-8')
        rsp_json = json.loads(rsp.text)
        self.assertTrue("acls" in rsp_json)
        self.assertTrue("hrefs" in rsp_json)
        acls = rsp_json["acls"]

        self.assertEqual(len(acls), expected_acl_count)

        for acl in acls:
            self.assertEqual(len(acl.keys()), len(acl_keys) + 1)
            self.assertTrue('userName' in acl)
            userName = acl['userName']
            self.assertTrue(userName in ("default", "test_user1"))
            if userName == "default":
                if expected_acl_count == 1:
                    # should just have the owner acl
                    self.assertTrue(False)
                for k in acl.keys():
                    if k == "userName":
                        continue
                    if k not in acl_keys:
                        self.assertTrue(False)
                    if k == "read":
                        self.assertEqual(acl[k], True)
                    else:
                        self.assertEqual(acl[k], False)
            else:
                for k in acl.keys():
                    if k == "userName":
                        continue
                    if k not in acl_keys:
                        self.assertTrue(False)
                    self.assertEqual(acl[k], True)

        # get root uuid
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # get the ACLs for the Group
        req = helper.getEndpoint() + '/groups/' + root_uuid + "/acls"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json; charset=utf-8')
        rsp_json = json.loads(rsp.text)
        self.assertTrue("acls" in rsp_json)
        self.assertTrue("hrefs" in rsp_json)
        acls = rsp_json["acls"]
        self.assertEqual(len(acls), expected_acl_count)


        # create a dataset
        payload = {'type': 'H5T_STD_I32LE', 'shape': 10,
             'link': {'id': root_uuid, 'name': 'dset'} }
        req = helper.getEndpoint() + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))

        # now try getting the ACLs for the dataset
        req = helper.getEndpoint() + '/datasets/' + dset_uuid + "/acls"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json; charset=utf-8')
        rsp_json = json.loads(rsp.text)
        self.assertTrue("acls" in rsp_json)
        self.assertTrue("hrefs" in rsp_json)
        acls = rsp_json["acls"]

        self.assertEqual(len(acls), expected_acl_count)

        # create a committed type
        payload = {
            'type': 'H5T_IEEE_F64LE',
            'link': {'id': root_uuid, 'name': 'dtype'}
        }

        req = helper.getEndpoint() + "/datatypes"
        # create a new ctype
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)
        dtype_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dtype_uuid) )

        # now try getting the ACLs for the datatype
        req = helper.getEndpoint() + '/datatypes/' + dtype_uuid + "/acls"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['content-type'], 'application/json; charset=utf-8')
        rsp_json = json.loads(rsp.text)
        self.assertTrue("acls" in rsp_json)
        self.assertTrue("hrefs" in rsp_json)
        acls = rsp_json["acls"]
        self.assertEqual(len(acls), expected_acl_count)

        # try fetching ACLs from a user who doesn't have readACL permissions
        req = helper.getEndpoint() + '/acls'
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 403) # forbidden


    def testPutAcl(self):
        print("testPutAcl", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # create an ACL for "test_user2" with read and update access
        req = helper.getEndpoint() + '/acls/test_user2'
        perm = {"read": True, "update": True}

        rsp = requests.put(req, headers=headers, data=json.dumps(perm))
        self.assertEqual(rsp.status_code, 201)

        # fetch the acl and verify it has been updated
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rsp_json = json.loads(rsp.text)
        self.assertTrue("acl" in rsp_json)
        self.assertTrue("hrefs" in rsp_json)
        acl = rsp_json["acl"]
        self.assertEqual(len(acl.keys()), len(acl_keys) + 2)  # acl_keys + "domain" + "username"

        for k in acl_keys:
            self.assertTrue(k in acl)
            if k in ("read", "update"):
                self.assertEqual(acl[k], True)
            else:
                self.assertEqual(acl[k], False)

        # The ACL should be fetchable by test_user2...
        req = helper.getEndpoint() + '/acls/test_user2'
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200) # ok

        # The default ACL should be fetchable by test_user2 as well...
        if config.get("default_public"):
            req = helper.getEndpoint() + '/acls/default'
            headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
            rsp = requests.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200) # ok

        # test_user2 shouldn't be able to read test_user1's ACL
        req = helper.getEndpoint() + '/acls/test_user1'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 403) # Forbidden


if __name__ == '__main__':
    #setup test files

    unittest.main()

