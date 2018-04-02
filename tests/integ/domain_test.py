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
import warnings
import requests
import time
import json
import config
import helper

# ----------------------------------------------------------------------

class GetDomainPatternsTest(unittest.TestCase):

    def assertLooksLikeUUID(self, s):
        self.assertTrue(helper.validateId(s))

    def __init__(self, *args, **kwargs):
        super(GetDomainPatternsTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)

        self.endpoint = helper.getEndpoint()
        self.headers = helper.getRequestHeaders()

        response = requests.get(
                self.endpoint + "/",
                headers = helper.getRequestHeaders(domain=self.base_domain))
        assert response.status_code == 200, f"HTTP code {response.status_code}"
        self.expected_root = response.json()["root"]

    # this is what we did to get our expected root
    def testHeaderHost(self):
        # domain is recorded as 'host' header
        headers_with_host = helper.getRequestHeaders(domain=self.base_domain)
        response = requests.get(
                self.endpoint + "/",
                headers=headers_with_host)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['content-type'], 'application/json')
        self.assertEqual(response.json()["root"], self.expected_root)

    def testHeaderHostMissingLeadSlash(self):
        bad_domain = self.base_domain[1:] # remove leading '/'
        headers = helper.getRequestHeaders(domain=bad_domain)
        response = requests.get(self.endpoint + "/", headers=headers)
        self.assertEqual(response.status_code, 400)

    def testQueryHost(self):
        params = {"host": self.base_domain}
        response = requests.get(
                self.endpoint + "/",
                headers=self.headers,
                params=params)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['content-type'], 'application/json')
        self.assertEqual(response.json()["root"], self.expected_root)

    def testQueryHostMissingLeadSlash(self):
        params = {"host": self.base_domain[1:]} # remove leading '/'
        response = requests.get(
                self.endpoint + "/",
                headers=self.headers)
        self.assertEqual(response.status_code, 400)

    def testQueryHostDNS(self):
        dns_domain = helper.getDNSDomain(self.base_domain)
        response = requests.get(
                self.endpoint + "/",
                headers=self.headers,
                params={"host": dns_domain})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['content-type'], 'application/json')
        self.assertEqual(response.json()["root"], self.expected_root)

    def testQueryHostDNSMalformed(self):
        dns_domain = helper.getDNSDomain(self.base_domain)
        req = self.endpoint + "/"

        for predomain in (
            "two.dots..are.bad.",
            "no/slash",
            ".",
            ".sure.leading.dot",
        ):
            domain = predomain + dns_domain
            response = requests.get(
                    req,
                    headers=self.headers,
                    params={"host": domain})
            self.assertEqual(
                    response.status_code,
                    400,
                    f"predomain '{predomain}' should fail")

    def testHeaderHostDNS(self):
        dns_domain = helper.getDNSDomain(self.base_domain)
        req = self.endpoint + '/'

        # verify we can access base domain as via dns name
        headers = helper.getRequestHeaders(domain=dns_domain)
        response = requests.get(req, headers=headers)
        self.assertEqual(response.status_code, 200)
        self.assertLooksLikeUUID(response.json()["root"])

    def testHeaderHostDNSMalformed(self):
        dns_domain = helper.getDNSDomain(self.base_domain)
        req = self.endpoint + '/'

        for predomain in (
            "two.dots..are.bad.",
            "no/slash",
            ".",
            ".sure.leading.dot",
        ):
            domain = predomain + dns_domain
            headers = helper.getRequestHeaders(domain=domain)
            response = requests.get(req, headers=headers)
            self.assertEqual(
                    response.status_code,
                    400,
                    f"predomain '{predomain}' should fail")

    def testQueryDomain(self):
        params = {"domain": self.base_domain}
        response = requests.get(
                self.endpoint + "/",
                headers=self.headers,
                params=params)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['content-type'], 'application/json')
        self.assertEqual(response.json()["root"], self.expected_root)

    def testQueryDomainMissingLeadSlash(self):
        params = {"domain": self.base_domain[1:]} # remove leading '/'
        response = requests.get(
                self.endpoint + "/",
                headers=self.headers)
        self.assertEqual(response.status_code, 400)

# ----------------------------------------------------------------------

@unittest.skipUnless(
        config.get("test_on_uploaded_file"),
        "sample file may not be present")
class OperationsOnUploadedTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(OperationsOnUploadedTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)

    def testGetDomain(self):
        domain = helper.getTestDomain("tall.h5")
        headers = helper.getRequestHeaders(domain=domain)

        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(
                rsp.status_code, 200, f"Failed to get domain {self.domain}")
        self.assertEqual(rsp.headers['content-type'], 'application/json')
        rspJson = rsp.json()

        now = time.time()
        self.assertTrue(rspJson["created"] < now - 60 * 5)
        self.assertTrue(rspJson["lastModified"] < now - 60 * 5)
        self.assertEqual(len(rspJson["hrefs"]), 7)
        self.assertTrue(rspJson["root"].startswith("g-"))
        self.assertTrue(rspJson["owner"])
        self.assertEqual(rspJson["class"], "domain")
        self.assertFalse(
                "num_groups" in rspJson,
                "'num_groups' should only show up with the verbose param")
        self.assertLooksLikeUUID(rspJson["root"])

    def testGetByPath(self):
        domain = helper.getTestDomain("tall.h5")
        headers = helper.getRequestHeaders(domain=domain)

        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(
                rsp.status_code, 200, f"Failed to get domain {self.domain}")
        domainJson = json.loads(rsp.text)
        self.assertTrue("root" in domainJson)
        root_id = domainJson["root"]

        # Get group at /g1/g1.1 by using h5path
        params = {"h5path": "/g1/g1.1"}
        rsp = requests.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        g11id = helper.getUUIDByPath(domain, "/g1/g1.1")
        self.assertEqual(g11id, rspJson["id"])
        self.assertTrue("root" in rspJson)
        self.assertEqual(root_id, rspJson["root"])

        # Get dataset at /g1/g1.1/dset1.1.1 by using relative h5path
        params = {"h5path": "./g1/g1.1/dset1.1.1"}
        rsp = requests.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        d111id = helper.getUUIDByPath(domain, "/g1/g1.1/dset1.1.1")
        self.assertEqual(d111id, rspJson["id"])
        self.assertTrue("root" in rspJson)
        self.assertEqual(root_id, rspJson["root"])

    def testDomainCollections(self):
        domain = helper.getTestDomain("tall.h5")
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + '/'

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200, f"Can't get domain: {domain}")

        rspJson = json.loads(rsp.text)
        for k in ("root", "owner", "created", "lastModified"):
             self.assertTrue(k in rspJson)

        root_id = rspJson["root"]
        self.assertLooksLikeUUID(root_id)

        # get the datasets collection
        req = helper.getEndpoint() + '/datasets'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("datasets" in rspJson)
        datasets = rspJson["datasets"]
        for objid in datasets:
            self.assertLooksLikeUUID(objid)
        self.assertEqual(len(datasets), 4)

        # get the first 2 datasets
        params = {"Limit": 2}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("datasets" in rspJson)
        batch = rspJson["datasets"]
        self.assertEqual(len(batch), 2)
        self.assertLooksLikeUUID(batch[0])
        self.assertEqual(batch[0], datasets[0])
        self.assertLooksLikeUUID(batch[1])
        self.assertEqual(batch[1], datasets[1])
        # next batch
        params["Marker"] = batch[1]
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("datasets" in rspJson)
        batch = rspJson["datasets"]
        self.assertEqual(len(batch), 2)
        self.assertLooksLikeUUID(batch[0])
        self.assertEqual(batch[0], datasets[2])
        self.assertLooksLikeUUID(batch[1])
        self.assertEqual(batch[1], datasets[3])

        # get the groups collection
        req = helper.getEndpoint() + '/groups'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("groups" in rspJson)
        groups = rspJson["groups"]
        self.assertEqual(len(groups), 5)
        # get the first 2 groups
        params = {"Limit": 2}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("groups" in rspJson)
        batch = rspJson["groups"]
        self.assertEqual(len(batch), 2)
        self.assertLooksLikeUUID(batch[0])
        self.assertEqual(batch[0], groups[0])
        self.assertLooksLikeUUID(batch[1])
        self.assertEqual(batch[1], groups[1])
        # next batch
        params["Marker"] = batch[1]
        params["Limit"] = 100
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("groups" in rspJson)
        batch = rspJson["groups"]
        self.assertEqual(len(batch), 3)
        for i in range(3):
            self.assertLooksLikeUUID(batch[i])
            self.assertEqual(batch[i], groups[2+i])

        # get the datatypes collection
        req = helper.getEndpoint() + '/datatypes'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("datatypes" in rspJson)
        datatypes = rspJson["datatypes"]
        self.assertEqual(len(datatypes), 0)  # no datatypes in this domain

    def testGetDomainVerbose(self):
        domain = helper.getTestDomain("tall.h5")
        headers = helper.getRequestHeaders(domain=domain)

        req = helper.getEndpoint() + '/'
        params = {"verbose": 1}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200, f"Can't get domain: {domain}")

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
        self.assertLooksLikeUUID(root_uuid)

        self.assertTrue("num_groups" in rspJson)
        self.assertEqual(rspJson["num_groups"], 5)
        self.assertTrue("num_datasets" in rspJson)
        self.assertEqual(rspJson["num_datasets"], 4)
        self.assertTrue("num_datatypes" in rspJson)
        self.assertEqual(rspJson["num_datatypes"], 0)
        self.assertTrue("allocated_bytes" in rspJson)

        # test that allocated_bytes falls in a given range
        self.assertTrue(rspJson["allocated_bytes"] > 5000)
        self.assertTrue(rspJson["allocated_bytes"] < 6000)
        self.assertTrue("num_chunks" in rspJson)
        self.assertTrue(rspJson["num_chunks"], 4)

# ----------------------------------------------------------------------

class DomainTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(DomainTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.base_endpoint = helper.getEndpoint() + "/"

    def assertLooksLikeUUID(self, s):
        self.assertTrue(helper.validateId(s))

    def assertDictHasOnlyTheseKeys(self, keys, d):
        for key in keys :
            self.assertTrue(key in d, "missing element: {key}")
        self.assertEqual(
                len(keys),
                len(d),
                list(d.keys()))

    def assertLooksLikePutFolderResponse(self, _json):
        putfolder_keys = (
            "owner",
            "acls",
            "created",
            "lastModified",
        )
        self.assertDictHasOnlyTheseKeys(putfolder_keys, _json)

    def assertLooksLikeGetFolderResponse(self, _json):
        getfolder_keys = (
            "owner",
            "hrefs",
            "created",
            "class",
            "lastModified",
        )
        self.assertDictHasOnlyTheseKeys(getfolder_keys, _json)
        self.assertEqual(_json["class"], "folder")

    def assertLooksLikePutDomainResponse(self, _json):
        putdomain_keys = (
            "root",
            "owner",
            "acls",
            "created",
            "lastModified",
        )
        self.assertDictHasOnlyTheseKeys(putdomain_keys, _json)
        self.assertLooksLikeUUID(_json["root"])

    def assertLooksLikeGetDomainResponse(self, _json):
        getdomain_keys = (
            "root",
            "owner",
            "created",
            "lastModified",
            "class",
            "hrefs",
        )
        self.assertDictHasOnlyTheseKeys(getdomain_keys, _json)
        self.assertEqual(_json["class"], "domain")
        self.assertLooksLikeUUID(_json["root"])

    def testGetFolderDomains(self):
        username = config.get("user_name")
        for domain in ( # demonstrate both path- and dns-like domain names
            "/home",
            "home",
            f"/home/{username}",
            f"{username}.home",
        ):
            headers = helper.getRequestHeaders(domain=domain)
            response = requests.get(self.base_endpoint, headers=headers)
            self.assertEqual(
                    response.status_code,
                    200,
                    f"Unable to get domain {domain}")
            self.assertLooksLikeGetFolderResponse(response.json())

    def testCreateAndGetDomain(self):
        domain = self.base_domain + "/newdomain.h6"
        headers = helper.getRequestHeaders(domain=domain)

        # must not already exist
        get_absent_code = requests.get(
                self.base_endpoint,
                headers=headers
        ).status_code
        self.assertEqual(get_absent_code, 404)

        # put up new Domain
        rsp = requests.put(self.base_endpoint, headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify expected elements returned
        rspJson = rsp.json()
        self.assertLooksLikePutDomainResponse(rspJson)
        root_id = rspJson["root"]

        # putting the same domain again fails with a 409 error
        self.assertEqual(
                requests.put( self.base_endpoint, headers=headers).status_code,
                409,
                "creating duplicate domain name not allowed")

        # verify that we can get the Domain back
        rsp = requests.get(self.base_endpoint, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertLooksLikeGetDomainResponse(rsp.json())

    def testCreateDomainNotAuthorizedFails401(self):
        domain = self.base_domain + "/user_infringement.h6"
        headers = helper.getRequestHeaders(domain=domain)
        other_user = "user2" if config.get("user_name") != "user2" else "user3"
        other_headers = helper.getRequestHeaders(
                domain=domain,
                username=other_user)

        # domain must not already exist
        response = requests.get(self.base_endpoint, headers=headers)
        self.assertEqual(response.status_code, 404, f"domain {domain} extant")

        # other user lacks permission to create domain
        response = requests.put(self.base_endpoint, headers=other_headers)
        self.assertEqual(response.status_code, 401, "should lack permission")

    def testCreateAndGetFolder(self):
        domain = self.base_domain + "/newfolder"
        headers = helper.getRequestHeaders(domain=domain)
        endpoint = self.base_endpoint

        # must not already exist
        get_absent_code = requests.get(endpoint, headers=headers).status_code
        self.assertEqual(get_absent_code, 404, f"domain {domain} extant")

        # put up new Domain, inspect object returned
        body = {"folder": True}
        rsp = requests.put(
                endpoint,
                headers=headers,
                data=json.dumps(body))
        self.assertEqual(rsp.status_code, 201, f"unable to put {domain}")
        self.assertLooksLikePutFolderResponse(rsp.json())

        # verify that putting the same folder again fails with a 409 error
        rsp = requests.put(endpoint, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 409, "duplicate folder not allowed")

        # verify that putting a domain with the same name also 409s
        rsp = requests.put(endpoint, headers=headers)
        self.assertEqual(rsp.status_code, 409, "duplicate domain not allowed")

        # get newly-created folder and inspect
        rsp = requests.get(endpoint, headers=headers)
        self.assertEqual(rsp.status_code, 200, "can't get folder")
        self.assertLooksLikeGetFolderResponse(rsp.json())

    def testCreateDomainInMissingFolderFails404(self):
        domain = self.base_domain + "/notafolder/newdomain.h5"
        headers = helper.getRequestHeaders(domain=domain)

        rsp = requests.put(self.base_endpoint, headers=headers)
        self.assertEqual(rsp.status_code, 404)

    def testGetMissingDomainFails404(self):
        domain =  self.base_domain + "/doesnotexist.h6"
        headers = helper.getRequestHeaders(domain=domain)

        rsp = requests.get(self.base_endpoint, headers=headers)
        self.assertEqual(rsp.status_code, 404)

    # TODO: if this fails, there may be a lingering "deleteme.h6" domain
    def testDeleteDomain(self):
        domain = self.base_domain + "/deleteme.h6"
        headers = helper.getRequestHeaders(domain=domain)
        endpoint = self.base_endpoint

        self.assertEqual(
                requests.get(endpoint, headers=headers).status_code,
                404,
                "domain {domain} extant")

        self.assertEqual(
                requests.put(endpoint, headers=headers).status_code,
                201,
                "cannot create domain")

        self.assertEqual(
                requests.get(endpoint, headers=headers).status_code,
                200,
                "can't get domain")

        other_headers = helper.getRequestHeaders(
                domain=self.base_domain,
                username="test_user2")
        self.assertEqual(
                requests.delete(endpoint, headers=other_headers).status_code,
                403,
                "other user should lack permssion to delete the domain")

        self.assertEqual(
                requests.delete(endpoint, headers=headers).status_code,
                200,
                "original user should be able to delete the domain")

        self.assertEqual(
                requests.get(endpoint, headers=headers).status_code,
                410,
                "domain should be deleted")

    # TODO: if this fails, there may be a lingering "deleteme.h6" domain
    def testReplaceDeletedDomain(self):
        domain = self.base_domain + "/deleteme.h6"
        headers = helper.getRequestHeaders(domain=domain)
        endpoint = self.base_endpoint

        # SETUP: create domain
        self.assertEqual(
                requests.put(endpoint, headers=headers).status_code,
                201,
                "can't create domain")

        # SETUP: delete the domain
        self.assertEqual(
                requests.delete(endpoint, headers=headers).status_code,
                200,
                "can't delete domain")

        # TEST: can create new domain with same name
        self.assertEqual(
                requests.put(endpoint, headers=headers).status_code,
                201,
                "can't replace domain")

        # CLEANUP: remove domain again
        self.assertEqual(
                requests.delete(endpoint, headers=headers).status_code,
                200,
                "unable to re-delete domain")

    @unittest.skip(
            "Destructive behavior impairs other tests. " +
            "even when supposedly executed last.")
    def testzzzDeleteTopLevelFolder(self):
        """Deletes domains and folders from bottom up.

        `zzz` in name should make this the last test run in the class.

        Stops at first element where the user does not have permission.

        This behavior is unintuitive, especially given the "403 - forbidden"
        response code. (TODO)

        Once removed, the user cannot replace their home folder?
        """
        home = "/home"
        headers = helper.getRequestHeaders(domain=home)
        endpoint = self.base_endpoint

        response = requests.delete(endpoint, headers=headers)
        self.assertEqual(
                response.status_code,
                 403,
                "delete /home should be forbidden")

        # can still get home folder
        response = requests.get(endpoint, headers=headers)
        self.assertEqual(response.status_code, 200, "/home should exist")

        # show that user's folder was deleted
        user_domain = home + "/" + config.get("user_name")
        headers = helper.getRequestHeaders(domain=user_domain)

        response = requests.get(endpoint, headers=headers)
        self.assertEqual(
                response.status_code,
                410,
                "/home/<user> should exist")

        # user cannot replace their home folder?!?
        self.assertEqual(
                requests.put(endpoint, headers=headers).status_code,
                403,
                "unable to recreate user folder")

    def testDeleteFolderWithSubdomains(self):
        folderpath = self.base_domain + "/deletemefolder"
        domainpaths = [f"{folderpath}/{i}" for i in range(3)]
        endpoint = self.base_endpoint

        # SETUP - make folder
        response = requests.put(
                endpoint, 
                headers=helper.getRequestHeaders(domain=folderpath),
                data=json.dumps({"folder": True}))
        self.assertEqual(response.status_code, 201, "unable to make folder")
        self.assertLooksLikePutFolderResponse(response.json())

        # SETUP - add child domains
        for domain in domainpaths:
            response = requests.put(
                    endpoint, 
                    headers=helper.getRequestHeaders(domain=domain))
            self.assertEqual(
                    response.status_code,
                    201,
                    f"unable to make domain {domain}")
            self.assertLooksLikePutDomainResponse(response.json())

        # delete folder
        response = requests.delete(
                endpoint, 
                headers=helper.getRequestHeaders(domain=folderpath))
        self.assertEqual(response.status_code, 200, "unable to delete folder")

        # verify that we can't get folder nor domains
        domainpaths.append(folderpath)
        for domain in domainpaths:
            self.assertEqual(
                    requests.get(
                            endpoint,
                            headers=helper.getRequestHeaders(domain=domain)
                    ).status_code,
                    410,
                    f"domain {domain} was not deleted")

    # TODO: refactor the snot out of this
    def testNewDomainCollections(self):
        # verify that newly added groups/datasets show up in the collections
        headers = helper.getRequestHeaders(domain=self.base_domain)
        root_endpoint = self.base_endpoint
        dset_endpoint = helper.getEndpoint() + "/datasets"
        group_endpoint = helper.getEndpoint() + "/groups"
        type_endpoint = helper.getEndpoint() + "/datatypes"

        # get root id
        rsp = requests.get(root_endpoint, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        self.assertLooksLikeUUID(root_uuid)

        def make_group(parent_id, name):
            # create new group
            payload = { 'link': { 'id': parent_id, 'name': name } }
            rsp = requests.post(group_endpoint, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)
            rspJson = json.loads(rsp.text)
            new_group_id = rspJson["id"]
            self.assertLooksLikeUUID(rspJson["id"])
            return new_group_id

        def make_dset(parent_id, name):
            type_vstr = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "strPad": "H5T_STR_NULLTERM",
                "length": "H5T_VARIABLE" }
            payload = {'type': type_vstr, 'shape': 10,
                'link': {'id': parent_id, 'name': name} }
            rsp = requests.post(dset_endpoint, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)  # create dataset
            rspJson = json.loads(rsp.text)
            dset_id = rspJson["id"]
            self.assertLooksLikeUUID(dset_id)
            return dset_id

        def make_ctype(parent_id, name):
            payload = {
                'type': 'H5T_IEEE_F64LE',
                'link': {'id': parent_id, 'name': name}
            }
            rsp = requests.post(type_endpoint, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)
            rspJson = json.loads(rsp.text)
            dtype_id = rspJson["id"]
            self.assertLooksLikeUUID(dtype_id)
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
        rsp = requests.get(group_endpoint, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)

        groups = rspJson["groups"]
        self.assertEqual(len(groups), len(group_ids))
        for objid in groups:
            self.assertLooksLikeUUID(objid)
            self.assertTrue(objid in group_ids)

        # get the datasets collection
        rsp = requests.get(dset_endpoint, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)

        datasets = rspJson["datasets"]
        self.assertEqual(len(datasets), len(dset_ids))
        for objid in datasets:
            self.assertLooksLikeUUID(objid)
            self.assertTrue(objid in dset_ids)

         # get the datatypes collection
        rsp = requests.get(type_endpoint, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)

        datatypes = rspJson["datatypes"]
        self.assertEqual(len(datatypes), len(ctype_ids))
        for objid in datatypes:
            self.assertLooksLikeUUID(objid)
            self.assertTrue(objid in ctype_ids)

    def testGetDomains(self):
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
            # TODO: ^ what?
            warnings.warn(f"Expected to find more domains in {domain}")
            return

        for item in domains:
            name = item["name"]
            self.assertTrue(name.startswith('/'))
            self.assertFalse(name.endswith('/'))
            self.assertTrue("owner" in item)
            self.assertTrue("created" in item)
            self.assertTrue("lastModified" in item)
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
        for host in (None, '/'):
            headers = helper.getRequestHeaders(domain=host)
            req = helper.getEndpoint() + '/domains'
            rsp = requests.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)
            self.assertEqual(rsp.headers['content-type'], 'application/json')
            domains = rsp.json()["domains"]

            # this should only happen in the very first test run
            # TODO: ^ what?
            if len(domains) == 0:
                warnings.warn(f"no domains found at top level ({host})")

            for item in domains:
                name = item["name"]
                self.assertTrue(name.startswith('/'))
                self.assertFalse(name.endswith('/'))
                self.assertTrue("owner" in item)
                self.assertTrue("created" in item)
                self.assertTrue("lastModified" in item)
                self.assertTrue(item["class"] in ("domain", "folder"))

# ----------------------------------------------------------------------

if __name__ == '__main__':
    unittest.main()

