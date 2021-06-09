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
import json
import config
import helper


class SetupTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(SetupTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)

    def setUp(self):
        self.session = helper.getSession()

    def tearDown(self):
        if self.session:
            self.session.close()

        # main

    def testHomeFolders(self):
        print("testHomeFolders")
        home_domain = "/home"
        user_name = config.get("user_name")
        user_domain = home_domain + "/" + user_name
        headers = helper.getRequestHeaders()
        # get headers for admin account if config is setup
        admin_headers = None
        try:
            admin_username = config.get("admin_username")
            admin_passwd = config.get("admin_password")
            print("using admin account:", admin_username, "admin_passwd: ", "*"*len(admin_passwd))
            admin_headers = helper.getRequestHeaders(username=admin_username, password=admin_passwd)
        except KeyError:
            pass

        req = helper.getEndpoint() + '/'
        params={"domain": home_domain}
        rsp = self.session.get(req, params=params, headers=headers)
        print("/home get status:", rsp.status_code)

        if rsp.status_code == 404:
            if not admin_headers:
                print("/home folder doesn't exist, set ADMIN_USERNAME AND ADMIN_PASSWORD environment variableS to enable creation")
                self.assertTrue(False)
            # Setup /home folder
            print("create home folder")
            body = {"folder": True}
            rsp = self.session.put(req, data=json.dumps(body), params=params, headers=admin_headers)
            print("put request status:", rsp.status_code)
            self.assertEqual(rsp.status_code, 201)
            # do the original request again
            rsp = self.session.get(req, params=params, headers=headers)
        elif rsp.status_code in (401, 403):
            print(f"Authorization failure, verify password for {user_name} and set env variable for USER_PASSWORD")
            self.assertTrue(False)
        
        # 
        print("got status code:", rsp.status_code)
        self.assertEqual(rsp.status_code, 200)
        
        rspJson = json.loads(rsp.text)
        print("home folder json:", rspJson)
        for k in ("owner", "created", "lastModified"):
             self.assertTrue(k in rspJson)
        self.assertFalse("root" in rspJson)  # no root -> folder

        params={"domain": user_domain}
        rsp = self.session.get(req, params=params, headers=headers)
        print(f"{user_domain} get status: {rsp.status_code}")
        if rsp.status_code == 404:
            if not admin_headers:
                print(f"{user_domain} folder doesn't exist, set ADMIN_USERNAME and ADMIN_PASSWORD environment variable to enable creation")
                self.assertTrue(False)
            # Setup user home folder
            print("create user folder")
            body = {"folder": True, "owner": user_name}
            rsp = self.session.put(req, data=json.dumps(body), params=params, headers=admin_headers)
            self.assertEqual(rsp.status_code, 201)
            # do the original request again
            rsp = self.session.get(req, params=params, headers=headers)

        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        print("user folder:", rspJson)
        self.assertFalse("root" in rspJson)  # no root group for folder domain
        self.assertTrue("owner" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("class" in rspJson)
        self.assertEqual(rspJson["class"], "folder")


if __name__ == '__main__':
    #setup test files

    unittest.main()
