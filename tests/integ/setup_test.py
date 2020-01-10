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


class SetupTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(SetupTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)

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
            admin_passwd = config.get("admin_password")
            print("got admin_passwd:", admin_passwd)
            admin_headers = helper.getRequestHeaders(username="admin", password=admin_passwd)
        except KeyError:
            pass

        req = helper.getEndpoint() + '/'
        params={"domain": home_domain}
        rsp = requests.get(req, params=params, headers=headers)
        print("/home get status:", rsp.status_code)

        if rsp.status_code == 404:
            if not admin_headers:
                print("/home folder doesn't exist, set ADMIN_PASSWORD environment variable to enable creation")
                self.assertTrue(False)
            # Setup /home folder
            print("create home folder")
            body = {"folder": True}
            rsp = requests.put(req, data=json.dumps(body), params=params, headers=admin_headers)
            self.assertEqual(rsp.status_code, 201)
            # do the original request again
            rsp = requests.get(req, params=params, headers=headers)

        self.assertEqual(rsp.status_code, 200)

        rspJson = json.loads(rsp.text)
        print("home folder json:", rspJson)
        for k in ("owner", "created", "lastModified"):
             self.assertTrue(k in rspJson)
        self.assertFalse("root" in rspJson)  # no root -> folder

        params={"domain": user_domain}
        rsp = requests.get(req, params=params, headers=headers)
        print(f"{user_domain} get status: {rsp.status_code}")
        if rsp.status_code == 404:
            if not admin_headers:
                print(f"{user_domain} folder doesn't exist, set ADMIN_PASSWORD environment variable to enable creation")
                self.assertTrue(False)
            # Setup user home folder
            print("create user folder")
            body = {"folder": True, "owner": user_name}
            rsp = requests.put(req, data=json.dumps(body), params=params, headers=admin_headers)
            self.assertEqual(rsp.status_code, 201)
            # do the original request again
            rsp = requests.get(req, params=params, headers=headers)

        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        print("user folder:", rspJson)
        self.assertFalse("root" in rspJson)  # no root group for folder domain
        self.assertTrue("owner" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("class" in rspJson)
        self.assertEqual(rspJson["class"], "folder")
        """
        domain = "test_user1.home"
        headers = helper.getRequestHeaders(domain=domain)

        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        """



if __name__ == '__main__':
    #setup test files

    unittest.main()
