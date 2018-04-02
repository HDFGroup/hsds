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

class GetUUIDByPathTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(GetUUIDByPathTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()

#    def setUp(self):
#        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
#        helper.setupDomain(self.base_domain)
#        self.endpoint = helper.getEndpoint()

#    def tearDown(self):
#        res = requests.delete(
#                self.endpoint + "/",
#                headers=helper.getRequestHeaders(domain=self.base_domain))
#        assert res.status_code == 200, "unable to delete test domain"

    def test_a_thing(self):
        pass

if __name__ == "__main__":
    unittest.main()


