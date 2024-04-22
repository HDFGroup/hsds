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
import sys

sys.path.append("../..")
from hsds.util.domainUtil import getParentDomain, isValidDomain, isValidHostDomain
from hsds.util.domainUtil import (
    isValidDomainPath,
    getBucketForDomain,
    getPathForDomain,
    isValidBucketName,
)


class DomainUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DomainUtilTest, self).__init__(*args, **kwargs)
        # main

    def testValidHostDomain(self):
        # test invalid dns names
        invalid_domains = (
            "x",  # too short
            ".x.y.z",  # period in front
            "x.y.z.",  # period in back
            "x.y..z",  # consecutive periods
            "192.168.1.100",  # looks like IP
            "172.17.0.9:5101",  # IP with port
            "mydomain/foobar",  # no dots
            None,
        )  # none
        for domain in invalid_domains:
            self.assertFalse(isValidHostDomain(domain))

        valid_domains = ("nex.nasa.gov",)
        for domain in valid_domains:
            self.assertTrue(isValidHostDomain(domain))

    def testValidDomain(self):
        invalid_domains = (123, "/", "abc/", "", None)
        for domain in invalid_domains:
            self.assertFalse(isValidDomain(domain))

        azure_path = "https://myaccount.blob.core.windows.net//home"
        s3_path = "s3://mybucket/home"
        file_path = "file://mybucket/home"

        valid_domains = ("/gov/nasa/nex", "/home", s3_path, file_path, azure_path)
        for domain in valid_domains:
            self.assertTrue(isValidDomain(domain))

    def testValidDomainPath(self):
        invalid_domains = (123, "home_test", "/home/test")
        for domain in invalid_domains:
            self.assertFalse(isValidDomainPath(domain))
        valid_domains = ("/home/test_user1/mytests/", "/")
        for domain in valid_domains:
            self.assertTrue(isValidDomainPath(domain))

    def testGetParentDomain(self):

        domain = "/nasa/nex"
        parent = getParentDomain(domain)
        self.assertEqual(parent, "/nasa")
        domain = "/nasa"
        parent = getParentDomain(domain)
        self.assertEqual(parent, "/")
        domain = "/"
        parent = getParentDomain(domain)
        self.assertEqual(parent, "/")

        domain = "gov/nasa/nex"
        parent = getParentDomain(domain)
        self.assertEqual(parent, "gov/nasa")
        domain = "gov/nasa"
        parent = getParentDomain(domain)
        self.assertEqual(parent, "gov/")
        domain = "gov/"
        parent = getParentDomain(domain)
        self.assertEqual(parent, "gov/")

        domain = "gov/nasa/nex/.domain.json"
        parent = getParentDomain(domain)
        self.assertEqual(parent, "gov/nasa")
        domain = "gov/nasa"
        parent = getParentDomain(domain)
        self.assertEqual(parent, "gov/")
        domain = "gov/.domain.json"
        parent = getParentDomain(domain)
        self.assertEqual(parent, "gov/")

    def testGetDomainFragments(self):
        domain = "/gov/nasa/nex/climate.h5"
        domain_path = getPathForDomain(domain)
        self.assertEqual(domain, domain_path)
        bucket = getBucketForDomain(domain)
        self.assertEqual(bucket, None)

        domain = "/home/test_user1/hsds_test/"
        domain_path = getPathForDomain(domain)
        self.assertEqual(domain, domain_path)
        bucket = getBucketForDomain(domain)
        self.assertEqual(bucket, None)

        domain = "mybucket/home/test_user1/myfile.h5"
        domain_path = getPathForDomain(domain)
        self.assertEqual(domain_path, "/home/test_user1/myfile.h5")
        bucket = getBucketForDomain(domain)
        self.assertEqual(bucket, "mybucket")

        domain = "s3://nasaeos/gov/nasa/nex/climate.h5"
        domain_path = getPathForDomain(domain)
        self.assertEqual("/gov/nasa/nex/climate.h5", domain_path)
        bucket = getBucketForDomain(domain)
        self.assertEqual(bucket, "s3://nasaeos")

        domain = "file://nasaeos/gov/nasa/nex/climate.h5"
        domain_path = getPathForDomain(domain)
        self.assertEqual("/gov/nasa/nex/climate.h5", domain_path)
        bucket = getBucketForDomain(domain)
        self.assertEqual(bucket, "file://nasaeos")

        domain = "https://myaccount.blob.core.windows.net/nasaeos/gov/nasa/nex/climate.h5"
        domain_path = getPathForDomain(domain)
        self.assertEqual("/gov/nasa/nex/climate.h5", domain_path)
        bucket = getBucketForDomain(domain)
        self.assertEqual(bucket, "https://myaccount.blob.core.windows.net/nasaeos")

    def testIsValidBucketName(self):
        # Illegal characters
        self.assertFalse(isValidBucketName("bucket;"))
        self.assertFalse(isValidBucketName("bucket|"))
        self.assertFalse(isValidBucketName("bucket&"))
        self.assertFalse(isValidBucketName("bucket\""))
        self.assertFalse(isValidBucketName("bucket "))
        self.assertFalse(isValidBucketName("bucket>"))
        self.assertFalse(isValidBucketName(""))
        self.assertFalse(isValidBucketName("s3:/mybucket"))

        self.assertTrue(isValidBucketName("bucket"))
        self.assertTrue(isValidBucketName("bucket_"))
        self.assertTrue(isValidBucketName("bucket1"))
        self.assertTrue(isValidBucketName("_"))
        self.assertTrue(isValidBucketName("___1234567890___"))

        self.assertTrue(isValidBucketName("bucket"))
        self.assertTrue(isValidBucketName("buck.et"))
        self.assertTrue(isValidBucketName("bucket1"))
        self.assertTrue(isValidBucketName("buck-et"))
        self.assertTrue(isValidBucketName("bucket-1.bucket1-.1"))

        self.assertTrue(isValidBucketName("s3://mybucket"))
        self.assertTrue(isValidBucketName("file://mybucket"))


if __name__ == "__main__":
    # setup test files

    unittest.main()
