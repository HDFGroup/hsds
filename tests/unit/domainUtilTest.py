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
import sys
import os

sys.path.append('../../hsds/util')
sys.path.append('../../hsds')
from domainUtil import getParentDomain, isValidDomain, isValidHostDomain
from domainUtil import getDomainForHost, getS3PrefixForDomain

class DomainUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DomainUtilTest, self).__init__(*args, **kwargs)
        # main
    

    def testValidHostDomain(self):       
        # test invalid dns names
        invalid_domains = ('x',       # too short
                           '.x.y.z',  # period in front
                           'x.y.z.',  # period in back
                           'x.y..z',  # consecutive periods
                           '192.168.1.100',  # looks like IP
                           'mydomain/foobar', # has a slash
                           None)      # none
        for domain in invalid_domains:
            self.assertEqual(isValidHostDomain(domain), False)  

        valid_domains =  ("nex.nasa.gov", "home")
        for domain in valid_domains:
            self.assertTrue(isValidHostDomain(domain))  

    def testValidDomain(self):
        invalid_domains = (123, 'abc/', '', None)
        for domain in invalid_domains:
            self.assertEqual(isValidDomain(domain), False)  

        valid_domains = ("/gov/nasa/nex", "/home")
        for domain in valid_domains:
            self.assertTrue(isValidDomain(domain))  
    
    def testGetDomainForHost(self):
        domain = getDomainForHost("nex.nasa.gov")
        self.assertEqual(domain, "/gov/nasa/nex")
        domain = getDomainForHost("my-data.nex.nasa.gov")
        self.assertEqual(domain, "/gov/nasa/nex/my-data")


    def testGetParentDomain(self):
        domain = "gov/nasa/ne"
        parent = getParentDomain(domain)
        self.assertEqual(parent, "gov/nasa")
        domain = "gov"
        parent = getParentDomain(domain)
        self.assertEqual(parent, None)

    def TestGetS3PrefixForDomain(self):
        domain = "/gov/nasa/nex/climate.h5"
        s3prefix = getS3PrefixForDomain(domain)
        self.assertEqual(s3prefix, "gov/nasa/nex/")
        domain = "/home/test_user1/hsds_test/"
        s3prefix = getS3PrefixForDomain(domain)
        self.assertEqual(s3prefix, "home/test_user1/")
                                  
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
