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
from domainUtil import getParentDomain, isValidDomain, getS3KeyForDomain

class DomainUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DomainUtilTest, self).__init__(*args, **kwargs)
        # main
    

    def testValidDomain(self):       
        # test invalid dns names
        invalid_domains = ('x',       # too short
                           '.x.y.z',  # period in front
                           'x.y.z.',  # period in back
                           'x.y..z',  # consecutive periods
                           '192.168.1.100',  # looks like IP
                           'mydomain/foobar') # has a slash
        for domain in invalid_domains:
            self.assertEqual(isValidDomain(domain), False)  

        valid_domains = ("nex.nasa.gov", "home")
        for domain in valid_domains:
            self.assertTrue(isValidDomain(domain))  


    def testGetS3KeyForDomain(self):
        s3path = getS3KeyForDomain("nex.nasa.gov")
        self.assertEqual(s3path, "gov/nasa/nex")
        s3path = getS3KeyForDomain("my-data.nex.nasa.gov")  # hyphen ok
        self.assertEqual(s3path, "gov/nasa/nex/my-data")
         
              

    def testGetParentDomain(self):
        domain = "nex.nasa.gov"
        parent = getParentDomain(domain)
        self.assertEqual(parent, "nasa.gov")
        domain = "gov"
        parent = getParentDomain(domain)
        self.assertEqual(parent, None)

                                  
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
