##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of H5Serv (HDF5 REST Server) Service, Libraries and      #
# Utilities.  The full HDF5 REST Server copyright notice, including          #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################
import unittest
import time
import sys
import os
 
 

sys.path.append('../../hsds')
from hsdsUtil import getS3Partition, createObjId
from domainUtil import getParentDomain, getS3KeyForDomain

class UtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(UtilTest, self).__init__(*args, **kwargs)
        # main
    
    

    def testCreateObjId(self):
        id_len = 38  # 36 for uuid plus two for prefix ("g-", "d-")
        ids = set()
        for obj_class in ('group', 'dataset', 'namedtype', 'chunk'):
            for i in range(100):
                id = createObjId(obj_class)
                self.assertEqual(len(id), id_len)
                self.assertTrue(id[0] in ('g', 'd', 'n', 'c'))
                self.assertEqual(id[1], '-')
                ids.add(id)

        self.assertEqual(len(ids), 400)
        try:
            createObjId("bad_class")
            self.assertTrue(False) # should throw exception
        except ValueError:
            pass # expected


    def testGetS3Partition(self):
        node_count = 12
        for obj_class in ('group', 'dataset', 'namedtype', 'chunk'):
            for i in range(100):
                id = createObjId(obj_class)
                node_number = getS3Partition(id, node_count)
                self.assertTrue(node_number >= 0)
                self.assertTrue(node_number < node_count)

    def testGetS3KeyForDomain(self):
        s3path = getS3KeyForDomain("nex.nasa.gov")
        self.assertEqual(s3path, "/gov/nasa/nex")
        s3path = getS3KeyForDomain("my-data.nex.nasa.gov")  # hyphen ok
        self.assertEqual(s3path, "/gov/nasa/nex/my-data")
        # test invalid dns names
        invalid_domains = ('x',       # too short
                           '.x.y.z',  # period in front
                           'x.y.z.',  # period in back
                           'x.y..z')  # consecutive periods
        for domain in invalid_domains:
            try:
                getS3KeyForDomain(domain)
                self.assertTrue(False)
            except ValueError:
                pass # epxected

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
    
