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
from idUtil import getObjPartition, isValidUuid, validateUuid, createObjId
 
class IdUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(IdUtilTest, self).__init__(*args, **kwargs)
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

    def testIsValidUuid(self):
        id = "g-1e76d862-7abe-11e6-8852-3c15c2da029e"
        bad_ids = ("g-1e76d862",
                   "g-1e76d862/7abe-11e6-8852-3c15c2da029e",
                   "1e76d862-7abe-11e6-8852-3c15c2da029e-g")
         
        self.assertTrue(isValidUuid(id))
        self.assertTrue(isValidUuid(id, obj_class="Group"))
        self.assertTrue(isValidUuid(id, obj_class="group"))
        self.assertTrue(not isValidUuid(id, obj_class="Dataset"))
        validateUuid(id)
        for item in bad_ids:
            self.assertEqual(isValidUuid(item), False)
        


    def testGetObjPartition(self):
        node_count = 12
        for obj_class in ('group', 'dataset', 'namedtype', 'chunk'):
            for i in range(100):
                id = createObjId(obj_class)
                node_number = getObjPartition(id, node_count)
                self.assertTrue(node_number >= 0)
                self.assertTrue(node_number < node_count)

    
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
