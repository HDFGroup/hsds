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
from idUtil import getObjPartition, isValidUuid, validateUuid, createObjId, getCollectionForId
 
class IdUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(IdUtilTest, self).__init__(*args, **kwargs)
        # main
    
    def testCreateObjId(self):
        id_len = 38  # 36 for uuid plus two for prefix ("g-", "d-")
        ids = set()
        for obj_class in ('groups', 'datasets', 'datatypes', 'chunks'):
            for i in range(100):
                id = createObjId(obj_class)
                self.assertEqual(len(id), id_len)
                self.assertTrue(id[0] in ('g', 'd', 't', 'c'))
                self.assertEqual(id[1], '-')
                ids.add(id)

        self.assertEqual(len(ids), 400)
        try:
            createObjId("bad_class")
            self.assertTrue(False) # should throw exception
        except ValueError:
            pass # expected

    def testIsValidUuid(self):
        group_id = "g-314d61b8-9954-11e6-a733-3c15c2da029e"
        dataset_id = "d-4c48f3ae-9954-11e6-a3cd-3c15c2da029e"
        ctype_id = "t-8c785f1c-9953-11e6-9bc2-0242ac110005"
        chunk_id = "c-8c785f1c-9953-11e6-9bc2-0242ac110005_7_2"
        bad_ids = ("g-1e76d862",
                   "g-1e76d862/7abe-11e6-8852-3c15c2da029e",
                   "1e76d862-7abe-11e6-8852-3c15c2da029e-g")
         
        self.assertTrue(isValidUuid(group_id))
        self.assertTrue(isValidUuid(group_id, obj_class="Group"))
        self.assertTrue(isValidUuid(group_id, obj_class="group"))
        self.assertTrue(isValidUuid(group_id, obj_class="groups"))
        self.assertTrue(isValidUuid(dataset_id, obj_class="datasets"))
        self.assertTrue(isValidUuid(ctype_id, obj_class="datatypes"))
        self.assertTrue(isValidUuid(chunk_id, obj_class="chunks"))
        validateUuid(group_id)
        for item in bad_ids:
            self.assertEqual(isValidUuid(item), False)
        


    def testGetObjPartition(self):
        node_count = 12
        for obj_class in ('groups', 'datasets', 'datatypes', 'chunks'):
            for i in range(100):
                id = createObjId(obj_class)
                node_number = getObjPartition(id, node_count)
                self.assertTrue(node_number >= 0)
                self.assertTrue(node_number < node_count)

    def testGetCollection(self):
        group_id = "g-314d61b8-9954-11e6-a733-3c15c2da029e"
        dataset_id = "d-4c48f3ae-9954-11e6-a3cd-3c15c2da029e"
        ctype_id = "t-8c785f1c-9953-11e6-9bc2-0242ac110005"
        bad_id = "x-59647858-9954-11e6-95d2-3c15c2da029e"
        self.assertEqual(getCollectionForId(group_id), "groups")
        self.assertEqual(getCollectionForId(dataset_id), "datasets")
        self.assertEqual(getCollectionForId(ctype_id), "datatypes")
        try:
            getCollectionForId(bad_id)
            self.assertTrue(False)
        except ValueError:   
            pass  # expected
        try:
            getCollectionForId(None)
            self.assertTrue(False)
        except ValueError:   
            pass  # expected
         

    
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
