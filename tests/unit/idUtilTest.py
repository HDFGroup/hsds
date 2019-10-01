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

sys.path.append('../../hsds/util')
sys.path.append('../../hsds')
from idUtil import getObjPartition, isValidUuid, validateUuid, createObjId, getCollectionForId
from idUtil import isObjId, isS3ObjKey, getS3Key, getObjId, isSchema2Id, isRootObjId, getRootObjId

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
        domain_id = "mybucket/bob/mydata.h5"
        valid_ids = (group_id, dataset_id, ctype_id, chunk_id, domain_id)
        bad_ids = ("g-1e76d862", "/bob/mydata.h5")

        self.assertTrue(isValidUuid(group_id))
        self.assertFalse(isSchema2Id(group_id))
        self.assertTrue(isValidUuid(group_id, obj_class="Group"))
        self.assertTrue(isValidUuid(group_id, obj_class="group"))
        self.assertTrue(isValidUuid(group_id, obj_class="groups"))
        self.assertTrue(isValidUuid(dataset_id, obj_class="datasets"))
        self.assertFalse(isSchema2Id(dataset_id))
        self.assertTrue(isValidUuid(ctype_id, obj_class="datatypes"))
        self.assertFalse(isSchema2Id(ctype_id))
        self.assertTrue(isValidUuid(chunk_id, obj_class="chunks"))
        self.assertFalse(isSchema2Id(chunk_id))
        validateUuid(group_id)
        try:
            isRootObjId(group_id)
            self.assertTrue(False)
        except ValueError:
            # only works for v2 schema
            pass # expected


        for item in valid_ids:
            self.assertTrue(isObjId(item))
            s3key = getS3Key(item)
            self.assertTrue(s3key[0] != '/')
            self.assertTrue(isS3ObjKey(s3key))
            if item.find('/') > 0:
                continue  # bucket name gets lost when domain ids get converted to s3keys
            objid = getObjId(s3key)
            self.assertEqual(objid, item)
        for item in bad_ids:
            self.assertFalse(isValidUuid(item))
            self.assertFalse(isObjId(item))



    def testGetObjPartition(self):
        node_count = 12
        for obj_class in ('groups', 'datasets', 'datatypes', 'chunks'):
            for i in range(100):
                id = createObjId(obj_class)
                node_number = getObjPartition(id, node_count)
                self.assertTrue(node_number >= 0)
                self.assertTrue(node_number < node_count)
        # try a domain partition
        node_number = getObjPartition("/home/test_user1", node_count)
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


    def testSchema2Id(self):
        root_id = createObjId("roots")
        group_id = createObjId("groups",rootid=root_id)
        dataset_id = createObjId("datasets", rootid=root_id)
        ctype_id = createObjId("datatypes", rootid=root_id)

        self.assertEqual(getCollectionForId(root_id), "groups")
        self.assertEqual(getCollectionForId(group_id), "groups")
        self.assertEqual(getCollectionForId(dataset_id), "datasets")
        self.assertEqual(getCollectionForId(ctype_id), "datatypes")
        chunk_id = 'c' + dataset_id[1:] + "_1_2"
        print(chunk_id)
        chunk_partition_id = 'c42-' + dataset_id[2:] + "_1_2"

        for id in (chunk_id, chunk_partition_id):
            try:
                getCollectionForId(id)
                self.assertTrue(False)
            except ValueError:
                pass # expected
        valid_ids = (group_id, dataset_id, ctype_id, chunk_id, chunk_partition_id, root_id)
        s3prefix = getS3Key(root_id)
        self.assertTrue(s3prefix.endswith("/.group.json"))
        s3prefix = s3prefix[:-(len(".group.json"))]
        for oid in valid_ids:
            print("oid:", oid)
            self.assertTrue(len(oid) >= 38)
            parts = oid.split('-')
            self.assertEqual(len(parts), 6)
            self.assertTrue(oid[0] in ('g', 'd', 't', 'c'))
            self.assertTrue(isSchema2Id(oid))
            if oid == root_id:
                self.assertTrue(isRootObjId(oid))
            else:
                self.assertFalse(isRootObjId(oid))
            self.assertEqual(getRootObjId(oid), root_id)

            s3key = getS3Key(oid)
            print(s3key)
            self.assertTrue(s3key.startswith(s3prefix))
            self.assertEqual(getObjId(s3key), oid)
            self.assertTrue(isS3ObjKey(s3key))


if __name__ == '__main__':
    #setup test files

    unittest.main()
