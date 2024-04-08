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
from hsds.util.idUtil import getObjPartition, isValidUuid, validateUuid
from hsds.util.idUtil import createObjId, getCollectionForId
from hsds.util.idUtil import isObjId, isS3ObjKey, getS3Key, getObjId, isSchema2Id
from hsds.util.idUtil import isRootObjId, getRootObjId


class IdUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(IdUtilTest, self).__init__(*args, **kwargs)
        # main

    def testCreateObjId(self):
        id_len = 38  # 36 for uuid plus two for prefix ("g-", "d-")
        ids = set()
        for obj_class in ("groups", "datasets", "datatypes", "chunks"):
            for i in range(100):
                id = createObjId(obj_class)
                self.assertEqual(len(id), id_len)
                self.assertTrue(id[0] in ("g", "d", "t", "c"))
                self.assertEqual(id[1], "-")
                ids.add(id)

        self.assertEqual(len(ids), 400)
        try:
            createObjId("bad_class")
            self.assertTrue(False)  # should throw exception
        except ValueError:
            pass  # expected

    def testIsValidUuid(self):
        group1_id = "g-314d61b8-9954-11e6-a733-3c15c2da029e"      # orig schema
        group2_id = "g-314d61b8-995411e6-a733-3c15c2-da029e"
        root_id = "g-f9aaa28e-d42e10e5-7122-2a065c-a6986d"
        dataset1_id = "d-4c48f3ae-9954-11e6-a3cd-3c15c2da029e"    # orig schema
        dataset2_id = "d-4c48f3ae-995411e6-a3cd-3c15c2-da029e"
        ctype1_id = "t-8c785f1c-9953-11e6-9bc2-0242ac110005"      # orig schema
        ctype2_id = "t-8c785f1c-995311e6-9bc2-0242ac-110005"
        chunk1_id = "c-8c785f1c-9953-11e6-9bc2-0242ac110005_7_2"  # orig schema
        chunk2_id = "c-8c785f1c-995311e6-9bc2-0242ac-110005_7_2"
        domain_id = "mybucket/bob/mydata.h5"
        s3_domain_id = "s3://mybucket/bob/mydata.h5"
        file_domain_id = "file://mybucket/bob/mydata.h5"
        azure_domain_id = "https://myaccount.blob.core.windows.net/mybucket/bob/mydata.h5"
        valid_id_map = {
            group1_id: "a49be-g-314d61b8-9954-11e6-a733-3c15c2da029e",
            group2_id: "db/314d61b8-995411e6/g/a733-3c15c2-da029e/.group.json",
            dataset1_id: "26928-d-4c48f3ae-9954-11e6-a3cd-3c15c2da029e",
            dataset2_id: "db/4c48f3ae-995411e6/d/a3cd-3c15c2-da029e/.dataset.json",
            ctype1_id: "5a9cf-t-8c785f1c-9953-11e6-9bc2-0242ac110005",
            ctype2_id: "db/8c785f1c-995311e6/t/9bc2-0242ac-110005/.datatype.json",
            chunk1_id: "dc4ce-c-8c785f1c-9953-11e6-9bc2-0242ac110005_7_2",
            chunk2_id: "db/8c785f1c-995311e6/d/9bc2-0242ac-110005/7_2",
            domain_id: "bob/mydata.h5/.domain.json",
            s3_domain_id: "bob/mydata.h5/.domain.json",
            file_domain_id: "bob/mydata.h5/.domain.json",
            azure_domain_id: "bob/mydata.h5/.domain.json", }

        bad_ids = ("g-1e76d862", "/bob/mydata.h5")

        self.assertTrue(isValidUuid(group1_id))
        self.assertFalse(isSchema2Id(group1_id))
        self.assertTrue(isValidUuid(group1_id, obj_class="Group"))
        self.assertTrue(isValidUuid(group1_id, obj_class="group"))
        self.assertTrue(isValidUuid(group1_id, obj_class="groups"))
        self.assertTrue(isSchema2Id(root_id))
        self.assertTrue(isValidUuid(root_id, obj_class="Group"))
        self.assertTrue(isValidUuid(root_id, obj_class="group"))
        self.assertTrue(isValidUuid(root_id, obj_class="groups"))
        self.assertTrue(isRootObjId(root_id))
        self.assertTrue(isValidUuid(dataset1_id, obj_class="datasets"))
        self.assertFalse(isSchema2Id(dataset1_id))
        self.assertTrue(isValidUuid(ctype1_id, obj_class="datatypes"))
        self.assertFalse(isSchema2Id(ctype1_id))
        self.assertTrue(isValidUuid(chunk1_id, obj_class="chunks"))
        self.assertFalse(isSchema2Id(chunk1_id))
        self.assertTrue(isValidUuid(group2_id))
        self.assertTrue(isSchema2Id(group2_id))
        self.assertTrue(isValidUuid(group2_id, obj_class="Group"))
        self.assertTrue(isValidUuid(group2_id, obj_class="group"))
        self.assertTrue(isValidUuid(group2_id, obj_class="groups"))
        self.assertFalse(isRootObjId(group2_id))
        self.assertTrue(isValidUuid(dataset2_id, obj_class="datasets"))
        self.assertTrue(isSchema2Id(dataset2_id))
        self.assertTrue(isValidUuid(ctype2_id, obj_class="datatypes"))
        self.assertTrue(isSchema2Id(ctype2_id))
        self.assertTrue(isValidUuid(chunk2_id, obj_class="chunks"))
        self.assertTrue(isSchema2Id(chunk2_id))
        validateUuid(group1_id)
        try:
            isRootObjId(group1_id)
            self.assertTrue(False)
        except ValueError:
            # only works for v2 schema
            pass  # expected

        for item in valid_id_map:
            self.assertTrue(isObjId(item))
            s3key = getS3Key(item)
            self.assertTrue(s3key[0] != "/")
            self.assertTrue(isS3ObjKey(s3key))
            expected = valid_id_map[item]
            self.assertEqual(s3key, expected)
            if item.find("/") > 0:
                continue  # bucket name gets lost when domain ids get converted to s3keys
            objid = getObjId(s3key)
            self.assertEqual(objid, item)
        for item in bad_ids:
            self.assertFalse(isValidUuid(item))
            self.assertFalse(isObjId(item))

    def testGetObjPartition(self):
        node_count = 12
        for obj_class in ("groups", "datasets", "datatypes", "chunks"):
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
        group_id = createObjId("groups", rootid=root_id)
        dataset_id = createObjId("datasets", rootid=root_id)
        ctype_id = createObjId("datatypes", rootid=root_id)

        self.assertEqual(getCollectionForId(root_id), "groups")
        self.assertEqual(getCollectionForId(group_id), "groups")
        self.assertEqual(getCollectionForId(dataset_id), "datasets")
        self.assertEqual(getCollectionForId(ctype_id), "datatypes")
        chunk_id = "c" + dataset_id[1:] + "_1_2"
        print(chunk_id)
        chunk_partition_id = "c42-" + dataset_id[2:] + "_1_2"

        for id in (chunk_id, chunk_partition_id):
            try:
                getCollectionForId(id)
                self.assertTrue(False)
            except ValueError:
                pass  # expected
        valid_ids = (
            group_id,
            dataset_id,
            ctype_id,
            chunk_id,
            chunk_partition_id,
            root_id,
        )
        s3prefix = getS3Key(root_id)
        self.assertTrue(s3prefix.endswith("/.group.json"))
        s3prefix = s3prefix[: -(len(".group.json"))]
        for oid in valid_ids:
            print("oid:", oid)
            self.assertTrue(len(oid) >= 38)
            parts = oid.split("-")
            self.assertEqual(len(parts), 6)
            self.assertTrue(oid[0] in ("g", "d", "t", "c"))
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


if __name__ == "__main__":
    # setup test files

    unittest.main()
