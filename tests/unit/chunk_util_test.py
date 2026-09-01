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
import logging
import sys
import numpy as np

from h5json import selections

sys.path.append("../..")
from hsds.util.chunkUtil import (
    ChunkIterator,
    chunkReadSelection,
    chunkWriteSelection,
    chunkReadPoints,
    chunkWritePoints,
    getNumChunks,
    getChunkIds,
    getChunkId,
    getPartitionKey,
    getChunkPartition,
    getChunkIndex,
    getChunkSelection,
    getChunkCoverage,
    getDataCoverage,
    getDatasetId,
)


class ChunkUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ChunkUtilTest, self).__init__(*args, **kwargs)
        # main
        logging.getLogger().setLevel(logging.ERROR)

    def testGetNumChunks(self):
        datashape = (100,)
        layout = (10,)
        selection = selections.select(datashape, ...)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 10)
        selection = selections.select(datashape, (slice(12, 83),))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 8)
        selection = selections.select(datashape, (slice(12, 80),))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 7)
        selection = selections.select(datashape, (slice(10, 83),))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 8)
        selection = selections.select(datashape, (slice(12, 17),))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 1)
        selection = selections.select(datashape, ([2, 5, 9, 88,],))  # coord list
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 2)
        # coord list
        coords = [1, 12, 23, 34, 45, 56, 67, 78, 89, 90]
        selection = selections.select(datashape, (coords,))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 10)

        # try with different increment
        selection = selections.select(datashape, (slice(0, 10, 5),))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 1)
        selection = selections.select(datashape, (slice(0, 11, 5),))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 2)
        selection = selections.select(datashape, (slice(6, 11, 5),),)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 1)
        selection = selections.select(datashape, (slice(12, 83, 2),))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 8)
        selection = selections.select(datashape, (slice(12, 83, 20),))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 4)
        selection = selections.select(datashape, (slice(10, 83, 20),))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 4)

        datashape = (100, 100)
        layout = (10, 5)
        selection = selections.select(datashape, ...)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 200)
        selection = selections.select(datashape, (slice(41, 49), slice(6, 9),))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 1)
        selection = selections.select(datashape, (slice(39, 47), slice(4, 7),))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 4)
        selection = selections.select(datashape, ((3, 6, 12, 35), slice(4, 7)))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 6)
        # try with different increment
        selection = selections.select(datashape, (slice(39, 47, 3), slice(4, 7, 2),))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 4)
        selection = selections.select(datashape, (slice(0, 100, 20), slice(0, 100, 40),))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 15)
        # test with scalar
        datashape = ()
        layout = (1, )
        selection = selections.select(datashape, ...)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 1)

    def testGetChunkIds(self):
        # getChunkIds(dset_id, selection, layout, dim=0, prefix=None, chunk_ids=None):
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"

        datashape = ()
        layout = (1,)

        selection = selections.select(datashape, ...)
        num_chunks = getNumChunks(selection, layout)

        self.assertEqual(num_chunks, 1)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 1)
        chunk_id = chunk_ids[0]
        self.assertTrue(chunk_id.startswith("c-"))
        self.assertTrue(chunk_id.endswith("_0"))
        self.assertEqual(chunk_id[2:-2], dset_id[2:])
        self.assertEqual(len(chunk_id), 2 + 36 + 2)
        self.assertEqual(getDatasetId(chunk_id), dset_id)

        selection = selections.select(datashape, ...)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 1)
        chunk_id = chunk_ids[0]
        self.assertTrue(chunk_id.startswith("c-"))
        self.assertTrue(chunk_id.endswith("_0"))
        self.assertEqual(chunk_id[2:-2], dset_id[2:])
        self.assertEqual(len(chunk_id), 2 + 36 + 2)
        self.assertEqual(getDatasetId(chunk_id), dset_id)

        datashape = (1,)
        layout = (1,)
        selection = selections.select(datashape, ...)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 1)
        chunk_id = chunk_ids[0]
        self.assertTrue(chunk_id.startswith("c-"))
        self.assertTrue(chunk_id.endswith("_0"))
        self.assertEqual(chunk_id[2:-2], dset_id[2:])
        self.assertEqual(len(chunk_id), 2 + 36 + 2)
        self.assertEqual(getDatasetId(chunk_id), dset_id)

        datashape = (100,)
        layout = (10,)
        selection = selections.select(datashape, ...)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        partition_count = 10
        self.assertEqual(len(chunk_ids), 10)
        for i in range(10):
            chunk_id = chunk_ids[i]
            # chunk_id should look like:
            # c-12345678-1234-1234-1234-1234567890ab_n
            # where 'n' is in the range 0-9
            self.assertTrue(chunk_id.startswith("c-"))
            self.assertTrue(chunk_id.endswith("_" + str(i)))
            self.assertEqual(chunk_id[2:-2], dset_id[2:])
            self.assertEqual(len(chunk_id), 2 + 36 + 2)
            chunk_id = getPartitionKey(chunk_id, partition_count)

            partition = getChunkPartition(chunk_id)
            self.assertTrue(partition is not None)
            self.assertTrue(partition >= 0)
            self.assertTrue(partition < partition_count)

        selection = selections.select(datashape, (slice(20, 100),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 8)
        for i in range(8):
            chunk_id = chunk_ids[i]
            self.assertTrue(chunk_id.startswith("c-"))
            self.assertTrue(chunk_id.endswith("_" + str(i + 2)))
            self.assertEqual(chunk_id[2:-2], dset_id[2:])
            self.assertEqual(len(chunk_id), 2 + 36 + 2)

        selection = selections.select(datashape, (slice(20, 81),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 7)
        for i in range(7):
            chunk_id = chunk_ids[i]
            self.assertTrue(chunk_id.startswith("c-"))
            self.assertTrue(chunk_id.endswith("_" + str(i + 2)))
            self.assertEqual(chunk_id[2:-2], dset_id[2:])
            self.assertEqual(len(chunk_id), 2 + 36 + 2)

        selection = selections.select(datashape, (slice(29, 81),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 7)
        for i in range(7):
            chunk_id = chunk_ids[i]
            self.assertTrue(chunk_id.startswith("c-"))
            self.assertTrue(chunk_id.endswith("_" + str(i + 2)))
            self.assertEqual(chunk_id[2:-2], dset_id[2:])
            self.assertEqual(len(chunk_id), 2 + 36 + 2)

        selection = selections.select(datashape, (slice(29, 81, 2),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 6)
        for i in range(6):
            chunk_id = chunk_ids[i]
            self.assertTrue(chunk_id.startswith("c-"))
            self.assertTrue(chunk_id.endswith("_" + str(i + 2)))
            self.assertEqual(chunk_id[2:-2], dset_id[2:])
            self.assertEqual(len(chunk_id), 2 + 36 + 2)

        selection = selections.select(datashape, (slice(29, 81, 20),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 3)
        for i in range(3):
            chunk_id = chunk_ids[i]
            self.assertTrue(chunk_id.startswith("c-"))
            self.assertTrue(chunk_id.endswith("_" + str(i * 2 + 2)))
            self.assertEqual(chunk_id[2:-2], dset_id[2:])
            self.assertEqual(len(chunk_id), 2 + 36 + 2)

        datashape = (3207353,)
        layout = (60000,)
        selection = selections.select(datashape, (slice(1234567, 1234568),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 1)
        self.assertTrue(chunk_ids[0].endswith("_20"))

        datashape = (100, 100)
        layout = (10, 20)
        selection = selections.select(datashape, ...)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 50)
        chunk_ids.reverse()  # so we can pop off the front
        for i in range(10):
            for j in range(5):
                chunk_id = chunk_ids.pop()
                self.assertTrue(chunk_id.startswith("c-"))
                index1 = int(chunk_id[-3])
                index2 = int(chunk_id[-1])
                self.assertEqual(index1, i)
                self.assertEqual(index2, j)

        selection = selections.select(datashape, (slice(12, 88), slice(23, 80)))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 24)
        chunk_ids.reverse()  # so we can pop off the front
        for i in range(8):
            for j in range(3):
                chunk_id = chunk_ids.pop()
                self.assertTrue(chunk_id.startswith("c-"))
                index1 = int(chunk_id[-3])
                index2 = int(chunk_id[-1])
                self.assertEqual(index1, i + 1)
                self.assertEqual(index2, j + 1)

        selection = selections.select(datashape, (slice(12, 88, 6), slice(23, 80, 16)))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 24)
        chunk_ids.reverse()  # so we can pop off the front
        for i in range(8):
            for j in range(3):
                chunk_id = chunk_ids.pop()
                self.assertTrue(chunk_id.startswith("c-"))
                index1 = int(chunk_id[-3])
                index2 = int(chunk_id[-1])
                self.assertEqual(index1, i + 1)
                self.assertEqual(index2, j + 1)

        selection = selections.select(datashape, (slice(12, 88, 16), slice(23, 80, 44)))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 10)
        chunk_ids.reverse()  # so we can pop off the front
        xindex = (1, 2, 4, 6, 7)
        yindex = (1, 3)
        for i in range(5):
            for j in range(2):
                chunk_id = chunk_ids.pop()
                self.assertTrue(chunk_id.startswith("c-"))
                index1 = int(chunk_id[-3])
                index2 = int(chunk_id[-1])
                self.assertEqual(index1, xindex[i])
                self.assertEqual(index2, yindex[j])

        # 3d test
        datashape = (365, 720, 1440)
        layout = (2, 180, 720)
        selection = selections.select(datashape, (slice(0, 1), slice(0, 720), slice(0, 1440)))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 8)
        chunk_ids.reverse()  # so we can pop off the front
        for i in range(4):
            for j in range(2):
                chunk_id = chunk_ids.pop()
                self.assertTrue(chunk_id.startswith("c-"))
                index0 = int(chunk_id[-5])
                index1 = int(chunk_id[-3])
                index2 = int(chunk_id[-1])
                self.assertEqual(index0, 0)
                self.assertEqual(index1, i)
                self.assertEqual(index2, j)

        selection = selections.select(
            datashape, (slice(0, 1, 1), slice(0, 720, 25), slice(0, 1440, 25))
        )
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 8)
        chunk_ids.reverse()  # so we can pop off the front
        for i in range(4):
            for j in range(2):
                chunk_id = chunk_ids.pop()
                self.assertTrue(chunk_id.startswith("c-"))
                index0 = int(chunk_id[-5])
                index1 = int(chunk_id[-3])
                index2 = int(chunk_id[-1])
                self.assertEqual(index0, 0)
                self.assertEqual(index1, i)
                self.assertEqual(index2, j)

        # 2d test - laarge number of chunks
        datashape = (7639, 6307)
        layout = (1, 6308)
        selection = selections.select(datashape, (slice(0, 7639), slice(0, 6307)))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 7639)
        index_set = set()
        for i in range(7639):
            chunk_id = chunk_ids.pop()
            self.assertTrue(chunk_id.startswith("c-"))
            fields = chunk_id.split("_")
            self.assertEqual(len(fields), 3)
            index1 = int(fields[1])
            index2 = int(fields[2])
            index_set.add(index1)
            self.assertEqual(index2, 0)
        self.assertEqual(len(index_set), 7639)

    def testGetChunkIndex(self):
        chunk_id = "c-12345678-1234-1234-1234-1234567890ab_6_4"
        index = getChunkIndex(chunk_id)
        self.assertEqual(index, [6, 4])
        chunk_id = "c-12345678-1234-1234-1234-1234567890ab_64"
        index = getChunkIndex(chunk_id)
        self.assertEqual(index, [64,])

    def testGetChunkSelection(self):
        # 1-d test
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        datashape = (100,)
        layout = (10,)
        selection = selections.select(datashape, (slice(42, 62),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 3)

        chunk_id = chunk_ids[0]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 42)
        self.assertEqual(sel[0].stop, 50)
        self.assertEqual(sel[0].step, 1)

        chunk_id = chunk_ids[1]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 50)
        self.assertEqual(sel[0].stop, 60)
        self.assertEqual(sel[0].step, 1)

        chunk_id = chunk_ids[2]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 60)
        self.assertEqual(sel[0].stop, 62)
        self.assertEqual(sel[0].step, 1)

        # 1-d with step
        selection = selections.select(datashape, (slice(42, 62, 4),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 2)

        chunk_id = chunk_ids[0]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 42)
        self.assertEqual(sel[0].stop, 50)
        self.assertEqual(sel[0].step, 4)

        chunk_id = chunk_ids[1]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 50)
        self.assertEqual(sel[0].stop, 62)
        self.assertEqual(sel[0].step, 4)

        # another 1-d with step
        selection = selections.select(datashape, (slice(40, 63, 2),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 3)

        chunk_id = chunk_ids[0]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 40)
        self.assertEqual(sel[0].stop, 50)
        self.assertEqual(sel[0].step, 2)

        chunk_id = chunk_ids[1]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 50)
        self.assertEqual(sel[0].stop, 60)
        self.assertEqual(sel[0].step, 2)

        chunk_id = chunk_ids[2]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 60)
        self.assertEqual(sel[0].stop, 64)
        self.assertEqual(sel[0].step, 2)

        # test with step > chunk size
        selection = selections.select(datashape, (slice(0, 100, 15),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 7)

        chunk_id = chunk_ids[0]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 15)
        self.assertEqual(sel[0].step, 15)

        chunk_id = chunk_ids[1]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 15)
        self.assertEqual(sel[0].stop, 30)
        self.assertEqual(sel[0].step, 15)

        chunk_id = chunk_ids[2]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 30)
        self.assertEqual(sel[0].stop, 45)
        self.assertEqual(sel[0].step, 15)

        # test with coordinate
        selection = selections.select(datashape, ([12, 13, 33],))
        chunk_ids = getChunkIds(dset_id, selection, layout)

        self.assertEqual(len(chunk_ids), 2)
        chunk_id = f"c-{dset_id[2:]}_1"
        self.assertTrue(chunk_id in chunk_ids)

        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0], [12, 13])
        chunk_id = f"c-{dset_id[2:]}_3"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0], [33,],)

        # 2-d test
        datashape = (100, 100)
        layout = (10, 10)
        selection = selections.select(datashape, (slice(42, 52), slice(46, 58)))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 4)

        chunk_id = f"c-{dset_id[2:]}_4_4"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 42)
        self.assertEqual(sel[0].stop, 50)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 46)
        self.assertEqual(sel[1].stop, 50)
        self.assertEqual(sel[1].step, 1)

        chunk_id = f"c-{dset_id[2:]}_4_5"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 42)
        self.assertEqual(sel[0].stop, 50)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 50)
        self.assertEqual(sel[1].stop, 58)
        self.assertEqual(sel[1].step, 1)

        chunk_id = f"c-{dset_id[2:]}_5_4"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 50)
        self.assertEqual(sel[0].stop, 52)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 46)
        self.assertEqual(sel[1].stop, 50)
        self.assertEqual(sel[1].step, 1)

        chunk_id = f"c-{dset_id[2:]}_5_5"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 50)
        self.assertEqual(sel[0].stop, 52)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 50)
        self.assertEqual(sel[1].stop, 58)
        self.assertEqual(sel[1].step, 1)

        # test with coordinate
        selection = selections.select(datashape, (slice(35, 45), [12, 13, 33]))

        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 4)
        chunk_id = f"c-{dset_id[2:]}_3_1"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0], slice(35, 40, 1))
        self.assertEqual(sel[1], [12, 13])

        chunk_id = f"c-{dset_id[2:]}_3_3"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0], slice(35, 40, 1))
        self.assertEqual(sel[1], [33,])
        chunk_id = f"c-{dset_id[2:]}_4_1"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0], slice(40, 45, 1))
        self.assertEqual(sel[1], [12, 13])
        chunk_id = f"c-{dset_id[2:]}_4_3"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0], slice(40, 45, 1))
        self.assertEqual(sel[1], [33,],)

        # 1-d test with fractional chunks
        datashape = (104,)
        layout = (10,)
        selection = selections.select(datashape, (slice(92, 102),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        chunk_ids.sort()
        self.assertEqual(len(chunk_ids), 2)

        chunk_id = chunk_ids[0]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        sel = sel[0]
        self.assertEqual(sel.start, 100)
        self.assertEqual(sel.stop, 102)
        self.assertEqual(sel.step, 1)

        chunk_id = chunk_ids[1]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        sel = sel[0]
        self.assertEqual(sel.start, 92)
        self.assertEqual(sel.stop, 100)
        self.assertEqual(sel.step, 1)

        # 3d test
        datashape = (365, 720, 1440)
        layout = (2, 180, 720)
        selection = selections.select(datashape, (slice(0, 1), slice(0, 200), slice(0, 300)))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 2)

        chunk_id = chunk_ids[0]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 1)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 0)
        self.assertEqual(sel[1].stop, 180)
        self.assertEqual(sel[1].step, 1)
        self.assertEqual(sel[2].start, 0)
        self.assertEqual(sel[2].stop, 300)
        self.assertEqual(sel[2].step, 1)
        chunk_id = chunk_ids[1]
        sel = getChunkSelection(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 1)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 180)
        self.assertEqual(sel[1].stop, 200)
        self.assertEqual(sel[1].step, 1)
        self.assertEqual(sel[2].start, 0)
        self.assertEqual(sel[2].stop, 300)
        self.assertEqual(sel[2].step, 1)

    def testGetChunkCoverage(self):
        # 1-d test
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        datashape = (100,)
        layout = (10,)
        selection = selections.select(datashape, (slice(42, 62),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 3)
        chunk_id = f"c-{dset_id[2:]}_4"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 2)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 1)

        chunk_id = f"c-{dset_id[2:]}_5"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 1)

        chunk_id = f"c-{dset_id[2:]}_6"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 2)
        self.assertEqual(sel[0].step, 1)

        # 1 D with coordinate selection
        selection = selections.select(datashape, ([32, 39, 61],))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 2)
        chunk_id = f"c-{dset_id[2:]}_3"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0], [2, 9])

        # 1-d with step
        selection = selections.select(datashape, (slice(42, 62, 4),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 2)

        chunk_id = chunk_ids[0]
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 2)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 4)

        chunk_id = chunk_ids[1]
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 12)
        self.assertEqual(sel[0].step, 4)

        # 2-d test
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        datashape = (100, 100)
        layout = (10, 10)
        selection = selections.select(datashape, (slice(42, 52), slice(46, 58)))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 4)

        chunk_id = chunk_ids[0]
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 2)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 6)
        self.assertEqual(sel[1].stop, 10)
        self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[1]
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 2)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 0)
        self.assertEqual(sel[1].stop, 8)
        self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[2]
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 2)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 6)
        self.assertEqual(sel[1].stop, 10)
        self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[3]
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 2)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 0)
        self.assertEqual(sel[1].stop, 8)
        self.assertEqual(sel[1].step, 1)

        # 2-d test - non-even chunks at boundary
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        datashape = (45, 54)
        layout = (10, 10)
        selection = selections.select(datashape, (slice(22, 23), slice(2, 52)))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 6)

        chunk_id = chunk_ids[0]
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 2)
        self.assertEqual(sel[0].stop, 3)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 2)
        self.assertEqual(sel[1].stop, 10)
        self.assertEqual(sel[1].step, 1)

        # the next 4 chunks will have same selection
        for i in range(1, 4):
            chunk_id = chunk_ids[i]
            sel = getChunkCoverage(chunk_id, selection, layout).slices
            self.assertEqual(sel[0].start, 2)
            self.assertEqual(sel[0].stop, 3)
            self.assertEqual(sel[0].step, 1)
            self.assertEqual(sel[1].start, 0)
            self.assertEqual(sel[1].stop, 10)
            self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[5]
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 2)
        self.assertEqual(sel[0].stop, 3)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 0)
        self.assertEqual(sel[1].stop, 2)
        self.assertEqual(sel[1].step, 1)

        # 2-d test wiith coordinates
        selection = selections.select((45, 70), (slice(15, 25, 1), [62, 69]))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 2)
        chunk_id = chunk_ids[0]
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 5)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1], [2, 9])
        chunk_id = chunk_ids[1]
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 5)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1], [2, 9])

        # 3-d test with coordinates
        datashape = (5, 1000, 1000)
        layout = (3, 500, 500)
        selection = selections.select(datashape, (slice(0, 5, 1), [1, 10, 100], [10, 100, 500]))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        chunk_ids.sort()
        self.assertEqual(len(chunk_ids), 4)
        chunk_id = chunk_ids[0]
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 3)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1], [1, 10])
        self.assertEqual(sel[2], [10, 100])
        chunk_id = chunk_ids[1]
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 3)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1], [100])
        self.assertEqual(sel[2], [0])

        # 1-d test with fractional chunks
        datashape = (104,)
        layout = (10,)
        selection = selections.select(datashape, (slice(92, 102),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        chunk_ids.sort()
        self.assertEqual(len(chunk_ids), 2)

        chunk_id = chunk_ids[0]
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        sel = sel[0]
        self.assertEqual(sel.start, 0)
        self.assertEqual(sel.stop, 2)
        self.assertEqual(sel.step, 1)

        chunk_id = chunk_ids[1]
        sel = getChunkCoverage(chunk_id, selection, layout).slices
        sel = sel[0]
        self.assertEqual(sel.start, 2)
        self.assertEqual(sel.stop, 10)
        self.assertEqual(sel.step, 1)

    def testGetDataCoverage(self):
        # 1-d test
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        datashape = (100,)
        layout = (10,)
        selection = selections.select(datashape, (slice(42, 62),))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 3)

        chunk_id = f"c-{dset_id[2:]}_4"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(len(sel), 1)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 8)
        self.assertEqual(sel[0].step, 1)

        chunk_id = f"c-{dset_id[2:]}_5"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(len(sel), 1)
        self.assertEqual(sel[0].start, 8)
        self.assertEqual(sel[0].stop, 18)
        self.assertEqual(sel[0].step, 1)

        chunk_id = f"c-{dset_id[2:]}_6"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(len(sel), 1)
        self.assertEqual(sel[0].start, 18)
        self.assertEqual(sel[0].stop, 20)
        self.assertEqual(sel[0].step, 1)

        # test with step
        selection = selections.select(datashape, (slice(42, 68, 4),))
        self.assertEqual(len(sel), 1)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 3)

        chunk_id = f"c-{dset_id[2:]}_4"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(len(sel), 1)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 2)
        self.assertEqual(sel[0].step, 1)

        chunk_id = f"c-{dset_id[2:]}_5"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(len(sel), 1)
        self.assertEqual(sel[0].start, 2)
        self.assertEqual(sel[0].stop, 5)
        self.assertEqual(sel[0].step, 1)

        chunk_id = f"c-{dset_id[2:]}_6"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(len(sel), 1)
        self.assertEqual(sel[0].start, 5)
        self.assertEqual(sel[0].stop, 7)
        self.assertEqual(sel[0].step, 1)

        # test with coordinates
        selection = selections.select(datashape, ([23, 28],))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 1)

        chunk_id = f"c-{dset_id[2:]}_2"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(len(sel), 1)
        self.assertEqual(sel[0], [0, 1])

        # 2-d test
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        datashape = (100, 100)
        layout = (10, 10)
        selection = selections.select(datashape, (slice(42, 52), slice(46, 58)))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 4)

        chunk_id = f"c-{dset_id[2:]}_4_4"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 8)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 0)
        self.assertEqual(sel[1].stop, 4)
        self.assertEqual(sel[1].step, 1)

        chunk_id = f"c-{dset_id[2:]}_4_5"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 8)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 4)
        self.assertEqual(sel[1].stop, 12)
        self.assertEqual(sel[1].step, 1)

        chunk_id = f"c-{dset_id[2:]}_5_4"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 8)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 0)
        self.assertEqual(sel[1].stop, 4)
        self.assertEqual(sel[1].step, 1)

        chunk_id = f"c-{dset_id[2:]}_5_5"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 8)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 4)
        self.assertEqual(sel[1].stop, 12)
        self.assertEqual(sel[1].step, 1)

        # test with coordinates
        selection = selections.select(datashape, (slice(45, 55, 1), [23, 28]))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 2)

        chunk_id = f"c-{dset_id[2:]}_4_2"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 5)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1], [0, 1])

        chunk_id = f"c-{dset_id[2:]}_5_2"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 5)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1], [0, 1])

        # test with two coordinates
        selection = selections.select(datashape, ([1, 5, 55], [23, 28, 57]))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 2)

        chunk_id = f"c-{dset_id[2:]}_5_5"
        self.assertTrue(chunk_id in chunk_ids)
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0], [2,])

        chunk_id = f"c-{dset_id[2:]}_0_2"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(len(sel), 1)
        self.assertEqual(sel[0], [0, 1])

        # 2-d test, non-regular chunks
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        datashape = (45, 54)
        layout = (10, 10)
        selection = selections.select(datashape, (slice(22, 23), slice(2, 52)))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 6)

        chunk_id = f"c-{dset_id[2:]}_2_0"
        self.assertTrue(chunk_id in chunk_ids)
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 1)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 0)
        self.assertEqual(sel[1].stop, 8)
        self.assertEqual(sel[1].step, 1)

        chunk_id = f"c-{dset_id[2:]}_2_1"
        self.assertTrue(chunk_id in chunk_ids)
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 1)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 8)
        self.assertEqual(sel[1].stop, 18)
        self.assertEqual(sel[1].step, 1)

        chunk_id = f"c-{dset_id[2:]}_2_5"
        self.assertTrue(chunk_id in chunk_ids)
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 1)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 48)
        self.assertEqual(sel[1].stop, 50)
        self.assertEqual(sel[1].step, 1)

        # 1-d test with fractional chunks
        datashape = (104,)
        layout = (10,)
        selection = selections.select(datashape, (slice(92, 102),))
        chunk_ids = getChunkIds(dset_id, selection, layout)

        self.assertEqual(len(chunk_ids), 2)

        chunk_id = f"c-{dset_id[2:]}_9"
        self.assertTrue(chunk_id in chunk_ids)

        sel = getDataCoverage(chunk_id, selection, layout).slices
        sel = sel[0]
        self.assertEqual(sel.start, 0)
        self.assertEqual(sel.stop, 8)
        self.assertEqual(sel.step, 1)

        chunk_id = f"c-{dset_id[2:]}_10"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        sel = sel[0]
        self.assertEqual(sel.start, 8)
        self.assertEqual(sel.stop, 10)
        self.assertEqual(sel.step, 1)

        # 3-d test with coord
        datashape = (792, 1602, 2976)
        layout = (66, 89, 93)
        selection = selections.select(
            datashape, (slice(0, 792, 1), slice(520, 521, 1), slice(1401, 1540, 1))
        )
        chunk_ids = getChunkIds(dset_id, selection, layout)
        chunk_id = f"c-{dset_id[2:]}_0_5_16"
        self.assertTrue(chunk_id in chunk_ids)
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0], slice(0, 66, 1))
        self.assertEqual(sel[1], slice(0, 1, 1))
        self.assertEqual(sel[2], slice(87, 139, 1))

        selection = selections.select(
            datashape, (slice(0, 792, 1), slice(520, 521, 1), [1401, 1501, 1540])
        )
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 24)
        chunk_id = f"c-{dset_id[2:]}_0_5_16"
        self.assertTrue(chunk_id in chunk_ids)
        sel = getDataCoverage(chunk_id, selection, layout).slices
        self.assertEqual(sel[0], slice(0, 66, 1))
        self.assertEqual(sel[1], slice(0, 1, 1))
        self.assertEqual(sel[2], [1, 2])

    def testGetChunkId(self):
        # getChunkIds(dset_id, selection, layout, dim=0, prefix=None, chunk_ids=None):
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"

        layout = (1,)
        chunk_id = getChunkId(dset_id, 0, layout)
        self.assertTrue(chunk_id.startswith("c-"))
        self.assertTrue(chunk_id.endswith("_0"))
        self.assertEqual(chunk_id[2:-2], dset_id[2:])
        self.assertEqual(len(chunk_id), 2 + 36 + 2)

        layout = (100,)
        chunk_id = getChunkId(dset_id, 2, layout)
        self.assertTrue(chunk_id.startswith("c-"))
        self.assertTrue(chunk_id.endswith("_0"))
        self.assertEqual(chunk_id[2:-2], dset_id[2:])
        self.assertEqual(len(chunk_id), 2 + 36 + 2)

        layout = (10,)
        chunk_id = getChunkId(dset_id, 23, layout)
        self.assertTrue(chunk_id.startswith("c-"))
        self.assertTrue(chunk_id.endswith("_2"))
        self.assertEqual(chunk_id[2:-2], dset_id[2:])
        self.assertEqual(len(chunk_id), 2 + 36 + 2)

        layout = (10, 20)
        chunk_id = getChunkId(dset_id, (23, 61), layout)
        self.assertTrue(chunk_id.startswith("c-"))
        self.assertTrue(chunk_id.endswith("_2_3"))
        self.assertEqual(chunk_id[2:-4], dset_id[2:])
        self.assertEqual(len(chunk_id), 2 + 36 + 4)

    def testDimQuery(self):
        request = {"dim_0": 23, "dim_1": 54, "dim_2": 2}
        dims = []
        dim = 0
        while True:
            k = "dim_{}".format(dim)
            if k in request:
                extent = int(request[k])
                dims.append(extent)
                dim += 1
            else:
                break

    def testChunkIterator1d(self):
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        dims = (100,)
        layout = [10,]

        selection = selections.select(dims, ...)
        it = ChunkIterator(dset_id, selection, layout)
        chunk_ids = set(getChunkIds(dset_id, selection, layout))
        count = 0

        while True:
            try:
                chunk_id = it.next()
                self.assertTrue(chunk_id) in chunk_ids
                count += 1
            except StopIteration:
                break

        self.assertEqual(count, 10)

    def testChunkIterator2d(self):
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        dims = (100, 100)
        layout = [50, 50]
        selection = selections.select(dims, ...)
        it = ChunkIterator(dset_id, selection, layout)

        chunk_ids = set(getChunkIds(dset_id, selection, layout))
        count = 0

        while True:
            try:
                chunk_id = it.next()
                self.assertTrue(chunk_id) in chunk_ids
                count += 1
            except StopIteration:
                break

        self.assertEqual(count, 4)

    def testChunkIterator3d(self):
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        dims = (100, 100, 20)
        layout = [50, 50, 5]
        selection = selections.select(dims, ...)
        it = ChunkIterator(dset_id, selection, layout)

        chunk_ids = set(getChunkIds(dset_id, selection, layout))
        count = 0

        while True:
            try:
                chunk_id = it.next()
                self.assertTrue(chunk_id) in chunk_ids
                count += 1
            except StopIteration:
                break

        self.assertEqual(count, 16)

    def testChunkReadSelection(self):
        chunk_arr = np.array([2, 3, 5, 7, 11, 13, 17, 19])
        selection = selections.select(chunk_arr.shape, (slice(3, 5, 1),))
        arr = chunkReadSelection(chunk_arr, selection=selection)
        self.assertEqual(arr.tolist(), [7, 11])
        selection = selections.select(chunk_arr.shape, (slice(3, 9, 2),))
        arr = chunkReadSelection(chunk_arr, selection=selection)
        self.assertEqual(arr.tolist(), [7, 13, 19])
        chunk_arr = np.zeros((3, 4))
        for i in range(3):
            chunk_arr[i] = list(range(i + 1, i + 1 + 4))
        selection = selections.select(chunk_arr.shape, (slice(1, 2, 1), slice(0, 4, 1)))
        arr = chunkReadSelection(chunk_arr, selection=selection)
        self.assertEqual(arr.tolist(), [[2.0, 3.0, 4.0, 5.0]])
        selection = selections.select(chunk_arr.shape, (slice(0, 3, 1), slice(2, 3, 1)))
        arr = chunkReadSelection(chunk_arr, selection=selection)
        self.assertEqual(arr.tolist(), [[3.0], [4.0], [5.0]])
        selection = selections.select(chunk_arr.shape, (slice(0, 1, 1), [0, 3]))
        arr = chunkReadSelection(chunk_arr, selection=selection)
        self.assertEqual(arr.tolist(), [[1.0, 4.0]])

    def testChunkWriteSelection(self):
        chunk_arr = np.zeros((8,))
        data = np.array([2, 3, 5, 7, 11, 13, 17, 19])
        selection = selections.select(chunk_arr.shape, (slice(0, 8, 1),))
        chunkWriteSelection(chunk_arr=chunk_arr, selection=selection, data=data)
        self.assertEqual(chunk_arr.tolist(), data.tolist())
        data = np.array([101, 121, 131])
        selection = selections.select(chunk_arr.shape, (slice(3, 6, 1),))
        chunkWriteSelection(chunk_arr=chunk_arr, selection=selection, data=data)
        self.assertEqual(chunk_arr.tolist(), [2, 3, 5, 101, 121, 131, 17, 19])

    def testChunkWriteSelectionArrayDtype(self):
        # for an array/subarray dtype (H5T_ARRAY, e.g. numpy's "3i1") the
        # chunk array's own shape absorbs the dtype's subarray dims
        # (chunk_arr.shape == dataset_shape + dt.shape) while the
        # selection is built against the dataset's logical shape only -
        # chunkWriteSelection()'s rank check allows chunk_arr's/data's
        # rank to exceed selection's by exactly those absorbed dims,
        # rather than requiring an exact rank match.
        dt = np.dtype(("i1", (3,)))
        dataset_shape = (2,)
        chunk_arr = np.zeros(dataset_shape, dtype=dt)
        self.assertEqual(chunk_arr.shape, (2, 3))  # subarray dims absorbed

        data = np.frombuffer(bytes([1, 2, 3, 4, 5, 6]), dtype=dt)
        self.assertEqual(data.shape, (2, 3))

        # selection is built against the dataset's own (1-D) logical shape,
        # not chunk_arr's subarray-absorbed shape
        selection = selections.select(dataset_shape, (slice(0, 2, 1),))
        chunkWriteSelection(chunk_arr=chunk_arr, selection=selection, data=data)
        self.assertEqual(chunk_arr.tolist(), data.tolist())

        # full (non-field-restricted) read back of the same bare
        # array-dtype chunk - chunkReadSelection() has the same rank
        # relaxation on its own selection-rank check
        arr = chunkReadSelection(chunk_arr, selection=selection)
        self.assertEqual(arr.tolist(), data.tolist())

    def testChunkWriteSelectionCompoundArrayField(self):
        # compound dtype with an array-typed field - unlike a bare
        # array/subarray dtype (see testChunkWriteSelectionArrayDtype
        # above), a compound dtype's own shape is NOT absorbed by its
        # array-typed field's dims, so chunk_arr.shape stays equal to the
        # plain dataset shape and chunkWriteSelection()/chunkReadSelection()
        # work correctly here - including for a partial (hyperslab)
        # selection, which nothing else exercises for this dtype shape.
        dt = np.dtype([("temp", ("<i8", (5,))), ("pressure", "<f4")])
        dataset_shape = (4,)
        chunk_arr = np.zeros(dataset_shape, dtype=dt)
        self.assertEqual(chunk_arr.shape, dataset_shape)  # no absorption

        data = np.zeros((2,), dtype=dt)
        for i in range(2):
            data[i]["temp"] = [i * 10 + j for j in range(5)]
            data[i]["pressure"] = i + 0.5

        # partial (hyperslab) write to just elements [1:3]
        selection = selections.select(dataset_shape, (slice(1, 3, 1),))
        chunkWriteSelection(chunk_arr=chunk_arr, selection=selection, data=data)

        # partial read back of the same region
        arr = chunkReadSelection(chunk_arr, selection=selection)
        self.assertEqual(arr.shape, (2,))
        for i in range(2):
            self.assertEqual(arr[i]["temp"].tolist(), data[i]["temp"].tolist())
            self.assertAlmostEqual(float(arr[i]["pressure"]), float(data[i]["pressure"]))

        # elements outside the written region should be untouched (fill)
        self.assertEqual(chunk_arr[0]["temp"].tolist(), [0, 0, 0, 0, 0])
        self.assertEqual(chunk_arr[3]["temp"].tolist(), [0, 0, 0, 0, 0])

    def testChunkWriteSelectionFieldUpdate(self):
        # field-restricted write (data.dtype has fewer fields than
        # chunk_arr's own dtype) - exercises chunkWriteSelection()'s
        # field_update branch, which otherwise only has integration-level
        # coverage (see tests/integ/value_test.py's field-write tests and
        # tests/integ/pointsel_test.py's testPostCompoundDataset). The
        # non-selected fields must be left completely untouched.
        dt = np.dtype([("a", "<i4"), ("b", "<i4"), ("c", "<i4")])
        chunk_arr = np.zeros((4,), dtype=dt)
        chunk_arr["a"] = [1, 2, 3, 4]
        chunk_arr["b"] = [10, 20, 30, 40]
        chunk_arr["c"] = [100, 200, 300, 400]

        # write just field "b" for elements [1:3]
        field_dt = np.dtype([("b", "<i4")])
        data = np.zeros((2,), dtype=field_dt)
        data["b"] = [999, 888]

        selection = selections.select(chunk_arr.shape, (slice(1, 3, 1),))
        chunkWriteSelection(chunk_arr=chunk_arr, selection=selection, data=data)

        self.assertEqual(chunk_arr["b"].tolist(), [10, 999, 888, 40])
        # fields "a" and "c" must be completely unchanged
        self.assertEqual(chunk_arr["a"].tolist(), [1, 2, 3, 4])
        self.assertEqual(chunk_arr["c"].tolist(), [100, 200, 300, 400])

        # multi-field write (two of the three fields) for elements [0:2]
        multi_field_dt = np.dtype([("a", "<i4"), ("c", "<i4")])
        multi_data = np.zeros((2,), dtype=multi_field_dt)
        multi_data["a"] = [111, 222]
        multi_data["c"] = [333, 444]
        selection2 = selections.select(chunk_arr.shape, (slice(0, 2, 1),))
        chunkWriteSelection(chunk_arr=chunk_arr, selection=selection2, data=multi_data)

        self.assertEqual(chunk_arr["a"].tolist(), [111, 222, 3, 4])
        self.assertEqual(chunk_arr["c"].tolist(), [333, 444, 300, 400])
        # field "b" must still be unchanged by either write
        self.assertEqual(chunk_arr["b"].tolist(), [10, 999, 888, 40])

        # field-restricted read
        arr = chunkReadSelection(chunk_arr, selection=selection, select_dt=field_dt)
        self.assertEqual(arr["b"].tolist(), [999, 888])

    def testChunkWriteSelectionFieldUpdateArrayField(self):
        # field-restricted write on a compound that ALSO has an
        # array-typed field - the combination of field-selection with an
        # array-typed field isn't covered anywhere else (unit or
        # integration level, HSDS or h5pyd)
        dt = np.dtype([("vec", "<i4", (3,)), ("scale", "<f4")])
        chunk_arr = np.zeros((3,), dtype=dt)
        chunk_arr["vec"] = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        chunk_arr["scale"] = [1.5, 2.5, 3.5]

        # write just the scalar "scale" field for element 1
        scale_dt = np.dtype([("scale", "<f4")])
        data = np.zeros((1,), dtype=scale_dt)
        data["scale"] = [99.5]

        selection = selections.select(chunk_arr.shape, (slice(1, 2, 1),))
        chunkWriteSelection(chunk_arr=chunk_arr, selection=selection, data=data)

        self.assertAlmostEqual(float(chunk_arr["scale"][1]), 99.5)
        # the array-typed "vec" field must be completely untouched
        self.assertEqual(chunk_arr["vec"].tolist(), [[1, 2, 3], [4, 5, 6], [7, 8, 9]])

        # now the reverse: write just the array-typed "vec" field
        vec_dt = np.dtype([("vec", "<i4", (3,))])
        data2 = np.zeros((1,), dtype=vec_dt)
        data2["vec"] = [[100, 101, 102]]
        chunkWriteSelection(chunk_arr=chunk_arr, selection=selection, data=data2)

        self.assertEqual(chunk_arr["vec"][1].tolist(), [100, 101, 102])
        # "scale" must be untouched by this second, array-field-only write
        self.assertAlmostEqual(float(chunk_arr["scale"][1]), 99.5)
        # other elements' "vec" values untouched
        self.assertEqual(chunk_arr["vec"][0].tolist(), [1, 2, 3])
        self.assertEqual(chunk_arr["vec"][2].tolist(), [7, 8, 9])

    def testChunkReadSelectionSingleArrayField(self):
        # a single-field selection where that one field is array-typed -
        # chunkReadSelection() used to have a single-field shortcut
        # (`arr[...] = output_arr[fields[0]]`) that only worked for a
        # scalar field; see git history / tests/integ/value_test.py's
        # testArrayFieldSingleFieldReadArrayField for the end-to-end case.
        dt = np.dtype([("vec", "<i4", (3,)), ("scale", "<f4")])
        chunk_arr = np.zeros((4,), dtype=dt)
        chunk_arr["vec"] = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10, 11]]
        chunk_arr["scale"] = [0.5, 1.5, 2.5, 3.5]

        select_dt = np.dtype([("vec", "<i4", (3,))])
        selection = selections.select(chunk_arr.shape, ...)
        arr = chunkReadSelection(chunk_arr, selection=selection, select_dt=select_dt)
        self.assertEqual(arr["vec"].tolist(), chunk_arr["vec"].tolist())

    def testChunkReadPoints1D(self):
        chunk_id = "c-00de6a9c-6aff5c35-15d5-3864dd-0740f8_12"
        chunk_layout = (100,)
        chunk_arr = np.array(list(range(100)))
        point_arr = np.array([[1200], [1299], [1244], [1222]], dtype=np.uint64)
        arr = chunkReadPoints(
            chunk_id=chunk_id,
            chunk_layout=chunk_layout,
            chunk_arr=chunk_arr,
            point_arr=point_arr,
        )
        self.assertEqual(arr.tolist(), [0, 99, 44, 22])

        point_arr = np.array([[1200], [1299], [1244], [1322]], dtype=np.uint64)
        try:
            chunkReadPoints(
                chunk_id=chunk_id,
                chunk_layout=chunk_layout,
                chunk_arr=chunk_arr,
                point_arr=point_arr,
            )
            self.assertTrue(False)  # expected exception
        except IndexError:
            pass  # expected

    def testChunkReadPoints2D(self):
        chunk_id = "c-00de6a9c-6aff5c35-15d5-3864dd-0740f8_3_4"
        chunk_layout = (100, 100)
        chunk_arr = np.zeros((100, 100))
        chunk_arr[:, 12] = 69
        chunk_arr[12, :] = 96

        point_arr = np.array(
            [[312, 498], [312, 412], [355, 412], [398, 497]], dtype=np.uint64
        )
        arr = chunkReadPoints(
            chunk_id=chunk_id,
            chunk_layout=chunk_layout,
            chunk_arr=chunk_arr,
            point_arr=point_arr,
        )
        self.assertEqual(arr.tolist(), [96, 96, 69, 0])

        point_arr = np.array(
            [[312, 498], [312, 412], [355, 412], [398, 397]], dtype=np.uint64
        )
        try:
            chunkReadPoints(
                chunk_id=chunk_id,
                chunk_layout=chunk_layout,
                chunk_arr=chunk_arr,
                point_arr=point_arr,
            )
            self.assertTrue(False)  # expected exception
        except IndexError:
            pass  # expected

    def testChunkWritePoints1D(self):
        chunk_id = "c-00de6a9c-6aff5c35-15d5-3864dd-0740f8_12"
        chunk_layout = (100,)
        chunk_arr = np.zeros((100,))
        #       (coord1, coord2, ...) | dset_dtype
        point_dt = np.dtype([("coord", np.uint64), ("val", chunk_arr.dtype)])
        # point_dt = np.dtype([("coord", np.uint64, (rank,)), ("val", chunk_arr.dtype)])
        indexes = (1203, 1245, 1288, 1212, 1299)
        num_points = len(indexes)
        point_arr = np.zeros((num_points,), dtype=point_dt)
        for i in range(num_points):
            e = point_arr[i]
            e[0] = indexes[i]
            e[1] = 42
        chunkWritePoints(
            chunk_id=chunk_id,
            chunk_layout=chunk_layout,
            chunk_arr=chunk_arr,
            point_arr=point_arr,
        )
        for i in range(100):
            if i + 1200 in indexes:
                self.assertEqual(chunk_arr[i], 42)
            else:
                self.assertEqual(chunk_arr[i], 0)

        e = point_arr[1]
        e[0] = 99  # index out of range
        try:
            chunkWritePoints(
                chunk_id=chunk_id,
                chunk_layout=chunk_layout,
                chunk_arr=chunk_arr,
                point_arr=point_arr,
            )
            self.assertTrue(False)  # expected exception
        except IndexError:
            pass  # expected

    def testChunkWritePoints2D(self):
        chunk_id = "c-00de6a9c-6aff5c35-15d5-3864dd-0740f8_3_2"
        chunk_layout = (10, 20)
        chunk_arr = np.zeros((10, 20))
        #       (coord1, coord2, ...) | dset_dtype
        point_dt = np.dtype([("coord", np.uint64, (2,)), ("val", chunk_arr.dtype)])
        indexes = ((32, 46), (38, 52), (35, 53))
        num_points = len(indexes)
        point_arr = np.zeros((num_points,), dtype=point_dt)
        for i in range(num_points):
            e = point_arr[i]
            e[0] = indexes[i]
            e[1] = 42
        chunkWritePoints(
            chunk_id=chunk_id,
            chunk_layout=chunk_layout,
            chunk_arr=chunk_arr,
            point_arr=point_arr,
        )
        chunk_index = (30, 40)
        for i in range(num_points):
            index = indexes[i]
            x = index[0] - chunk_index[0]
            y = index[1] - chunk_index[1]
            self.assertEqual(chunk_arr[x, y], 42)

        e = point_arr[0]
        e[0] = (42, 46)  # index out of range
        try:
            chunkWritePoints(
                chunk_id=chunk_id,
                chunk_layout=chunk_layout,
                chunk_arr=chunk_arr,
                point_arr=point_arr,
            )
            self.assertTrue(False)  # expected exception
        except IndexError:
            pass  # expected


if __name__ == "__main__":

    unittest.main()
