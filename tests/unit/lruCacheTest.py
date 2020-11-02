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
import random
import sys
import numpy as np

sys.path.append('../..')
from hsds.util.lruCache import LruCache
from hsds.util.idUtil import createObjId

class LruCacheTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(LruCacheTest, self).__init__(*args, **kwargs)
        # main

    def testSimple(self):
        """ check basic functions by adding one chunk to cache """
        cc = LruCache(mem_target=1000*1000*10)
        cc.consistencyCheck()

        self.assertEqual(len(cc), 0)
        self.assertEqual(cc.dump_lru(), "->\n<-\n")

        self.assertFalse("xyz" in cc)

        id = createObjId("chunks")
        try:
            # only dict objects can be added
            cc[id] = list(range(20))
            self.assertTrue(False)
        except TypeError:
            pass # expected
        arr = np.empty((16, 16), dtype='i4')
        id = createObjId("datasets")
        try:
            cc[id] = arr
            self.assertTrue(False)
        except ValueError:
            pass # expected - not a chunk id

        rand_id = createObjId("chunks")
        np_arr = np.random.random((500, 500))  # smaller than our chunk cache size
        cc[rand_id] = np_arr  # add to cache
        cc.consistencyCheck()
        self.assertEqual(len(cc), 1)
        self.assertTrue(rand_id in cc)
        lru_str = "->" + rand_id + "\n<-" + rand_id + "\n"
        mem_tgt = cc.memTarget
        self.assertEqual(mem_tgt, 1000*1000*10)
        mem_used = cc.memUsed
        self.assertEqual(mem_used, 500*500*8)
        mem_dirty = cc.memDirty
        self.assertEqual(mem_dirty, 0)
        mem_per = cc.cacheUtilizationPercent
        self.assertEqual(mem_per, 20)   # have used 20% of target memory

        # try adding the same id to the cache again
        cc[rand_id] = np_arr
        cc.consistencyCheck()
        self.assertEqual(len(cc), 1)
        self.assertTrue(rand_id in cc)

        # try out the dirty flags
        self.assertFalse(cc.isDirty(rand_id))
        self.assertEqual(cc.dirtyCount, 0)
        cc.setDirty(rand_id)
        cc.consistencyCheck()
        self.assertTrue(cc.isDirty(rand_id))
        self.assertEqual(cc.dirtyCount, 1)
        self.assertEqual(cc.dump_lru(), lru_str)
        cc.consistencyCheck()
        cc.clearDirty(rand_id)
        cc.consistencyCheck()
        self.assertFalse(cc.isDirty(rand_id))
        self.assertEqual(cc.dirtyCount, 0)
        # chunk should not have been evicted from cache
        self.assertEqual(len(cc), 1)
        self.assertTrue(rand_id in cc)
        # delete from cache
        del cc[rand_id]
        cc.consistencyCheck()
        # check cache is empty
        self.assertEqual(len(cc), 0)
        self.assertFalse(rand_id in cc)
        mem_tgt = cc.memTarget
        self.assertEqual(mem_tgt, 1000*1000*10)
        mem_used = cc.memUsed
        self.assertEqual(mem_used, 0)
        mem_dirty = cc.memDirty
        self.assertEqual(mem_dirty, 0)
        mem_per = cc.cacheUtilizationPercent
        self.assertEqual(mem_per, 0)   # no memory used


    def testLRU(self):
        """ Check LRU replacement logic """
        cc = LruCache(mem_target=1024*1024*1024) # big enough that there shouldn't be any cleanup
        self.assertEqual(len(cc), 0)
        ids = []
        # add chunks to the cache
        for i in range(10):
            id = createObjId("chunks")
            ids.append(id)
            arr = np.empty((16, 16), dtype='i4')  # 1024 bytes
            arr[...] = i
            cc[id] = arr
        for id in cc:
            self.assertTrue(id.startswith("c-"))
            self.assertTrue(id in ids)
        self.assertEqual(len(cc), 10)
        self.assertEqual(cc._lru_head._id, ids[-1])
        self.assertEqual(cc._lru_tail._id, ids[0])
        self.assertEqual(cc.dirtyCount, 0)
        cc.consistencyCheck()

        node = cc._lru_head
        for i in range(10):
            self.assertEqual(node._id, ids[9 - i])
            node = node._next
        self.assertTrue(node is None)

        chunk_5 = ids[5]
        cc.consistencyCheck()

        np_arr = cc[chunk_5]
        self.assertEqual(np_arr[0,0], 5)
        # the get should have moved this guy to the front
        self.assertEqual(cc._lru_head._id, chunk_5)
        for i in range(10):
            self.assertFalse(cc.isDirty(ids[i]))
        # shouldn't have effected the position
        self.assertEqual(cc._lru_head._id, chunk_5)
        # set chunk 7 to dirty
        chunk_7 = ids[7]
        cc.consistencyCheck()
        cc.setDirty(chunk_7)
        cc.consistencyCheck()
        self.assertEqual(cc.dirtyCount, 1)
        # clear dirty
        cc.clearDirty(chunk_7)
        self.assertEqual(cc.dirtyCount, 0)

        random.shuffle(ids)  # randomize the order we remove chunks
        for i in range(10):
            # remove random chunk
            chunk_id = ids[i]
            del cc[chunk_id]
            cc.consistencyCheck()
        self.assertEqual(len(cc), 0)
        self.assertEqual(cc._lru_head, None)
        self.assertEqual(cc._lru_tail, None)
        cc.consistencyCheck()

    def testClearCache(self):
        """ Check LRU clear logic """
        cc = LruCache(mem_target=1024*1024*1024) # big enough that there shouldn't be any cleanup
        self.assertEqual(len(cc), 0)
        ids = []
        # add chunks to the cache
        for i in range(10):
            id = createObjId("chunks")
            ids.append(id)
            arr = np.empty((16, 16), dtype='i4')  # 1024 bytes
            arr[...] = i
            cc[id] = arr
        for id in cc:
            self.assertTrue(id.startswith("c-"))
            self.assertTrue(id in ids)
        self.assertEqual(len(cc), 10)
        self.assertEqual(cc._lru_head._id, ids[-1])
        self.assertEqual(cc._lru_tail._id, ids[0])
        self.assertEqual(cc.dirtyCount, 0)
        cc.consistencyCheck()

        cc.clearCache()
        self.assertEqual(len(cc), 0)

        cc.consistencyCheck()


    def testMemUtil(self):
        """ Test memory usage tracks target """
        cc = LruCache(mem_target=5000)
        self.assertEqual(len(cc), 0)
        ids = set()
        for i in range(10):
            id = createObjId("chunks")
            ids.add(id)
            arr = np.empty((16, 16), dtype='i4')  # 1024 bytes
            arr[...] = i
            cc[id] = arr
            self.assertTrue(id in cc)

        cc.consistencyCheck()
        self.assertTrue(len(cc) < 10) # given mem-target, some items should have been removed
        mem_per = cc.cacheUtilizationPercent
        self.assertTrue(mem_per < 100)
        mem_dirty = cc.memDirty
        self.assertEqual(mem_dirty, 0)

        # add 10 more chunks, but set dirty to true each time
        for i in range(10):
            id = createObjId("chunks")
            ids.add(id)
            arr = np.empty((16, 16), dtype='i4')  # 1024 bytes
            arr[...] = i
            cc[id] = arr
            self.assertTrue(id in cc)
            cc.setDirty(id)
            cc.consistencyCheck()
            mem_dirty = cc.memDirty
            self.assertEqual(mem_dirty, 1024 * (i+1))


        mem_per = cc.cacheUtilizationPercent
        # chunks are dirty so percent is over 100%
        self.assertTrue(mem_per > 100)

        # clear dirty flags (allowing memory to be released)
        id_list = []
        for id in cc:
            id_list.append(id)

        random.shuffle(id_list)  # randomize the order we clear dirty flag

        id=id_list[0]
        cc.clearDirty(id)
        cc.consistencyCheck()

        for id in id_list:
            self.assertTrue(id in ids)
            mem_dirty = cc.memDirty
            if cc.isDirty(id):
                cc.clearDirty(id)
                self.assertTrue(cc.memDirty < mem_dirty)
        mem_per = cc.cacheUtilizationPercent
        # mem percent should be less than 100 now

        self.assertTrue(mem_per <= 100)

    def testMetaDataCache(self):
        """ check metadata cache functionality """
        cc = LruCache(mem_target=1024*10, name="ChunkCache")
        cc.consistencyCheck()

        self.assertEqual(len(cc), 0)
        self.assertEqual(cc.dump_lru(), "->\n<-\n")

        id = createObjId("datasets")
        try:
            # only numpy arrays an be added
            cc[id] = np.zeros((3,4))
            self.assertTrue(False)
        except TypeError:
            pass # expected
        data = { "x": 123, "y": 456}
        arr = np.zeros((10,))
        id = createObjId("chunks")
        try:
            cc[id] = arr
            self.assertTrue(False)
        except TypeError:
            pass # expected - not a dict

        rand_id = createObjId("groups")
        data = {"foo": "bar"}
        cc[rand_id] = data  # add to cache
        cc.consistencyCheck()
        self.assertEqual(len(cc), 1)
        self.assertTrue(rand_id in cc)
        lru_str = "->" + rand_id + "\n<-" + rand_id + "\n"
        mem_tgt = cc.memTarget
        self.assertEqual(mem_tgt, 1024*10)
        mem_used = cc.memUsed
        self.assertEqual(mem_used, 1024)  # not based on actual size
        mem_per = cc.cacheUtilizationPercent
        self.assertEqual(mem_per, 10)   # have used 10% of target memory
        # try out the dirty flags
        self.assertFalse(cc.isDirty(rand_id))
        self.assertEqual(cc.dirtyCount, 0)
        cc.setDirty(rand_id)
        cc.consistencyCheck()
        self.assertTrue(cc.isDirty(rand_id))
        self.assertEqual(cc.dirtyCount, 1)
        self.assertEqual(cc.dump_lru(), lru_str)
        cc.clearDirty(rand_id)
        cc.consistencyCheck()
        self.assertFalse(cc.isDirty(rand_id))
        self.assertEqual(cc.dirtyCount, 0)
        # chunk should not have been evicted from cache
        self.assertEqual(len(cc), 1)
        self.assertTrue(rand_id in cc)
        # delete from cache
        del cc[rand_id]
        cc.consistencyCheck()
        # check cache is empty
        self.assertEqual(len(cc), 0)
        self.assertFalse(rand_id in cc)
        mem_tgt = cc.memTarget
        self.assertEqual(mem_tgt, 1024*10)
        mem_used = cc.memUsed
        self.assertEqual(mem_used, 0)
        mem_per = cc.cacheUtilizationPercent
        self.assertEqual(mem_per, 0)   # no memory used


if __name__ == '__main__':
    #setup test files

    unittest.main()
