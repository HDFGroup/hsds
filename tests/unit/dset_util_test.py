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

sys.path.append("../..")
from hsds.util.dsetUtil import getHyperslabSelection, getSelectionShape
from hsds.util.dsetUtil import getSelectionList, ItemIterator, getSelectionPagination, expandChunk
from hsds.util.dsetUtil import guessChunk, shrinkChunk, getChunkSize, getContiguousLayout


class DsetUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DsetUtilTest, self).__init__(*args, **kwargs)
        # main
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.WARNING)

    def testGuessChunk(self):

        typesize = "H5T_VARIABLE"
        logging.debug("hello")

        shape = {"class": "H5S_NULL"}
        layout = guessChunk(shape, typesize)
        self.assertTrue(layout is None)

        shape = {"class": "H5S_SCALAR"}
        layout = guessChunk(shape, typesize)
        self.assertEqual(layout, (1,))

        shape = {"class": "H5S_SIMPLE", "dims": [100, 100]}
        layout = guessChunk(shape, typesize)
        self.assertTrue(len(layout), 2)
        for i in range(2):
            self.assertTrue(layout[i] >= 1)
            self.assertTrue(layout[i] <= 100)

        typesize = 8
        layout = guessChunk(shape, typesize)
        self.assertTrue(len(layout), 2)
        for i in range(2):
            self.assertTrue(layout[i] >= 1)
            self.assertTrue(layout[i] <= 100)

        shape = {"class": "H5S_SIMPLE", "dims": [5]}
        layout = guessChunk(shape, typesize)
        self.assertEqual(layout, (5,))

        shape = {"class": "H5S_SIMPLE", "dims": [100, 100, 100]}
        layout = guessChunk(shape, typesize)
        print("layout:", layout)
        self.assertTrue(len(layout), 3)
        for i in range(3):
            self.assertTrue(layout[i] >= 1)
            self.assertTrue(layout[i] <= 100)

        shape = {"class": "H5S_SIMPLE", "dims": [100, 0], "maxdims": [100, 0]}
        layout = guessChunk(shape, typesize)
        self.assertTrue(len(layout), 2)
        for i in range(2):
            self.assertTrue(layout[i] >= 1)
            self.assertTrue(layout[i] <= 1024)

        shape = {"class": "H5S_SCALAR"}
        layout = guessChunk(shape, typesize)
        self.assertEqual(layout, (1,))

        shape = {"class": "H5S_NULL"}
        layout = guessChunk(shape, typesize)
        self.assertEqual(layout, None)

    def testShrinkChunk(self):
        CHUNK_MIN = 500
        CHUNK_MAX = 5000
        typesize = 1
        layout = (1, 2, 3)
        shrunk = shrinkChunk(layout, typesize, chunk_max=CHUNK_MAX)
        self.assertEqual(shrunk, layout)

        layout = (100, 200, 300)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes > CHUNK_MAX)
        shrunk = shrinkChunk(layout, typesize, chunk_max=CHUNK_MAX)
        rank = len(layout)
        for i in range(rank):
            self.assertTrue(shrunk[i] >= 1)
            self.assertTrue(shrunk[i] <= 1000 * (i + 1))
        num_bytes = getChunkSize(shrunk, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

        layout = (300, 200, 100)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes > CHUNK_MAX)
        shrunk = shrinkChunk(layout, typesize, chunk_max=CHUNK_MAX)
        rank = len(layout)
        for i in range(rank):
            self.assertTrue(shrunk[i] >= 1)
            self.assertTrue(shrunk[i] <= 1000 * (3 - i))
        num_bytes = getChunkSize(shrunk, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

        CHUNK_MIN = 1 * 1024 * 1024
        CHUNK_MAX = 4 * 1024 * 1024
        typesize = 4
        layout = (117, 201, 189, 1)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes > CHUNK_MAX)
        shrunk = shrinkChunk(layout, typesize, chunk_max=CHUNK_MAX)
        self.assertEqual(shrunk, (59, 101, 95, 1))
        num_bytes = getChunkSize(shrunk, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

    def testExpandChunk(self):
        CHUNK_MIN = 5000
        CHUNK_MAX = 50000

        typesize = 20
        shape = {"class": "H5S_SIMPLE", "dims": [12, ], "maxdims": [20, ]}
        layout = (20,)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape, chunk_min=CHUNK_MIN)
        num_bytes = getChunkSize(expanded, typesize)
        # chunk layout can't be larger than dataspace
        self.assertTrue(num_bytes < CHUNK_MIN)
        self.assertEqual(expanded, (20,))

        typesize = 1
        shape = {"class": "H5S_SIMPLE", "dims": [10, 10, 10]}
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape, chunk_min=CHUNK_MIN)
        num_bytes = getChunkSize(expanded, typesize)
        # chunk layout can't be larger than dataspace
        self.assertTrue(num_bytes < CHUNK_MIN)
        self.assertEqual(expanded, (10, 10, 10))

        shape = {"class": "H5S_SIMPLE", "dims": [1000, 2000, 3000]}
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape, chunk_min=CHUNK_MIN)
        num_bytes = getChunkSize(expanded, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

        shape = {"class": "H5S_SIMPLE", "dims": [1000,]}
        layout = (10,)
        num_bytes = getChunkSize(layout, "H5T_VARIABLE")
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, "H5T_VARIABLE", shape, chunk_min=CHUNK_MIN)
        num_bytes = getChunkSize(expanded, "H5T_VARIABLE")
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

        shape = {
            "class": "H5S_SIMPLE",
            "dims": [1000, 10, 1000],
            "maxdims": [1000, 100, 1000],
        }
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape, chunk_min=CHUNK_MIN)
        num_bytes = getChunkSize(expanded, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

        shape = {
            "class": "H5S_SIMPLE",
            "dims": [1000, 0, 1000],
            "maxdims": [1000, 100, 1000],
        }
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape, chunk_min=CHUNK_MIN)
        num_bytes = getChunkSize(expanded, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

        shape = {
            "class": "H5S_SIMPLE",
            "dims": [1000, 10, 1000],
            "maxdims": [1000, 0, 1000],
        }
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape, chunk_min=CHUNK_MIN)
        num_bytes = getChunkSize(expanded, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

    def testGetContiguousLayout(self):
        typesize = 4
        chunk_min = 400
        chunk_max = 800

        kwargs = {"chunk_min": chunk_min, "chunk_max": chunk_max}

        def get_num_bytes(dims):
            num_bytes = typesize
            for n in dims:
                num_bytes *= n
            return num_bytes

        try:
            shape = {"class": "H5S_SIMPLE", "dims": [100, 100]}
            layout = getContiguousLayout(shape, "H5T_VARIABLE", **kwargs)
            self.assertTrue(False)
        except ValueError:
            pass  # expected

        shape = {"class": "H5S_NULL"}
        layout = getContiguousLayout(shape, typesize, **kwargs)
        self.assertTrue(layout is None)

        shape = {"class": "H5S_SCALAR"}
        layout = getContiguousLayout(shape, typesize, **kwargs)
        self.assertEqual(layout, (1,))

        for extent in (1, 100, 10000):
            dims = [
                extent,
            ]
            shape = {"class": "H5S_SIMPLE", "dims": dims}
            layout = getContiguousLayout(shape, typesize, **kwargs)
            self.assertTrue(len(layout), 1)
            chunk_bytes = get_num_bytes(layout)
            space_bytes = get_num_bytes(dims)
            if space_bytes > chunk_min:
                self.assertTrue(chunk_bytes >= chunk_min)

            self.assertTrue(chunk_bytes <= chunk_max)

        for extent in (1, 9, 90):
            dims = [extent, extent]
            shape = {"class": "H5S_SIMPLE", "dims": dims}
            layout = getContiguousLayout(shape, typesize, **kwargs)
            self.assertTrue(len(layout), 2)
            for i in range(2):
                self.assertTrue(layout[i] >= 1)
                self.assertTrue(layout[i] <= extent)
            self.assertEqual(layout[1], extent)
            chunk_bytes = get_num_bytes(layout)
            space_bytes = get_num_bytes(dims)

            if space_bytes > chunk_min:
                self.assertTrue(chunk_bytes >= chunk_min)
            self.assertTrue(chunk_bytes <= chunk_max)

        for extent in (1, 10, 100):
            dims = [extent, extent, 50]
            shape = {"class": "H5S_SIMPLE", "dims": dims}
            layout = getContiguousLayout(shape, typesize, **kwargs)
            self.assertTrue(len(layout), 3)
            for i in range(3):
                self.assertTrue(layout[i] >= 1)
                self.assertTrue(layout[i] <= dims[i])

            chunk_bytes = get_num_bytes(layout)
            space_bytes = get_num_bytes(dims)

            if space_bytes > chunk_min:
                self.assertTrue(chunk_bytes >= chunk_min)
            self.assertTrue(chunk_bytes <= chunk_max)

        for extent in (1, 100, 1000):
            dims = [extent, 4]
            shape = {"class": "H5S_SIMPLE", "dims": dims}
            layout = getContiguousLayout(shape, typesize, **kwargs)
            self.assertTrue(len(layout), 2)
            for i in range(2):
                self.assertTrue(layout[i] >= 1)
                self.assertTrue(layout[i] <= dims[i])

            chunk_bytes = get_num_bytes(layout)
            space_bytes = get_num_bytes(dims)

            if space_bytes > chunk_min:
                self.assertTrue(chunk_bytes >= chunk_min)
            self.assertTrue(chunk_bytes <= chunk_max)

    def testGetHyperslabSelection(self):
        # getHyperslabSelection(dsetshape, start, stop, step)
        # 1-D case
        datashape = [100,]
        slices = getHyperslabSelection(datashape)
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0], slice(0, 100, 1))

        slices = getHyperslabSelection(datashape, 20)
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0], slice(20, 100, 1))

        slices = getHyperslabSelection(datashape, 20, 80)
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0], slice(20, 80, 1))

        slices = getHyperslabSelection(datashape, 20, 80, 2)
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0], slice(20, 80, 2))

        datashape = [100, 50]
        slices = getHyperslabSelection(datashape)
        self.assertEqual(len(slices), 2)
        self.assertEqual(slices[0], slice(0, 100, 1))
        self.assertEqual(slices[1], slice(0, 50, 1))

        slices = getHyperslabSelection(datashape, (10, 20))
        self.assertEqual(len(slices), 2)
        self.assertEqual(slices[0], slice(10, 100, 1))
        self.assertEqual(slices[1], slice(20, 50, 1))

        slices = getHyperslabSelection(datashape, (10, 20), (90, 30))
        self.assertEqual(len(slices), 2)
        self.assertEqual(slices[0], slice(10, 90, 1))
        self.assertEqual(slices[1], slice(20, 30, 1))

        slices = getHyperslabSelection(datashape, (10, 20), (90, 30), (1, 2))
        self.assertEqual(len(slices), 2)
        self.assertEqual(slices[0], slice(10, 90, 1))
        self.assertEqual(slices[1], slice(20, 30, 2))

    def testGetSelectionShape(self):
        sel = [slice(3, 7, 1),]
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [4,],)

        sel = [slice(3, 7, 3),]  # select points 3, 6
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [2,],)

        sel = [slice(44, 52, 1), slice(48, 52, 1)]
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [8, 4])

        sel = [slice(0, 4, 2),]  # select points 0, 2
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [2,],)

        sel = [slice(0, 5, 2),]  # select points 0, 2, 4
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [3,],)

        sel = [[2, 3, 5, 7, 11]]  # coordinate list
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [5,],)

        sel = [slice(0, 100, 1), slice(50, 51, 1), [23, 35, 56]]
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [100, 1, 3])

        sel = [slice(0, 100, 1), [2, 3, 5, 7, 11]]
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [100, 5])

        sel = [[1, 2, 5, 9], [11, 3, 5, 7]]
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [4,])

        sel = [(0, 1, 3), (7, 4, 2)]
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [3,])

        try:
            sel = [(0, 1, 3), (7, 4,)]
            shape = getSelectionShape(sel)
            self.assertTrue(False)
        except Exception:
            pass  # expected

    def testGetSelectionPagination(self):
        itemsize = 4  # will use 4 for most tests

        # 1D case

        datashape = [200,]
        max_request_size = 120
        select = [(slice(20, 40)),]  # 80 byte selection
        # should return one page equivalent to original selection
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 1)
        page = pages[0]
        self.assertEqual(len(page), 1)
        s = page[0]
        self.assertEqual(s.start, 20)
        self.assertEqual(s.stop, 40)

        select = [(slice(0, 200)),]  # 800 byte selection
        # should create 7 pages
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 8)
        start = 0
        # verify pages are contiguous
        for page in pages:
            self.assertEqual(len(page), 1)
            s = page[0]
            self.assertTrue(isinstance(s, slice))
            self.assertEqual(s.start, start)
            self.assertEqual(s.step, 1)
            self.assertTrue(s.stop > s.start)
            count = s.stop - s.start
            self.assertTrue(count * itemsize < max_request_size)
            start = s.stop
        self.assertEqual(s.stop, 200)

        select = [
            (slice(0, 200, 8)),
        ]  # 80 byte selection
        # should create 1 page
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 1)
        page = pages[0]
        self.assertEqual(len(page), 1)
        s = page[0]
        self.assertTrue(isinstance(s, slice))
        self.assertEqual(s.start, 0)
        self.assertEqual(s.stop, 200)
        self.assertEqual(s.step, 8)

        select = [
            (slice(0, 195, 4)),
        ]  # 156 byte selection
        # should create 4 pages
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 2)
        start = 0
        for page in pages:
            self.assertEqual(len(page), 1)
            s = page[0]
            self.assertTrue(isinstance(s, slice))
            self.assertEqual(s.start, start)
            self.assertEqual(
                s.start % 4, 0
            )  # start value always falls in step intervals
            self.assertEqual(s.step, 4)
            self.assertTrue(s.stop > s.start + 4)
            count = (s.stop - s.start) // 4
            self.assertTrue(count * itemsize < max_request_size)
            start = s.stop

        coords = []
        for i in range(50):
            coords.append(i * 4)
        select = [
            coords,
        ]  # 160 byte coordinate selection
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        print("pages:", pages)
        self.assertEqual(len(pages), 2)
        for page in pages:
            self.assertEqual(len(page), 1)
            s = page[0]
            self.assertTrue(isinstance(s, tuple))
            count = len(s)
            print("count:", count)
            self.assertTrue(len(s) > 20)
            self.assertTrue(count * itemsize <= max_request_size)

        # 2D case

        datashape = [200, 300]
        max_request_size = 1000
        select = [(slice(0, 10)), (slice(0, 20))]  # 800 byte selection
        # should return one page equivalent to original selection
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 1)
        page = pages[0]
        self.assertEqual(len(page), 2)

        for i in range(2):
            self.assertEqual(page[i].start, select[i].start)
            self.assertEqual(page[i].stop, select[i].stop)

        select = [(slice(20, 60)), (slice(0, 20))]  # 3200 byte selection
        # should return one page equivalent to original selection
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 4)
        start = 20
        for page in pages:
            self.assertEqual(len(page), 2)
            self.assertEqual(page[0].start, start)
            # second dimension shouldn't change
            self.assertEqual(page[1].start, select[1].start)
            self.assertEqual(page[1].stop, select[1].stop)
            start = page[0].stop
        self.assertEqual(start, select[0].stop)

        select = [(40,), (slice(0, 300))]  # 1200 byte selection
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 2)
        start = 0

        # pagination should happen along the second dimension,
        # since there's only one coordinate in the first
        for page in pages:
            self.assertEqual(len(page), 2)
            self.assertEqual(page[1].start, start)
            # second dimension shouldn't change
            self.assertEqual(page[0], (40,))
            start = page[1].stop
        self.assertEqual(start, select[1].stop)

        itemsize = 2
        datashape = (1300, 1300, 1300)
        max_request_size = 100 * 1024 * 1024

        select = [
            (slice(200, 400)),
            (slice(0, 1300)),
            (slice(0, 1300)),
        ]  # 644 MB selection
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 8)
        start = 200
        for page in pages:
            self.assertEqual(len(page), 3)
            self.assertEqual(page[0].start, start)
            self.assertEqual(page[1], slice(0, 1300))
            self.assertEqual(page[2], slice(0, 1300))
            page_size = (page[0].stop - page[0].start) * 1300 * 1300 * 2
            self.assertTrue(page_size < max_request_size)
            start = page[0].stop

        select = [
            (slice(0, 1300)),
            (slice(0, 1300)),
            (slice(0, 1300)),
        ]  # 4.1GB selection
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 44)
        start = 0
        for page in pages:
            # print(page)
            self.assertEqual(len(page), 3)
            self.assertEqual(page[0].start, start)
            self.assertEqual(page[1], slice(0, 1300))
            self.assertEqual(page[2], slice(0, 1300))
            page_size = (page[0].stop - page[0].start) * 1300 * 1300 * 2
            self.assertTrue(page_size < max_request_size)
            start = page[0].stop

    def testItemIterator(self):
        # 1-D case
        datashape = [10,]
        slices = getHyperslabSelection(datashape)
        it = ItemIterator(slices)

        indices = []
        count = 0

        while True:
            try:
                index = it.next()
                count += 1
                indices.append(index)
            except StopIteration:
                break
        self.assertEqual(count, 10)
        self.assertEqual(indices, list(range(10)))

        # 2-D case
        datashape = [4, 5]
        slices = getHyperslabSelection(datashape)
        it = ItemIterator(slices)

        indices = []
        count = 0
        while True:
            try:
                index = it.next()
                self.assertTrue(len(index), 2)
                self.assertTrue(index[0] >= 0)
                self.assertTrue(index[0] < 4)
                self.assertTrue(index[1] >= 0)
                self.assertTrue(index[1] < 5)
                count += 1
                indices.append(index)
            except StopIteration:
                break
        self.assertEqual(count, 20)

    def testSelectionList1D(self):
        dims = [100,]

        for select in ("", []):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 100, 1))

        for select in (
            "[5]",
            [5,],
        ):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(5, 6, 1))

        for select in (
            "[:]",
            [":",],
        ):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 100, 1))

        for select in (
            "[3:7]",
            ["3:7",],
        ):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(3, 7, 1))

        for select in (
            "[:4]",
            [":4",],
        ):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 4, 1))

        for select in (
            "[0:100]",
            ["0:100",],
        ):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 100, 1))

        for select in ("[[3,4,7]]", ["[3,4,7]"], [[3, 4, 7]]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, list))
            self.assertEqual(s1, [3, 4, 7])

        for select in (
            "[30:70:5]",
            ["30:70:5",],
        ):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(30, 70, 5))

        body = {"start": 3, "stop": 7}
        selection = getSelectionList(body, dims)
        self.assertEqual(len(selection), 1)
        s1 = selection[0]
        self.assertTrue(isinstance(s1, slice))
        self.assertEqual(s1, slice(3, 7, 1))

        body = {"start": 30, "stop": 70, "step": 5}
        selection = getSelectionList(body, dims)
        self.assertEqual(len(selection), 1)
        s1 = selection[0]
        self.assertTrue(isinstance(s1, slice))
        self.assertEqual(s1, slice(30, 70, 5))

    def testSelectionList2D(self):
        dims = [50, 100, ]

        for select in ("", []):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 2)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 50, 1))
            s2 = selection[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(0, 100, 1))

        for select in ("[5,40]", ["5", "40"], [5, 40]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 2)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(5, 6, 1))
            s2 = selection[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(40, 41, 1))

        for select in ("[3:7,12]", ["3:7", "12"], ["3:7", 12]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 2)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(3, 7, 1))
            s2 = selection[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(12, 13, 1))

        for select in ("[:,[3,4,7]]", [":", "[3,4,7]"], [":", [3, 4, 7]]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 2)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, dims[0], 1))
            s2 = selection[1]
            self.assertTrue(isinstance(s2, list))
            self.assertEqual(s2, [3, 4, 7])

        for select in ("[[2, 5, 8],[3,4,7]]", ["[2, 5, 8]", "[3,4,7]"], [[2, 5, 8], [3, 4, 7]]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 2)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, list))
            self.assertEqual(s1, [2, 5, 8])
            s2 = selection[1]
            self.assertTrue(isinstance(s2, list))
            self.assertEqual(s2, [3, 4, 7])

        for select in ("[[2,5,8],[7,4,3]]", ["[2, 5, 8]", "[7,4,3]"], [[2, 5, 8], [7, 4, 3]]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 2)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, list))
            self.assertEqual(s1, [2, 5, 8])
            s2 = selection[1]
            self.assertTrue(isinstance(s2, list))
            self.assertEqual(s2, [7, 4, 3])

        for select in ("[1:20, 30:70:5]", ["1:20", "30:70:5"]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 2)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(1, 20, 1))
            s2 = selection[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(30, 70, 5))

        for select in ("[0:50, 0:100]", ["0:50", "0:100"]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 2)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 50, 1))
            s2 = selection[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(0, 100, 1))

        body = {"start": [3, 5], "stop": [7, 9]}
        selection = getSelectionList(body, dims)
        self.assertEqual(len(selection), 2)
        s1 = selection[0]
        self.assertTrue(isinstance(s1, slice))
        self.assertEqual(s1, slice(3, 7, 1))
        s2 = selection[1]
        self.assertTrue(isinstance(s2, slice))
        self.assertEqual(s2, slice(5, 9, 1))

        body = {"start": [0, 30], "stop": [10, 70], "step": [1, 5]}
        selection = getSelectionList(body, dims)
        self.assertEqual(len(selection), 2)
        s1 = selection[0]
        self.assertTrue(isinstance(s1, slice))
        self.assertEqual(s1, slice(0, 10, 1))
        s2 = selection[1]
        self.assertTrue(isinstance(s2, slice))
        self.assertEqual(s2, slice(30, 70, 5))

    def testInvalidSelectionList(self):
        dims = [50, 100,]

        try:
            # no bracket
            getSelectionList("2", dims)
            self.assertTrue(False)
        except ValueError:
            pass  # expected

        try:
            # selection doesn't match dimension
            getSelectionList("[2]", dims)
            self.assertTrue(False)
        except ValueError:
            pass  # expected

        try:
            # invalid character
            getSelectionList("[2,x]", dims)
            self.assertTrue(False)
        except ValueError:
            pass  # expected

        try:
            # too many colons
            getSelectionList("[6, 1:2:3:4]", dims)
            self.assertTrue(False)
        except ValueError:
            pass  # expected

        try:
            # out of bounds
            getSelectionList("[2, 101]", dims)
            self.assertTrue(False)
        except ValueError:
            pass  # expected

        try:
            # out of bounds - range
            getSelectionList("[2, 22:101]", dims)
            self.assertTrue(False)
        except ValueError:
            pass  # expected

        try:
            # out of bounds - coordinate list
            getSelectionList("[2, [1,2,3,101]]", dims)
            self.assertTrue(False)
        except ValueError:
            pass  # expected

        try:
            # out of bounds - reversed selection
            getSelectionList("[2, 50:20]", dims)
            self.assertTrue(False)
        except ValueError:
            pass  # expected

        try:
            # out of bounds - coordinate list non-increasing
            getSelectionList("[2, [1,2,2]]", dims)
        except ValueError:
            self.assertTrue(False)  # supported now

        try:
            # missing key
            getSelectionList({"start": [30, 40]}, dims)
            self.assertTrue(False)
        except KeyError:
            pass  # expected

        try:
            # out of bounds
            getSelectionList({"start": [30, 40], "stop": [2, 101]}, dims)
            self.assertTrue(False)
        except ValueError:
            pass  # expected

        try:
            # wrong number of dimensions
            getSelectionList({"start": [30, 40], "stop": [2, 7, 101]}, dims)
            self.assertTrue(False)
        except ValueError:
            pass  # expected


if __name__ == "__main__":
    # setup test files

    unittest.main()
