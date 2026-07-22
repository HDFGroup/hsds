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

from h5json import selections

sys.path.append("../..")
from hsds.util.dsetUtil import get_slices
from hsds.util.dsetUtil import getSelectionList, getSelectionPagination


class DsetUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DsetUtilTest, self).__init__(*args, **kwargs)
        # main
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.WARNING)

    def testGetSlices(self):
        dset_json = {"id": "d-b4b3b3d6-94343adc-1727-28bebf-12caac"}
        datashape = {"class": "H5S_SCALAR"}
        cprops = {"layout": {"class": "H5D_CONTIGUOUS"}}
        dtype_json = {"class": "H5T_INTEGER", "base": "H5T_STD_I32LE"}
        dset_json["shape"] = datashape
        dset_json["creationProperties"] = cprops
        dset_json["type"] = dtype_json

        slices = get_slices("", dset_json)
        self.assertEqual(len(slices.shape), 1)
        self.assertEqual(slices.slices[0], slice(0, 1, 1))

        slices = get_slices(None, dset_json)
        self.assertEqual(len(slices.shape), 1)
        self.assertEqual(slices.slices[0], slice(0, 1, 1))

    def testGetSelectionPagination(self):
        itemsize = 4  # will use 4 for most tests

        # 1D case

        datashape = (200,)
        max_request_size = 120
        select = selections.select(datashape, (slice(20, 40),))  # 80 byte selection
        # should return one page equivalent to original selection
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 1)
        page = pages[0]
        self.assertEqual(len(page.shape), 1)
        s = page.slices[0]
        self.assertEqual(s.start, 20)
        self.assertEqual(s.stop, 40)

        select = selections.select(datashape, (slice(0, 200),))  # 800 byte selection
        # should create 8 pages
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 8)
        start = 0
        # verify pages are contiguous
        for page in pages:
            self.assertEqual(len(page.shape), 1)
            s = page.slices[0]
            self.assertTrue(isinstance(s, slice))
            self.assertEqual(s.start, start)
            self.assertEqual(s.step, 1)
            self.assertTrue(s.stop > s.start)
            count = s.stop - s.start
            self.assertTrue(count * itemsize < max_request_size)
            start = s.stop
        self.assertEqual(s.stop, 200)

        select = selections.select(datashape, (slice(0, 200, 8),))  # 100 byte selection
        # should create 1 page
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 1)
        page = pages[0]
        self.assertEqual(len(page.shape), 1)
        s = page.slices[0]
        self.assertTrue(isinstance(s, slice))
        self.assertEqual(s.start, 0)
        # count of 25 points (start + count, per h5json's slice convention)
        self.assertEqual(s.stop, 25)
        self.assertEqual(s.step, 8)

        select = selections.select(datashape, (slice(0, 195, 4),))  # 196 byte selection
        # should create 3 pages
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 3)
        total_points = 0
        for page in pages:
            self.assertEqual(len(page.shape), 1)
            s = page.slices[0]
            self.assertTrue(isinstance(s, slice))
            self.assertEqual(
                s.start % 4, 0
            )  # start value always falls in step intervals
            self.assertEqual(s.step, 4)
            count = s.stop - s.start  # h5json encodes stop as start + count
            self.assertTrue(count * itemsize <= max_request_size)
            total_points += count
        self.assertEqual(total_points, 49)  # covers all 49 selected points

        coords = []
        for i in range(50):
            coords.append(i * 4)
        select = selections.select(datashape, (coords,))  # 200 byte coordinate selection
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 2)
        total_coords = 0
        for page in pages:
            self.assertEqual(len(page.shape), 1)
            s = page.slices[0]
            self.assertTrue(isinstance(s, list))
            count = len(s)
            self.assertTrue(count > 20)
            self.assertTrue(count * itemsize <= max_request_size)
            total_coords += count
        self.assertEqual(total_coords, 50)

        # 2D case

        datashape = (200, 300)
        max_request_size = 1000
        select = selections.select(datashape, (slice(0, 10), slice(0, 20)))  # 800 byte selection
        # should return one page equivalent to original selection
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 1)
        page = pages[0]
        self.assertEqual(len(page.shape), 2)

        for i in range(2):
            self.assertEqual(page.slices[i].start, select.slices[i].start)
            self.assertEqual(page.slices[i].stop, select.slices[i].stop)

        select = selections.select(datashape, (slice(20, 60), slice(0, 20)))  # 3200 byte selection
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 4)
        start = 20
        for page in pages:
            self.assertEqual(len(page.shape), 2)
            self.assertEqual(page.slices[0].start, start)
            # second dimension shouldn't change
            self.assertEqual(page.slices[1].start, select.slices[1].start)
            self.assertEqual(page.slices[1].stop, select.slices[1].stop)
            start = page.slices[0].stop
        self.assertEqual(start, select.slices[0].stop)

        select = selections.select(datashape, ([40], slice(0, 300)))  # 1200 byte selection
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 2)
        start = 0

        # pagination should happen along the second dimension,
        # since there's only one coordinate in the first
        for page in pages:
            self.assertEqual(len(page.shape), 2)
            self.assertEqual(page.slices[1].start, start)
            # first dimension shouldn't change
            self.assertEqual(page.slices[0], [40])
            start = page.slices[1].stop
        self.assertEqual(start, select.slices[1].stop)

        itemsize = 2
        datashape = (1300, 1300, 1300)
        max_request_size = 100 * 1024 * 1024

        select = selections.select(
            datashape, (slice(200, 400), slice(0, 1300), slice(0, 1300))
        )  # 644 MB selection
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 8)
        start = 200
        for page in pages:
            self.assertEqual(len(page.shape), 3)
            self.assertEqual(page.slices[0].start, start)
            self.assertEqual(page.slices[1], slice(0, 1300, 1))
            self.assertEqual(page.slices[2], slice(0, 1300, 1))
            page_size = (page.slices[0].stop - page.slices[0].start) * 1300 * 1300 * 2
            self.assertTrue(page_size < max_request_size)
            start = page.slices[0].stop

        select = selections.select(
            datashape, (slice(0, 1300), slice(0, 1300), slice(0, 1300))
        )  # 4.1GB selection
        pages = getSelectionPagination(select, datashape, itemsize, max_request_size)
        self.assertEqual(len(pages), 44)
        start = 0
        for page in pages:
            self.assertEqual(len(page.shape), 3)
            self.assertEqual(page.slices[0].start, start)
            self.assertEqual(page.slices[1], slice(0, 1300, 1))
            self.assertEqual(page.slices[2], slice(0, 1300, 1))
            page_size = (page.slices[0].stop - page.slices[0].start) * 1300 * 1300 * 2
            self.assertTrue(page_size < max_request_size)
            start = page.slices[0].stop

    def testSelectionList1D(self):
        dims = [100,]

        for select in ("", []):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 1)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 100, 1))

        for select in (
            "[5]",
            [5,],
        ):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 1)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(5, 6, 1))

        for select in (
            "[:]",
            [":",],
        ):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 1)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 100, 1))

        for select in (
            "[3:7]",
            ["3:7",],
        ):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 1)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(3, 7, 1))

        for select in (
            "[:4]",
            [":4",],
        ):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 1)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 4, 1))

        for select in (
            "[0:100]",
            ["0:100",],
        ):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 1)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 100, 1))

        for select in ("[[3,4,7]]", ["[3,4,7]"], [[3, 4, 7]]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 1)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, list))
            self.assertEqual(s1, [3, 4, 7])

        for select in (
            "[30:70:5]",
            ["30:70:5",],
        ):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 1)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(30, 38, 5))

        body = {"start": 3, "stop": 7}
        selection = getSelectionList(body, dims)
        self.assertEqual(len(selection.shape), 1)
        s1 = selection.slices[0]
        self.assertTrue(isinstance(s1, slice))
        self.assertEqual(s1, slice(3, 7, 1))

        body = {"start": 30, "stop": 70, "step": 5}
        selection = getSelectionList(body, dims)
        self.assertEqual(len(selection.shape), 1)
        s1 = selection.slices[0]
        self.assertTrue(isinstance(s1, slice))
        self.assertEqual(s1, slice(30, 38, 5))

    def testSelectionList2D(self):
        dims = [50, 100, ]

        for select in ("", []):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 2)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 50, 1))
            s2 = selection.slices[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(0, 100, 1))

        for select in ("[5,40]", ["5", "40"], [5, 40]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 2)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(5, 6, 1))
            s2 = selection.slices[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(40, 41, 1))

        for select in ("[3:7,12]", ["3:7", "12"], ["3:7", 12]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 2)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(3, 7, 1))
            s2 = selection.slices[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(12, 13, 1))

        for select in ("[:,[3,4,7]]", [":", "[3,4,7]"], [":", [3, 4, 7]]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 2)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, dims[0], 1))
            s2 = selection.slices[1]
            self.assertTrue(isinstance(s2, list))
            self.assertEqual(s2, [3, 4, 7])

        for select in ("[[2, 5, 8],[3,4,7]]", ["[2, 5, 8]", "[3,4,7]"], [[2, 5, 8], [3, 4, 7]]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 2)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, list))
            self.assertEqual(s1, [2, 5, 8])
            s2 = selection.slices[1]
            self.assertTrue(isinstance(s2, list))
            self.assertEqual(s2, [3, 4, 7])

        for select in ("[[2,5,8],[7,4,3]]", ["[2, 5, 8]", "[7,4,3]"], [[2, 5, 8], [7, 4, 3]]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 2)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, list))
            self.assertEqual(s1, [2, 5, 8])
            s2 = selection.slices[1]
            self.assertTrue(isinstance(s2, list))
            self.assertEqual(s2, [7, 4, 3])

        for select in ("[1:20, 30:70:5]", ["1:20", "30:70:5"]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 2)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(1, 20, 1))
            s2 = selection.slices[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(30, 38, 5))

        for select in ("[0:50, 0:100]", ["0:50", "0:100"]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection.shape), 2)
            s1 = selection.slices[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 50, 1))
            s2 = selection.slices[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(0, 100, 1))

        body = {"start": [3, 5], "stop": [7, 9]}
        selection = getSelectionList(body, dims)
        self.assertEqual(len(selection.shape), 2)
        s1 = selection.slices[0]
        self.assertTrue(isinstance(s1, slice))
        self.assertEqual(s1, slice(3, 7, 1))
        s2 = selection.slices[1]
        self.assertTrue(isinstance(s2, slice))
        self.assertEqual(s2, slice(5, 9, 1))

        body = {"start": [0, 30], "stop": [10, 70], "step": [1, 5]}
        selection = getSelectionList(body, dims)
        self.assertEqual(len(selection.shape), 2)
        s1 = selection.slices[0]
        self.assertTrue(isinstance(s1, slice))
        self.assertEqual(s1, slice(0, 10, 1))
        s2 = selection.slices[1]
        self.assertTrue(isinstance(s2, slice))
        self.assertEqual(s2, slice(30, 38, 5))

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
