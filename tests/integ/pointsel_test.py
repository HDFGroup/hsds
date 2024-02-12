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
import base64
import unittest
import json
import numpy as np
import helper
import config
from hsds.util.arrayUtil import arrayToBytes


class PointSelTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(PointSelTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()

    def setUp(self):
        self.session = helper.getSession()

    def tearDown(self):
        if self.session:
            self.session.close()

    def testPost1DDataset(self):

        # Test selecting points in a dataset using POST value
        print("testPost1DDataset", self.base_domain)

        points = [
            2,
            3,
            5,
            7,
            11,
            13,
            17,
            19,
            23,
            29,
            31,
            37,
            41,
            43,
            47,
            53,
            59,
            61,
            67,
            71,
            73,
            79,
            83,
            97,
            98,
        ]

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        # pass in layout specification so that we can test selection across chunk boundries
        data = {"type": "H5T_STD_I32LE", "shape": (100,)}
        data["creationProperties"] = {
            "layout": {
                "class": "H5D_CHUNKED",
                "dims": [
                    20,
                ],
            }
        }

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # try reading points from uninitialized chunks
        body = {"points": points}
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        ret_value = rspJson["value"]
        self.assertEqual(len(ret_value), len(points))
        expected_result = [0, ] * len(points)
        self.assertEqual(ret_value, expected_result)

        # write to the dset
        data = list(range(100))
        data.reverse()  # 99, 98, ..., 0

        payload = {"value": data}
        req = self.endpoint + "/datasets/" + dset_id + "/value"

        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)

        body = {"points": points}
        # read selected points
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        ret_value = rspJson["value"]
        self.assertEqual(len(ret_value), len(points))
        expected_result = [
            97,
            96,
            94,
            92,
            88,
            86,
            82,
            80,
            76,
            70,
            68,
            62,
            58,
            56,
            52,
            46,
            40,
            38,
            32,
            28,
            26,
            20,
            16,
            2,
            1,
        ]
        self.assertEqual(ret_value, expected_result)

    def testPost2DDataset(self):
        # Test POST value with selection for 2d dataset
        print("testPost2DDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)

        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        # pass in layout specification so that we can test selection across chunk boundries
        data = {"type": "H5T_STD_I32LE", "shape": [20, 30]}
        data["creationProperties"] = {
            "layout": {"class": "H5D_CHUNKED", "dims": [10, 10]}
        }

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset2d'
        name = "dset2d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # make up some data
        arr2d = []
        for i in range(20):
            row = []
            for j in range(30):
                row.append(i * 10000 + j)
            arr2d.append(row)

        # write some values
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        payload = {"value": arr2d}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # do a point select
        points = []
        for i in range(3):
            for j in range(5):
                pt = [i * 5 + 5, j * 5 + 5]
                points.append(pt)
        body = {"points": points}
        # read a selected points
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        expected_result = [
            50005,
            50010,
            50015,
            50020,
            50025,
            100005,
            100010,
            100015,
            100020,
            100025,
            150005,
            150010,
            150015,
            150020,
            150025,
        ]
        self.assertTrue("value" in rspJson)
        values = rspJson["value"]
        self.assertEqual(values, expected_result)

    def testPost1DDatasetBinary(self):

        # Test selecting points in a dataset using POST value
        print("testPost1DDatasetBinary", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"
        headers_bin_reqrsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_reqrsp["accept"] = "application/octet-stream"
        headers_bin_reqrsp["Content-Type"] = "application/octet-stream"
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        # pass in layout specification so that we can test selection across chunk boundries
        data = {"type": "H5T_STD_I32LE", "shape": (100,)}
        data["creationProperties"] = {
            "layout": {
                "class": "H5D_CHUNKED",
                "dims": [
                    20,
                ],
            }
        }

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        arr = np.zeros((100,), dtype="i4")
        for i in range(100):
            arr[i] = 99 - i
        # write to the dset
        data = arr.tobytes()

        req = self.endpoint + "/datasets/" + dset_id + "/value"

        rsp = self.session.put(req, data=data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        points = [
            2,
            3,
            5,
            7,
            11,
            13,
            17,
            19,
            23,
            29,
            31,
            37,
            41,
            43,
            47,
            53,
            59,
            61,
            67,
            71,
            73,
            79,
            83,
            97,
            98,
        ]
        num_points = len(points)
        arr_points = np.asarray(points, dtype="u8")  # must use unsigned 64-bit int
        data = arr_points.tobytes()

        # read selected points
        rsp = self.session.post(req, data=data, headers=headers_bin_reqrsp)
        self.assertEqual(rsp.status_code, 200)
        rsp_data = rsp.content
        self.assertEqual(len(rsp_data), num_points * 4)
        arr_rsp = np.frombuffer(rsp_data, dtype="i4")
        rsp_values = arr_rsp.tolist()
        expected_result = [
            97,
            96,
            94,
            92,
            88,
            86,
            82,
            80,
            76,
            70,
            68,
            62,
            58,
            56,
            52,
            46,
            40,
            38,
            32,
            28,
            26,
            20,
            16,
            2,
            1,
        ]
        self.assertEqual(rsp_values, expected_result)

    def testPost2DDatasetBinary(self):
        # Test POST value with selection for 2d dataset
        print("testPost2DDatasetBinary", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"
        headers_bin_reqrsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_reqrsp["accept"] = "application/octet-stream"
        headers_bin_reqrsp["Content-Type"] = "application/octet-stream"

        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        # pass in layout specification so that we can test selection across chunk boundries
        data = {"type": "H5T_STD_I32LE", "shape": [20, 30]}
        data["creationProperties"] = {
            "layout": {"class": "H5D_CHUNKED", "dims": [10, 10]}
        }

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset2d'
        name = "dset2d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        arr = np.zeros((20, 30), dtype="i4")
        for i in range(20):
            for j in range(30):
                arr[i, j] = i * 10000 + j
        arr_bytes = arr.tobytes()

        # write some values
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.put(req, data=arr_bytes, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # do a point select
        points = []
        for i in range(3):
            for j in range(5):
                pt = [i * 5 + 5, j * 5 + 5]
                points.append(pt)
        num_points = len(points)
        arr_points = np.asarray(points, dtype="u8")  # must use unsigned 64-bit int
        pt_bytes = arr_points.tobytes()
        print(arr_points)
        print(type(arr_points))
        print(arr_points.shape)
        self.assertTrue(False)

        # read selected points
        rsp = self.session.post(req, data=pt_bytes, headers=headers_bin_reqrsp)
        self.assertEqual(rsp.status_code, 200)
        rsp_data = rsp.content
        self.assertEqual(len(rsp_data), num_points * 4)
        arr_rsp = np.frombuffer(rsp_data, dtype="i4")
        values = arr_rsp.tolist()

        expected_result = [
            50005,
            50010,
            50015,
            50020,
            50025,
            100005,
            100010,
            100015,
            100020,
            100025,
            150005,
            150010,
            150015,
            150020,
            150025,
        ]

        self.assertEqual(values, expected_result)

    def testPostContiguousDataset(self):
        print("testPostContiguousDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")
        if not hdf5_sample_bucket:
            print(
                "hdf5_sample_bucket config not set, skipping testPostContiguousDataset"
            )
            return

        tall_json = helper.getHDF5JSON("tall.json")
        if not tall_json:
            print("tall.json file not found, skipping testPostContiguousDataset")
            return

        if "tall.h5" not in tall_json:
            self.assertTrue(False)

        chunk_info = tall_json["tall.h5"]
        if "/g1/g1.1/dset1.1.2" not in chunk_info:
            self.assertTrue(False)

        dset112_info = chunk_info["/g1/g1.1/dset1.1.2"]
        if "byteStreams" not in dset112_info:
            self.assertTrue(False)
        byteStreams = dset112_info["byteStreams"]

        # should be just one element for this contiguous dataset
        self.assertTrue(len(byteStreams), 1)
        byteStream = byteStreams[0]
        dset112_offset = byteStream["file_offset"]
        dset112_size = byteStream["size"]
        self.assertEqual(dset112_size, 80)

        if "/g2/dset2.2" not in chunk_info:
            self.assertTrue(False)
        dset22_info = chunk_info["/g2/dset2.2"]
        if "byteStreams" not in dset22_info:
            self.assertTrue(False)
        byteStreams = dset22_info["byteStreams"]
        self.assertTrue(len(byteStreams), 1)
        byteStream = byteStreams[0]
        dset22_offset = byteStream["file_offset"]
        dset22_size = byteStream["size"]
        self.assertEqual(dset22_size, 60)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create dataset fodr /g1/g1.1/dset1.1.2
        s3path = "s3://" + hdf5_sample_bucket + "/data/hdf5test" + "/tall.h5"
        data = {"type": "H5T_STD_I32BE", "shape": 20}
        layout = {
            "class": "H5D_CONTIGUOUS_REF",
            "file_uri": s3path,
            "offset": dset112_offset,
            "size": dset112_size,
        }
        data["creationProperties"] = {"layout": layout}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset112_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset112_id))

        # link new dataset as 'dset112'
        name = "dset112"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset112_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # create dataset for /g2/dset2.2
        data = {"type": "H5T_IEEE_F32BE", "shape": [3, 5]}
        layout = {
            "class": "H5D_CONTIGUOUS_REF",
            "file_uri": s3path,
            "offset": dset22_offset,
            "size": dset22_size,
        }
        data["creationProperties"] = {"layout": layout}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset22_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset22_id))

        # link new dataset as 'dset22'
        name = "dset22"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset22_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # do a point selection read on dset22
        req = self.endpoint + "/datasets/" + dset112_id + "/value"
        points = [2, 3, 5, 7, 11, 13, 17, 19]
        body = {"points": points}
        # add nonstrict
        params = {"nonstrict": 1}  # enable SN to invoke lambda func

        rsp = self.session.post(
            req, params=params, data=json.dumps(body), headers=headers
        )
        if rsp.status_code == 404:
            msg = f"s3object: {s3path} not found, "
            msg += "skipping point read chunk reference contiguous test"
            print(msg)

            return

        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        ret_value = rspJson["value"]
        self.assertEqual(len(ret_value), len(points))
        self.assertEqual(
            ret_value, points
        )  # get back the points since the dataset in the range 0-20

        # do a point selection read on dset22
        req = self.endpoint + "/datasets/" + dset22_id + "/value"
        points = [(0, 0), (1, 1), (2, 2)]
        body = {"points": points}
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        ret_value = rspJson["value"]
        self.assertEqual(len(ret_value), len(points))

    def testPostChunkedRefDataset(self):
        print("testPostChunkedRefDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")
        if not hdf5_sample_bucket:
            print("hdf5_sample_bucket config not set, skipping testChunkedRefDataset")
            return

        s3path = "s3://" + hdf5_sample_bucket + "/data/hdf5test" + "/snp500.h5"
        SNP500_ROWS = 3207353

        snp500_json = helper.getHDF5JSON("snp500.json")
        if not snp500_json:
            print("snp500.json file not found, skipping testPostChunkedRefDataset")
            return

        if "snp500.h5" not in snp500_json:
            self.assertTrue(False)

        chunk_dims = [
            60000,
        ]  # chunk layout used in snp500.h5 file

        chunk_info = snp500_json["snp500.h5"]
        dset_info = chunk_info["/dset"]
        if "byteStreams" not in dset_info:
            self.assertTrue(False)
        byteStreams = dset_info["byteStreams"]

        # construct map of chunks
        chunks = {}
        for item in byteStreams:
            index = item["index"]
            chunk_key = str(index)
            chunks[chunk_key] = (item["file_offset"], item["size"])

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # define types we need

        s10_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 10,
            "strPad": "H5T_STR_NULLPAD",
        }
        s4_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 4,
            "strPad": "H5T_STR_NULLPAD",
        }

        fields = (
            {"name": "date", "type": s10_type},
            {"name": "symbol", "type": s4_type},
            {"name": "sector", "type": "H5T_STD_I8LE"},
            {"name": "open", "type": "H5T_IEEE_F32LE"},
            {"name": "high", "type": "H5T_IEEE_F32LE"},
            {"name": "low", "type": "H5T_IEEE_F32LE"},
            {"name": "volume", "type": "H5T_IEEE_F32LE"},
            {"name": "close", "type": "H5T_IEEE_F32LE"},
        )

        datatype = {"class": "H5T_COMPOUND", "fields": fields}

        data = {
            "type": datatype,
            "shape": [
                SNP500_ROWS,
            ],
        }
        layout = {
            "class": "H5D_CHUNKED_REF",
            "file_uri": s3path,
            "dims": chunk_dims,
            "chunks": chunks,
        }
        data["creationProperties"] = {"layout": layout}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # do a point selection
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        points = [1234567, ]
        body = {"points": points}
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        if rsp.status_code == 404:
            msg = "s3object: {s3path} not found, skipping point chunk ref test"
            print(msg)
        else:
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            self.assertTrue("value" in rspJson)
            value = rspJson["value"]
            self.assertEqual(len(value), len(points))
            item = value[0]
            self.assertEqual(item[0], "1998.10.22")
            self.assertEqual(item[1], "MHFI")
            self.assertEqual(item[2], 3)

    def testPostChunkedRefIndirectDataset(self):
        print("testPostChunkedRefIndirectDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")
        if not hdf5_sample_bucket:
            msg = "hdf5_sample_bucket config not set, "
            msg += "skipping testPostChunkedRefIndirectDataset"
            print(msg)
            return

        s3path = "s3://" + hdf5_sample_bucket + "/data/hdf5test" + "/snp500.h5"
        SNP500_ROWS = 3207353

        snp500_json = helper.getHDF5JSON("snp500.json")
        if not snp500_json:
            print("snp500.json file not found, skipping testChunkedRefDataset")
            return

        if "snp500.h5" not in snp500_json:
            self.assertTrue(False)

        chunk_dims = [60000,]  # chunk layout used in snp500.h5 file
        num_chunks = (SNP500_ROWS // chunk_dims[0]) + 1

        chunk_info = snp500_json["snp500.h5"]
        dset_info = chunk_info["/dset"]
        if "byteStreams" not in dset_info:
            self.assertTrue(False)
        byteStreams = dset_info["byteStreams"]

        self.assertEqual(len(byteStreams), num_chunks)

        chunkinfo_data = [(0, 0)] * num_chunks

        # fill the numpy array with info from bytestreams data
        for i in range(num_chunks):
            item = byteStreams[i]
            index = item["index"]
            chunkinfo_data[index] = (item["file_offset"], item["size"])

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create table to hold chunkinfo
        # create a dataset to store chunk info
        fields = (
            {"name": "offset", "type": "H5T_STD_I64LE"},
            {"name": "size", "type": "H5T_STD_I32LE"},
        )
        chunkinfo_type = {"class": "H5T_COMPOUND", "fields": fields}
        req = self.endpoint + "/datasets"
        # Store 40 chunk locations
        chunkinfo_dims = [
            num_chunks,
        ]
        payload = {"type": chunkinfo_type, "shape": chunkinfo_dims}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        chunkinfo_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(chunkinfo_uuid))

        # link new dataset as 'chunks'
        name = "chunks"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": chunkinfo_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write to the chunkinfo dataset
        payload = {"value": chunkinfo_data}

        req = self.endpoint + "/datasets/" + chunkinfo_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # define types we need

        s10_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 10,
            "strPad": "H5T_STR_NULLPAD",
        }
        s4_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 4,
            "strPad": "H5T_STR_NULLPAD",
        }

        fields = (
            {"name": "date", "type": s10_type},
            {"name": "symbol", "type": s4_type},
            {"name": "sector", "type": "H5T_STD_I8LE"},
            {"name": "open", "type": "H5T_IEEE_F32LE"},
            {"name": "high", "type": "H5T_IEEE_F32LE"},
            {"name": "low", "type": "H5T_IEEE_F32LE"},
            {"name": "volume", "type": "H5T_IEEE_F32LE"},
            {"name": "close", "type": "H5T_IEEE_F32LE"},
        )

        datatype = {"class": "H5T_COMPOUND", "fields": fields}

        data = {
            "type": datatype,
            "shape": [
                SNP500_ROWS,
            ],
        }
        layout = {
            "class": "H5D_CHUNKED_REF_INDIRECT",
            "file_uri": s3path,
            "dims": chunk_dims,
            "chunk_table": chunkinfo_uuid,
        }
        data["creationProperties"] = {"layout": layout}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # do a point selection
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        points = [1234567, ]
        body = {"points": points}
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        if rsp.status_code == 404:
            msg = f"s3object: {s3path} not found, "
            msg += "skipping point read chunk reference indirect test"
            print(msg)
            return

        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), len(points))
        item = value[0]
        self.assertEqual(item[0], "1998.10.22")
        self.assertEqual(item[1], "MHFI")
        self.assertEqual(item[2], 3)

    def testPut1DDataset(self):
        # Test writing using point selection for a 1D dataset
        print("testPut1DDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        # pass in layout specification so that we can test selection across chunk boundries
        data = {"type": "H5T_STD_I8LE", "shape": (100,)}
        data["creationProperties"] = {
            "layout": {
                "class": "H5D_CHUNKED",
                "dims": [20, ],
            }
        }

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # Do a point selection write
        primes = [
            2,
            3,
            5,
            7,
            11,
            13,
            17,
            19,
            23,
            29,
            31,
            37,
            41,
            43,
            47,
            53,
            59,
            61,
            67,
            71,
            73,
            79,
            83,
            89,
            97,
        ]
        # write 1's at indexes that are prime
        value = [1,] * len(primes)

        # write 1's to all the prime indexes
        payload = {"points": primes, "value": value}
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        # verify the correct elements got set
        value = rspJson["value"]
        for i in range(100):
            if i in primes:
                self.assertEqual(value[i], 1)
            else:
                self.assertEqual(value[i], 0)

        # read back data as one big hyperslab selection
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        self.assertEqual(len(ret_values), 100)
        for i in range(100):
            if i in primes:
                self.assertEqual(ret_values[i], 1)
            else:
                self.assertEqual(ret_values[i], 0)

    def testPut2DDataset(self):
        # Test writing with point selection for 2d dataset
        print("testPut2DDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)

        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        # pass in layout specification so that we can test selection across chunk boundries
        data = {"type": "H5T_STD_I32LE", "shape": [20, 30]}
        data["creationProperties"] = {
            "layout": {"class": "H5D_CHUNKED", "dims": [10, 10]}
        }

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset2d'
        name = "dset2d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # make up some points
        points = []
        for i in range(20):
            points.append((i, i))
        value = [1, ] * 20

        # write 1's to all the point locations
        payload = {"points": points, "value": value}
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        # verify the correct elements got set
        value = rspJson["value"]
        # print("value:", value)
        for x in range(20):
            row = value[x]
            for y in range(30):
                if x == y:
                    self.assertEqual(row[y], 1)
                else:
                    self.assertEqual(row[y], 0)

    def testPut1DDatasetBinary(self):
        # Test writing using point selection for a 1D dataset
        print("testPut1DDatasetBinary", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        # pass in layout specification so that we can test selection across chunk boundries
        data = {"type": "H5T_STD_I8LE", "shape": (100,)}
        data["creationProperties"] = {
            "layout": {
                "class": "H5D_CHUNKED",
                "dims": [20, ],
            }
        }

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # Do a point selection write
        primes = [
            2,
            3,
            5,
            7,
            11,
            13,
            17,
            19,
            23,
            29,
            31,
            37,
            41,
            43,
            47,
            53,
            59,
            61,
            67,
            71,
            73,
            79,
            83,
            89,
            97,
        ]

        # create binary array for the values
        byte_array = bytearray(len(primes))
        for i in range(len(primes)):
            byte_array[i] = 1
        value_base64 = base64.b64encode(bytes(byte_array))
        value_base64 = value_base64.decode("ascii")

        # write 1's to all the prime indexes
        payload = {"points": primes, "value_base64": value_base64}
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        # verify the correct elements got set
        value = rspJson["value"]
        for i in range(100):
            if i in primes:
                self.assertEqual(value[i], 1)
            else:
                self.assertEqual(value[i], 0)

        # read back data as one big hyperslab selection
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        self.assertEqual(len(ret_values), 100)
        for i in range(100):
            if i in primes:
                self.assertEqual(ret_values[i], 1)
            else:
                self.assertEqual(ret_values[i], 0)

    def testPut2DDatasetBinary(self):
        # Test writing with point selection for 2d dataset with binary data
        print("testPut2DDatasetBinary", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)

        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        # pass in layout specification so that we can test selection across chunk boundries
        data = {"type": "H5T_STD_I32LE", "shape": [20, 30]}
        data["creationProperties"] = {
            "layout": {"class": "H5D_CHUNKED", "dims": [10, 10]}
        }

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset2d'
        name = "dset2d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # make up some points
        points = []
        for i in range(20):
            points.append((i, i))
        value = [
            1,
        ] * 20
        # create a byter array of 20 ints with value 1
        # create binary array for the values
        byte_array = bytearray(20 * 4)
        for i in range(20):
            byte_array[i * 4] = 1
        value_base64 = base64.b64encode(bytes(byte_array))
        value_base64 = value_base64.decode("ascii")

        # write 1's to all the point locations
        payload = {"points": points, "value_base64": value_base64}
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        # verify the correct elements got set
        value = rspJson["value"]
        # print("value:", value)
        for x in range(20):
            row = value[x]
            for y in range(30):
                if x == y:
                    self.assertEqual(row[y], 1)
                else:
                    self.assertEqual(row[y], 0)

    def testDatasetChunkPartitioning(self):
        # test Dataset partitioning logic for large datasets
        print("testDatasetChunkPartitioning:", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        # 50K x 80K x 90K dataset
        dims = [50000, 80000, 90000]
        payload = {"type": "H5T_STD_I32LE", "shape": dims}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)

        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'big_dset'
        name = "big_dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], "H5D_CHUNKED")
        self.assertTrue("dims" in layout_json)
        if config.get("max_chunks_per_folder") > 0:
            self.assertTrue("partition_count" in layout_json)
            self.assertTrue(layout_json["partition_count"] > 1)

        # make up some points
        NUM_POINTS = 20
        points = []
        value = []
        for i in range(NUM_POINTS):
            x = (dims[0] // NUM_POINTS) * i
            y = (dims[1] // NUM_POINTS) * i
            z = (dims[2] // NUM_POINTS) * i
            points.append((x, y, z))
            value.append(i)

        # write 1's to all the point locations
        payload = {"points": points, "value": value}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back data
        body = {"points": points}
        # read a selected points
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)

        self.assertTrue("value" in rspJson)
        # verify the correct elements got set
        value = rspJson["value"]
        self.assertEqual(len(value), NUM_POINTS)
        for i in range(NUM_POINTS):
            self.assertEqual(value[i], i)

    def testScalarDataset(self):
        print("testScalarDataset:", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create a dataset obj
        data = {"type": "H5T_IEEE_F32LE"}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # write to the dset
        data = [42, ]

        payload = {"value": data}
        req = self.endpoint + "/datasets/" + dset_id + "/value"

        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        points = [0, ]
        body = {"points": points}
        # read selected points
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        # point select not supported on zero-dimensional datasets
        self.assertEqual(rsp.status_code, 400)

    def testSelect1DDataset(self):
        # Test select query for 1d dataset using POST
        print("testSelect1DDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp["accept"] = "application/octet-stream"

        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        data = {"type": "H5T_STD_I32LE", "shape": 10}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # add an attribute
        attr_payload = {"type": "H5T_STD_I32LE", "value": 42}
        attr_name = "attr1"
        req = self.endpoint + "/datasets/" + dset_id + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(attr_payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # created

        # link new dataset as 'dset1d'
        name = "dset1d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write to the dset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        data = list(range(10))  # write 0-9
        payload = {"value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read coordinate selection
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        select = [
            [0, 1, 3, 7],
        ]
        body = {"select": select}
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], [0, 1, 3, 7])

        # read coordinate selection with binary response
        rsp = self.session.post(req, data=json.dumps(body), headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)

        data = rsp.content
        expect_count = len(select[0])
        self.assertEqual(len(data), expect_count * 4)
        for i in range(len(data)):
            if i % 4 != 0:
                self.assertEqual(data[i], 0)
            elif i == 0:
                self.assertEqual(data[i], 0)
            elif i == 4:
                self.assertEqual(data[i], 1)
            elif i == 8:
                self.assertEqual(data[i], 3)
            elif i == 12:
                self.assertEqual(data[i], 7)
            else:
                self.assertTrue(False)  # unexpected

        # read a selection
        body = {"select": "[2:8]"}  # read 6 elements, starting at index 2
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], list(range(2, 8)))

        # read with binary response
        rsp = self.session.post(req, data=json.dumps(body), headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        expect_count = len(select[0])
        data = rsp.content
        expect_count = 6
        self.assertEqual(len(data), expect_count * 4)
        for i in range(len(data)):
            if i % 4 != 0:
                self.assertEqual(data[i], 0)
            else:
                self.assertEqual(data[i], i // 4 + 2)

        # read one element.  cf test for PR #84
        body = {"select": "[3]"}  # read 4th element
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], [3])

        # read with binary response
        rsp = self.session.post(req, data=json.dumps(body), headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.content, b"\x03\x00\x00\x00")

        # try to read beyond the bounds of the array
        body = {"select": "[2:18]"}  # read 6 elements, starting at index 2
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 400)

    def testSelect2DDataset(self):
        """Test Select query  for 2d dataset"""
        print("testSelect2DDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp["accept"] = "application/octet-stream"
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        num_col = 8
        num_row = 4
        data = {"type": "H5T_STD_I32LE", "shape": [num_row, num_col]}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset2d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write to the dset
        json_data = []
        for i in range(num_row):
            row = []
            for j in range(num_col):
                row.append(i * 10 + j)
            json_data.append(row)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        payload = {"value": json_data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read a selection
        body = {"select": "[3:4,2:8]"}  # read 3 elements,
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(
            rspJson["value"],
            [
                json_data[3][2:8],
            ],
        )

        # read selection with binary response
        rsp = self.session.post(req, data=json.dumps(body), headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), 6 * 4)
        for i in range(6):
            r = i * 4
            s = (i + 1) * 4
            n = int.from_bytes(data[r:s], "little")
            self.assertEqual(n, i + 32)

        # read a coordinate selection
        body = {"select": "[3:4,[0,2,5]]"}  # read 3 elements
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], [[30, 32, 35],], )

        # read a coordinate selection with binary response
        rsp = self.session.post(req, data=json.dumps(body), headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), 3 * 4)
        self.assertEqual(data, b"\x1e\x00\x00\x00 \x00\x00\x00#\x00\x00\x00")

    def testPostCompoundDataset(self):

        # Test selecting points in a compound dataset using POST value
        print("testPostCompoundDataset", self.base_domain)

        points = [
            2,
            3,
            5,
            7,
            11,
            13,
            17,
            19,
            23,
            29,
            31,
            37,
            41,
            43,
            47,
            53,
            59,
            61,
            67,
            71,
            73,
            79,
            83,
            97,
            98,
        ]

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        # pass in layout specification so that we can test selection across chunk boundries
        data = {"type": "H5T_STD_I32LE", "shape": (100,)}
        #
        # create 1d dataset
        #

        field_names = ("x1", "x2", "x3", "x4", "x5")

        fields = []
        for field_name in field_names:
            field = {"name": field_name, "type": "H5T_STD_I32LE"}
            fields.append(field)

        datatype = {"class": "H5T_COMPOUND", "fields": fields}

        num_elements = 100
        payload = {"type": datatype, "shape": num_elements}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset_compound'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # try reading points from uninitialized chunks
        body = {"points": points}
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        ret_value = rspJson["value"]
        self.assertEqual(len(ret_value), len(points))
        for i in range(len(points)):
            self.assertEqual(ret_value[i], [0, 0, 0, 0, 0])

        # write to the dset by field
        for field in field_names:
            x = int(field[1])  # get the number part of the field name
            data = [(x * i) for i in range(num_elements)]

            payload = {"value": data, "fields": field}
            req = self.endpoint + "/datasets/" + dset_id + "/value"

            rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 200)

        # read back selected points by field
        for field in field_names:
            x = int(field[1])
            body = {"points": points, "fields": field}
            rsp = self.session.post(req, data=json.dumps(body), headers=headers)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            self.assertTrue("value" in rspJson)
            ret_value = rspJson["value"]
            self.assertEqual(len(ret_value), len(points))
            for i in range(len(points)):
                self.assertEqual(ret_value[i], [x * points[i]])

        # Write "100" to first field and "200" to second field through body
        data = [(100, 200) for i in range(num_elements)]
        payload = {"value": data, "fields": field_names[0] + ":" + field_names[1]}
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back entire dataset and check values
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        ret_value = np.array(rspJson["value"], dtype=int)
        self.assertTrue(np.array_equal(ret_value[:, 0],
                                       np.full(shape=num_elements, fill_value=100, dtype=int)))
        self.assertTrue(np.array_equal(ret_value[:, 1],
                                       np.full(shape=num_elements, fill_value=200, dtype=int)))
        for i in range(2, 5):
            self.assertTrue(np.array_equal(ret_value[:, i], [(i + 1) * j for j in range(100)]))

        # Write 300 to third field and 400 to fourth field through URL
        data = [(300, 400) for i in range(num_elements)]
        payload = {"value": data}
        req = self.endpoint + "/datasets/" + dset_id + \
            "/value?fields=" + field_names[2] + ":" + field_names[3]
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back entire dataset and check values
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        ret_value = np.array(rspJson["value"], dtype=int)
        for i in range(1, 4):
            expected = np.full(shape=num_elements, fill_value=((i + 1) * 100), dtype=int)
            self.assertTrue(np.array_equal(ret_value[:, i], expected))
        self.assertTrue(np.array_equal(ret_value[:, 4], [5 * j for j in range(100)]))

        # Test non-adjacent fields
        # Write 1000 to first field and 500 to fifth field through body
        data = [(1000, 500) for i in range(num_elements)]
        payload = {"value": data, "fields": field_names[0] + ":" + field_names[4]}
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back entire dataset and check values
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        ret_value = np.array(rspJson["value"], dtype=int)
        self.assertTrue(np.array_equal(ret_value[:, 0],
                                       np.full(shape=num_elements, fill_value=1000, dtype=int)))
        for i in range(2, 5):
            self.assertTrue(np.array_equal(ret_value[:, i], [(i + 1) * 100 for j in range(100)]))

        # try to write to first field through binary request
        arr = np.array([(10000,) for i in range(num_elements)], dtype=np.int32)
        data = arrayToBytes(arr)
        req = self.endpoint + "/datasets/" + dset_id + "/value?fields=" + field_names[0]
        headers["Content-Type"] = "application/octet-stream"
        rsp = self.session.put(req, data=data, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back entire dataset and check values
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        headers["Content-Type"] = "application/json"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        ret_value = np.array(rspJson["value"], dtype=int)
        self.assertTrue(np.array_equal(ret_value[:, 0],
                                       np.full(shape=num_elements, fill_value=10000, dtype=int)))
        for i in range(2, 5):
            self.assertTrue(np.array_equal(ret_value[:, i], [(i + 1) * 100 for j in range(100)]))


if __name__ == "__main__":
    # setup test files

    unittest.main()
