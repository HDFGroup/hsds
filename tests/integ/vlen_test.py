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
import json
import helper
import numpy as np
import sys

from h5json.hdf5dtype import createDataType
from h5json.array_util import arrayToBytes, bytesToArray


class VlenTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(VlenTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()

    def setUp(self):
        self.session = helper.getSession()

    def tearDown(self):
        if self.session:
            self.session.close()

        # main

    def testPutVLenInt(self):
        # Test PUT value for 1d attribute with variable length int types
        print("testPutVLenInt", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        vlen_type = {
            "class": "H5T_VLEN",
            "base": {"class": "H5T_INTEGER", "base": "H5T_STD_I32LE"},
        }
        payload = {
            "type": vlen_type,
            "shape": [4, ],
        }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset" + helper.getRandomName()
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write values to dataset
        data = [
            [1, ],
            [1, 2],
            [1, 2, 3],
            [1, 2, 3, 4],
        ]
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        payload = {"value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read values from dataset
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 4)

        for i in range(4):
            self.assertEqual(value[i], data[i])

        # read back a selection
        params = {"select": "[2:3]"}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 1)
        self.assertEqual(value[0], data[2])

        # read back a point selection
        points = [1, 3]
        body = {"points": points}
        # read selected points
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 2)
        self.assertEqual(value[0], [1, 2])
        self.assertEqual(value[1], [1, 2, 3, 4])

    def testPutVLenIntBinary(self):
        # Test PUT value for 1d attribute with variable length int types using binary transfer
        print("testPutVLenIntBinary", self.base_domain)

        count = 4
        test_values = []
        for i in range(count):
            e = [1,]
            for j in range(0, i):
                e.append(j + 2)
            test_values.append(e)
        # test_values == [[1], [1,2]]

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"
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
        vlen_type = {
            "class": "H5T_VLEN",
            "base": {"class": "H5T_INTEGER", "base": "H5T_STD_I32LE"},
        }
        payload = {
            "type": vlen_type,
            "shape": [count, ],
        }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset" + helper.getRandomName()
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # create numpy vlen array
        dt = np.dtype("O", metadata={"vlen": np.dtype("int32")})
        arr = np.zeros((count,), dtype=dt)
        for i in range(count):
            arr[i] = np.int32(test_values[i])

        # write as binary data
        data = arrayToBytes(arr)
        self.assertEqual(len(data), 56)
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read values from dataset with json
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), count)

        for i in range(count):
            self.assertEqual(value[i], test_values[i])

        # read as binary
        rsp = self.session.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["Content-Type"], "application/octet-stream")
        data = rsp.content
        self.assertEqual(len(data), 56)
        arr = bytesToArray(data, dt, [count, ])
        for i in range(count):
            self.assertEqual(value[i], test_values[i])

        # read back a selection
        params = {"select": "[2:3]"}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 1)
        self.assertEqual(value[0], [1, 2, 3])

    def testPutVLen2DInt(self):
        # Test PUT value for 1d attribute with variable length int types
        print("testPutVLen2DInt", self.base_domain)
        nrow = 2
        ncol = 2

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        vlen_type = {
            "class": "H5T_VLEN",
            "base": {"class": "H5T_INTEGER", "base": "H5T_STD_I32LE"},
        }
        payload = {"type": vlen_type, "shape": [nrow, ncol]}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset" + helper.getRandomName()
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write values to dataset
        data = []
        for i in range(nrow):
            row = []
            for j in range(ncol):
                start = i + j
                end = start + j + 1
                row.append(list(range(start, end)))
            data.append(row)

        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        payload = {"value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read values from dataset
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), nrow)

        for i in range(nrow):
            for j in range(ncol):
                self.assertEqual(value[i][j], data[i][j])

        # read values from dataset using selection
        params = {"select": "[0:1,0:2]"}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 1)
        self.assertEqual(len(value[0]), 2)
        self.assertEqual(value[0][0], [0])
        self.assertEqual(value[0][1], [1, 2])

    def testPutVLenString(self):
        # Test PUT value for 1d attribute with variable length string types
        print("testPutVLenString", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        vlen_type = {
            "class": "H5T_STRING",
            "charSet": "H5T_CSET_ASCII",
            "strPad": "H5T_STR_NULLTERM",
            "length": "H5T_VARIABLE",
        }
        payload = {
            "type": vlen_type,
            "shape": [4, ],
        }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset" + helper.getRandomName()
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write values to dataset
        data = ["This is", "a variable length", "string", "array"]
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        payload = {"value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read values from dataset
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 4)
        for i in range(4):
            self.assertEqual(value[i], data[i])

        # read a point selection
        points = [1, 3]
        body = {"points": points}
        # read selected points
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 2)
        self.assertEqual(value[0], data[1])
        self.assertEqual(value[1], data[3])

    def testPutVLenStringBinary(self):
        # Test PUT value for 1d attribute with variable length string types
        print("testPutVLenStringBinary", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"
        headers_bin_rsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp["accept"] = "application/octet-stream"
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
        vlen_type = {
            "class": "H5T_STRING",
            "charSet": "H5T_CSET_ASCII",
            "strPad": "H5T_STR_NULLTERM",
            "length": "H5T_VARIABLE",
        }
        payload = {
            "type": vlen_type,
            "shape": [4, ],
        }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset" + helper.getRandomName()
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write values to dataset
        data = ["This is", "a variable length", "string", "array"]

        dt_str = createDataType(vlen_type)

        # create numpy vlen array

        arr = np.zeros((4,), dtype=dt_str)
        for i in range(4):
            arr[i] = data[i]

        # write as binary data
        bin_data = arrayToBytes(arr)

        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=bin_data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read as binary
        rsp = self.session.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["Content-Type"], "application/octet-stream")
        bin_data = rsp.content
        arr = bytesToArray(bin_data, dt_str, [4,])
        for i in range(4):
            self.assertEqual(arr[i].decode(), data[i])

        # prepare a binary list of points to send
        points = [1, 3]
        arr_points = np.asarray(points, dtype="u8")  # must use unsigned 64-bit int
        req_data = arr_points.tobytes()

        # read selected points with binary request, binary response
        rsp = self.session.post(req, data=req_data, headers=headers_bin_reqrsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["Content-Type"], "application/octet-stream")
        bin_data = rsp.content
        arr = bytesToArray(bin_data, dt_str, [2,])

        self.assertEqual(arr.shape[0], 2)
        self.assertEqual(arr[0].decode(), data[1])
        self.assertEqual(arr[1].decode(), data[3])

    def testPutVLenCompound(self):
        # Test PUT value for 1d attribute with variable length int types
        print("testPutVLenCompound", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)

        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        count = 4

        # create dataset
        fixed_str8_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 8,
            "strPad": "H5T_STR_NULLPAD",
        }
        fields = [
            {
                "type": {"class": "H5T_INTEGER", "base": "H5T_STD_U64BE"},
                "name": "VALUE1",
            },
            {"type": fixed_str8_type, "name": "VALUE2"},
            {
                "type": {
                    "class": "H5T_ARRAY",
                    "dims": [2],
                    "base": {
                        "class": "H5T_STRING",
                        "charSet": "H5T_CSET_ASCII",
                        "strPad": "H5T_STR_NULLTERM",
                        "length": "H5T_VARIABLE",
                    },
                },
                "name": "VALUE3",
            },
        ]

        datatype = {"class": "H5T_COMPOUND", "fields": fields}
        payload = {
            "type": datatype,
            "shape": [
                count,
            ],
        }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset" + helper.getRandomName()
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write values to dataset
        data = []
        for i in range(count):
            s = ""
            for j in range(i + 5):
                offset = (i + j) % 256
                s += chr(ord("A") + offset)
            e = [i + 1, s, ["Hi! " * (i + 1), "Bye!" * (i + 1)]]
            data.append(e)
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        payload = {"value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read values from dataset
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), count)

        # read a point selection
        points = [1, 3]
        body = {"points": points}
        # read selected points
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 2)
        self.assertEqual(value[0], data[1])
        self.assertEqual(value[1], data[3])

    def testPutVLenCompoundBinary(self):
        # Test PUT value for 1d attribute with variable length int types
        print("testPutVLenCompoundBinary", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"
        headers_bin_rsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp["accept"] = "application/octet-stream"
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

        count = 4

        # create dataset
        fixed_str8_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 8,
            "strPad": "H5T_STR_NULLPAD",
        }
        fields = [
            {
                "type": {"class": "H5T_INTEGER", "base": "H5T_STD_U64BE"},
                "name": "VALUE1",
            },
            {"type": fixed_str8_type, "name": "VALUE2"},
            {
                "type": {
                    "class": "H5T_ARRAY",
                    "dims": [2],
                    "base": {
                        "class": "H5T_STRING",
                        "charSet": "H5T_CSET_ASCII",
                        "strPad": "H5T_STR_NULLTERM",
                        "length": "H5T_VARIABLE",
                    },
                },
                "name": "VALUE3",
            },
        ]

        datatype = {"class": "H5T_COMPOUND", "fields": fields}
        payload = {
            "type": datatype,
            "shape": [count, ],
        }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset" + helper.getRandomName()
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        dt_compound = createDataType(datatype)

        # create numpy vlen array

        arr = np.zeros((count,), dtype=dt_compound)
        for i in range(count):
            e = arr[i]
            e["VALUE1"] = i + 1
            s = ""
            for j in range(i + 5):
                offset = (i + j) % 26
                s += chr(ord("A") + offset)
            e["VALUE2"] = s
            e["VALUE3"] = ["Hi! " * (i + 1), "Bye!" * (i + 1)]

        # write as binary data
        data = arrayToBytes(arr)
        self.assertEqual(len(data), 192)  # will vary based on count
        arr_copy = bytesToArray(data, dt_compound, (count,))
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read values from dataset as json
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), count)

        # read as binary
        rsp = self.session.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["Content-Type"], "application/octet-stream")
        data = rsp.content
        self.assertEqual(len(data), 192)
        arr_rsp = bytesToArray(data, dt_compound, [count,])
        for i in range(count):
            req_row = arr[i]
            rsp_row = arr_rsp[i]
            self.assertEqual(rsp_row["VALUE1"], req_row["VALUE1"])
            self.assertEqual(rsp_row["VALUE2"], req_row["VALUE2"])
            req_value3 = req_row["VALUE3"]
            rsp_value3 = rsp_row["VALUE3"]
            self.assertEqual(len(req_value3), len(rsp_value3))
            for j in range(len(req_value3)):
                req_item = req_value3[j]
                rsp_item = rsp_value3[j]
                # strings are showing up as bytes in the response
                self.assertEqual(req_item, rsp_item.decode())

        # prepare a binary list of points to send
        points = [1, 3]
        arr_points = np.asarray(points, dtype="u8")  # must use unsigned 64-bit int
        req_data = arr_points.tobytes()

        # read selected points with binary request, binary response
        rsp = self.session.post(req, data=req_data, headers=headers_bin_reqrsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["Content-Type"], "application/octet-stream")
        bin_data = rsp.content
        arr_rsp = bytesToArray(bin_data, dt_compound, [2,])
        self.assertEqual(arr_rsp.shape[0], 2)

        for i in range(2):
            if i == 0:
                req_row = arr[1]
            else:
                req_row = arr[3]
            rsp_row = arr_rsp[i]
            self.assertEqual(rsp_row["VALUE1"], req_row["VALUE1"])
            self.assertEqual(rsp_row["VALUE2"], req_row["VALUE2"])
            req_value3 = req_row["VALUE3"]
            rsp_value3 = rsp_row["VALUE3"]
            self.assertEqual(len(req_value3), len(rsp_value3))
            for j in range(len(req_value3)):
                req_item = req_value3[j]
                rsp_item = rsp_value3[j]
                # strings are showing up as bytes in the response
                self.assertEqual(req_item, rsp_item.decode())

    def testPutVlenVlenError(self):
        # Test PUT value for 1d dataset with vlen seq of vlen utf-8 strings
        # HSDS does not currently support this datatype, but previous versions
        # of HSDS crashed when a request contained them. This test is to
        # ensure that HSDS still responds successfully with an error.
        print("testPutVlenVlenError", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        vlen_utf8_type = {
            "charSet": "H5T_CSET_UTF8",
            "class": "H5T_STRING",
            "length": "H5T_VARIABLE",
            "strPad": "H5T_STR_NULLPAD",
        }

        datatype = {"class": "H5T_VLEN", "base": vlen_utf8_type}

        payload = {
            "type": datatype,
            "shape": "H5S_SCALAR",
        }

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset" + helper.getRandomName()
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        data = u"one: \u4e00"
        payload = {"value": data}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertTrue(rsp.status_code >= 400)

        # Check that HSDS still responds to requests by getting the root
        req = self.endpoint + "/"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)


if __name__ == "__main__":
    # setup test files

    unittest.main()
