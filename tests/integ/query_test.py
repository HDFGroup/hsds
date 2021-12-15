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
import config


class QueryTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(QueryTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()

    def setUp(self):
        self.session = helper.getSession()

    def tearDown(self):
        if self.session:
            self.session.close()

        # main

    def testSimpleQuery(self):
        # Test query value for 1d dataset
        print("testSimpleQuery", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        #
        #create 1d dataset
        #
        fixed_str4_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 4,
                "strPad": "H5T_STR_NULLPAD" }
        fixed_str8_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 8,
                "strPad": "H5T_STR_NULLPAD" }
        fields = (  {'name': 'stock_symbol', 'type': fixed_str4_type},
                    {'name': 'date', 'type': fixed_str8_type},
                    {'name': 'open', 'type': 'H5T_STD_I32LE'},
                    {'name': 'close', 'type': 'H5T_STD_I32LE'} )
        datatype = {'class': 'H5T_COMPOUND', 'fields': fields }

        num_elements = 12
        payload = {'type': datatype, 'shape': num_elements}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset

        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset1'
        name = 'dset'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write entire array
        value = [
            ("EBAY", "20170102", 3023, 3088),
            ("AAPL", "20170102", 3054, 2933),
            ("AMZN", "20170102", 2973, 3011),
            ("EBAY", "20170103", 3042, 3128),
            ("AAPL", "20170103", 3182, 3034),
            ("AMZN", "20170103", 3021, 2788),
            ("EBAY", "20170104", 2798, 2876),
            ("AAPL", "20170104", 2834, 2867),
            ("AMZN", "20170104", 2891, 2978),
            ("EBAY", "20170105", 2973, 2962),
            ("AAPL", "20170105", 2934, 3010),
            ("AMZN", "20170105", 3018, 3086)
        ]

        payload = {'value': value}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # read first row with AAPL
        params = {'query': "stock_symbol == b'AAPL'" }
        params["Limit"] = 1
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        #self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue("index" in rspJson)
        readData = rspJson["value"]
        self.assertEqual(len(readData), 1)
        item = readData[0]
        self.assertEqual(item, ["AAPL", "20170102", 3054, 2933])
        self.assertEqual(item[0], "AAPL")
        indices = rspJson["index"]
        self.assertEqual(indices, [1])
        del params["Limit"]

        # get back rows for AAPL
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue("index" in rspJson)
        readData = rspJson["value"]
        self.assertEqual(len(readData), 4)
        for item in readData:
            self.assertEqual(item[0], "AAPL")
        indices = rspJson["index"]
        self.assertEqual(indices, [1,4,7,10])

        # combine with a selection
        params["select"] = "[2:12]"
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        #self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue("index" in rspJson)
        readData = rspJson["value"]
        self.assertEqual(len(readData), 3)
        for item in readData:
            self.assertEqual(item[0], "AAPL")
        indices = rspJson["index"]
        self.assertEqual(indices, [4,7,10])

        # combine with Limit
        params["Limit"] = 2
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        #self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue("index" in rspJson)
        readData = rspJson["value"]
        self.assertEqual(len(readData), 2)
        for item in readData:
            self.assertEqual(item[0], "AAPL")
        indices = rspJson["index"]
        self.assertEqual(indices, [4,7])

        # try bad Limit
        params["Limit"] = "abc"
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        # try invalid query string
        params = {'query': "foobar" }
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        # try boolean query
        params = {'query': "(open > 3000) & (open < 3100)" }
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        #self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue("index" in rspJson)
        readData = rspJson["value"]
        self.assertEqual(len(readData), 5)
        for item in readData:
            self.assertTrue(item[2] > 3000)
            self.assertTrue(item[2] < 3100)
        indices = rspJson["index"]
        self.assertEqual(indices, [0, 1, 3, 5, 11])


    def testChunkedRefIndirectDataset(self):
        print("testChunkedRefIndirectDatasetQuery", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")
        if not hdf5_sample_bucket:
            print("hdf5_sample_bucket config not set, skipping testChunkedRefIndirectDataset")
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

        chunkinfo_data = [(0,0)]*num_chunks

        # fill the numpy array with info from bytestreams data
        for i in range(num_chunks):
            item = byteStreams[i]
            index = item["index"]
            chunkinfo_data[index] = (item["file_offset"], item["size"])

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create table to hold chunkinfo
        # create a dataset to store chunk info
        fields = ({'name': 'offset', 'type': 'H5T_STD_I64LE'},
                  {'name': 'size', 'type': 'H5T_STD_I32LE'})
        chunkinfo_type = {'class': 'H5T_COMPOUND', 'fields': fields }
        req = self.endpoint + "/datasets"
        # Store 40 chunk locations
        chunkinfo_dims = [num_chunks,]
        payload = {'type': chunkinfo_type, 'shape': chunkinfo_dims }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        chunkinfo_uuid = rspJson['id']
        self.assertTrue(helper.validateId(chunkinfo_uuid))

        # link new dataset as 'chunks'
        name = "chunks"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": chunkinfo_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write to the chunkinfo dataset
        payload = {'value': chunkinfo_data}

        req = self.endpoint + "/datasets/" + chunkinfo_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value


        # define types we need

        s10_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 10,
                "strPad": "H5T_STR_NULLPAD" }
        s4_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 4,
                "strPad": "H5T_STR_NULLPAD" }

        fields = ({'name': 'date', 'type': s10_type},
                  {'name': 'stock_symbol', 'type': s4_type},
                  {'name': 'sector', 'type': 'H5T_STD_I8LE'},
                  {'name': 'open', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'high', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'low', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'volume', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'close', 'type': 'H5T_IEEE_F32LE'})


        datatype = {'class': 'H5T_COMPOUND', 'fields': fields }

        data = { "type": datatype, "shape": [SNP500_ROWS,] }
        layout = {"class": 'H5D_CHUNKED_REF_INDIRECT', "file_uri": s3path, "dims": chunk_dims, "chunk_table": chunkinfo_uuid}
        data['creationProperties'] = {'layout': layout}

        req = self.endpoint + '/datasets'
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

        # read a selection
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        params = {'query': "stock_symbol == b'AAPL'" } # query for AAPL
        #params = {'query': "stock_symbol == b'CVX'" } # query for CVX
        #params["select"] = "[0:100]"
        params["nonstrict"] = 1 # enable SN to invoke lambda func
        rsp = self.session.get(req, params=params, headers=headers)

        if rsp.status_code == 404:
            print("s3object: {} not found, skipping hyperslab read chunk reference indirect test".format(s3path))
            return

        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        #self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue("index" in rspJson)
        readData = rspJson["value"]
        self.assertEqual(len(readData), 8813)
        item = readData[0]
        self.assertEqual(item[0], "1980.12.12")

        self.assertEqual(item[1], "AAPL")
        indices = rspJson["index"]
        self.assertEqual(len(indices), 8813)
        self.assertEqual(indices[0], 128912)

         

    def testPutQuery(self):
        # Test PUT query for 1d dataset
        print("testPutQuery", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        #
        #create 1d dataset
        #
        fixed_str4_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 4,
                "strPad": "H5T_STR_NULLPAD" }
        fixed_str8_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 8,
                "strPad": "H5T_STR_NULLPAD" }
        fields = (  {'name': 'stock_symbol', 'type': fixed_str4_type},
                    {'name': 'date', 'type': fixed_str8_type},
                    {'name': 'open', 'type': 'H5T_STD_I32LE'},
                    {'name': 'close', 'type': 'H5T_STD_I32LE'} )
        datatype = {'class': 'H5T_COMPOUND', 'fields': fields }

        num_elements = 12
        payload = {'type': datatype, 'shape': num_elements}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset

        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset1'
        name = 'dset'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)


        # write entire array
        value = [
            ("EBAY", "20170102", 3023, 3088),
            ("AAPL", "20170102", 3054, 2933),
            ("AMZN", "20170102", 2973, 3011),
            ("EBAY", "20170103", 3042, 3128),
            ("AAPL", "20170103", 3182, 3034),
            ("AMZN", "20170103", 3021, 2788),
            ("EBAY", "20170104", 2798, 2876),
            ("AAPL", "20170104", 2834, 2867),
            ("AMZN", "20170104", 2891, 2978),
            ("EBAY", "20170105", 2973, 2962),
            ("AAPL", "20170105", 2934, 3010),
            ("AMZN", "20170105", 3018, 3086)
        ]

        payload = {'value': value}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # set any rows with AAPL to have open of 999
        params = {'query': "stock_symbol == b'AAPL'" }
        update_value = {"open": 999}
        payload = {'value': update_value}
        rsp = self.session.put(req, params=params, data=json.dumps(update_value), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue("index" in rspJson)
        readData = rspJson["value"]
        self.assertEqual(len(readData), 4)
        for item in readData:
            self.assertEqual(item[0], "AAPL")
        indices = rspJson["index"]
        self.assertEqual(indices, [1,4,7,10])

        # read values and verify the expected changes where made
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        read_values = rspJson["value"]
        self.assertEqual(len(read_values), len(value))
        for i in range(len(value)):
            orig_item = value[i]
            mod_item = read_values[i]
            self.assertEqual(orig_item[0], mod_item[0])
            self.assertEqual(orig_item[1], mod_item[1])
            self.assertEqual(orig_item[3], mod_item[3])

            if orig_item[0] == "AAPL":
                self.assertEqual(mod_item[2], 999)
            else:
                self.assertEqual(orig_item[2], mod_item[2])

        # re-write values
        payload = {'value': value}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # set just one row with AAPL to have open of 42
        params = {'query': "stock_symbol == b'AAPL'" }
        params["Limit"] = 1
        update_value = {"open": 999}
        payload = {'value': update_value}
        rsp = self.session.put(req, params=params, data=json.dumps(update_value), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue("index" in rspJson)
        readData = rspJson["value"]
        self.assertEqual(len(readData), 1)
        for item in readData:
            self.assertEqual(item[0], "AAPL")
        indices = rspJson["index"]
        self.assertEqual(indices, [1])

        # read values and verify the expected changes where made
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        read_values = rspJson["value"]
        self.assertEqual(len(read_values), len(value))
        for i in range(len(value)):
            orig_item = value[i]
            mod_item = read_values[i]
            self.assertEqual(orig_item[0], mod_item[0])
            self.assertEqual(orig_item[1], mod_item[1])
            self.assertEqual(orig_item[3], mod_item[3])

            if orig_item[0] == "AAPL" and i == 1:
                self.assertEqual(mod_item[2], 999)
            else:
                self.assertEqual(orig_item[2], mod_item[2])






if __name__ == '__main__':
    #setup test files

    unittest.main()
