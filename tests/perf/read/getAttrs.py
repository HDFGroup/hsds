import h5pyd
import time
import requests
import base64
import json
import sys
import os
import numpy as np

"""Time attribute creation and retrieval with multi vs singular API

Sample result with a local HSDS instance and python 3.12:

Time to create 1000 attributes with multi API: 0.0982 seconds
Time to retrieve 1000 attributes with multi API: 0.0535 seconds
Time to create 1000 attributes individually: 1.9302 seconds
Time to retrieve 1000 attributes individually: 1.7448 seconds
"""


def benchmark_attribute_multi(headers, endpoint):
    session = requests.Session()
    # Get root uuid
    req = endpoint + "/"
    rsp = session.get(req, headers=headers)

    if rsp.status_code != 200:
        msg = f"Couldn't get root uuid: {rsp.status_code}"
        raise ValueError(msg)

    rspJson = json.loads(rsp.text)

    if "root" not in rspJson:
        raise ValueError("Couldn't get root from response JSON")

    root_uuid = rspJson["root"]

    dtype = "H5T_STD_U8LE"
    shape = [100]
    value = list(range(shape[0]))

    # Create many attributes using multi API
    start = time.time()
    attr_dict = {}

    for i in range(attr_count):
        attr_name = "attr_multi" + str(i)
        value[0] = i
        attr_dict[attr_name] = {"type": dtype, "shape": shape, "value": value}

    data = {"attributes": attr_dict}
    # Create attributes on root group
    req = endpoint + "/groups/" + root_uuid + "/attributes"
    rsp = session.put(req, data=json.dumps(data), headers=headers)

    if rsp.status_code != 201:
        msg = f"Could not create multiple attributes: {rsp.status_code}"
        raise ValueError(msg)

    end = time.time()
    msg = f"Time to create {attr_count} attributes with multi API: {(end - start):6.4f} seconds"
    print(msg)

    # Retrieve many attributes using multi API
    start = time.time()

    req = endpoint + "/groups/" + root_uuid + "/attributes"
    params = {"IncludeData": 1}
    rsp = session.get(req, headers=headers, params=params)

    if rsp.status_code != 200:
        msg = f"Could not retrieve multiple attributes: {rsp.status_code}"
        raise ValueError(msg)

    end = time.time()
    msg = f"Time to retrieve {attr_count} attributes with multi API: {(end - start):6.4f} seconds"
    print(msg)

    # Check return correctness
    rspJson = json.loads(rsp.text)

    if "attributes" not in rspJson:
        raise ValueError("Respinse to multi-attribute GET did not contain attributes info")

    attributes_json = rspJson["attributes"]

    if len(attributes_json) != attr_count:
        msg = f"Incorrect number of attributes returned: expected {attr_count}, \
            actual = {len(attributes_json)}"
        raise ValueError(msg)

    for i in range(attr_count):
        attr_info = attributes_json[i]

        # attribute names are sorted alphabetically, not in numeric order
        if "name" not in attr_info:
            raise ValueError("Returned attribute info contained no name")

        if attr_info["name"] not in attr_dict:
            raise ValueError("Returned attribute info contained unrequested name")

        out_type = attr_info["type"]

        if out_type["class"] != "H5T_INTEGER":
            raise ValueError("attribute response contained wrong dtype class")

        if out_type["base"] != "H5T_STD_U8LE":
            raise ValueError("attribute response contained wrong dtype base")

        expected_value = attr_dict[attr_info["name"]]["value"]

        if "value" not in attr_info:
            raise ValueError("Returned attribute info contained no value")

        if not np.array_equal(attr_info["value"], expected_value):
            raise ValueError("Returned attribute value was incorrect")

    print("Multi attribute data is correct!")


def benchmark_attribute_serial(headers, endpoint):
    session = requests.Session()
    req = endpoint + "/"
    rsp = session.get(req, headers=headers)

    if rsp.status_code != 200:
        msg = f"Couldn't get root uuid: {rsp.status_code}"
        raise ValueError(msg)

    rspJson = json.loads(rsp.text)

    if "root" not in rspJson:
        raise ValueError("Couldn't get root from response JSON")

    root_uuid = rspJson["root"]

    # Create many attributes in root group individually

    dtype = "H5T_STD_U8LE"
    shape = [100]
    value = list(range(shape[0]))

    start = time.time()
    for i in range(attr_count):
        attribute_name = "attr_serial" + str(i)
        req = endpoint + "/groups/" + root_uuid + "/attributes/" + attribute_name

        value[0] = i
        data = {"type": dtype, "shape": shape, "value": value}
        rsp = session.put(req, data=json.dumps(data), headers=headers)

        if rsp.status_code != 201:
            msg = f"Could not create attribute #{i}: {rsp.status_code}"
            raise ValueError(msg)
    end = time.time()
    msg = f"Time to create {attr_count} attributes individually: {(end - start):6.4f} seconds"
    print(msg)

    # Retrieve many attributes from root group individually
    attribute_rsp = [None] * attr_count
    start = time.time()
    for i in range(attr_count):
        attribute_name = "attr_serial" + str(i)
        req = endpoint + "/groups/" + root_uuid + "/attributes/" + attribute_name
        rsp = session.get(req, headers=headers)

        if rsp.status_code != 200:
            msg = f"Could not get attribute #{i}: {rsp.status_code}"
            raise ValueError(msg)

        attribute_rsp[i] = rsp

    end = time.time()
    msg = f"Time to retrieve {attr_count} attributes individually: {(end - start):6.4f} seconds"
    print(msg)

    # Check return correctness
    for i in range(attr_count):
        rspJson = json.loads(attribute_rsp[i].text)

        if "name" not in rspJson:
            raise ValueError("attribute response contained no name info")

        if not rspJson["name"].startswith("attr_serial"):
            raise ValueError("attribute response contained wrong name")

        out_type = rspJson["type"]

        if out_type["class"] != "H5T_INTEGER":
            raise ValueError("attribute response contained wrong dtype class")

        if out_type["base"] != "H5T_STD_U8LE":
            raise ValueError("attribute response contained wrong dtype base")

        # generate expected value
        value[0] = int(rspJson["name"].split("attr_serial")[1])
        if rspJson["value"] != value:
            raise ValueError("attribute response contained wrong value")


# Set up connection information
endpoint = os.environ["HSDS_ENDPOINT"]
username = os.environ["USER_NAME"]
password = os.environ["USER_PASSWORD"]

# Set up headers
filepath = "/home/" + username + "/attribute_benchmark.h5"
headers = {}
auth_string = username + ":" + password
auth_string = auth_string.encode('utf-8')
auth_string = base64.b64encode(auth_string)
auth_string = b"Basic " + auth_string
headers['Authorization'] = auth_string
headers['X-Hdf-domain'] = filepath


if (len(sys.argv) < 2) or (sys.argv[1] in ("-h", "--help")):
    sys.exit(f"usage: python {sys.argv[0]} count")
else:
    attr_count = int(sys.argv[1])

f = h5pyd.File(filepath, mode="w")

benchmark_attribute_multi(headers=headers, endpoint=endpoint)
benchmark_attribute_serial(headers=headers, endpoint=endpoint)

print("Benchmark complete")
