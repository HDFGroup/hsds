import h5pyd
import time
import requests
import base64
import json
import sys
import os

"""Time link creation and retrieval with multi vs singular API

Got the following result with a local HSDS instance and python 3.9:

Time to create 10000 links with multi API:     0.4732 seconds
Time to retrieve 10000 links with multi API:   0.1801 seconds
Time to create 10000 links individually:      94.9879 seconds
Time to retrieve 10000 links individually:   160.7956 seconds
"""


def benchmark_link_multi(headers, endpoint, file):
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

    # Create many links using multi API
    start = time.time()
    link_dict = {}

    for i in range(link_count):
        link_name = "link_multi" + str(i)
        link_dict[link_name] = {"id": root_uuid, "class": "H5L_TYPE_HARD"}

    data = {"links": link_dict}
    req = endpoint + "/groups/" + root_uuid + "/links"
    rsp = session.put(req, data=json.dumps(data), headers=headers)

    if rsp.status_code != 201:
        msg = f"Could not create multiple links: {rsp.status_code}"
        raise ValueError(msg)

    end = time.time()
    msg = f"Time to create {link_count} links with multi API: {(end-start):6.4f} seconds"
    print(msg)

    # Retrieve many links using multi API
    start = time.time()

    req = endpoint + "/groups/" + root_uuid + "/links"
    rsp = session.get(req, headers=headers)

    if rsp.status_code != 200:
        msg = f"Could not retrieve multiple links: {rsp.status_code}"
        raise ValueError(msg)

    end = time.time()
    msg = f"Time to retrieve {link_count} links with multi API: {(end-start):6.4f} seconds"
    print(msg)

    # Check return correctness
    rspJson = json.loads(rsp.text)

    if "links" not in rspJson:
        raise ValueError("Respinse to multilink GET did not contain links info")

    links_json = rspJson["links"]

    if len(links_json) != link_count:
        msg = f"Incorrect number of links returned: expected {link_count}, \
            actual = {len(links_json)}"
        raise ValueError(msg)

    for link_info in links_json:

        if link_info["id"] != root_uuid:
            raise ValueError("Returned link from multilink GET contained wrong target id")


def benchmark_link_serial(headers, endpoint, file):
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

    # Create many links in root group individually
    start = time.time()
    for i in range(link_count):
        link_name = "link_serial" + str(i)
        req = endpoint + "/groups/" + root_uuid + "/links/" + link_name
        data = {"id": root_uuid, "class": "H5L_TYPE_HARD"}
        rsp = session.put(req, data=json.dumps(data), headers=headers)

        if rsp.status_code != 201:
            msg = f"Could not create link #{i}: {rsp.status_code}"
            raise ValueError(msg)
    end = time.time()
    msg = f"Time to create {link_count} links individually: {(end-start):6.4f} seconds"
    print(msg)

    # Retrieve many links from root group individually
    link_rsp = [None] * link_count
    start = time.time()
    for i in range(link_count):
        link_name = "link_serial" + str(i)
        req = endpoint + "/groups/" + root_uuid + "/links/" + link_name
        rsp = session.get(req, headers=headers)

        if rsp.status_code != 200:
            msg = f"Could not get link #{i}: {rsp.status_code}"
            raise ValueError(msg)

        link_rsp[i] = rsp

    end = time.time()
    msg = f"Time to retrieve {link_count} links individually: {(end-start):6.4f} seconds"
    print(msg)

    # Check return correctness
    for i in range(link_count):
        rspJson = json.loads(link_rsp[i].text)

        if "link" not in rspJson:
            raise ValueError("Link response contained no link info")

        link_json = rspJson["link"]

        if link_json["id"] != root_uuid:
            raise ValueError("Link response contained wrong id")


# Set up connection information
endpoint = os.environ["HSDS_ENDPOINT"]
username = os.environ["USER_NAME"]
password = os.environ["USER_PASSWORD"]

# Set up headers
filepath = "/home/" + username + "/link_benchmark.h5"
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
    link_count = int(sys.argv[1])

f = h5pyd.File(filepath, mode="w")

benchmark_link_multi(headers=headers, endpoint=endpoint, file=f)
benchmark_link_serial(headers=headers, endpoint=endpoint, file=f)

print("Benchmark complete")
