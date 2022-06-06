import sys
import requests
import json
import base64
from config import Config

config = Config()

"""
Helper - get default request headers for domain
"""


def getRequestHeaders(domain=None, username=None, password=None, **kwargs):
    if username is None:
        username = config["hs_username"]
        if not username:
            sys.exit("username not defined")
    if password is None:
        password = config["hs_password"]
        if not password:
            sys.exit("password not defined")
    headers = {}
    if domain is not None:
        headers["X-Hdf-domain"] = domain.encode("utf-8")
    if username and password:
        auth_string = username + ":" + password
        auth_string = auth_string.encode("utf-8")
        auth_string = base64.b64encode(auth_string)
        auth_string = b"Basic " + auth_string
        headers["Authorization"] = auth_string

    for k in kwargs.keys():
        headers[k] = kwargs[k]
    return headers


#
# Main
#

if len(sys.argv) > 1:
    domain = sys.argv[1]
elif config["domain"]:
    domain = config["domain"]
else:
    sys.exit("no domain specified")
print("domain:", domain)


endpoint = config["hs_endpoint"]
if not endpoint:
    sys.exit("Endpoint not defined")
print("endpoint:", endpoint)

session = requests.Session()

headers = getRequestHeaders(domain=domain)

# create domain
req = endpoint + "/"
rsp = session.put(req, headers=headers)
if rsp.status_code == 409:
    sys.exit("Domain already exists")
elif rsp.status_code != 201:
    sys.exit("Failed to create domain: {}".format(rsp.status_code))
rsp_json = json.loads(rsp.text)
root_id = rsp_json["root"]
print("root_id:", root_id)

extent = [792, 1602, 2976]

# create dataset
payload = {"type": "H5T_IEEE_F64LE", "shape": extent}
req = endpoint + "/datasets"
rsp = session.post(req, data=json.dumps(payload), headers=headers)
if rsp.status_code != 201:
    sys.exit("Failed to create datset: {}".format(rsp.status_code))

rsp_json = json.loads(rsp.text)
dset_id = rsp_json["id"]

# link the new dataset
name = "dset"
req = endpoint + "/groups/" + root_id + "/links/" + name
payload = {"id": dset_id}
print(dset_id)
rsp = session.put(req, data=json.dumps(payload), headers=headers)
if rsp.status_code != 201:
    sys.exit("Failed to link datset: {}".format(rsp.status_code))
rsp_json = json.loads(rsp.text)

session.close()
