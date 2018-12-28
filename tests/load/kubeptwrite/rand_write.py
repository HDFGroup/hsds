import sys
import requests
import json
import random
import base64
import time
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
    headers = { }
    if domain is not None:
        headers['X-Hdf-domain'] = domain.encode('utf-8')
    if username and password:
        auth_string = username + ':' + password
        auth_string = auth_string.encode('utf-8')
        auth_string = base64.b64encode(auth_string)
        auth_string = b"Basic " + auth_string
        headers['Authorization'] = auth_string

    for k in kwargs.keys():
        headers[k] = kwargs[k]
    return headers

def json_req(req, session=None, headers=None, retries=0):
   
    rsp_json = None
    backoff = 0.1
    for retry in range(retries+1):
        rsp = session.get(req, headers=headers)
        if rsp.status_code in (502,503,504):
            print("WARN:> Error {} for req: {} sleeping for: {}".format(rsp.status_code, req, backoff)) 
            time.sleep(backoff)
            backoff *= 2.0
            if backoff > 1.0:
                backoff = 1.0
        elif rsp.status_code == 200:
            # success
            rsp_json = json.loads(rsp.text)
        else:
            # unexpected error
            sys.exit("req {} failed, error: {}".format(req, rsp.status_code))

    if rsp_json is None:
        sys.exit("req {} failed, after {} rertries".format(req, retries))
    return rsp_json


def write_value(indx, value, session=None, endpoint=None, headers=None, dset_id=None, retries=0):
    # Get all the values for a given geographic point
    req = endpoint + "/datasets/" + dset_id + "/value"

    payload = { 'start': indx, 'stop': indx+1,  'value': value }

    # request binary response
    success = False
    backoff = 0.1
    
    for retry in range(retries+1):
        rsp = session.put(req, data=json.dumps(payload), headers=headers)
        if rsp.status_code == 503:
            print("WARN:> 503 ServiceUnavailable, sleeping for {}".format(backoff)) 
            time.sleep(backoff)
            backoff *= 2.0
            if backoff > 1.0:
                backoff = 1.0
        elif rsp.status_code == 200:
            # success
            success = True
            break
        else:
            sys.exit("failed to write dataset value, error: {}".format(rsp.status_code))

    if not success:
        sys.exit("failed to write dataset value after: {} attempts".format(retries+1))

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

if len(sys.argv) > 2:
    runs = int(sys.argv[2])
elif config["run_count"]:
    runs = int(config["run_count"])
else:
    runs = 10
print("runs:", runs)

endpoint = config["hs_endpoint"]
if not endpoint:
    sys.exit("Endpoint not defined")
print("endpoint:", endpoint)

if config["retries"]:
  retries = int(config["retries"])
else:
  retries = 3  # will die after 10 minutes
print("retries:", retries)


session = requests.Session()

headers = getRequestHeaders(domain)
req = endpoint + '/'
rsp_json = json_req(req, session=session, headers=headers, retries=retries)
root_id = rsp_json["root"]

req = endpoint + "/groups/" + root_id + "/links/dset" 
rsp_json = json_req(req, session=session, headers=headers, retries=retries)
link = rsp_json["link"]
dset_id = link["id"]

req = endpoint + "/datasets/" + dset_id
rsp_json = json_req(req, session=session, headers=headers, retries=retries)
shape = rsp_json["shape"]
dims = shape["dims"]
extent = dims[0]
print("extent:", extent)

req = endpoint + "/datasets/" + dset_id + "/value" 

for i in range(runs):
    indx = random.randint(0, extent-1)
    time_start = time.time()
    write_value(indx, 42, session=session, endpoint=endpoint, headers=headers, dset_id=dset_id, retries=retries)
    run_time = time.time() - time_start
    print("{0:05d}: {1:12d} {2:6.2f}s".format(i, indx, run_time))

session.close()
