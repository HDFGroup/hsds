import sys
import os
import time
import requests

#
# Continually check server status and output time and state.
#

if "HSDS_ENDPOINT" not in os.environ:
    print("HSDS_ENDPOINT not set")
    sys.exit(1)

hsds_endpoint = os.environ["HSDS_ENDPOINT"]

while True:
    now = int(time.time())
    state = ""
    status = 503
    node_count = 0
    try:
        rsp = requests.get(f"{hsds_endpoint}/about")
        status_code = rsp.status_code

        if rsp.status_code == 200:
            rsp_json = rsp.json()
            state = rsp_json["state"]
            node_count = rsp_json["node_count"]
    except Exception as e:
        state = str(e)
    print(f"{now}: {status_code}: {state} ({node_count} nodes)")
    time.sleep(1)