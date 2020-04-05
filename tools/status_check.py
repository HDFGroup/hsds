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
    rc = 0
    msg = ""
    try:
        rsp = requests.get(f"{hsds_endpoint}/about")

        if rsp.status_code == 200:
            rsp_json = rsp.json()
            state = rsp_json["state"]
            if state == "READY":
                rc = 1
            else:
                rc = 0
                msg = state
        else:
            rc = 0
            msg = rsp.status_code
    except Exception as e:
        msg = str(e)
    print(f"{now}: {rc} {msg}")
    time.sleep(1)