import sys
import os
import time
import requests

#
# Continually check server status and output time and state.
#

hsds_endpoint = None
no_stream = False
usage = f"usage: python {sys.argv[0]} [--endpoint <server_endpoint] [--no-stream] [--quiet]"
status = 0
quiet = False
state = ""

if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help"):
    print()
    sys.exit(usage)

argn = 1
while len(sys.argv) > argn:
    arg = sys.argv[argn]
    if arg == "--no-stream":
        no_stream = True
        argn += 1
    elif arg == "--quiet":
        quiet = True
        argn += 1
    elif arg == "--endpoint":
        if len(sys.argv) == argn:
            sys.exit(usage)
        endpoint = sys.argv[argn + 1]
        argn += 2
    else:
        sys.exit(usage)

if hsds_endpoint is None:
    if "HSDS_ENDPOINT" in os.environ:
        hsds_endpoint = os.environ["HSDS_ENDPOINT"]
    else:
        sys.exit("HSDS_ENDPOINT not set")

while True:
    now = int(time.time())
    state = ""
    node_count = 0
    try:
        rsp = requests.get(f"{hsds_endpoint}/about")
        status = rsp.status_code

        if status == 200:
            rsp_json = rsp.json()
            state = rsp_json["state"]
            node_count = rsp_json["node_count"]
    except Exception as e:
        state = str(e)
    if not quiet:
        print(f"{now}: {status}: {state} ({node_count} nodes)")
    if no_stream:
        break
    time.sleep(1)

if state == "READY":
    sys.exit(0)
else:
    sys.exit(1)
