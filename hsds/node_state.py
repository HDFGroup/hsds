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

"""Print this node's state and exit 0 only when it is READY.

Intended as a container liveness/readiness probe. /info is listed in
INFO_METHODS in hsds_logger.request and so bypasses the node_state gate, which
means an httpGet probe against it returns 200 even while the node is stuck in
WAITING and returning 503 to every real request. A probe that distinguishes a
serving node from a wedged one therefore has to inspect node_state itself.

The port is derived from NODE_TYPE, so the sn and dn containers can share one
identical probe command rather than each hardcoding a port.

Exit codes:
    0  node reports READY
    1  node reports any other state, or /info could not be reached
    2  NODE_TYPE has no configured port (misconfiguration, not a wedged node)
"""

import json
import os
import sys
import urllib.request

from . import config


def main():
    node_type = os.environ.get("NODE_TYPE") or "sn"
    port = config.get(f"{node_type}_port")
    if not port:
        print(f"no port configured for NODE_TYPE={node_type}", file=sys.stderr)
        return 2

    url = f"http://localhost:{port}/info"
    try:
        with urllib.request.urlopen(url, timeout=10) as rsp:
            state = json.load(rsp)["node"]["state"]
    except Exception as e:
        print(f"{url}: {e}", file=sys.stderr)
        return 1

    # stdout is the state itself, so the same command is useful for diagnostics
    print(state)
    return 0 if state == "READY" else 1


if __name__ == "__main__":
    sys.exit(main())
