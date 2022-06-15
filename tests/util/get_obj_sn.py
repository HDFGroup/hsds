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
import requests
import sys
import base64


def printUsage():
    msg = "Usage: python get_obj_sn.py [-endpoint=<server_ip>] [-port=<port>] "
    msg += "[-user=<username>] [-password=<password>] -domain=<domain> uri"
    print(msg)


endpoint = "127.0.0.1"
domain = None
port = 5102
username = "test_user1"
password = "test"

nargs = len(sys.argv) - 1
for arg in sys.argv:
    if arg.startswith("-h") or nargs < 1:
        printUsage()
        sys.exit(0)
    if arg.startswith("-port="):
        nlen = len("-port=")
        port = int(arg[nlen:])
    elif arg.startswith("-endpoint="):
        nlen = len("-endpoint=")
        endpoint = arg[nlen:]
    elif arg.startswith("-domain="):
        nlen = len("-domain=")
        domain = arg[nlen:]
    elif arg.startswith("-user="):
        nlen = len("-user=")
        username = arg[nlen:]
    elif arg.startswith("-password="):
        nlen = len("-password=")
        password = arg[nlen:]


if domain is None:
    sys.exit("no domain given")

uri = sys.argv[nargs]
print("uri:", uri)
print("user:", username)

if uri[0] != "/":
    sys.exit("uri must start wtih '/'")

headers = {"host": domain}
if username and password:
    auth_string = username + ":" + password
    auth_string = auth_string.encode("utf-8")
    auth_string = base64.b64encode(auth_string)
    auth_string = b"Basic " + auth_string
    headers["Authorization"] = auth_string

req = "http://" + endpoint + ":" + str(port) + uri
print("headers: ", headers)
print("req:", req, "domain=", domain)
rsp = requests.get(req, headers=headers)
print("<{}>".format(rsp.status_code))
if rsp.status_code == 200:
    print(rsp.json())
