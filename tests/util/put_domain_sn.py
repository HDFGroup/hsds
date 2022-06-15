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
import json
import base64


def printUsage():
    msg = "Usage: python put_domain_sn.py [-endpoint=<server_ip>] [-port=<port>] "
    msg += "[-user=<username>] [-password=<password>]  domain"
    print(msg)


endpoint = "127.0.0.1"
port = 5102

if len(sys.argv) < 2:
    printUsage()
    sys.exit(-1)

domain = None
username = "test_user1"
password = "test"

for arg in sys.argv:
    if arg.startswith("-h"):
        printUsage()
        sys.exit(0)
    if arg.startswith("-port="):
        nlen = len("-port=")
        port = int(arg[nlen:])
    elif arg.startswith("-endpoint="):
        nlen = len("-endpoint=")
        endpoint = arg[nlen:]
    elif arg.startswith("-user="):
        nlen = len("-user=")
        username = arg[nlen:]
    else:
        domain = arg

if domain is None:
    printUsage()
    sys.exit(0)

headers = {"host": domain}
if username and password:
    auth_string = username + ":" + password
    auth_string = auth_string.encode("utf-8")
    auth_string = base64.b64encode(auth_string)
    auth_string = b"Basic " + auth_string
    headers["Authorization"] = auth_string

req = "http://" + endpoint + ":" + str(port) + "/"
print("req:", req)
body = {}
rsp = requests.put(req, headers=headers, data=json.dumps(body))
print("<{}>".format(rsp.status_code))
if rsp.status_code == 201:
    print(rsp.json())
