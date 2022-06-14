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


def printUsage():
    msg = "Usage: python delete_obj_dn.py [-endpoint=<server_ip>] [-port=<port>]  uri"
    print(msg)


endpoint = "127.0.0.1"
domain = None
port = 5101
username = None
password = None

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

uri = sys.argv[nargs]
print("uri:", uri)

if uri[0] != "/":
    sys.exit("uri must start wtih '/'")

req = "http://" + endpoint + ":" + str(port) + uri

print("req:", req)
rsp = requests.delete(req)
print("<{}>".format(rsp.status_code))
if rsp.status_code == 200:
    print(rsp.json())
