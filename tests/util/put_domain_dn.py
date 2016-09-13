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

def printUsage():
   print("Usage: python get_domain_dn.py [-endpoint=<server_ip>] [-port=<port>] [-owner=<username>] domain")
 
endpoint = '127.0.0.1'
port = 5101
 
if len(sys.argv) < 2:
   printUsage()
   sys.exit(-1)

domain = None
owner = "test_user1"

for arg in sys.argv:
    if arg.startswith('-h'):
        printUsage()
        sys.exit(0)
    if arg.startswith('-port='):
        port = int(arg[len('-port='):])
    elif arg.startswith('-endpoint='):
        endpoint = arg[len('-endpoint='):]
    elif arg.startswith('-user='):
        owner = arg[len('-user='):]
    else:
	    domain = arg

if domain is None:
    printUsage()
    sys.exit(0)

body = { "owner": owner }
acls = {}
acl = {'read': True, 'readACL': True}
acls["default"] = acl
acl = {'writeACL': True, 'update': True, 'delete': True, 'readACL': True, 'read': True, 'create': True}
acls["owner"] = acl
body["acls"] = acls

req = "http://" + endpoint + ':' + str(port) + "/domains/" + domain
print("req:", req)
rsp = requests.put(req, data=json.dumps(body))
print("<{}>".format(rsp.status_code))
if rsp.status_code == 200:
    print(rsp.json())
    
    
