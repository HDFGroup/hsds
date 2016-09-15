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
import uuid

def printUsage():
   print("Usage: python create_group_dn.py [-endpoint=<server_ip>] [-port=<port>] [-root=<uuid>] [-domain=domain]")

# example - create a group with toc root as root:
# python create_group_dn.py -root=g-00000000-0000-0000-0000-000000000000
 
endpoint = '127.0.0.1'
port = 5101

root_id = None
domain = None
 
for arg in sys.argv:
    if arg.startswith('-h'):
        printUsage()
        sys.exit(0)
    if arg.startswith('-port='):
        port = int(arg[len('-port='):])
    elif arg.startswith('-endpoint='):
        endpoint = arg[len('-endpoint='):]
    elif arg.startswith('-root='):
        root_id = arg[len('-root='):]
    elif arg.startswith('-domain='):
        domain= arg[len('-domain='):]

 
# make a new group_id

group_id = "g-" + str(uuid.uuid1())
if root_id is None:
    root_id = group_id  # make this a root group

req = "http://" + endpoint + ':' + str(port) + "/groups" 
body = {"id": group_id, "root": root_id }
if domain is not None:
    body["domain"] = domain
print("req:", req)

rsp = requests.post(req, data=body)
 
print("<{}>".format(rsp.status_code))
if rsp.status_code == 201:
    print(rsp.json())
    
    
