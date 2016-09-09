import requests
import sys
import json
import base64

def printUsage():
   print("Usage: python get_obj_dn.py [-endpoint=<server_ip>] [-port=<port>]  uri")
 
endpoint = '127.0.0.1'
domain = None
port = 5101
username = None
password = None
 
nargs = len(sys.argv) - 1
for arg in sys.argv:
    if arg.startswith('-h') or nargs < 1:
        printUsage()
        sys.exit(0)
    if arg.startswith('-port='):
        port = int(arg[len('-port='):])
    elif arg.startswith('-endpoint='):
        endpoint = arg[len('-endpoint='):]
    
uri = sys.argv[nargs]
print("uri:", uri)

if uri[0] != '/':
    sys.exit("uri must start wtih '/'")

req = "http://" + endpoint + ':' + str(port) + uri  

print("req:", req)
rsp = requests.get(req)
print("<{}>".format(rsp.status_code))
if rsp.status_code == 200:
    print(rsp.json())
    
    
