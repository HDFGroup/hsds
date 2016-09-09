import requests
import sys
import json
import base64

def printUsage():
   print("Usage: python get_obj_sn.py [-endpoint=<server_ip>] [-port=<port>] [-user=<username>] [-password=<password>] -domain=<domain> uri")
 
endpoint = '127.0.0.1'
domain = None
port = 5102
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
    elif arg.startswith('-domain='):
        domain = arg[len('-domain='):]
    elif arg.startswith('-user='):
        username = arg[len('-user='):]
    elif arg.startswith('-password='):
        password = arg[len('-password='):]
    

if domain is None:
    sys.exit("no domain given")

uri = sys.argv[nargs]
print("uri:", uri)
print("user:", username)

if uri[0] != '/':
    sys.exit("uri must start wtih '/'")
 
headers = {'host': domain} 
if username and password:
    auth_string = username + ':' + password
    auth_string = auth_string.encode('utf-8')
    auth_string = base64.b64encode(auth_string)
    auth_string = b"Basic " + auth_string
    print("auth_string:", auth_string)
    headers['Authorization'] = auth_string

req = "http://" + endpoint + ':' + str(port) + uri  
print("headers: ", headers)
print("req:", req, "domain=", domain)
rsp = requests.get(req, headers=headers)
print("<{}>".format(rsp.status_code))
if rsp.status_code == 200:
    print(rsp.json())
    
    
