import requests
import sys
import json

def printUsage():
   print("Usage: python get_group_dn.py [-endpoint=<server_ip>] [-port=<port>] [-id=<uuid>]")
 
endpoint = '127.0.0.1'
port = 5101
id = 'g-00000000-0000-0000-0000-000000000000' # toc root group
 
for arg in sys.argv:
    if arg.startswith('-h'):
        printUsage()
        sys.exit(0)
    if arg.startswith('-port='):
        port = int(arg[len('-port='):])
    elif arg.startswith('-endpoint='):
        endpoint = arg[len('-endpoint='):]
    elif arg.startswith('-id='):
        id = arg[len('-id='):]

 

req = "http://" + endpoint + ':' + str(port) + "/groups/" + id
print("req:", req)
rsp = requests.get(req)
print("<{}>".format(rsp.status_code))
if rsp.status_code == 200:
    print(rsp.json())
    
    
