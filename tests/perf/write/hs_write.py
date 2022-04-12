import os
import sys
import time
import numpy as np
import h5pyd
import logging

if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
    print("usage: python hs_write.py domain")
    sys.exit(1)

if 'LOG_LEVEL' in os.environ:
    level_env = os.environ['LOG_LEVEL']
    if level_env == "DEBUG":
        loglevel = logging.DEBUG
    elif level_env == "INFO":
        loglevel = logging.INFO
    elif level_env == "WARNING":
        loglevel = logging.WARNING
    else:
        loglevel = logging.ERROR
else:
    loglevel = logging.ERROR
logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)

domain = sys.argv[1]
 
print("domain:", domain)

if "HS_USERNAME" in os.environ:
    hs_username = os.environ["HS_USERNAME"]
else:
    hs_username = None

if "HS_PASSWORD" in os.environ:
    hs_password = os.environ["HS_PASSWORD"]
else:
    hs_password = None

if "HS_ENDPOINT" in os.environ:
    hs_endpoint = os.environ["HS_ENDPOINT"]
else:
    hs_endpoint = None

f = h5pyd.File(domain, 'a', username=hs_username, password=hs_password, endpoint=hs_endpoint)
dset = f["dset"]
print("dset:", dset)
print("dset chunks:", dset.chunks)
print("chunk_size:", np.prod(dset.chunks)*dset.dtype.itemsize)

table = f["chunk_list"]
if "HOSTNAME" in os.environ:
    pod_name = os.environ["HOSTNAME"]
    print(f"pod_name: {pod_name}")
else:
    pod_name = ""
    print("no pod_name")
condition = "start == 0" 

while True:
    now = int(time.time())
    update_val = {"start": now, "status": -1, "pod": pod_name}

    # query for row with 0 start value and update it to now
    indices = table.update_where(condition, update_val, limit=1)
    if indices is None or len(indices) == 0:
        break  # no more work
    index = indices[0]

    if index < 0 or index >= table.nrows:
        print("got invalid index:", index)
        break
    entry = table[index]
    print(entry)
    nrow = entry['nrow']
    ncol = entry['ncol']
    arr = np.random.rand(nrow, ncol)
    x = entry['x']
    y = entry['y']
    print(f"dset[{x}:{x+nrow}, {y}:{y+ncol}] = arr")
    dset[x:x+nrow, y:y+ncol] = arr
    entry['done'] = int(time.time())
    entry['status'] = 1
    table[index] = entry

print("done!")
 
