import os
import sys
import time
import numpy as np
import h5pyd


if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
    print("usage: python hs_write.py domain")
    sys.exit(1)

domain = sys.argv[1]
 
print("domain:", domain)
 
f = h5pyd.File(domain, 'a')
dset = f["dset"]
print("dset:", dset)
print("dset chunks:", dset.chunks)
print("chunk_size:", np.prod(dset.chunks)*dset.dtype.itemsize)

table = f["chunk_list"]
now = int(time.time())
if "POD_NAME" in os.environ:
    pod_name = os.environ["POD_NAME"]
else:
    pod_name = ""
condition = "start == 0" 

while True:
    update_val = {"start": now, "status": -1, "pos": pod_name}

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
    dset[y:y+nrow, x:x+ncol] = arr
    entry['done'] = int(time.time())
    entry['status'] = 1
    table[index] = entry

print("done!")
 
