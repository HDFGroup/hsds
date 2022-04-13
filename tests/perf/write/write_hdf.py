import os
import sys
import time
import numpy as np
import h5pyd
import logging

try:
    # probably don't need this for running in Docker or k8s
    import h5py
except ModuleNotFoundError:
    pass # ignore


def getNextChunkTableIndex(chunk_table):
    ''' Get first row of chunk table where start field is 0.
        Return -1 is now row exist
    '''
    index = -1
    now = time.time()

    # query for row with 0 start value and update it to now
    if isinstance(chunk_table.id.id, str):
        # HSDS dataset - use query selection
        condition = "status == 0" 
        update_val = {"start": now, "status": -1, "pod": pod_name}
        indices = table.update_where(condition, update_val, limit=1)
        if indices is None or len(indices) == 0:
            index = -1
        else:
            index = indices[0]
    else:
        # HDF5 dataset, search table directly
        for s in chunk_table.iter_chunks():
            arr = chunk_table[s]
            for i in range(len(arr)):
                row = arr[i]
                if row['status'] == 0:
                    row['start'] = now # update start time
                    row['status'] = -1
                    index = s[0].start + i
                    # write back updated row
                    chunk_table[index] = row  
                    break
            if index > -1:
                # found a chunk to write to
                break
    return index


#
# Main
#

if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
    print("usage: python hs_write.py filepath")
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

if sys.argv[1] == "-v":
    verbose = True
    filepath = sys.argv[2]
else:
    verbose = False
    filepath = sys.argv[1]
 
print("filepath:", filepath)

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

if filepath.startswith("hdf5://"):
    f = h5pyd.File(filepath, 'a', username=hs_username, password=hs_password, endpoint=hs_endpoint)
else:
    f = h5py.File(filepath, 'a')
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

print("writing data...")
while True:
    index = getNextChunkTableIndex(table)
    
    if index < 0:
        print("no more chunks")
        break
    entry = table[index]
    nrow = entry['nrow']
    ncol = entry['ncol']
    arr = np.random.rand(nrow, ncol)
    x = entry['x']
    y = entry['y']
    dset[x:x+nrow, y:y+ncol] = arr
    if verbose:
        print(f"wrote dset[{x}:{x+nrow}, {y}:{y+ncol}]")
    entry['done'] = time.time()
    entry['status'] = 1
    table[index] = entry

f.close()
print("done!")
 
