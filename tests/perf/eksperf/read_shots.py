import sys
import random
import h5py
import h5pyd
import s3fs
from h5pyd import H5Image
import os
import time
import config
import logging

FIELD_FILENAME=0
FIELD_START=1
FIELD_DONE=2
FIELD_RC=3
FIELD_PODNAME=4

CHUNKS_PER_PAGE=8

def is_grp(obj):
    # this should work with either h5py or h5pyd
    if obj.__class__.__name__ == "Group":
        return True
    else:
        return False

def is_dataset(obj):
    # this should work with either h5py or h5pyd
    if obj.__class__.__name__ == "Dataset":
        return True
    else:
        return False
    
def read_group(grp, data_map):
    print(f"    group:   {grp.name} pr")
    for k in grp:
        obj = grp[k]
        if is_grp(obj):
            # recursive call to sub-group
            read_group(obj, data_map)
        elif is_dataset(obj):
            # read all the data for the dataset
            arr = obj[...]
            # save to the map
            data_map[obj.name] = arr
        else:
            # ignore ctypes, external links, softlinks, etc.
            print(f"ignoring {obj.name} - type: {type(obj)}")

def get_pod_name():
    if "POD_NAME" in os.environ:
        pod_name = os.environ["POD_NAME"]
    else:
        pod_name = ""
    return pod_name

def get_shot_index(table, pod_name=None):
    condition = "start == 0"  # query for files that haven't been proccessed
    now = int(time.time())
    update_val = {"start": now, "lstatus": -1, "podname": pod_name}

    # query for row with 0 start value and update it to now
    indices = table.update_where(condition, update_val, limit=1)
    print("indices:", indices)
    if not indices:
        return None
    
    index = indices[0]
    return index

def read_shot(shots):
    if get_pod_name():
        # apparently we are running in a pod, get hsds auth from config
        endpoint = config.get("hs_local")  # use local endpoint to avoid overloading global hsds
        username = config.get("hs_username")
        password = config.get("hs_password")
    else:
        # use the defaults from .hscfg
        endpoint = None
        username = None
        password = None

    print("read_shots")

    filename = None
    index = None
    row = None
    pod_name = get_pod_name()
    condition = "start == 0"  # query for files that haven't been proccessed
    now = int(time.time())
    update_val = {"start": now, "status": -1, "podname": pod_name}
    
    # query an HSDS table for an unprocessed shot
    indices = shots.update_where(condition, update_val, limit=1)
    if not indices:
        print("no indices returned for update_where query")
        return -1
    index = indices[0]
    row = shots[index]
    print("row...", row)
    filename = row[FIELD_FILENAME].decode()

    if not filename:
        return -1

    print(f"got domain: {filename} for index: {index}")

    if filename.startswith("s3://"):
        # use s3fs for access HDF5 files
        s3 = s3fs.S3FileSystem()
        f = h5py.File(s3.open(filename, "rb"))
    elif filename.startswith("/cmodh5"):
        # open HDF5 file image
        f = h5py.File(H5Image(filename, chunks_per_page=CHUNKS_PER_PAGE))
    else:
        # open HSDS domain
        f = h5pyd.File(filename, endpoint=endpoint, username=username, password=password, use_cache=False)
        if "h5image" in f:
            # HDF5 file image, open with h5py and H5Image
            f.close()
            f = h5py.File(H5Image(filename, chunks_per_page=CHUNKS_PER_PAGE))
    data_map = {}
    read_group(f, data_map)
    for k in data_map:
        v = data_map[k]
        print(f"        {k}: {v.shape}")

    if row:
        # update inventory table/list
        print("row:", row)
        row[FIELD_DONE] = int(time.time())
        row[FIELD_RC] = 0  # rc
        shots[index] = row

    f.close()
    
    return 0


def main():
    
    loglevel = logging.WARNING
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
    if get_pod_name():
        # apparently we are running in a pod, get hsds auth from config
        endpoint = config.get("hs_global")
        username = config.get("hs_username")
        password = config.get("hs_password")
    else:
        # use the defaults from .hscfg
        endpoint = None
        username = None
        password = None

    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(f"usage: python {sys.argv[0]} <count> <inventory_domain>  <seed>")
        sys.exit(1)

    if len(sys.argv) > 1:
        num_shots = int(sys.argv[1])
    else:
        num_shots = None

    if len(sys.argv) > 2:
        inventory_domain = sys.argv[2]
    else:
        inventory_domain = config.get("inventory_domain")

    if len(sys.argv) > 3:
        rand_seed = int(sys.argv[3])
        random.seed(rand_seed)

    # open the inventory file
    print("opening inventory domain for updating:", inventory_domain)
    f = h5pyd.File(inventory_domain, mode="a", endpoint=endpoint, username=username, password=password)
    shots = f["inventory"]

    shots_read = 0
    t4 = time.time()
    while True:
        rc = read_shot(shots)

        if rc < 0:
            break
        shots_read += 1

        if num_shots and shots_read >= num_shots:
            break

    t5 = time.time()
    if shots_read > 0:
        elapsed = t5 - t4
        print(f"time read shots: {elapsed:4.2f} s for {shots_read} shots, {elapsed/shots_read:4.3f} shots/sec")
    print("done!")

    # just hange around if this is kuberentes
    if get_pod_name():
        while True:
            time.sleep(1)

main()

