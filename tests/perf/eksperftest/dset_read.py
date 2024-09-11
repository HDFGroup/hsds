import h5pyd
import numpy as np
import config
import os
import sys
import time

hsds_global = config.get("hsds_global")
hsds_local = config.get("hsds_local")
username = config.get("hs_username")
password = config.get("hs_password")
inventory_domain = config.get("inventory_domain")
if "POD_NAME" in os.environ:
    pod_name = os.environ["POD_NAME"]
else:
    pod_name = ""

def visit(h5path, dset):
    if not isinstance(dset, h5pyd.Dataset):
        return
    if dset.dtype != np.float32:
        return
    arr = dset[...]
    print(f"    {dset.name:60} min: {arr.min():12.4f} max: {arr.max():12.4f} mean: {arr.mean():12.4f}")

def read_domain(domain_path):
    print(f"opening {domain_path}")
    with h5pyd.File(domain_path) as g:
        g.visititems(visit)


def read_domains():
    f = h5pyd.File(inventory_domain, "r+", use_cache=False) # , endpoint=hsds_global, username=username, password=password)

    table = f["inventory"]
    print("table.nrows:", table.nrows)
    print("table.dtype:", table.dtype)

    condition = "start == 0"  # query for files that haven't been proccessed

    while True:

        now = int(time.time())
        update_val = {"start": now, "status": -1, "podname": pod_name}

        # query for row with 0 start value and update it to now
        indices = table.update_where(condition, update_val, limit=1)
        print("indices:", indices)

        if indices is not None and len(indices) > 0:
            index = indices[0]
            print(f"getting row: {index}")
            row = table[index]
            print("got row:", row)
            filename = row[0].decode("utf-8")
            rc = 1
            try:
                read_domain(filename)
                print(f"read_domain({filename} - complete - no errors")
                rc = 0
            except IOError as ioe:
                print(f"load({filename} - IOError: {ioe}")
            except Exception as e:
                print(f"load({filename} - Unexpected exception: {e}") 

            # update inventory table
            row[2] = int(time.time())
            row[3] = rc
            table[index] = row
        else:
            print("no more rows to process")
            break

#
# main
# 
if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help"):
    sys.exit(f"usage: {sys.argv[0]} [domain_path]")

if len(sys.argv) > 1:
    domain_path = sys.argv[1]
    read_domain(domain_path)
else:
    read_domains()

print("done")