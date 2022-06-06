# With the following code we create a "file" and then we read parts of it.
# We measure the reading times.
# We can use this code with reguar HDF5 files or with a HSDS service (make <USE_HSDS> variable true)

import sys
import random
import numpy as np
import logging
import time
import h5py
import h5pyd

DSET_NAME = "windspeed_80m"  # name of dataset to use


def run_hyperslab_select(dset, axis=0):
    # ************ READ DATA ***************************************

    x = random.randint(0, dset.shape[axis] - 1)
    tStart = time.time()
    if axis == 0:
        arr = dset[x, :, :]
        sel_str = f"{x:4d},:,:"
    elif axis == 1:
        arr = dset[:, x, :]
        sel_str = f":,{x:4d},:"
    elif axis == 2:
        arr = dset[:, :, x]
        sel_str = f":,:,{x:4d}"
    else:
        raise ValueError("invalid axis")

    nbytes = np.prod(arr.shape) * 4
    logging.debug(f"read {nbytes} bytes")
    tEnd = time.time()
    tElapsed = tEnd - tStart
    mb_per_sec = (nbytes / tElapsed) / (1024 * 1024)
    print(
        f"Elapsed time hyperslab select :: dset[{sel_str}]:  {tElapsed:6.3f} s, {mb_per_sec:6.2f} Mb/s"
    )
    return (axis, tElapsed, mb_per_sec)


def get_mean(results, axis=None):
    total_elapsed = 0.0
    total_mb_per_sec = 0.0
    count = 0
    for result in results:
        if axis is None or result[0] == axis:
            total_elapsed += result[1]
            total_mb_per_sec += result[2]
            count += 1
    if count == 0:
        return (0.0, 0.0)
    else:
        return (total_elapsed / count, total_mb_per_sec / count)


#
# main
#
loglevel = logging.ERROR
logging.basicConfig(format="%(asctime)s %(message)s", level=loglevel)

use_hsds = False  # Make it '1' if we want to use the HSDS implementation.\
iter_count = 5  # number of times to run each test
use_shared_mem = False
filepath = None

if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
    print(
        f"Usage: python {sys.argv[0]} [--use_hsds] [--use_shared_mem] [--iter_count=n] <filepath>"
    )
    sys.exit(0)

for i in range(1, len(sys.argv)):
    arg = sys.argv[i]
    if arg == "--use_hsds":
        use_hsds = True
    elif arg == "--use_shared_mem":
        use_shared_mem = True
    elif arg.startswith("--iter_count="):
        iter_count = int(arg[len("--iter_count=") :])
    else:
        filepath = arg

if not filepath:
    print("not filepath given!")
    sys.exit(1)

if filepath.startswith("hdf5://"):
    use_hsds = True

kwargs = {"mode": "r"}
if use_hsds:
    kwargs["use_shared_mem"] = use_shared_mem

if use_hsds:
    f2 = h5pyd.File(filepath, **kwargs)
else:
    f2 = h5py.File(filepath, **kwargs)
dset = f2[DSET_NAME]
logging.info(f"dataset {DSET_NAME} shape: {dset.shape}")
logging.info(f"dataset chunks: {dset.chunks}")
results = []
for axis in range(3):
    for _ in range(iter_count):
        result = run_hyperslab_select(dset, axis=axis)
        results.append(result)
f2.close()


for i in range(3):
    avg = get_mean(results, axis=i)
    print(f"  mean axis {i}:  {avg[0]:6.3f} s, {avg[1]:6.3f} mb/s")
avg = get_mean(results)
print(f"mean all axes:  {avg[0]:6.3f} s, {avg[1]:6.3f} mb/s")
