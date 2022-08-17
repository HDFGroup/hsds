##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of HSDS (HDF5 Scalable Data Service), Libraries and      #
# Utilities.  The full HSDS copyright notice, including                      #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be fouERRORnd at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################
from __future__ import print_function
import math
import numpy as np
import logging
import time
import sys

ncols = 12000
nrows = 2200
use_h5py = False
filepath = None
compression = "gzip"
loglevel = "warn"
CHUNKS = (1500, 275)



if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
    s = "usage: python stream_test_h5pyd.py "
    s += "[--ncols=n] "
    s += "[--nrows=m] "
    s += "[--comp=none|gzip]"
    s += "[--loglevel=debug|info|warn|error]"
    print(s)
    print("for filepath, use hdf5:// prefix for h5pyd, posix path for h5py")
    print("defaults...")
    print(f"ncols={ncols}")
    print(f"nrows={nrows}")
    print(f"comp={compression}")
    print(f"loglevel={loglevel}")
    print("")
    print("example: python stream_test_h5py.py hdf5://home/test_user1/bigfile.h5")
    sys.exit(0)


for arg in sys.argv:
    print(arg)
    if arg == sys.argv[0]:
        continue
    if arg.startswith("--ncols="):
        arglen = len("--ncols=")
        ncols = int(arg[arglen:])
    elif arg.startswith("--nrows="):
        arglen = len("--nrows=")
        nrows = int(arg[arglen:])
    elif arg.startswith("--comp="):
        arglen = len("--comp=")
        if len(arg) == arglen:
            compression = None  # no compresion
        else:
            compression = arg[arglen:]
    elif arg.startswith("--loglevel"):
        arglen = len("--loglevel=")
        loglevel = arg[arglen:]
    elif arg.startswith("--"):
        sys.exit(f"unexpected option: {arg}")
    else:
        filepath = arg

if loglevel == "debug":
    level = logging.DEBUG
elif loglevel == "info":
    level = logging.INFO
elif loglevel == "warn":
    level = logging.WARNING
elif loglevel == "error":
    level = logging.ERROR
else:
    sys.exit(f"unexpected loglevel: {loglevel}")

logging.basicConfig(format='%(asctime)s %(message)s', level=level)


if filepath.startswith("hdf5://"):
    import h5pyd as h5py
else:
    import h5py


dt = np.dtype("u8")

print(f"opening: {filepath}")

f = h5py.File(filepath, "a")

if "dset2d" in f:
    # delete if dims have been updated
    dset2d = f["dset2d"]
    if dset2d.shape[0] != nrows or dset2d.shape[1] != ncols:
        del f["dset2d"]

if "dset2d" not in f:
    # create dataset
    f.create_dataset("dset2d", (nrows, ncols), dtype=dt, chunks=CHUNKS, compression=compression)

dset2d = f["dset2d"]

# initialize numpy array to test values
print("initialzing data")

arr = np.zeros((nrows, ncols), dtype=dt)
exp = int(math.log10(ncols)) + 1
for i in range(nrows):
    row_start_value = i * 10 ** exp
    for j in range(ncols):
        arr[i, j] = row_start_value + j + 1

print("writing...")
num_bytes = nrows * ncols * dt.itemsize
ts = time.time()
dset2d[:, :] = arr[:, :]
elapsed = time.time() - ts
mb_per_sec = num_bytes / (1024 * 1024 * elapsed)
print(f" elapsed: {elapsed:.2f} s, {mb_per_sec:.2f} MB/s")

# read back the data as binary
print("reading...")
ts = time.time()
arr_copy = dset2d[:, :]

elapsed = time.time() - ts
mb_per_sec = num_bytes / (1024 * 1024 * elapsed)
print(f" elapsed: {elapsed:.2f} s, {mb_per_sec:.2f} MB/s")

if not np.array_equal(arr, arr_copy):
    print("arrays don't match!")
else:
    print("passed!")
