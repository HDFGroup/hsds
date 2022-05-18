##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of HSDS (HDF5 Scalable Data Service), Libraries and      #
# Utilities.  The full HSDS copyright notice, including                      #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################
from __future__ import print_function
import math
import numpy as np
import time

# set to 0 to use h5pyd/HSDS
USE_H5PY=1

NCOLS=1200
NROWS=2200
CHUNKS=(1500, 275)

if USE_H5PY:
    import h5py
    FILE_NAME = "bigfile.h5"
else:
    import h5pyd as h5py
    FILE_NAME = "/home/h5user/stream/bigfile.h5"

dt = np.dtype('u8')

f = h5py.File(FILE_NAME, 'a')

if "dset2d" in f:
    # delete if dims have been updated
    dset2d = f['dset2d']
    if dset2d.shape[0] != NROWS or dset2d.shape[1] != NCOLS:
        del f['dset2d']

if "dset2d" not in f:
    # create dataset
    f.create_dataset('dset2d', (NROWS,NCOLS), dtype=dt, chunks=CHUNKS)

dset2d = f['dset2d']

# initialize numpy array to test values
print("initialzing data")

arr = np.zeros((NROWS, NCOLS), dtype=dt)       
exp = int(math.log10(NCOLS)) + 1
for i in range(NROWS):
    row_start_value = i * 10 ** exp
    for j in range(NCOLS):
        arr[i,j] = row_start_value + j + 1
        
print("writing...")
num_bytes = NROWS*NCOLS*dt.itemsize
ts = time.time()
dset2d[:,:] = arr[:,:]        
elapsed = time.time() - ts
mb_per_sec = num_bytes / (1024 * 1024 * elapsed)
print(f" elapsed: {elapsed:.2f} s, {mb_per_sec:.2f} mb/s")

# read back the data as binary
print("reading...")
ts = time.time()
arr_copy = dset2d[:,:]
        
elapsed = time.time() - ts
mb_per_sec = num_bytes / (1024 * 1024 * elapsed)
print(f" elapsed: {elapsed:.2f} s, {mb_per_sec:.2f} mb/s")
         
if not np.array_equal(arr, arr_copy):
    print("arrays don't match!")
else:
    print("passed!")

