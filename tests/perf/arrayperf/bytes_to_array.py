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
import time
import numpy as np
import sys

from hsds.util.arrayUtil import (
    arrayToBytes,
    bytesToArray,
    getByteArraySize,
)

if len(sys.argv) < 2:
    count = 1_000_000
elif sys.argv[1] in ("-h", "--help"):
    sys.exit(f"usage: python {sys.argv[0]} count")
else:
    count = int(sys.argv[1])

# VLEN of strings
dt = np.dtype("O", metadata={"vlen": str})

strings = np.array("the quick brown fox jumps over the lazy dog".split(), dtype=dt)

arr = np.random.choice(strings, count)

then = time.time()
buffer_size = getByteArraySize(arr)
now = time.time()
msg = f"getByteArraySize - elapsed: {(now-then):6.4f} for {count} elements, "
msg += f"returned {buffer_size}"
print(msg)
then = time.time()
buffer = arrayToBytes(arr)
now = time.time()
print(f"arrayToBytes - elpased: {(now-then):6.4f} for {count} elements")
if len(buffer) != buffer_size:
    raise ValueError(f"unexpected buffer length: {len(buffer)}")
then = time.time()
copy = bytesToArray(buffer, dt, (count,))
now = time.time()
if copy.shape[0] != count:
    raise ValueError(f"unexpected array shape: {copy.shape}")
print(f"bytesToArray - elapsed: {(now-then):6.4f}")
