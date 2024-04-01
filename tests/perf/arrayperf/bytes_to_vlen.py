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
import random
import numpy as np
import sys

from hsds.util.arrayUtil import (
    arrayToBytes,
    bytesToArray,
    getByteArraySize,
)

""" Time bytesToArray and arrayToBytes with VLEN str type

Got the following result with python 3.11:

    $ time python bytes_to_vlen.py 50000
    getByteArraySize - elapsed: 0.0298 for 50000 elements, returned 2728131
    arrayToBytes - elpased: 0.4168 for 50000 elements
    bytesToArray - elpased: 0.0986 for 50000 elements
"""

if len(sys.argv) < 2:
    count = 50_000
elif sys.argv[1] in ("-h", "--help"):
    sys.exit(f"usage: python {sys.argv[0]} count")
else:
    count = int(sys.argv[1])

dt = np.dtype("O", metadata={"vlen": str})
arr = np.zeros((count,), dtype=dt)

# create a list of random strings from 1 to 100 chars
buffer = bytearray(100)
for j in range(count):
    str_len = random.randint(1, 100)
    for i in range(str_len):
        buffer[i] = ord('a') + random.randint(0, 25)
    s = buffer[:str_len].decode()
    arr[j] = s

then = time.time()
buffer_size = getByteArraySize(arr)
now = time.time()
msg = f"getByteArraySize - elapsed: {(now - then):6.4f} for {count} elements, "
msg += f"returned {buffer_size}"
print(msg)
then = time.time()
buffer = arrayToBytes(arr)
now = time.time()
print(f"arrayToBytes - elpased: {(now - then):6.4f} for {count} elements")

# convert back to a numpy array
then = time.time()
arr_ret = bytesToArray(buffer, dt, [count, ])
now = time.time()
print(f"bytesToArray - elpased: {(now - then):6.4f} for {count} elements")

# verify that same original strings got returned
for i in range(count):
    if arr[i] != arr_ret[i]:
        msg = f"compare failure for element {i}: "
        msg += f"{arr[i]} vs {arr_ret[i]}"
        raise ValueError(msg)
