import sys
import socket
import tempfile
import time
from random import randint
from multiprocessing import shared_memory
import numpy as np
import config

host = config.get("host")
port = int(config.get("port"))

NUM_BYTES = int(config.get("num_bytes"))
if NUM_BYTES % 8 != 0:
    print("choose a num_bytes value divisible by 8")
    sys.exit(1)
SOCKET_TYPE = config.get("socket_type")  # AF_UNIX or AF_INET
if SOCKET_TYPE == "AF_UNIX":
    socket_type = socket.AF_UNIX
else:
    socket_type = socket.AF_INET
nextent = NUM_BYTES // 8  # np.random uses 8-byte floats
use_shared_mem = config.get("use_shared_mem")
if use_shared_mem:
    shm_block = shared_memory.SharedMemory(create=True, size=NUM_BYTES)
else:
    shm_block = None

# print("creating rand arr")
# print(time.time())
arr = np.random.rand(nextent)
# print(time.time())
# print('to buffer')
if shm_block:
    shm_block.buf[:NUM_BYTES] = arr.tobytes()[:]
    buffer = shm_block.name.encode("ascii")
else:
    buffer = arr.tobytes()
# print(time.time())
tmp_dir = tempfile.TemporaryDirectory()

with socket.socket(socket_type, socket.SOCK_STREAM) as s:
    if SOCKET_TYPE == "AF_UNIX":
        addr = tmp_dir.name + "/sock_perf.s"
        print(f"connecting to {addr}")
    else:
        if not port:
            # pick a random port
            port = randint(49152, 65535)
        addr = (host, port)
        print(f"connecting to: {host}:{port}")
    s.bind(addr)
    s.listen()
    conn, addr = s.accept()
    with conn:
        print("Connected by", addr)
        remaining = len(buffer)
        while remaining > 0:
            print(f"sending {remaining}")
            sent = conn.send(buffer[(len(buffer) - remaining) :])
            print(f"{sent} bytes sent")
            remaining -= sent

if shm_block:
    shm_block.close()
print("done")
