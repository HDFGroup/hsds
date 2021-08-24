import socket
import sys
import time
from multiprocessing import shared_memory
import config

BATCH_SIZE = int(config.get("batch_size"))
NUM_BYTES = int(config.get("num_bytes"))
if len(sys.argv) < 2 or sys.argv[1] in ("-h", ("--help")):
    print("usage: python client.py <addr>")
    sys.exit(1)
addr = sys.argv[1]
if addr.find(':') < 0:
    socket_type = socket.AF_UNIX
else:
    socket_type = socket.AF_INET
    fields = addr.split(':')
    host = fields[0]
    port = int(fields[1])

data = None
tStart = None
tEnd = None
total_bytes = 0
use_shared_mem = config.get("use_shared_mem")
shm_block_name = None
buffer = bytearray(NUM_BYTES)

with socket.socket(socket_type, socket.SOCK_STREAM) as s:
    if socket_type == socket.AF_UNIX:
        s.connect(addr) 
    else:
        s.connect((host, port))
    tStart = time.time()
    while True:
        try:
            data = s.recv(BATCH_SIZE)
            if not data:
                break
            #print(f"got {len(data)} bytes")
            if use_shared_mem:
                # read the name of the shm block from socket
                shm_name = data.decode("ascii")
                print("got shared memory name:", shm_name)
            else:
                # copy bytes to buffer
                buffer[total_bytes:(total_bytes+len(data))] = data
            total_bytes += len(data)
        except KeyboardInterrupt:
            print("quiting")
            break
tEnd = time.time()


if use_shared_mem:
    if not shm_name:
        print("expected to get shared memory block name")
        sys.exit(1)
    # try attaching to the shm block
    shm_block = shared_memory.SharedMemory(name=shm_name)
    # copy data from shared memory block to buffer
    print("copying data from shared memory block")
    print(time.time())
    buffer[:] = shm_block.buf[:NUM_BYTES]
    print(f"copy complete: {NUM_BYTES} bytes")
    total_bytes = NUM_BYTES
    print(time.time())
    shm_block.close()
    shm_block.unlink()

if total_bytes > 1024*1024:
    print(f"mb: {total_bytes//(1024*1024)}")
else:
    print(f"bytes: {total_bytes}")
print(f"Elapsed time :: {(tEnd - tStart):6.3f} s, {(total_bytes/(tEnd-tStart))/(1024*1024):6.2f} Mb/s")
   

