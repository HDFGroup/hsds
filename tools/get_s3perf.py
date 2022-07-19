import sys
import numpy as np

#
# Read given log file and return min/max/avg latency for s3 read requestso
#
data = []
start_time = None
finish_time = None
total_bytes = 0


if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
    print("usage: python get_s3perf.py <dn_logfile>")
    sys.exit(1)

with open(sys.argv[1]) as file:
    for line in file:
        fields = line.split()
        if len(fields) < 4:
            continue
        start = fields[-4]
        finish = fields[-3]
        numbytes = fields[-1]
        if not start.startswith("start="):
            continue
        if not finish.startswith("finish="):
            continue
        if not numbytes.startswith("bytes="):
            continue
        n = start.find("=") + 1
        start = (float)(start[n:])
        n = finish.find("=") + 1
        finish = (float)(finish[n:])
        if start_time is None or start < start_time:
            start_time = start
        if finish_time is None or finish > finish_time:
            finish_time = finish
        n = numbytes.find("=") + 1
        numbytes = (int)(numbytes[n:])
        total_bytes += numbytes
        data.append((start, finish, numbytes))

print("-------------------")

if len(data) == 0:
    print("no relevant log lines found")
    sys.exit(1)

print(f"start_time:  {start_time}")
print(f"finish_time: {finish_time}")
print(f"elpased_time: {(finish_time - start_time):6.2f}")
print(f"total_bytes: {total_bytes}")
bytes_per_sec = total_bytes / (finish_time - start_time)
print(f"MiB/s: {(bytes_per_sec/(1024.0*1024.0)):6.2f}")

# get the maximun number of inflight requests and idle time

dt = np.dtype([("start", float), ("finish", float), ("numbytes", int)])
arr = np.array(data, dtype=dt)
arr = np.sort(arr, order="start")

idle_time = 0.0
max_inflight = 0
inflight = set()  # set of finish times we are waiting for
for i in range(len(arr)):
    row = arr[i]
    start = row[0]
    finish = row[1]
    inflight.add(finish)
    max_min = None
    while True:
        min_finish = min(inflight)
        if min_finish < start:
            inflight.remove(min_finish)
            max_min = min_finish
        else:
            break
    if max_min and len(inflight) == 1:
        # we must have been idle before this operation
        idle_time += start - max_min
    if len(inflight) > max_inflight:
        max_inflight = len(inflight)
print(f"max_inflight: {max_inflight}")
print(f"idle_time: {idle_time:6.2f}")
