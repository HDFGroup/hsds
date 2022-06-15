import sys
import h5pyd
import h5py

if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
    print("usage: python get_status.py [-v] filepath")
    sys.exit(1)

if sys.argv[1] == "-v":
    verbose = True
    filepath = sys.argv[2]
else:
    filepath = sys.argv[1]
    verbose = False

print("filepath:", filepath)

if filepath.startswith("hdf5://"):
    f = h5pyd.File(filepath)
else:
    f = h5py.File(filepath)

task_table = f["task_list"]
pod_counts = {}

if verbose:
    header = ""
    for name in task_table.dtype.names:
        header += name
        header += "\t"
    print(header)

complete_count = 0
inprogress_count = 0
pending_count = 0
start_time = None
finish_time = None
for s in task_table.iter_chunks():
    arr = task_table[s]
    for i in range(len(arr)):
        row = arr[i]
        line = ""
        for name in task_table.dtype.names:
            line += str(row[name])
            line += "\t"
        status = row["status"]
        if row["start"] > 0 and (start_time is None or start_time > row["start"]):
            start_time = row["start"]
        if row["done"] > 0 and (finish_time is None or finish_time < row["done"]):
            finish_time = row["done"]
        pod_name = row["pod"].decode("ascii")
        if pod_name:
            if pod_name not in pod_counts:
                pod_counts[pod_name] = 0
            pod_counts[pod_name] += 1
        if status == -1:
            inprogress_count += 1
        elif status == 1:
            complete_count += 1
        else:
            pending_count += 1
        if verbose:
            print(line)

print("-----------")
if len(pod_counts) > 0:
    print(f"pod counts ({len(pod_counts)} pods):")
    for k in pod_counts:
        print(f"    {k}: {pod_counts[k]}")
    print("----------")

print(f"pending:     {pending_count}")
print(f"complete:    {complete_count}")
print(f"in progress: {inprogress_count}")

if start_time is not None and finish_time is not None:
    print(f"elapsed time: {(finish_time - start_time):.2f} s")
