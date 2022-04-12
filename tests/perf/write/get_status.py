import sys
import h5pyd

if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
    print("usage: python get_status.py domain")
    sys.exit(1)

domain = sys.argv[1]
 
print("domain:", domain)
 
f = h5pyd.File(domain)
 
table = f["chunk_list"]

header = ""
for name in table.dtype.names:
    header += name
    header += '\t'
print(header)
cursor = table.create_cursor()
complete_count = 0
inprogress_count = 0
pending_count = 0
pod_counts = {}
for row in cursor:
    line = ""
    for name in table.dtype.names:
        line += str(row[name])
        line += '\t'
    status = row['status']
    pod_name = row['pod'].decode('ascii')
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
    print(line)

print("-----------")
if len(pod_counts) > 0:
    print("pod counts:")
    for k in pod_counts:
        print(f"    {k}: {pod_counts[k]}") 
    print("----------")

print(f"pending:     {pending_count}")
print(f"complete:    {complete_count}")
print(f"in progress: {inprogress_count}")
 
 
    
