from datetime import datetime
import sys
import h5pyd
import config

def formatTime(timestamp):
    # local_timezone = tzlocal.get_localzone() # get pytz timezone
    local_time = datetime.fromtimestamp(timestamp) # , local_timezone)
    return local_time

if len(sys.argv) > 1:
    if sys.argv[1] in ("-h", "--help"):
        print(f"usage: python {sys.argv[0]} [inventory_domain]")
        sys.exit(0)
    else:
        inventory_domain = sys.argv[1]
else:
    inventory_domain = config.get("inventory_domain")  

f = h5pyd.File(inventory_domain, "r")
print(f"{inventory_domain} found, owner: {f.owner}, last madified: {datetime.fromtimestamp(f.modified)}")
print("Contents")
print("\tfilename\tStart\tDone\tRuntime\tStatus\tPod")
print("-"*160)
table = f["inventory"]
num_succeeded = 0
num_failed = 0
num_inprogress = 0
num_not_started = 0

for row in table:
    filename = row[0].decode('utf-8')
    if row[1]:
        start = formatTime(row[1])
    else:
        start = 0
    if row[2]:
        stop = formatTime(row[2])
    else:
        stop = 0
    rc = row[3]
    podname = row[4].decode('utf-8')
    if row[2] > 0:
        runtime = f"{int(row[2] - row[1]) // 60:4d}m {(row[2] - row[1]) % 60:2}s"
    else:
        runtime = "0"

    if row[1] == 0:
        num_not_started += 1
    elif row[2] > 0:
        if rc == 0:
            num_succeeded += 1
        else:
            num_failed += 1
    else:
        num_inprogress += 1

    
    print(f"{filename}\t{start}\t{stop}\t{runtime}\t{rc}\t{podname}")

print("="*80)    
print(f"{table.nrows} rows")
print(f"succeeded: \t{num_succeeded}")
print(f"failed: \t {num_failed}")
print(f"in progress: \t {num_inprogress}")
print(f"not started: \t {num_not_started}")
