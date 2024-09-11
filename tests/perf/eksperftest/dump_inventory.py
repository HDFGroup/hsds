from datetime import datetime
import h5pyd
import config

def formatTime(timestamp):
    # local_timezone = tzlocal.get_localzone() # get pytz timezone
    local_time = datetime.fromtimestamp(timestamp) # , local_timezone)
    return local_time


inventory_domain = config.get("inventory_domain")  

f = h5pyd.File(inventory_domain, "r")
print(f"{inventory_domain} found, owner: {f.owner}, last madified: {datetime.fromtimestamp(f.modified)}")
print("Contents")
print("\tfilename\tStart\tDone\tStatus\tPod")
print("-"*160)
table = f["inventory"]
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
    
    print(f"{filename}\t{start}\t{stop}\t{runtime}\t{rc}\t{podname}")
    
print(f"{table.nrows} rows")
