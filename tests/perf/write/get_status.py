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
success_count = 0
fail_count = 0
pending_count = 0
for row in cursor:
    line = ""
    for name in table.dtype.names:
        line += str(row[name])
        line += '\t'
    print(line)
 
 
    
