import sys
import h5pyd

if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
    print("usage: python get_status.py domain")
    sys.exit(1)

domain = sys.argv[1]
 
print("domain:", domain)
 
f = h5pyd.File(domain)
 
#dt = [("start", "i8"), ("done", "i8"), ('status', "i4"), ('loadpodname', "S40"),
#       ("x", "i4"), ("y", "i4"), ("nrow", "i4"), ("ncol", "i4")]
table = f["chunk_list"]

header = ""
for name in table.dtype.names:
    header += name
    header += '\t'
print(header)
cursor = table.create_cursor()
for row in cursor:
    line = ""
    for name in table.dtype.names:
        line += str(row[name])
        line += '\t'
    print(line)
 
 
    
