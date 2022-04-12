import sys
import numpy as np
import h5pyd

if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
    print("usage: python create_empty.py domain nrows ncols")
    sys.exit(1)

domain = sys.argv[1]
nrows = int(sys.argv[2])
ncols = int(sys.argv[3])

print("domain:", domain)
print("nrows:", nrows)
print("ncols:", ncols)

f = h5pyd.File(domain, 'w')
dset = f.create_dataset("dset", (nrows, ncols), dtype='f8')
print("dset:", dset)
print("dset chunks:", dset.chunks)
print("dset id:", dset.id.id)

dt = [("start", "i8"), ("done", "i8"), ('status', "i4"), 
       ("x", "i4"), ("y", "i4"), ("nrow", "i4"), ("ncol", "i4"), ('pod', "S40")]

num_chunks = 0
for _ in dset.iter_chunks():
    num_chunks += 1
print(f"{num_chunks} chunks in dataset dset")
arr = np.zeros((num_chunks,), dtype=dt)  
index = 0 
for s in dset.iter_chunks():
    x = s[0].start
    y = s[1].start
    nrow = s[0].stop - s[0].start
    ncol = s[1].stop - s[1].start
    arr[index] = (0,0,0,x,y,nrow,ncol,"")
    index += 1
f.create_dataset("chunk_list", data=arr)
print("created chunk_list dataset")
 