import sys
import numpy as np
import h5pyd
import h5py

if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
    print("usage: python create_empty.py filepath nrows ncols")
    print("   use 'hdf5://' prefix for HSDS domains")
    sys.exit(1)

filepath = sys.argv[1]
nrows = int(sys.argv[2])
ncols = int(sys.argv[3])

print("filepath:", filepath)
print("nrows:", nrows)
print("ncols:", ncols)

if filepath.startswith("hdf5://"):
    f = h5pyd.File(filepath, 'w')
else:
    f = h5py.File(filepath, 'w')

chunks = [700, 700] # 3.7 MB/chunk
if chunks[0] > nrows:
    chunks[0] = nrows
if chunks[1] > ncols:
    chunks[1] = ncols
chunks = tuple(chunks)
dset = f.create_dataset("dset", (nrows, ncols), dtype='f8', chunks=chunks)
print("dset:", dset)
print("dset chunks:", dset.chunks)
print("dset id:", dset.id.id)

dt = [("start", "f8"), ("done", "f8"), ('status', "i4"), 
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
num_chunks = len(arr)
print("num_chunks:", num_chunks)
if num_chunks < 1000:
    table_chunks = (num_chunks,)
else:
    table_chunks = (1000,)
f.create_dataset("chunk_list", data=arr, chunks=table_chunks)
f.close()
print("created chunk_list dataset")
 