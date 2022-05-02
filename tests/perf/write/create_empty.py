import sys
import numpy as np
import h5pyd
import h5py

if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
    print("usage: python create_empty.py filepath nrows ncols [row|col|block")
    print("  filepath: posix or hsds path to file/domain")
    print("   use 'hdf5://' prefix for HSDS domains")
    print("  nrows: number of rows in dataset")
    print("  cols: number of cols in dataset")
    print("  row|col|block: iteration type; create task table based on...")
    print("     row: row by row selection")
    print("     col: column by column selection")
    print("     block: chunk by chunk selection")
    sys.exit(1)

filepath = sys.argv[1]
nrows = int(sys.argv[2])
ncols = int(sys.argv[3])
iter_type = sys.argv[4]

print("filepath:", filepath)
print("nrows:", nrows)
print("ncols:", ncols)
print("iter_type:", iter_type)
if iter_type not in ("row", "col", "block"):
    raise ValueError("invalid iteration type")

if filepath.startswith("hdf5://"):
    f = h5pyd.File(filepath, 'w')
else:
    f = h5py.File(filepath, 'w')

chunks = [500, 1000] # 3.8 MB/chunk
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

num_tasks = 0
if iter_type == "block":
    for _ in dset.iter_chunks():
        num_tasks += 1
elif iter_type == "row":
    num_tasks = nrows
else:
    # col
    num_tasks = ncols
print(f"{num_tasks} tasks")
if num_tasks < 1000:
    task_chunks = (num_tasks,)
else:
    task_chunks = (1000,)

arr = np.zeros((num_tasks,), dtype=dt)  
index = 0 

if iter_type == "block":
    for s in dset.iter_chunks():
        x = s[0].start
        y = s[1].start
        nrow = s[0].stop - s[0].start
        ncol = s[1].stop - s[1].start
        arr[index] = (0,0,0,x,y,nrow,ncol,"")
        index += 1
        
elif iter_type == "row":
    for i in range(nrows):
        x = i
        y = 0
        nrow = 1
        ncol = ncols
        arr[index] = (0,0,0,x,y,nrow,ncol,"")
        index += 1
else:
    # col
    for i in range(ncols):
        x = 0
        y = i
        nrow = nrows
        ncol = 1
        arr[index] = (0,0,0,x,y,nrow,ncol,"")
        index += 1

if index != num_tasks:
    print("index:", index)
    print("num_tasks:", num_tasks)
    raise ValueError("expected index to be equal to num_tasks")

f.create_dataset("task_list", data=arr, chunks=task_chunks)

f.close()
print("created chunk_list dataset")
 
