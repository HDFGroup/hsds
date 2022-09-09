import h5pyd
import h5py
import time
import random
import sys


def get_option(options, arg):
    if not arg.startswith("--"):
        raise ValueError(f"no an option arg: {arg}")
    n = arg.find("=")
    if n < 0:
        raise ValueError(f"no '=' char in arg: {arg}")
    key = arg[2:n]
    if key not in options:
        raise KeyError(f"Invalid option: {arg}")
    val = arg[(n + 1):]
    default = options[key]
    if isinstance(default, int):
        val = int(val)
    elif isinstance(default, float):
        val = float(val)
    options[key] = val


filepath = "hdf5://shared/sample/snp500.h5"
filepath = "hdf5://shared/ghcn/ghcn.h5"
options = {}
options["bucket"] = None
options["h5path"] = "/data"
options["field"] = "data_value"
options["count"] = 5000
options["stride"] = 5

if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
    msg = f"Usage: python {sys.argv[0]} "
    msg += "[--bucket=bucket_name] "
    msg += "[--h5path=h5path] "
    msg += "[--field=field_name] "
    msg += "[--count=read_count] "
    msg += "[--stride=stride] "
    msg += "filepath"
    print(msg)
    print("\nExamples:\n")
    print(f"    python {sys.argv[0]} hdf5://shared/ghcn/ghcn.h5")
    print(f"    python {sys.argv[0]} --field=open --h5path=dset /shared/sample/snp500.h5")
    sys.exit(1)

for arg in sys.argv:
    if arg == sys.argv[0]:
        continue
    if arg.startswith("--"):
        get_option(options, arg)
    else:
        filepath = arg

if filepath.startswith("hdf5://"):
    bucket = options["bucket"]
    f = h5pyd.File(filepath, bucket=bucket)
else:
    f = h5py.File(filepath)

h5path = options["h5path"]
dset = f[h5path]

num_rows = dset.shape[0]
print(f"num_rows: {num_rows}")

# read contiguous set of rows
read_count = options["count"]

start = random.randint(0, num_rows - read_count)
end = start + read_count
ts = time.time()
arr = dset[start:end]
te = time.time()

field_name = options["field"]

arr_field = arr[field_name]
msg = f"consecutive read[{start}:{end}]: {arr_field.min():4.2f}, {arr_field.max():4.2f}, "
msg += f"{arr_field.mean():4.2f}, {te-ts:4.2f} s"
print(msg)

# read random set of columns
indices = []
while len(indices) < read_count:
    n = random.randint(0, num_rows - 1)
    if n not in indices:
        indices.append(n)
indices.sort()

ts = time.time()
arr = dset[indices]
te = time.time()

arr_field = arr[field_name]
msg = "random index with stride read[[n0,n1,...,nx]]: "
msg += f"f{arr_field.min():4.2f}, {arr_field.max():4.2f}, "
msg += f"{arr_field.mean():4.2f}, {te-ts:4.2f} s"
print(msg)


# read with stride
stride = options["stride"]
if stride == 0:
    print("stride value is zero, skipping stride test")
elif num_rows // stride < read_count:
    print("stride value too high, skipping stride test")
else:
    start = random.randint(0, num_rows - (read_count * stride))
    end = start + (read_count * stride)
    ts = time.time()
    arr = dset[start:end:stride]
    te = time.time()
    arr_field = arr[field_name]
    msg = f"random index read[{start}:{end}:{stride}]: "
    msg += f"{arr_field.min():4.2f}, {arr_field.max():4.2f}, "
    msg += f"{arr_field.mean():4.2f}, {te-ts:4.2f} s"
    print(msg)
