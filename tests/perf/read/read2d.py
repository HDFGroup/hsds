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
options["xlen"] = 1000
options["ylen"] = 1000
options["stride"] = 5
options["iter"] = 1

if len(sys.argv) == 1 or sys.argv[1] in ("-h", "--help"):
    msg = f"Usage: python {sys.argv[0]} "
    msg += "[--bucket=bucket_name] "
    msg += "[--h5path=h5path] "
    msg += "[--xlen=xlen] "
    msg += "[--ylen=ylen] "
    msg += "[--stride=stride] "
    msg += "[--iter=n] "
    msg += "filepath"
    print(msg)
    print("\nExamples:\n")
    example1 = "--bucket=nrel-pds-hsds --h5path=wind_speed "
    example1 += "hdf5://nrel/nsrdb/conus/nsrdb_conus_2018.h5"
    print(f"    python {sys.argv[0]} {example1}")
    sys.exit(1)

for arg in sys.argv:
    if arg == sys.argv[0]:
        continue
    if arg.startswith("--"):
        get_option(options, arg)
    else:
        filepath = arg

print("filepath:", filepath)

if filepath.startswith("hdf5://"):
    bucket = options["bucket"]
    print("bucket:", bucket)
    f = h5pyd.File(filepath, bucket=bucket)
else:
    f = h5py.File(filepath)

h5path = options["h5path"]
dset = f[h5path]

print(f"dset shape: {dset.shape}")
chunks = dset.chunks
if isinstance(chunks, dict):
    chunks = chunks["dims"]
print(f"dset chunks: {chunks}")

xlen = options["xlen"]
if xlen > dset.shape[0]:
    raise ValueError(f"xlen must be less than {dset.shape[0]}")

ylen = options["ylen"]
if ylen > dset.shape[1]:
    raise ValueError(f"ylen must be less than {dset.shape[1]}")

iter_count = options["iter"]
for iter_num in range(iter_count):
    if iter_count > 1:
        print(f"\niteration: {iter_num}\n")

    # do simple hyperslab of xlen:ylen:zlen at random location
    i = random.randint(0, dset.shape[0] - xlen)
    j = random.randint(0, dset.shape[1] - ylen)

    ts = time.time()
    arr = dset[i:(i + xlen), j:(j + ylen)]
    te = time.time()

    msg = f"contiguous selection: [{i}:{i + xlen}, {j}:{j + ylen}]: "
    msg += f"{arr.min():4.2f}, {arr.max():4.2f}, "
    msg += f"{arr.mean():4.2f}, {te-ts:4.2f} s"
    print(msg)

    # do strided selection
    stride = options["stride"]
    if stride == 0:
        print("stride value is zero, skipping stride test")
    elif dset.shape[0] // stride < xlen:
        print("stride value too high, skipping stride test")
    else:
        start = random.randint(0, dset.shape[0] - (xlen * stride))
        end = start + (xlen * stride)
        j = random.randint(0, dset.shape[1] - ylen)

        ts = time.time()
        arr = dset[start:end:stride, j:(j + ylen)]
        te = time.time()
        print("arr.shape:", arr.shape)

        msg = f"strided selection: [{start}:{end}:{stride}, {j}:{j + ylen}]: "
        msg += f"{arr.min():4.2f}, {arr.max():4.2f}, "
        msg += f"{arr.mean():4.2f}, {te-ts:4.2f} s"
        print(msg)

    # do fancy selection
    if xlen > dset.shape[0] // 2:
        print("xlen to large, skipping fancy selection")
    else:
        indices = []
        while len(indices) < xlen:
            n = random.randint(0, dset.shape[0] - 1)
            if n not in indices:
                indices.append(n)
        indices.sort()
        j = random.randint(0, dset.shape[1] - ylen)
        ts = time.time()
        arr = dset[indices, j:(j + ylen)]
        te = time.time()
        print("arr.shape:", arr.shape)

        msg = f"fancy selection: [[{indices[0]}, {indices[1]},..., "
        msg += f"{indices[-2]}, {indices[-1]}], "
        msg += f"{j}:{j + ylen}]: "
        msg += f"{arr.min():4.2f}, {arr.max():4.2f}, "
        msg += f"{arr.mean():4.2f}, {te-ts:4.2f} s"
        print(msg)
