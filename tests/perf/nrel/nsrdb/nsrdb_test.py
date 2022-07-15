import os
import sys
import random
import logging
import time
import h5py
import h5pyd
import s3fs
import numpy as np

HSDS_BUCKET = "hdflab2"
HDF5_BUCKET = "hdf5.sample"
S3_FOLDER = "data/NREL"
HSDS_FOLDER = "/shared/NREL/nsrdb"
FILENAME = "nsrdb_2000_wind_speed.h5"
NUM_COLS = 17568
NUM_ROWS = 2018392
H5_PATH = "/wind_speed"
OPTIONS = ("--hdf5", "--hsds", "--ros3", "--s3fs")

# Note: currently the ros3 option needs the h5py build from conda-forge

# parse command line args
option = None  # one of OPTIONS
index = None
block = None
log_level = logging.WARNING
for narg in range(1, len(sys.argv)):
    arg = sys.argv[narg]
    if arg in OPTIONS:
        option = arg
    elif arg.startswith("--index="):
        nlen = len("--index=")
        index = int(arg[nlen:])
    elif arg.startswith("--block="):
        nlen = len("--block=")
        block = int(arg[nlen:])
    elif arg.startswith("--loglevel="):
        nlen = len("--loglevel=")
        level = arg[nlen:]
        if level == "debug":
            log_level = logging.DEBUG
        elif level == "info":
            log_level = logging.INFO
        elif level == "warning":
            log_level = logging.WARNING
        elif level == "error":
            log_level = logging.ERROR
        else:
            print("unexpected log level:", log_level)
            sys.exit(1)
    else:
        print(f"unexpected argument: {arg}")

if option is None:
    msg = f"usage: python nsrdb_test.py {OPTIONS} "
    msg == "[--index=n] [--block=n] [--loglevel=debug|info|warning|error]"
    print(msg)
    sys.exit(0)

if index is None:
    # choose a random index
    index = random.randrange(0, NUM_COLS)
if block is None:
    # read entire column in one call
    block = NUM_ROWS

logging.basicConfig(format="%(asctime)s %(message)s", level=log_level)

if option == "--hsds":
    f = h5pyd.File(
        f"{HSDS_FOLDER}/{FILENAME}",
        mode="r",
        use_cache=False,
        bucket=HSDS_BUCKET,
        retries=1,
    )
elif option == "--ros3":
    secret_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_id = secret_id.encode("utf-8")
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    secret_key = secret_key.encode("utf-8")
    s3Url = f"http://{HDF5_BUCKET}.us-west-2.s3.amazonaws.com/{S3_FOLDER}/{FILENAME}"
    h5py._errors.unsilence_errors()  # for better error reporting
    f = h5py.File(
        s3Url,
        mode="r",
        driver="ros3",
        aws_region=b"us-west-2",
        secret_id=secret_id,
        secret_key=secret_key,
    )
elif option == "--s3fs":
    s3 = s3fs.S3FileSystem()
    s3Url = f"s3://{HDF5_BUCKET}/{S3_FOLDER}/{FILENAME}"
    f = h5py.File(s3.open(s3Url, "rb"), "r")
else:
    # --hdf5
    f = h5py.File(FILENAME, mode="r")

# read dataset
dset = f[H5_PATH]
result = np.zeros((NUM_ROWS,), dtype=dset.dtype)
# read by blocks
num_blocks = -(-NUM_ROWS // block)  # integer ceiling
for i in range(num_blocks):
    start = i * block
    end = start + block
    if end > NUM_ROWS:
        end = NUM_ROWS
    ts = time.time()
    arr = dset[index, start:end]
    te = time.time()
    result[start:end] = arr
    msg = f"    read[{start}:{end}]: {arr.min():4.2f}, {arr.max():4.2f}, "
    msg += f"{arr.mean():4.2f}, {te-ts:4.2f} s"
    print(msg)

print(f"{H5_PATH}[{index}:]: {result}")
print(f"{result.min()}, {result.max()}, {result.mean():4.2f}")
