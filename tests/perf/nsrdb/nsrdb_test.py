import os
import sys
import random
import h5py
import h5pyd

HSDS_BUCKET = "nrel-pds-hsds" 
HDF5_BUCKET = "nrel-pds-nsrdb"
HSDS_FOLDER = "/nrel/nsrdb/"
FILENAME = "v3/nsrdb_2000.h5" 
SHAPE = (17568, 2018392)
H5_PATH = "/wind_speed"
OPTIONS = ("--hdf5", "--hsds", "--ros3")

index = None  

if len(sys.argv) < 2 or sys.argv[1] not in OPTIONS:
    print(f"usage: python nsrdb_test.py {OPTIONS} [--index=n]")
    sys.exit(0)
if sys.argv[1] == "--hsds":
    f = h5pyd.File(HSDS_FOLDER+FILENAME, mode='r', use_cache=False, bucket=HSDS_BUCKET)
elif sys.argv[1] == "--ros3":
    secret_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_id = secret_id.encode('utf-8')
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    secret_key = secret_key.encode('utf-8')
    s3Url = f"http://{HDF5_BUCKET}.s3.amazonaws.com/{FILENAME}"
    f = h5py.File(s3Url, mode='r', driver='ros3', aws_region=b'us-west-2', secret_id=secret_id, secret_key=secret_key)
else:
    # --hdf5
    f = h5py.File(FILENAME, mode='r')
if len(sys.argv) > 2 and sys.argv[2].startswith("--index="):
    index = int(sys.argv[2][len("--index="):])
else:
    # choose a random index
    index = random.randrange(0, SHAPE[0])

dset = f[H5_PATH]
print(dset)
arr = dset[index, :]
print(f"{H5_PATH}[{index}:]: {arr}")
print(f"{arr.min():4.2f}, {arr.max():4.2f}, {arr.mean():4.2f}")






