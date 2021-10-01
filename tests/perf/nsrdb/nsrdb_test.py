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
OPTIONS = ("--hdf5", "--hsds")

index = random.randrange(0, SHAPE[0])

if len(sys.argv) < 2 or sys.argv[1] not in OPTIONS:
    print(f"usage: python nsrdb_test.py {OPTIONS}")
    sys.exit(0)
if sys.argv[1] == "--hsds":
    f = h5pyd.File(HSDS_FOLDER+FILENAME, mode='r', use_cache=False, bucket=HSDS_BUCKET)
    
else:
    # --hdf5
    f = h5py.File(FILENAME, mode='r')
dset = f[H5_PATH]
print(dset)
arr = dset[index, :]
print(f"{H5_PATH}[{index}:]: {arr}")
print(f"{arr.min():4.2f}, {arr.max():4.2f}, {arr.mean():4.2f}")






