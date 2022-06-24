import random
import os
import sys
import time
import logging
from multiprocessing import Pool
import h5pyd

cfg = {}


def get_argval(arg):
    # get text after '=' char
    fields = arg.split("=")
    if len(fields) != 2:
        raise ValueError(f"Unexpected argument: {arg}")
    return fields[1]


def get_timeseries(filepath):
    # return timeseries for given filepath
    use_cache = cfg["use_cache"]
    bucket = cfg["bucket"]
    h5path = cfg["h5path"]
    index = cfg["index"]
    f = h5pyd.File(filepath, "r", use_cache=use_cache, bucket=bucket)
    dset = f[h5path]
    logging.info(f"dset: {dset} id: {dset.id.id}")
    logging.info(f"shape: {dset.shape}")
    logging.info(f"chunks: {dset.chunks}")

    ts = time.time()
    arr = dset[:, index]
    elapsed = time.time() - ts
    logging.info(f"get_timeseries {filepath}[::,{index}]: {elapsed:6.2f}s")
    return arr


def print_stats(filepath, index, arr):
    # print min, max, mean valaues
    msg = f"{filepath} - arr[:,{index}]: {arr.min():6.2f}, "
    msg += f"{arr.max():6.2f}, {arr.mean():6.2f}"
    print(msg)


#
# Main
#
station_count = 2488136
folderpath = "/nrel/wtk/conus/"
year_start = 2007
num_years = 8
iter_count = 1
use_mp = False
cfg["h5path"] = "windspeed_80m"
cfg["bucket"] = "nrel-pds-hsds"
cfg["use_cache"] = False

loglevel = logging.WARNING
logging.basicConfig(format="%(asctime)s %(message)s", level=loglevel)


if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help"):
    msg = "usage: python wtk_conus_test.py [--folder=<folderpath>] "
    msg += "[--h5path=dataset_name] [--index=index] [--bucket=bucket_name] "
    msg += "[--iter=count] [--usecache] [--mp]"
    print(msg)
    print(f"    --folder: path to wtk conus files (defalt: {folderpath})")
    print(f"    --h5path: hdf5 path to dataset (default: {cfg['h5path']})")
    print("    --index:  location index [0-2488135] (default: random))")
    print(f"    --bucket_name: S3 bucket name (default: {cfg['bucket']}")
    print(f"    --iter:  number of times to repeat test (default: {iter_count})")
    print("    --mp: use multiprocessing to run year look ups in parallel ")
    print("           (default: run serially)")
    print("    --usecache: set use_cache to True in h5py.File open (default: False)")
    print(" ")
    print("example: python wtk_conus_test.py --folder=/nrel/wtk/conus/")
    print("or with bucket arg: ")
    print("python wtk_conus_test.py --folder=/nrel/wtk/conus --bucket=nrel-pds-hsds")
    sys.exit(0)


for arg in sys.argv:
    if arg == sys.argv[0]:
        pass
    elif arg.startswith("--folderpath="):
        folderpath = get_argval(arg)
    elif arg.startswith("--h5path="):
        cfg["h5path"] = get_argval(arg)
    elif arg.startswith("--bucket="):
        cfg["bucket"] = get_argval(arg)
    elif arg.startswith("--index="):
        cfg["index"] = int(get_argval(arg))
    elif arg.startswith("--iter="):
        iter_count = int(get_argval(arg))
    elif arg == "--mp":
        use_mp = True
    elif arg == "--usecache":
        cfg["use_cache"] = True
    else:
        raise ValueError(f"unexpected argument: {arg}")

max_index = station_count - iter_count
logging.info(f"cfg: {cfg}")

index = None
if index is None:
    index = random.randint(0, max_index)
elif index > max_index:
    raise ValueError("index is too large")
logging.info(f"index: {index}")
cfg["index"] = index

filepaths = []
for i in range(num_years):
    filename = f"wtk_conus_{2007+i}.h5"
    filepath = os.path.join(folderpath, filename)
    filepaths.append(filepath)


if __name__ == "__main__":
    ts = time.time()

    if use_mp:
        with Pool(num_years) as pool:
            for i in range(iter_count):
                year_arrs = pool.map(get_timeseries, filepaths)
                for i in range(num_years):
                    filepath = filepaths[i]
                    arr = year_arrs[i]
                    print_stats(filepath, index, arr)

                cfg["index"] += 1
    else:
        for i in range(iter_count):
            for i in range(num_years):
                filepath = filepaths[i]
                arr = get_timeseries(filepath)
                print_stats(filepath, index, arr)

            cfg["index"] += 1

    print("------------")

    elapsed = time.time() - ts
    print(f"elapsed time: {elapsed:6.2f}")
