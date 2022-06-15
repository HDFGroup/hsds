import datetime
import sys
import random
import logging
import time
import h5py
import h5pyd
import numpy as np

DSET_NAME = "data"
CHUNK_LAYOUT = 262144  # 4 MB chunks

sensor_seq = {}


def usage():
    """usage message and quit"""
    msg = "usage: python append_1d.py [--loglevel=debug|info|warning|error] [--maxrows=n] "
    msg += "[--dump] filepath"
    print(msg)
    print(" use hdf5:// prefix to denote use of HSDS domain rather than hdf5 filepath")
    sys.exit(0)


def addRow(dset):
    """add a row to the datset"""
    rows = dset.shape[0]
    now = time.time()
    sensor = random.randrange(0, 16)
    if sensor not in sensor_seq:
        sensor_seq[sensor] = 0
    sensor_seq[sensor] += 1
    mtype = random.randrange(0, 4)
    value = random.random()  # range 0 - 1
    row = (now, sensor, sensor_seq[sensor], mtype, value)
    if isinstance(dset.id.id, str):
        # use table append method
        dset.append(
            [
                row,
            ]
        )
    else:
        dset.resize(rows + 1, axis=0)
        dset[rows] = (now, sensor, sensor_seq[sensor], mtype, value)
    return True


#
# main
#

# parse command line args
log_level = logging.WARNING
filepath = None
maxrows = 0
mode = "a"
for narg in range(1, len(sys.argv)):
    arg = sys.argv[narg]
    if arg.startswith("--loglevel="):
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
    elif arg == "--dump":
        # will print last numrows
        mode = "r"
    elif arg in ("-h", "--help"):
        usage()
    elif arg.startswith("--maxrows="):
        nlen = len("--maxrows=")
        maxrows = int(arg[nlen:])
    elif arg.startswith("-"):
        usage()

    else:
        filepath = arg

if filepath is None:
    usage()


# setup logging
logging.basicConfig(format="%(asctime)s %(message)s", level=log_level)

# open file with h5py or h5pyd based on prefix
if filepath.startswith("hdf5://"):
    f = h5pyd.File(filepath, mode=mode)
else:
    # --hdf5
    f = h5py.File(filepath, mode=mode)

# initialize dataset if not created already
if DSET_NAME not in f:
    # 16 bytes per element
    dt = np.dtype(
        [
            ("timestamp", np.float64),
            ("sensor", np.int16),
            ("seq", np.int32),
            ("mtype", np.int16),
            ("value", np.float32),
        ]
    )
    dset = f.create_dataset(
        DSET_NAME, (0,), maxshape=(None,), chunks=(CHUNK_LAYOUT,), dtype=dt
    )
dset = f[DSET_NAME]
print(dset)
start = dset.shape[0]
if mode == "r":
    # dump out maxrows
    if maxrows == 0 or maxrows > start:
        maxrows = start
    # go back maxrows
    start -= maxrows
count = 0
start_ts = time.time()

if maxrows == 0:
    print("press ^C to quit")

# append rows to maxrows (if not 0) is reached or user quits
try:
    while True:
        if mode == "r":
            index = count + start
            e = dset[index]
            ts = datetime.datetime.fromtimestamp(e["timestamp"])
            s = ts.isoformat(sep=" ", timespec="milliseconds")
            msg = f"{index:12}: {s} {e['sensor']:8} {e['seq']:8} "
            msg += f"{e['mtype']:8} -- {e['value']:06.4f}"
            print(msg)
            count += 1
        else:
            if addRow(dset):
                count += 1
        if maxrows > 0 and count == maxrows:
            break
except KeyboardInterrupt:
    print("got keyboard interrupt")
    # end forever loop


f.close()

# print out stats
if mode == "a":
    end_ts = time.time()
    print(f"added {count} rows in {(end_ts-start_ts):8.4f} seconds")
    print(f"{count/(end_ts-start_ts):5.4f} rows/sec")
