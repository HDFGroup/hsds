import sys
import random
import h5pyd as h5py
from h5pyd import MultiManager
import logging

SHOT_FOLDER = "/cmod/"

def read_group(grp, datasets):
    for k in grp:
        obj = grp[k]
        if isinstance(obj, h5py.Group):
            read_group(obj, datasets)  # recursive call
        elif isinstance(obj, h5py.Dataset):
            # all to the list of datasets
            datasets.append(obj)
        else:
            # ignore ctypes, external links, softlinks, etc.
            print(f"ignoring {obj.name} - type: {type(obj)}")
    

def read_shot(domain_path):
    print(f"domain: {domain_path}")
    f = h5py.File(domain_path)
    datasets = []
    read_group(f, datasets)

    if datasets:
        # do a multi-read
        print(f"mm {len(datasets)} datasets")
        mm = MultiManager(datasets)
        arr_list = mm[...]
        for i in range(len(datasets)):
            dset = datasets[i]
            arr = arr_list[i]
            print(f"{dset.name}: {arr.shape}")
 
def main():

    loglevel = logging.DEBUG
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)

    run_forever = False
    if len(sys.argv) > 1:
        if sys.argv[1] in ("-h", "--help"):
            print(f"usage: python {sys.argv[0]} <shot_count|--nostop> <seed>")
            sys.exit(1)
        if sys.argv[1] == "--nostop":
            run_forever = True
            num_shots = 1
        else:
            num_shots = int(sys.argv[1])
    else:
        num_shots = 0

    if len(sys.argv) > 2:
        rand_seed = int(sys.argv[2])
        random.seed(rand_seed)

    names = []

    with open("shots.txt") as f:
        while True:
            line = f.readline()
            if not line:
                break
            name = line.strip()
            if not name or name[0] == "#":
                continue

            names.append(name)  
    print("num_shots:", num_shots)    
    while True:
        shots = random.sample(names, num_shots)
        for shot in shots:
            read_shot(SHOT_FOLDER+shot)
        if not run_forever:
            break

    print("done!")

main()

