import sys
import random
import h5pyd as h5py
import logging

SHOT_FOLDER = "/cmod/"

def read_group(grp, data_map):
    print(f"    group:   {grp.name} pr")
    for k in grp:
        obj = grp[k]
        if isinstance(obj, h5py.Group):
            # recursive call to sub-group
            read_group(obj, data_map)
        elif isinstance(obj, h5py.Dataset):
            # read all the data for the dataset
            arr = obj[...]
            # save to the map
            data_map[obj.name] = arr
        else:
            # ignore ctypes, external links, softlinks, etc.
            print(f"ignoring {obj.name} - type: {type(obj)}")

def read_shot(domain_path):
    print(f"domain: {domain_path}")
    f = h5py.File(domain_path)
    data_map = {}
    read_group(f, data_map)
    for k in data_map:
        v = data_map[k]
        print(f"        {k}: {v.shape}")

def main():
    
    loglevel = logging.WARNING
    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)

    if len(sys.argv) > 1:
        if sys.argv[1] in ("-h", "--help"):
            print(f"usage: python {sys.argv[0]} <shot_count> <seed>")
            sys.exit(1)
        num_shots = int(sys.argv[1])
    else:
        num_shots = 100

    if len(sys.argv) > 2:
        rand_seed = int(sys.argv[2])
        random.seed(rand_seed)
    
    names = []
    # shots.txt is expected be a list of shot numbers
    with open("shots.txt") as f:
        while True:
            line = f.readline()
            if not line:
                break
            name = line.strip()
            if not name or name[0] == "#":
                continue

            names.append(name)  
        
    shots = random.sample(names, num_shots)
    for shot in shots:
        read_shot(SHOT_FOLDER+shot)
    print("done!")

main()

