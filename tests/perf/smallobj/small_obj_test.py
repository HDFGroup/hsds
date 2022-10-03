import sys
import time
import asyncio
import logging
import base64
import numpy as np
from aiostream import stream, pipe
from aiohttp import ClientSession, TCPConnector, ClientError
import h5pyd as h5py

globals = {}

class ThingItem:

    def __init__(self, name, age, version, data):
        self.name = name
        self.age = age
        self.version = version
        self.data = data


#Storing approaches

def get_headers():
    """ Return http headers for hsds request """
    headers = {}
    auth_string = globals["username"] + ":" + globals["password"]
    auth_string = auth_string.encode("utf-8")
    auth_string = base64.b64encode(auth_string)
    auth_string = auth_string.decode("utf-8")
    auth_string = "Basic " + auth_string
    headers["Authorization"] = auth_string
    return headers

def get_type_for_value(value):
    """ Return HSDS type for given value """
    if isinstance(value, str):
        data_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": len(value),
            "strPad": "H5T_STR_NULLPAD",
        }
    elif isinstance(value, int):
        data_type = "H5T_STD_I32LE"
    elif isinstance(value, float):
        data_type = "H5T_IEEE_F32LE"
    else:
        raise TypeError("unsupported type")
    return data_type


async def create_attribute(obj_id, attr_name, value):
    logging.info(f"create_attribute({obj_id}, {attr_name}, {value})")
    if obj_id.startswith("g-"):
        req = globals["endpoint"] + "/groups/"
    elif obj_id.startswith("d-"):
        req = globals["endpoint"] + "/datasets/"
    else:
        raise ValueError(f"invalid obj_id: {obj_id}")
        
    req += obj_id + "/attributes/" + attr_name
    headers = get_headers()
    params = {"domain": globals["domain"]}
    client = globals["client"]
    
    attr_type = get_type_for_value(value)

    body = {"type": attr_type, "value": value}
    async with client.put(req, headers=headers, params=params, json=body) as rsp:
        if rsp.status != 201:
            msg = f"PUT {req} failed with status: {rsp.status}, rsp: {rsp}"
            logging.error(msg)
            globals["grp_failed_posts"] += 1
            raise ClientError(f"Unexpected error: status_code: {rsp.status}")
        else:
            logging.info(f"created attribute: {attr_name} for: {obj_id}")
            globals["attribute_count"] += 1


async def create_group(parent_grp_id, grp_name):
    logging.info(f"create_group: {grp_name}")
    req = globals["endpoint"] + "/groups"
    headers = get_headers()
    params = {"domain": globals["domain"]}
    client = globals["client"]
    body = {"link": {"id": parent_grp_id, "name": grp_name}}
    group_id = None
    async with client.post(req, headers=headers, params=params, json=body) as rsp:
        if rsp.status != 201:
            logging.error(f"POST {req} failed with status: {rsp.status}, rsp: {rsp}")
            raise ClientError(f"Unexpected error: status_code: {rsp.status}")
        else:
            logging.info(f"group: {grp_name} created")
        rsp_json = await rsp.json()
        group_id = rsp_json["id"]

    globals["group_count"] += 1
    return group_id


async def create_dataset(parent_grp_id, dataset_name, value=None):
    logging.info("create_dataset:  {dataset_name}")
    req = globals["endpoint"] + "/datasets"
    headers = get_headers()
    params = {"domain": globals["domain"]}
    client = globals["client"]
    dset_type = get_type_for_value(value)
    body =  {"type": dset_type}
    dset_id = None

    # create the dataset
    async with client.post(req, headers=headers, params=params, json=body) as rsp:
        if rsp.status != 201:
            logging.error(f"POST {req} failed with status: {rsp.status}, rsp: {rsp}")
            raise ClientError(f"Unexpected error: status_code: {rsp.status}")
        else:
            logging.info(f"dataset: {dataset_name} created")
        rsp_json = await rsp.json()
        dset_id = rsp_json["id"]

    # link the dataset as dataset_name
    req = globals['endpoint'] + "/groups/" + parent_grp_id + "/links/" + dataset_name
    body = {"id": dset_id}
    async with client.put(req, headers=headers, params=params, json=body) as rsp:
        if rsp.status != 201:
            logging.error(f"PUT {req} failed with status: {rsp.status}, rsp: {rsp}")
            raise ClientError(f"Unexpected error: status_code: {rsp.status}")
        else:
            logging.info(f"dataset: {dataset_name} linked")

    if value is None:
        return dset_id

    # write the scalar value
    req =  globals["endpoint"] + "/datasets/" + dset_id + "/value"
    body = {"value": value}
    async with client.put(req, headers=headers, params=params, json=body) as rsp:
        if rsp.status != 200:
            logging.error(f"PUT {req} failed with status: {rsp.status}, rsp: {rsp}")
            raise ClientError(f"Unexpected error: status_code: {rsp.status}")
        else:
            logging.info(f"dataset: {dataset_name} written")

    globals["dataset_count"] += 1
    return dset_id


async def store(group_name):
    logging.info(f"store({group_name})")
    parent_grp_id = globals["parent_grp_id"]
    group_id = await create_group(parent_grp_id, group_name)
    logging.info(f"store: got group_id: {group_id} for group_name: {group_name}")
    things = globals["things"]
    
    for key, val in things.items():
        logging.debug(f"{key}: {val}")
        if type(val) == dict:
            logging.debug("dict")
            val_grp_id = await create_group(group_id, key)
            logging.debug(f"got val_grp_id: {val_grp_id}")
            # store(g, val)
        elif type(val) == ThingItem:
            logging.info(f"ThingItem - create_group_attributes name for group: {group_id} ")
            val_grp_id = await create_group(group_id, key)
            await create_attribute(val_grp_id, "name", val.name)
            await create_attribute(val_grp_id, "age", val.age)
            await create_attribute(val_grp_id, "version", val.version)
        else:
            await create_dataset(group_id, key, value=val)
            #group.create_dataset(key, data=val)
        
async def store_items(grp_names):
    task_limit = globals["task_limit"]
    max_tcp_connections = globals["max_tcp_connections"]
    session = ClientSession(loop=loop, connector=TCPConnector(limit=max_tcp_connections))
    globals["client"] = session 
    xs = stream.iterate(grp_names) | pipe.map(store, ordered=False, task_limit=task_limit)
    await(xs)
    await session.close()


#
# main
#
N = 100
max_tcp_connections = 10
task_limit = 10
log_level = "error"
domain = None

usage = f"usage {sys.argv[0]} [--N={N}] "
usage += f"[--max-tcp-conn={max_tcp_connections}] [--task-limit={task_limit}] "
usage += f"--loglevel={log_level}] domain"


if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):    
    print(usage)
    sys.exit(1)

for arg in sys.argv:
    if arg == sys.argv[0]:
        continue
    if arg.startswith("-"):
        s = "--max-tcp-conn="
        if arg.startswith(s):
            max_tcp_connections = int(arg[len(s):])
            continue
        s = "--task-limit="
        if arg.startswith(s):
            task_limit = int(arg[len(s):])
            continue
        s = "--N="
        if arg.startswith(s):
            N = int(arg[len(s):])
            continue
        s = "--loglevel="
        if arg.startswith(s):
            log_level = arg[len(s):]
            continue
        raise ValueError(f"unexpected argument: {arg}")
    domain = arg

if not domain:
    print(usage)
    sys.exit(1)

print(f"N: {N}")
print(f"max_tcp_connections: {max_tcp_connections}")
print(f"task_limit: {task_limit}")
print(f"log_level: {log_level}")
print(f"domain: {domain}")

        
if log_level == "debug":
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
elif log_level == "info":
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
elif log_level == "warning":
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.WARNING)
elif log_level == "error":
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.ERROR)
else:
    raise ValueError(f"unexepcted loglevel: {log_level}")

# set globals

globals["domain"] = domain
globals["N"] = N
globals["max_tcp_connections"] = max_tcp_connections
globals["task_limit"] = task_limit
globals["dataset_count"] = 0
globals["group_count"] = 0
globals["attribute_count"] = 0


totRunningTime = 0

# Creating test data

child= {}
child["name"] = "John"
child["age"] = "32"
child["address"] = "some street"

itm = ThingItem("Jens", 42, 1, child)

things = {}
things["item1"] = 42
things["item2"] = "string test"
things["child1"] = itm
things["child2"] = itm
things["child3"] = itm
things["child4"] = itm

globals["things"] = things


loop = asyncio.get_event_loop()

#
# Running the test
#


timingsData = np.zeros(N)
timingsIm = np.zeros(N)

logging.info("creating domain: {fqdn}")
with h5py.File(domain, mode="w") as f:
    start = time.time()
    g = f.require_group("/test")
    globals["parent_grp_id"] = g.id.id
    globals["endpoint"] = f.id.http_conn.endpoint
    globals["username"] = f.id.http_conn.username
    globals["password"] = f.id.http_conn.password

grp_names = []
for i in range(N):
    grp_names.append(f"g{i:04d}")
loop.run_until_complete(store_items(grp_names))

end = time.time()

timingsData[i] = end-start
print(f"Saving small datasets: {timingsData[i]:.2f} s" )

im = np.random.randint(0,10,size=[6000,4000], dtype=np.int16)

things["im"] = im
start = time.time()
with h5py.File(domain, mode="a") as f:
    f["im"] = im

end = time.time()
timingsIm[i] = end-start
print(f"Saving image: {timingsIm[i]:.2f} s" )

print("group count:", globals["group_count"])
print("dataset count:", globals["dataset_count"])
print("attribute_count:", globals["attribute_count"])

logging.info("done")

print("")

