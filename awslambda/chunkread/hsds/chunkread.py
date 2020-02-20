import time
import base64
from . import config
from . import hsds_logger as log
from .util.dsetUtil import getChunkLayout, getDeflateLevel, isShuffle
from .util.hdf5dtype import createDataType, getItemSize
from .util.chunkUtil import getChunkSize, chunkReadSelection, chunkReadPoints
from .util.idUtil import getS3Key
from .util.storUtil import getStorBytes
from .util.arrayUtil import bytesToArray, arrayToBytes


def get_app():
    app = {}
    app["start_time"] = time.time()
    app["bucket_name"] = config.get("bucket_name")
    return app

def get_chunk(app, chunk_id, dset_json, bucket=None, s3path=None, s3offset=0, s3size=0):

    chunk_arr = None
    chunk_dims = getChunkLayout(dset_json)
    type_json = dset_json["type"]
    dt = createDataType(type_json)
    item_size = getItemSize(type_json)
    if not isinstance(item_size, int):
        item_size = 8  # pointer to varible length data

    # note - officially we should follow the order in which the filters are defined in the filter_list,
    # but since we currently have just deflate and shuffle we will always apply deflate then shuffle on read,
    # and shuffle then deflate on write
    # also note - get deflate and shuffle will update the deflate and shuffle map so that the s3sync will do the right thing
    deflate_level = getDeflateLevel(dset_json)
    shuffle = isShuffle(dset_json)
    s3key = None

    if s3path:
        if not s3path.startswith("s3://"):
            # TBD - verify these at dataset creation time?
            log.error(f"unexpected s3path for getChunk: {s3path}")
            raise  KeyError()
        path = s3path[5:]
        index = path.find('/')   # split bucket and key
        if index < 1:
            log.error(f"s3path is invalid: {s3path}")
            raise KeyError()
        bucket = path[:index]
        s3key = path[(index+1):]
        log.debug(f"Using s3path bucket: {bucket} and  s3key: {s3key}")
    else:
        s3key = getS3Key(chunk_id)
        log.debug(f"getChunk chunkid: {chunk_id} bucket: {bucket}")

    chunk_size = getChunkSize(chunk_dims, item_size)
    chunk_bytes = getStorBytes(app, s3key, shuffle=shuffle, deflate_level=deflate_level, offset=s3offset, length=s3size, bucket=bucket)

    if isinstance(chunk_size, int) and len(chunk_bytes) != chunk_size:
        log.error(f"Expected to read {chunk_size} bytes, but  got {len(chunk_bytes)}")
        raise KeyError()
    chunk_arr = bytesToArray(chunk_bytes, dt, chunk_dims)

    return chunk_arr


# read hyperslab from chunk
def read_hyperslab(app, params):
    if "chunk_id" not in params:
        msg = "chunk_id not in params"
        log.warn(msg)
        raise KeyError()
    chunk_id = params["chunk_id"]

    if "dset_json" not in params:
        msg = "dset_json not in params"
        log.warn(msg)
        raise KeyError()
    dset_json = params["dset_json"]

    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = config.get("bucket_name")
    if not bucket:
        msg = "bucket not specified"
        log.warn(msg)
        raise KeyError()

    chunk_arr = get_chunk(app, chunk_id, dset_json, bucket=bucket)

    if "slices" in params:
        arr = chunkReadSelection(chunk_arr, slices=params["slices"])
    else:
        arr = chunk_arr

    bdata = arrayToBytes(arr)
    base64data = base64.b64encode(bdata)

    return base64data.decode('ascii')

# 
# read point selection from chunk
def read_points(app, params):
    if "chunk_id" not in params:
        msg = "chunk_id not in params"
        log.warn(msg)
        raise KeyError()
    chunk_id = params["chunk_id"]

    if "dset_json" not in params:
        msg = "dset_json not in params"
        log.warn(msg)
        raise KeyError()
    dset_json = params["dset_json"]

    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = config.get("bucket_name")
    if not bucket:
        msg = "bucket not specified"
        log.warn(msg)
        raise KeyError()

    if "point_arr" not in params:
        msg = "point_arr not in params"
        log.warn(msg)
        raise KeyError()

    point_arr = params["point_arr"]

    chunk_layout = getChunkLayout(dset_json)

    chunk_arr = get_chunk(app, chunk_id, dset_json, bucket=bucket)

    arr = chunkReadPoints(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, point_arr=point_arr)

    return arr
