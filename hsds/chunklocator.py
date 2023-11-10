import sys
import time
import h5py
import s3fs
import numpy as np
from . import config
from . import hsds_logger as log
from .util.arrayUtil import bytesArrayToList, getNumElements
from .util.dsetUtil import getSelectionList, getSelectionShape


def get_cmd_options():
    """ read command line options and return as dict """
    required = ("fileuri", "h5path", "select")
    cmd_options = {}
    for option in required:
        val = config.getCmdLineArg(option)
        if val is None:
            msg = f"expected cmd argument for: {option}"
            log.error(msg)
            sys.exit(-1)
        cmd_options[option] = val
    return cmd_options


def h5open(fileuri):
    """ return handle to given hdf5 file """
    if fileuri.startswith("s3://"):
        s3 = s3fs.S3FileSystem()
        f = h5py.File(s3.open(fileuri, 'rb'))
    else:
        # just regular file open
        f = h5py.File(fileuri)
    return f


def get_chunktable_dims(dset):
    """ Get expected dimensions of the dataset's chunk table,
        or return None if the dataset is not chunked """
    rank = len(dset.shape)
    if not dset.chunks:
        return None
    chunk_dims = dset.chunks
    table_dims = []
    for dim in range(rank):
        dset_extent = dset.shape[dim]
        chunk_extent = chunk_dims[dim]

        if chunk_extent > 0:
            table_extent = -(dset_extent // -chunk_extent)
        else:
            table_extent = 0
        table_dims.append(table_extent)
    table_dims = tuple(table_dims)
    return table_dims


def get_chunktable_dtype(include_file_uri=False):
    """ get dtype for chunktable """
    if include_file_uri:
        log.warning("include_file_uri not yet supported")
    else:
        dt = np.dtype([("offset", np.int64), ("size", np.int32)])
    return dt


def get_chunk_table_index(chunk_offset, chunk_dims):
    """ get index into the chunktable for given offset """
    if len(chunk_offset) != len(chunk_dims):
        msg = f"Unexpectted chunk offset: {chunk_offset}"
        log.error(msg)
        sys.exit(-1)
    rank = len(chunk_offset)
    chunk_index = []
    for i in range(rank):
        chunk_index.append(chunk_offset[i] // chunk_dims[i])
    return tuple(chunk_index)


def get_storage_info(dset, select=None):
    """Collect dataset storage information"""

    def chunk_callback(chunk_info, args):
        # log.debug(f"chunk_callback - chunk_info: {chunk_info}")
        if "chunkinfo_arr" not in args:
            msg = "expected chunkinfo_arr key to be in callback args"
            log.error(msg)
            raise KeyError(msg)
        arr = args["chunkinfo_arr"]
        if "select" not in args:
            msg = "expected select key to be found in callback args"
            log.error(msg)
            raise KeyError(msg)
        slices = args["select"]
        rank = len(slices)
        index = get_chunk_table_index(chunk_info.chunk_offset, chunk_dims)
        skip = False
        arr_index = []
        for dim in range(rank):
            s = slices[dim]
            n = index[dim]
            if n < s.start or n >= s.stop:
                skip = True
                break
            arr_index.append(n - s.start)
        if not skip:
            e = (chunk_info.byte_offset, chunk_info.size)
            arr[tuple(arr_index)] = e
            log.debug(f"added arr[{arr_index}] = {e}")
    # end chunk_callback

    if dset.shape is None:
        # Empty (null) dataset...
        log.warn("Null space dataset")
        return None

    rank = len(dset.shape)
    if rank < 1:
        # no chunking for scalar datasets
        log.warn("Scalar space dataset")
        return None

    chunk_dims = dset.chunks
    if chunk_dims is None:
        msg = f"Dataset {dset.name} is not chunked"
        log.warn(msg)
        return None

    log.debug(f"got chunk_dims: {chunk_dims}")

    chunktable_dims = get_chunktable_dims(dset)
    log.debug(f"using chunktable_dims: {chunktable_dims}")

    if select:
        slices = getSelectionList(select, chunktable_dims)
    else:
        slices = []
        for i in range(rank):
            slices.append(slice(0, chunktable_dims[i]))

    log.debug(f"got slices: {slices}")
    arr_shape = getSelectionShape(slices)
    log.debug(f"arr_shape: {arr_shape}")

    dtype = get_chunktable_dtype()
    # initilize chunk table array
    chunkinfo_arr = np.zeros(arr_shape, dtype=dtype)

    dsid = dset.id

    num_chunks = dsid.get_num_chunks()
    if num_chunks == 0:
        log.info(f"no chunks found in dataset; {dset.name}")
        return chunkinfo_arr  # return zero array

    log.info(f"dataset {dset.name} contains {num_chunks} chunks")

    numChunkTableElements = getNumElements(chunktable_dims)
    log.debug(f"max count of chunks: {numChunkTableElements}")
    numSelectElements = getNumElements(arr_shape)
    log.debug(f"numSelectElements: {numSelectElements}")

    # Go over all the chunks...
    # TBD: when a selection is used, would it be better to use get_chunk_info_by_coord?
    #   Might be slower than current method of getting all chunks and throwing out what we
    #   don't need.  cf. arrayUtil.IndexIterator to iterate through all indices
    use_iter = True
    chunk_callback_args = {"chunkinfo_arr": chunkinfo_arr, "select": slices}
    try:
        h5py.h5d.visitchunks(dsid, chunk_callback, chunk_callback_args)
    except AttributeError:
        log.warn("visitchunks not supported")
        use_iter = False

    if not use_iter:
        for i in range(num_chunks):
            chunk_info = dsid.get_chunk_info(i)  # TBD: space_id would help?
            chunk_callback(chunk_info, chunk_callback_args)

    return chunkinfo_arr


#
# main
#
def main():
    # setup log config
    log_level = config.get("log_level")
    prefix = config.get("log_prefix")
    log_timestamps = config.get("log_timestamps", default=False)
    log.setLogConfig(log_level, prefix=prefix, timestamps=log_timestamps)
    start_time = time.time()
    log.info(f"chunklocator start: {start_time:.2f}")

    cmd_options = get_cmd_options()
    h5path = cmd_options["h5path"]
    log.info(f"h5path: {h5path}")
    fileuri = cmd_options["fileuri"]
    log.info(f"fileuri: {fileuri}")

    select = cmd_options["select"]
    log.info(f"select: {select}")
    try:
        with h5open(fileuri) as f:
            if h5path not in f:
                msg = f"Did not find {h5path} in {fileuri}"
                log.warn(msg)
                sys.exit(-1)
            dset = f[h5path]
            log.info(f"got dataset: {dset}")

            arr = get_storage_info(dset, select)
            if arr is None:
                msg = f"no chunk array returned for {h5path} in {fileuri} - not chunked?"
                log.warn(msg)
                sys.exit(-1)
            log.info(f"got chunk array shape: {arr.shape}")
            try:
                json_data = bytesArrayToList(arr)
            except ValueError as err:
                raise err
            # print list data to stdout
            print(json_data)
            log.info(f"got {len(json_data)} json elements")

    except FileNotFoundError:
        msg = f"file not found: {fileuri}"
        log.warn(msg)
        sys.exit(1)

    log.info('done')
    stop_time = time.time()
    log.info(f"chunklocator stop: {stop_time:.2f}")
    log.info(f"chunklocator elapsed: {(stop_time-start_time):.2f}")
