##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of HSDS (HDF5 Scalable Data Service), Libraries and      #
# Utilities.  The full HSDS copyright notice, including                      #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################

import numpy as np

MAX_VLEN_ELEMENT=1000000  # restrict largest vlen element to one million

"""
Convert list that may contain bytes type elements to list of string elements

TBD: Need to deal with non-string byte data (hexencode?)
"""
def bytesArrayToList(data):
    if type(data) in (bytes, str):
        is_list = False
    elif isinstance(data, (np.ndarray, np.generic)):
        if len(data.shape) == 0:
            is_list = False
            data = data.tolist()  # tolist will return a scalar in this case
            if type(data) in (list, tuple):
                is_list = True
            else:
                is_list = False
        else:
            is_list = True
    elif type(data) in (list, tuple):
        is_list = True
    else:
        is_list = False

    if is_list:
        out = []
        for item in data:
            out.append(bytesArrayToList(item)) # recursive call
    elif type(data) is bytes:
        out = data.decode("utf-8")
    else:
        out = data

    return out

"""
Convert a list to a tuple, recursively.
Example. [[1,2],[3,4]] -> ((1,2),(3,4))
"""
def toTuple(rank, data):
    if type(data) in (list, tuple):
        if rank > 0:
            return list(toTuple(rank-1, x) for x in data)
        else:
            return tuple(toTuple(rank-1, x) for x in data)
    else:
        return data

"""
Get size in bytes of a numpy array.
"""
def getArraySize(arr):
    nbytes = arr.dtype.itemsize
    for n in arr.shape:
        nbytes *= n
    return nbytes

"""
Helper - get num elements defined by a shape
TODO: refactor some function in dsetUtil.py
"""
def getNumElements(dims):
    num_elements = 0
    if isinstance(dims, int):
        num_elements = dims
    elif isinstance(dims, (list, tuple)):
        num_elements = 1
        for dim in dims:
            num_elements *= dim
    else:
        raise ValueError("Unexpected argument")
    return num_elements

"""
Get dims from a given shape json.  Return [1,] for Scalar datasets,
  None for null dataspaces
"""
def getShapeDims(shape):
    dims = None
    if isinstance(shape, int):
        dims = [shape,]
    elif isinstance(shape, list) or isinstance(shape, tuple):
        dims = shape  # can use as is
    elif isinstance(shape, str):
        # only valid string value is H5S_NULL
        if shape != 'H5S_NULL':
            raise ValueError("Invalid value for shape")
        dims = None
    elif isinstance(shape, dict):
        if "class" not in shape:
            raise ValueError("'class' key not found in shape")
        if shape["class"] == 'H5S_NULL':
            dims = None
        elif shape["class"] == 'H5S_SCALAR':
            dims = [1,]
        elif shape["class"] == 'H5S_SIMPLE':
            if "dims" not in shape:
                raise ValueError("'dims' key expected for shape")
            dims = shape["dims"]
        else:
            raise ValueError("Unknown shape class: {}".format(shape["class"]))
    else:
        raise ValueError("Unexpected shape class: {}".format(type(shape)))

    return dims


"""
Return numpy array from the given json array.
"""
def jsonToArray(data_shape, data_dtype, data_json):
    # utility function to initialize vlen array
    def fillVlenArray(rank, data, arr, index):
        for i in range(len(data)):
            if rank > 1:
                index = fillVlenArray(rank-1, data[i], arr, index)
            else:
                arr[index] = data[i]
                index += 1
        return index


    # need some special conversion for compound types --
    # each element must be a tuple, but the JSON decoder
    # gives us a list instead.
    if len(data_dtype) > 1 and not isinstance(data_json, (list, tuple)):
        raise TypeError("expected list data for compound data type")
    npoints = getNumElements(data_shape)
    np_shape_rank = len(data_shape)

    if type(data_json) in (list, tuple):
        converted_data = []
        if npoints == 1 and len(data_json) == len(data_dtype):
            converted_data.append(toTuple(0, data_json))
        else:
            converted_data = toTuple(np_shape_rank, data_json)
        data_json = converted_data
    else:
        data_json = [data_json,]  # listify

    if isVlen(data_dtype):
        arr = np.zeros((npoints,), dtype=data_dtype)
        fillVlenArray(np_shape_rank, data_json, arr, 0)
    else:
        try:
            arr = np.array(data_json, dtype=data_dtype)
        except UnicodeEncodeError as ude:
            msg = "Unable to encode data"
            raise ValueError(msg) from ude
    # raise an exception of the array shape doesn't match the selection shape
    # allow if the array is a scalar and the selection shape is one element,
    # numpy is ok with this
    if arr.size != npoints:
        msg = "Input data doesn't match selection number of elements"
        msg += " Expected {}, but received: {}".format(npoints, arr.size)
        raise ValueError(msg)
    if arr.shape != data_shape:
        arr = arr.reshape(data_shape)  # reshape to match selection

    return arr

"""
Return True if the type contains variable length elements
"""
def isVlen(dt):
    is_vlen = False
    if len(dt) > 1:
        names = dt.names
        for name in names:
            if isVlen(dt[name]):
                is_vlen = True
                break
    else:
        if dt.metadata and "vlen" in dt.metadata:
            is_vlen = True
    return is_vlen

"""
Get number of byte needed to given element as a bytestream
"""
def getElementSize(e, dt):
    #print("getElementSize - e: {}  dt: {}".format(e, dt))
    if len(dt) > 1:
        count = 0
        for name in dt.names:
            field_dt = dt[name]
            field_val = e[name]
            count += getElementSize(field_val, field_dt)
    elif not dt.metadata or "vlen" not in dt.metadata:
        count = dt.itemsize  # fixed size element
    else:
        # variable length element
        vlen = dt.metadata["vlen"]
        if isinstance(e, int):
            if e == 0:
                count = 4  # non-initialized element
            else:
                raise ValueError("Unexpected value: {}".format(e))
        elif isinstance(e, bytes):
            count = len(e) + 4
        elif isinstance(e, str):
            count = len(e.encode('utf-8')) + 4
        elif isinstance(e, np.ndarray):
            nElements = np.prod(e.shape)
            if e.dtype.kind != 'O':
                count = e.dtype.itemsize * nElements
            else:
                arr1d = e.reshape((nElements,))
                count = 0
                for item in arr1d:
                    count += getElementSize(item, dt)
            count += 4  # byte count
        elif isinstance(e, list) or isinstance(e, tuple):
            #print("got list for e:", e)
            count = len(e) * vlen.itemsize + 4  # +4 for byte count
        else:
            raise TypeError("unexpected type: {}".format(type(e)))
    return count


"""
Get number of bytes needed to store given numpy array as a bytestream
"""
def getByteArraySize(arr):
    if not isVlen(arr.dtype):
        return arr.itemsize * np.prod(arr.shape)
    nElements = np.prod(arr.shape)
    # reshape to 1d for easier iteration
    arr1d = arr.reshape((nElements,))
    dt = arr1d.dtype
    count = 0
    for e in arr1d:
        count += getElementSize(e, dt)
    return count

"""
Copy to buffer at given offset
"""
def copyBuffer(src, des, offset):
    #print(f"copyBuffer - src: {src} offset: {offset}")
    for i in range(len(src)):
        des[i+offset] = src[i]

    #print("returning:", offset + len(src))
    return offset + len(src)

"""
Copy element to bytearray
"""
def copyElement(e, dt, buffer, offset):
    #print(f"copyElement - dt: {dt}  offset: {offset}")
    if len(dt) > 1:
        for name in dt.names:
            field_dt = dt[name]
            field_val = e[name]
            offset = copyElement(field_val, field_dt, buffer, offset)
    elif not dt.metadata or "vlen" not in dt.metadata:
        #print("e vlen: {} type: {} itemsize: {}".format(e, type(e), dt.itemsize))
        e_buf = e.tobytes()
        #print("tobytes:", e_buf)
        if len(e_buf) < dt.itemsize:
            # extend the buffer for fixed size strings
            #print("extending buffer")
            e_buf_ex = bytearray(dt.itemsize)
            for i in range(len(e_buf)):
                e_buf_ex[i] = e_buf[i]
            e_buf = bytes(e_buf_ex)

        #print("length:", len(e_buf))
        offset = copyBuffer(e_buf, buffer, offset)
    else:
        # variable length element
        vlen = dt.metadata["vlen"]
        #print("copyBuffer vlen:", vlen)
        if isinstance(e, int):
            #print("copyBuffer int")
            if e == 0:
                # write 4-byte integer 0 to buffer
                offset = copyBuffer(b'\x00\x00\x00\x00', buffer, offset)
            else:
                raise ValueError("Unexpected value: {}".format(e))
        elif isinstance(e, bytes):
            #print("copyBuffer bytes")
            count = np.int32(len(e))
            if count > MAX_VLEN_ELEMENT:
                raise ValueError("vlen element too large")
            offset = copyBuffer(count.tobytes(), buffer, offset)
            offset = copyBuffer(e, buffer, offset)
        elif isinstance(e, str):
            #print("copyBuffer, str")
            text = e.encode('utf-8')
            count = np.int32(len(text))
            if count > MAX_VLEN_ELEMENT:
                raise ValueError("vlen element too large")
            offset = copyBuffer(count.tobytes(), buffer, offset)
            offset = copyBuffer(text, buffer, offset)

        elif isinstance(e, np.ndarray):
            nElements = np.prod(e.shape)
            #print("copyBuffer ndarray, nElements:", nElements, "kind:", e.dtype.kind)

            if e.dtype.kind != 'O':
                count = np.int32(e.dtype.itemsize * nElements)
                #print("copyBuffeer got vlen count:", count)
                #print("copyBuffer e:", e)
                if count > MAX_VLEN_ELEMENT:
                    raise ValueError("vlen element too large")
                offset = copyBuffer(count.tobytes(), buffer, offset)
                #print("copyBuffer write new count, offset:", offset)
                offset = copyBuffer(e.tobytes(), buffer, offset)
                #print("copyBuffer write data, offset:", offset)
            else:
                arr1d = e.reshape((nElements,))
                for item in arr1d:
                    offset = copyElement(item, dt, buffer, offset)

        elif isinstance(e, list) or isinstance(e, tuple):
            #print("cooyBuffer list/tuple  vlen:", vlen, "e:", e)
            count = np.int32(len(e) * vlen.itemsize)
            offset = copyBuffer(count.tobytes(), buffer, offset)
            if isinstance(e, np.ndarray):
                arr = e
            else:
                arr = np.asarray(e, dtype=vlen)
            offset = copyBuffer(arr.tobytes(), buffer, offset)

        else:
            raise TypeError("unexpected type: {}".format(type(e)))
        #print("buffer: {}".format(buffer))
    return offset

"""
Get the count value from persisted vlen array
"""
def getElementCount(buffer, offset):
    count_bytes = bytes(buffer[offset:(offset+4)])

    try:
        count = int(np.frombuffer(count_bytes, dtype="<i4"))
    except TypeError as e:
        msg = "Unexpected error reading count value for variable length elemennt: {}".format(e)
        raise TypeError(msg)
    if count < 0:
        # shouldn't be negative
        raise ValueError("Unexpected count value for variable length element")
    if count > MAX_VLEN_ELEMENT:
        # expect variable length element to be between 0 and 1mb
        raise ValueError("Variable length element size expected to be less than 1MB")
    return count


"""
Read element from bytearrray
"""
def readElement(buffer, offset, arr, index, dt):
    #print(f"readElement, offset: {offset}, index: {index} dt: {dt}")

    if len(dt) > 1:
        e = arr[index]
        for name in dt.names:
            field_dt = dt[name]
            offset = readElement(buffer, offset, e, name, field_dt)
    elif not dt.metadata or "vlen" not in dt.metadata:
        count = dt.itemsize
        e_buffer = buffer[offset:(offset+count)]
        offset += count
        try:
            e = np.frombuffer(bytes(e_buffer), dtype=dt)
            #print(f"setting index: {index} to {e}")
            arr[index] = e[0]
        except ValueError:
            #print(f"ERROR: ValueError setting {e_buffer} and dtype: {dt}")
            raise
    else:
        # variable length element
        vlen = dt.metadata["vlen"]
        #print("reading vlen element:", vlen)
        e = arr[index]

        if isinstance(e, np.ndarray):
            nelements = np.prod(dt.shape)
            #print("ndarray, nelements:", nelements)
            e.reshape((nelements,))
            for i in range(nelements):
                offset = readElement(buffer, offset, e, i, dt)
            e.reshape(dt.shape)
        else:
            count = getElementCount(buffer, offset)
            #print("getElementCount:", count)
            offset += 4
            if count > 0:
                e_buffer = buffer[offset:(offset+count)]
                offset += count

                if vlen is bytes:
                    arr[index] = bytes(e_buffer)
                elif vlen is str:
                    s = e_buffer.decode("utf-8")
                    arr[index] = s
                else:
                    #print("np.fromBuffer buffer len: ", len(e_buffer), "dtype:", vlen)
                    try:
                        e = np.frombuffer(bytes(e_buffer), dtype=vlen)
                    except ValueError:
                        #print("ValueError -- e_buffer:", e_buffer, "dtype:", vlen)
                        raise
                    arr[index] = e
    #print("readElement returning offset:", offset)
    return offset



"""
Return byte representation of numpy array
"""
def arrayToBytes(arr):
    if not isVlen(arr.dtype):
        # can just return normal numpy bytestream
        return arr.tobytes()

    nSize = getByteArraySize(arr)
    buffer = bytearray(nSize)
    offset = 0
    nElements = np.prod(arr.shape)
    arr1d = arr.reshape((nElements,))
    for e in arr1d:
        #print("arrayToBytes:", e)
        offset = copyElement(e, arr1d.dtype, buffer, offset)
    return bytes(buffer)

"""
Create numpy array based on byte representation
"""
def bytesToArray(data, dt, shape):
    #print(f"bytesToArray({len(data)}, {dt}, {shape}")
    nelements = getNumElements(shape)
    if not isVlen(dt):
        # regular numpy from string
        arr = np.frombuffer(data, dtype=dt)
    else:
        arr = np.zeros((nelements,), dtype=dt)
        offset = 0
        for index in range(nelements):
            offset = readElement(data, offset, arr, index, dt)
    arr = arr.reshape(shape)
    # check that we can update the array if needed
    # Note: this seems to have been required starting with numpuy v 1.17
    # Setting the flag directly is not recommended. cf: https://github.com/numpy/numpy/issues/9440

    if not arr.flags['WRITEABLE']:
        arr_copy = arr.copy()
        arr = arr_copy

    return arr
