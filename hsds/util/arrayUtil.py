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
import hsds_logger as log

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
    # need some special conversion for compound types --
    # each element must be a tuple, but the JSON decoder
    # gives us a list instead.
    if len(data_dtype) > 1 and not isinstance(data_json, (list, tuple)):
        raise TypeError("expected list data for compound data type")
    npoints = getNumElements(data_shape)
    
    if type(data_json) in (list, tuple):
        np_shape_rank = len(data_shape)
        converted_data = []
        if npoints == 1 and len(data_json) == len(data_dtype):
            converted_data.append(toTuple(0, data_json))
        else:  
            converted_data = toTuple(np_shape_rank, data_json)
        data_json = converted_data

    arr = np.array(data_json, dtype=data_dtype)
    # raise an exception of the array shape doesn't match the selection shape
    # allow if the array is a scalar and the selection shape is one element,
    # numpy is ok with this
    if arr.size != npoints:
        msg = "Input data doesn't match selection number of elements"
        msg += " Expected {}, but received: {}".format(npoints, arr.size)
        log.warn(msg)
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
        elif isinstance(e, np.ndarray) or isinstance(e, list) or isinstance(e, tuple):
            count = len(e) * vlen.itemsize + 4  # +4 for byte count
        else:
            raise TypeError("unexpected type: {}".format(type(e)))
    #print("size for {}: {}".format(e, count))
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
    for i in range(len(src)):
        des[i+offset] = src[i]
         
    return offset + len(src)

"""
Copy element to bytearray
"""
def copyElement(e, dt, buffer, offset):
    #print("copyElement: {} offset: {}".format(e, offset))
    
    if len(dt) > 1:
        for name in dt.names:
            field_dt = dt[name]
            field_val = e[name]
            offset = copyElement(field_val, field_dt, buffer, offset) 
    elif not dt.metadata or "vlen" not in dt.metadata:
        #print("e novlen: {} type: {}".format(e, type(e)))
        e_buf = e.tobytes()
        offset = copyBuffer(e_buf, buffer, offset)
    else:
        # variable length element
        vlen = dt.metadata["vlen"]
        if isinstance(e, int):
            if e == 0:
                # write 4-byte integer 0 to buffer
                offset = copyBuffer(b'\x00\x00\x00\x00', buffer, offset)  
            else:
                raise ValueError("Unexpected value: {}".format(e))
        elif isinstance(e, bytes):
            count = np.int32(len(e))
            offset = copyBuffer(count.tobytes(), buffer, offset)
            offset = copyBuffer(e, buffer, offset)
        elif isinstance(e, str):
            count = np.int32(len(e))
            offset = copyBuffer(count.tobytes(), buffer, offset)
            text = e.encode('utf-8')
            count = len(text)
            offset = copyBuffer(text, buffer, offset)
        elif isinstance(e, np.ndarray) or isinstance(e, list) or isinstance(e, tuple):
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
Read element from bytearrray 
"""
def readElement(buffer, offset, dt):
    #print("readElement, offset: {}".format(offset))
    
    if len(dt) > 1:
        raise TypeError("Compound type not valid with readElement")
    elif not dt.metadata or "vlen" not in dt.metadata:
        count = dt.itemsize
        e_buffer = buffer[offset:(offset+count)]
        offset += count
        retval = np.fromstring(bytes(e_buffer), dtype=dt)  
    else:
        # variable length element
        vlen = dt.metadata["vlen"]
        count_bytes = bytes(buffer[offset:(offset+4)])
        print("count_bytes:", count_bytes, "type:", type(count_bytes))
        try:
            count = int(np.fromstring(count_bytes, dtype="<i4"))
        except TypeError as e:
            msg = "Unexpected error reading count value for variable length elemennt: {}".format(e)
            log.error(msg)
            raise TypeError(msg)
        print("count:", count)
        if count < 0:
            # shouldn't be negative
            raise ValueError("Unexpected count value for variable length element")
        if count > 1024*1024*1024:
            # expect variable length element to be between 0 and 1mb
            raise ValueError("Variable length element size expected to be less than 1MB")
        offset += 4
        if count == 0:
            retval = 0  # null element
        else:
            e_buffer = buffer[offset:(offset+count)]
            offset += count
            if vlen is bytes:
                retval = e_buffer
            elif vlen is str:
                retval = e_buffer.decode('utf-8')
            else:
                # assume numpy array   
                retval = np.fromstring(bytes(e_buffer), dtype=vlen)
             
        #print("retval: {}".format(retval))
    return retval, offset


"""
Read compound element from bytearrray 
"""
def readCompound(buffer, offset, e, dt):
    #print("readElement, offset: {}".format(offset))
    
    for name in dt.names:
        field_dt = dt[name]
        if len(field_dt) > 1:
            offset = readCompound(buffer, offset, e[name], field_dt)
        else:
            field_val, offset = readElement(buffer, offset, field_dt)
            e[name] = field_val
    return offset
    
             
""" 
Return byte representation of numpy array
"""
def arrayToBytes(arr):
    if not isVlen(arr.dtype):
        # can just return normal numpy bytestream
        return arr.tobytes()  
    
    nSize = getByteArraySize(arr)
    #print("nsize:", nSize)
    buffer = bytearray(nSize)
    offset = 0
    nElements = np.prod(arr.shape)
    arr1d = arr.reshape((nElements,))
    for e in arr1d:
        offset = copyElement(e, arr1d.dtype, buffer, offset)
        #print("offset: {}".format(offset))
    return buffer

"""
Create numpy array based on byte representation
"""
def bytesToArray(data, dt, shape):
    nelements = getNumElements(shape)
    if not isVlen(dt):
        # regular numpy from string
        arr = np.fromstring(data, dtype=dt)  
    else:
        arr = np.zeros((nelements,), dtype=dt)
        offset = 0
        for index in range(nelements):
            if len(dt) > 1:
                e = arr[index]
                offset = readCompound(data, offset, e, dt)
            else:
                e, offset = readElement(data, offset, dt)
                #print("e: {} type: {}".format(e, type(e)))
                arr[index] = e
    arr = arr.reshape(shape)
    return arr


        


    

