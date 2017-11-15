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
