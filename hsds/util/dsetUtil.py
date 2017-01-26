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

from aiohttp.errors import HttpBadRequest, HttpProcessingError
import hsds_logger as log


def getHyperslabSelection(dsetshape, start=None, stop=None, step=None):
    """
    Get slices given lists of start, stop, step values

    TBD: for step>1, adjust the slice to not extend beyond last data point returned
    """
    rank = len(dsetshape)
    if start:
        if not isinstance(start, (list, tuple)):
            start = [start]
        if len(start) != rank:
            msg = "Bad Request: start array length not equal to dataset rank"
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        for dim in range(rank):
            if start[dim] < 0 or start[dim] >= dsetshape[dim]:
                msg = "Bad Request: start index invalid for dim: " + str(dim)
                log.warn(msg)
                raise HttpBadRequest(message=msg)
    else:
        start = []
        for dim in range(rank):
            start.append(0)
    
    if stop:
        if not isinstance(stop, (list, tuple)):
            stop = [stop]
        if len(stop) != rank:
            msg = "Bad Request: stop array length not equal to dataset rank"
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        for dim in range(rank):
            if stop[dim] <= start[dim] or stop[dim] > dsetshape[dim]:
                msg = "Bad Request: stop index invalid for dim: " + str(dim)
                log.warn(msg)
                raise HttpBadRequest(message=msg)
    else:
        stop = []
        for dim in range(rank):
            stop.append(dsetshape[dim])

    if step:
        if not isinstance(step, (list, tuple)):
            step = [step]
        if len(step) != rank:
            msg = "Bad Request: step array length not equal to dataset rank"
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        for dim in range(rank):
            if step[dim] <= 0 or step[dim] > dsetshape[dim]:
                msg = "Bad Request: step index invalid for dim: " + str(dim)
                log.warn(msg)
                raise HttpBadRequest(message=msg)
    else:
        step = []
        for dim in range(rank):
            step.append(1)

    slices = []
    
    for dim in range(rank):
        
        try:
            s = slice(int(start[dim]), int(stop[dim]), int(step[dim]))
        except ValueError:
            msg = "Bad Request: invalid start/stop/step value"
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        slices.append(s)
    return tuple(slices)

def getSelectionShape(selection):
    """ Return the shape of the given selection. 
      Examples (selection -> returned shape):
      [(3,7,1)] -> [4]
      [(3, 7, 3)] -> [1]
      [(44, 52, 1), (48,52,1)] -> [8, 4]
    """
    shape = []
    rank = len(selection)
    for i in range(rank):
        s = selection[i]
        extent = 0
        if s.stop > s.start:
            extent = s.stop - s.start
        if s.step > 1 and extent > 0:
            extent = (extent // s.step)
        if (s.stop - s.start) % s.step != 0:
            extent += 1
        shape.append(extent)
    return shape


"""
Herlper function, get query parameter value from request.
If body is provided (as a JSON object) look in JSON and if not found
look for query param.  Return default value (or None) if not found
"""
def getQueryParameter(request, query_name, body=None, default=None):
    # as a convience, look up different capitilizations of query name
    query_names = []
    query_names.append(query_name.lower())
    query_names.append(query_name.upper())
    query_names.append(query_name[0].upper() + query_name[1:].lower())
    if query_name not in query_names:
        query_names.insert(0, query_name)
    val = None
    if body is not None:
        for query_name in query_names:
            if query_name in body:
                val = body[query_name]
                break

    if val is None:
        # look for a query param
        for query_name in query_names:
            if query_name in request.GET:
                val = request.GET[query_name]

    if val and default is not None and isinstance(default, int):
        # convert to int Type
        try:
            val = int(val)
        except ValueError:
            msg = "Invalid request parameter: {}".format(query_name)
            log.warn(msg)
            raise HttpBadRequest(message=msg)
    if val is None:
        if  default is not None:
            val = default
        else:
            msg = "Request parameter is missing: {}".format(query_name)
            log.warn(msg)
            raise HttpBadRequest(message=msg)
    return val

"""
Helper method - return slice for dim based on query params

    Query arg should be in the form: [<dim1>, <dim2>, ... , <dimn>]
        brackets are optional for one dimensional arrays.
         Each dimension, valid formats are:
            single integer: n
            start and end: n:m
            start, end, and stride: n:m:s
"""
def getSliceQueryParam(request, dim, extent, body=None):        
    # Get optional query parameters for given dim
    log.info("getSliceQueryParam: " + str(dim) + ", " + str(extent))

    start = 0
    stop = extent
    step = 1

    if body and "start" in body:
        # look for start params in body JSON
        start_val = body["start"]
        if isinstance(start_val, (list, tuple)):
            if len(start_val) < dim:
                msg = "Not enough dimensions supplied to body start key"
                log.arn(msg)
                raise HttpBadRequest(message=msg)
            start = start_val[dim]
        else:
            start = start_val
        
    if body and "stop" in body:
        stop_val = body["stop"]
        if isinstance(stop_val, (list, tuple)):
            if len(stop_val) < dim:
                msg = "Not enough dimensions supplied to body stop key"
                log.arn(msg)
                raise HttpBadRequest(message=msg)
            stop = stop_val[dim]
        else:
            stop = stop_val
    if body and "step" in body:
        step_val = body["step"]
        if isinstance(step_val, (list, tuple)):
            if len(step_val) < dim:
                msg = "Not enough dimensions supplied to body step key"
                log.arn(msg)
                raise HttpBadRequest(message=msg)
            step = step_val[dim]
        else:
            step = step_val

    if "select" in request.GET:
        query = request.GET["select"]
        log.info("select query value:" + query )

        if not query.startswith('['):
            msg = "Bad Request: selection query missing start bracket"
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        if not query.endswith(']'):
            msg = "Bad Request: selection query missing end bracket"
            log.warn(msg)
            raise HttpBadRequest(message=msg)

        # now strip out brackets
        query = query[1:-1]

        query_array = query.split(',')
        if dim > len(query_array):
            msg = "Not enough dimensions supplied to query argument"
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        dim_query = query_array[dim].strip()
    
        if dim_query.find(':') < 0:
            # just a number - return start = stop for this value
            try:
                start = int(dim_query)
            except ValueError:
                msg = "Bad Request: invalid selection parameter (can't convert to int) for dimension: " + str(dim)
                log.warn(msg)
                raise HttpBadRequest(message=msg)
            stop = start
        elif dim_query == ':':
            # select everything
            pass
        else:
            fields = dim_query.split(":")
            log.info("got fields: {}".format(fields))
            if len(fields) > 3:
                msg = "Bad Request: Too many ':' seperators for dimension: " + str(dim)
                log.warn(msg)
                raise HttpBadRequest(message=msg)
            try:
                if fields[0]:
                    start = int(fields[0])
                if fields[1]:
                    stop = int(fields[1])
                if len(fields) > 2 and fields[2]:
                    step = int(fields[2])
            except ValueError:
                msg = "Bad Request: invalid selection parameter (can't convert to int) for dimension: " + str(dim)
                log.info(msg)
                raise HttpBadRequest(message=msg)
    log.info("start: {}, stop: {}, step: {}".format(start, stop, step))
    # now, validate whaterver start/stop/step values we got
    if start < 0 or start > extent:
        msg = "Bad Request: Invalid selection start parameter for dimension: " + str(dim)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if stop > extent:
        msg = "Bad Request: Invalid selection stop parameter for dimension: " + str(dim)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if step <= 0:
        msg = "Bad Request: invalid selection step parameter for dimension: " + str(dim)
        log.info(msg)
        raise HttpBadRequest(message=msg)
    s = slice(start, stop, step)
    log.info("dim query[" + str(dim) + "] returning: start: " +
            str(start) + " stop: " + str(stop) + " step: " + str(step))
    return s

"""
Helper method - set query parameter for given shape + selection

    Query arg should be in the form: [<dim1>, <dim2>, ... , <dimn>]
        brackets are optional for one dimensional arrays.
         Each dimension, valid formats are:
            single integer: n
            start and end: n:m
            start, end, and stride: n:m:s
"""
def setSliceQueryParam(params, sel):  
    # pass dimensions, and selection as query params
    rank = len(sel)
    if rank > 0:
        sel_param="["
        for i in range(rank):
            s = sel[i]
            sel_param += str(s.start)
            sel_param += ':'
            sel_param += str(s.stop)
            if s.step > 1:
                sel_param += ':'
                sel_param += str(s.step)
            if i < rank - 1:
                sel_param += ','
        sel_param += ']'
        log.info("select query param: {}".format(sel_param))
        params["select"] = sel_param

"""
Helper method - set chunk dim param
    Send the chunk dimensions as a query param
    Query arg should be in the form: [<dim1>, <dim2>, ... , <dimn>]
        brackets are optional for one dimensional arrays.
         Each dimension, valid formats are:
            single integer: n
            start and end: n:m
            start, end, and stride: n:m:s
"""
def setChunkDimQueryParam(params, dims):  
    # pass dimensions, and selection as query params
    rank = len(dims)
    if rank > 0:
        dim_param="["
        for i in range(rank):
            extent = dims[i]
            dim_param += str(extent)
        dim_param += ']'
        log.info("dim query param: {}".format(dim_param))
        params["dim"] = dim_param
 

"""
Helper - get num elements defined by a shape
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
Get dims from a given shape.  Return [1,] for Scalar datasets

Use with H5S_NULL datasets will throw a 500 error.
"""
def getDsetDims(dset_json):
    if "shape" not in dset_json:
        log.error("No shape found in dset_json")
        raise HttpProcessingError(message="Unexpected error", code=500)
    shape_json = dset_json["shape"]
    dims = None
    if shape_json['class'] == 'H5S_NULL':
        msg = "Expected shape class other than H5S_NULL"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    elif shape_json['class'] == 'H5S_SCALAR':
        dims = [1,]
    elif shape_json['class'] == 'H5S_SIMPLE':
        dims = shape_json["dims"]
    else:
        log.error("Unexpected shape class: {}".format(shape_json['class']))
        raise HttpProcessingError(message="Unexpected error", code=500)
    return dims

""" Get chunk layout.  Throw 500 if used with non-H5D_CHUNKED layout
"""
def getChunkLayout(dset_json):
    if "layout" not in dset_json:
        log.error("No layout found in dset_json: {}".format(dset_json))
        raise HttpProcessingError(message="Unexpected error", code=500)
    layout_json = dset_json["layout"]
    if layout_json["class"] != 'H5D_CHUNKED':
        log.error("Unexpected shape layout: {}".format(layout_json["class"]))
        raise HttpProcessingError(message="Unexpected error", code=500)
    layout = layout_json["dims"]
    return layout


"""Helper method - return query options for a "reasonable" size
    data preview selection. Return None if the dataset is small
    enough that a preview is not needed.
"""
def getPreviewQuery(dims):   
    select = "select=["
    rank = len(dims)

    ncols = dims[rank-1]
    if rank > 1:
        nrows = dims[rank-2]
    else:
        nrows = 1

    # use some rough heuristics to define the selection
    # aim to return no more than 100 elements
    if ncols > 100:
        ncols = 100
    if nrows > 100:
        nrows = 100
    if nrows*ncols > 100:
        if nrows > ncols:
            nrows = 100 // ncols
        else:
            ncols = 100 // nrows

    for i in range(rank):
        if i == rank-1:
            select += "0:" + str(ncols)
        elif i == rank-2:
            select += "0:" + str(nrows) + ","
        else:
            select += "0:1,"
    select += "]"
    return select

"""
Return fill value if defined, otherwise 0 elements
"""
def getFillValue(dset_json):
    fill_value = None
    if "creationProperties" in dset_json:
        cprops = dset_json["creationProperties"]
        if "fillValue" in cprops:
            fill_value = cprops["fillValue"]
            if isinstance(fill_value, list):
                fill_value = tuple(fill_value)
    return fill_value



        
