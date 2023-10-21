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

from .util.hdf5dtype import createDataType
from .util.arrayUtil import getNumpyValue
from . import hsds_logger as log


def getFillValue(dset_json):
    """ Return the fill value of the given dataset as a numpy array.
      If no fill value is defined, return an zero array of given type """

    fill_value = None
    type_json = dset_json["type"]
    dt = createDataType(type_json)

    if "creationProperties" in dset_json:
        cprops = dset_json["creationProperties"]
        if "fillValue" in cprops:
            fill_value_prop = cprops["fillValue"]
            log.debug(f"got fo;;+value_prop: {fill_value_prop}")
            encoding = cprops.get("fillValue_encoding")
            fill_value = getNumpyValue(fill_value_prop, dt=dt, encoding=encoding)
    if fill_value:
        arr = np.empty((1,), dtype=dt, order="C")
        arr[...] = fill_value
    else:
        arr = np.zeros([1,], dtype=dt, order="C")

    return arr
