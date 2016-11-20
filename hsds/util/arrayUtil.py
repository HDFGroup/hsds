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
