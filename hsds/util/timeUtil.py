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
from datetime import datetime
import time
import pytz


def unixTimeToUTC(timestamp):
    """Convert unix timestamp (seconds since Jan 1, 1970, to ISO-8601
    compatible UTC time string.

    """
    utc = pytz.utc
    dtTime = datetime.fromtimestamp(timestamp, utc)
    iso_str = dtTime.isoformat()
    # isoformat returns a string like this:
    # '2014-10-30T04:25:21+00:00'
    # strip off the '+00:00' and replace
    # with 'Z' (both are ISO-8601 compatible)
    npos = iso_str.rfind('+')
    iso_z = iso_str[:npos] + 'Z'
    return iso_z


def elapsedTime(timestamp):
    """Get Elapsed time from given timestamp"""
    delta = int(time.time()) - timestamp
    if delta < 0:
        return "Invalid timestamp!"
    day_length = 24*60*60
    days = 0
    hour_length = 60*60
    hours = 0
    minute_length = 60
    minutes = 0
    ret_str = ''

    if delta > day_length:
        days = delta // day_length
        delta = delta % day_length
        ret_str += "{} days ".format(days)
    if delta > hour_length or days > 0:
        hours = delta // hour_length
        delta = delta % hour_length
        ret_str += "{} hours ".format(hours)
    if delta > minute_length or days > 0 or hours > 0:
        minutes = delta // minute_length
        delta = delta % minute_length
        ret_str += "{} minutes ".format(minutes)
    ret_str += "{} seconds".format(delta)
    return ret_str

