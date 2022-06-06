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
#
# Simple looger
#

import config


def info(msg):
    if config.get("log_level") == "INFO":
        print("INFO> " + msg)


def warn(msg):
    if config.get("log_level") != "ERROR":
        print("WARN> " + msg)


def error(msg):
    print("ERROR> " + msg)


def request(req):
    print("REQ> {}: {} host:[{}]".format(req.method, req.path, req.headers["host"]))


def response(req, resp=None, code=None, message=None):
    level = "INFO"
    if code is None:
        # rsp needs to be set otherwise
        code = resp.status
    if message is None:
        message = resp.reason
    if code > 399:
        if code < 500:
            level = "WARN"
        else:
            level = "ERROR"

    log_level = config.get("log_level")
    if (
        log_level == "INFO"
        or (log_level == "WARN" and level != "INFO")
        or (log_level == "ERROR" and level == "ERROR")
    ):
        print("{} RSP> <{}> ({}): {}".format(level, code, message, req.path))
