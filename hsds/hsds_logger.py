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
# Simple looger for hsds
#

import asyncio
import time
import sys
from aiohttp.web_exceptions import HTTPServiceUnavailable
from .util.domainUtil import getDomainFromRequest

# Levels copied from python logging module
DEBUG = 10
INFO = 20
WARNING = 30
ERROR = 40

req_count = {"GET": 0, "POST": 0, "PUT": 0, "DELETE": 0, "num_tasks": 0}
log_count = {"DEBUG": 0, "INFO": 0, "WARN": 0, "ERROR": 0}
# the following defaults will be adjusted by the app
config = {"log_level": DEBUG, "prefix": "", "timestamps": False}

# Support logging UTF-8 characters
sys.stdout.reconfigure(encoding='utf-8')


def _getLevelName(level):
    if level == DEBUG:
        name = "DEBUG"
    elif level == INFO:
        name = "INFO"
    elif level == WARNING:
        name = "WARN"
    elif level == ERROR:
        name = "ERROR"
    else:
        name = "????"
    return name


def setLogConfig(level, prefix=None, timestamps=None):
    if level == "DEBUG":
        config["log_level"] = DEBUG
    elif level == "INFO":
        config["log_level"] = INFO
    elif level == "WARNING":
        config["log_level"] = WARNING
    elif level == "WARN":
        config["log_level"] = WARNING
    elif level == "ERROR":
        config["log_level"] = ERROR
    else:
        raise ValueError(f"unexpected log_level: {level}")
    # print(f"setLogConfig - level={level}")
    if prefix is not None:
        config["prefix"] = prefix
    if timestamps is not None:
        config["timestamps"] = timestamps


def _activeTaskCount():
    count = 0
    for task in asyncio.all_tasks():
        if not task.done():
            count += 1
    return count


def _timestamp():

    if config["timestamps"]:
        now = time.time()
        ts = f"{now:.3f} "
    else:
        ts = ""

    return ts


def _logMsg(level, msg):
    if config["log_level"] > level:
        return  # ignore

    ts = _timestamp()

    prefix = config["prefix"]

    level_name = _getLevelName(level)

    print(f"{prefix}{ts}{level_name}> {msg}")

    log_count[level_name] += 1


def debug(msg):
    _logMsg(DEBUG, msg)


def info(msg):
    _logMsg(INFO, msg)


def warn(msg):
    _logMsg(WARNING, msg)


def warning(msg):
    _logMsg(WARNING, msg)


def error(msg):
    _logMsg(ERROR, msg)


def request(req):
    app = req.app
    domain = getDomainFromRequest(req, validate=False)
    prefix = config["prefix"]
    ts = _timestamp()

    msg = f"{prefix}{ts}REQ> {req.method}: {req.path}"
    if domain:
        msg += f" [{domain}]"
    print(msg)

    INFO_METHODS = (
        "/about",
        "/register",
        "/info",
        "/nodeinfo",
        "/nodestate",
        "/register",
    )
    if req.path in INFO_METHODS:
        # always service these state requests regardles of node state and
        # task load
        return
    node_state = app["node_state"] if "node_state" in app else None
    if node_state != "READY":
        warning(f"returning 503 - node_state: {node_state}")
        raise HTTPServiceUnavailable()
    if req.method in ("GET", "POST", "PUT", "DELETE"):
        req_count[req.method] += 1
    num_tasks = len(asyncio.all_tasks())
    active_tasks = _activeTaskCount()
    req_count["num_tasks"] = num_tasks
    if config["log_level"] == DEBUG:
        debug(f"num tasks: {num_tasks} active tasks: {active_tasks}")

    max_task_count = app["max_task_count"]
    if app["node_type"] == "sn":
        if max_task_count and active_tasks > max_task_count:
            warning(f"more than {max_task_count} tasks, returning 503")
            raise HTTPServiceUnavailable()
        else:
            debug(f"active_tasks: {active_tasks} max_tasks: {max_task_count}")


def response(req, resp=None, code=None, message=None):
    """
    Output "RSP..." to log on conclusion of request
    """
    level = INFO
    if code is None:
        # rsp needs to be set otherwise
        code = resp.status
    if message is None:
        message = resp.reason
    if code > 399:
        if code < 500:
            level = WARNING
        else:
            level = ERROR

    log_level = config["log_level"]

    if log_level == DEBUG:
        prefix = config["prefix"]
        ts = _timestamp()

        num_tasks = len(asyncio.all_tasks())
        active_tasks = _activeTaskCount()

        debug(f"rsp - num tasks: {num_tasks} active tasks: {active_tasks}")

        s = "{}{} RSP> <{}> ({}): {}"
        print(s.format(prefix, ts, code, message, req.path))

    elif log_level <= level:
        prefix = config["prefix"]
        ts = _timestamp()

        num_tasks = len(asyncio.all_tasks())
        active_tasks = _activeTaskCount()

        debug(f"num tasks: {num_tasks} active tasks: {active_tasks}")

        s = "{}{} RSP> <{}> ({}): {}"
        print(s.format(prefix, ts, code, message, req.path))
    else:
        pass
