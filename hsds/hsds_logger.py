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
# Simple logger for hsds
#
# Supports "text" and "json" output formats and W3C trace context
# (traceparent header) propagation so that log lines can be correlated
# per-request across SN and DN nodes (and with upstream clients).
#

import asyncio
import json
import re
import secrets
import time
from contextvars import ContextVar
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
config = {"log_level": DEBUG, "prefix": "", "timestamps": False, "log_format": "text"}

# (trace_id, span_id, trace_flags) for the request being handled by the
# current asyncio task tree - see newTraceContext()
_trace_ctx = ContextVar("hsds_trace_ctx", default=None)

_TRACE_ID_RGX = re.compile(r"[0-9a-f]{32}")
_TRACE_FLAGS_RGX = re.compile(r"[0-9a-f]{2}")


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


def setLogConfig(level, prefix=None, timestamps=None, log_format=None):
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
    if prefix is not None:
        config["prefix"] = prefix
    if timestamps is not None:
        config["timestamps"] = timestamps
    if log_format is not None:
        if log_format not in ("text", "json"):
            raise ValueError(f"unexpected log_format: {log_format}")
        config["log_format"] = log_format


def newTraceContext(traceparent=None):
    """Set the trace context for the current asyncio task tree and return
    the trace id.

    If traceparent is a valid W3C traceparent header ("00-<trace_id>-
    <parent_span_id>-<flags>"), the incoming trace id and flags are adopted;
    otherwise a new trace id is generated.  A new span id is generated
    either way to identify this node's hop.
    """
    trace_id = None
    flags = "01"
    if traceparent:
        parts = traceparent.strip().lower().split("-")
        if len(parts) == 4 and _TRACE_ID_RGX.fullmatch(parts[1]):
            if parts[1] != "0" * 32:
                trace_id = parts[1]
                if _TRACE_FLAGS_RGX.fullmatch(parts[3]):
                    flags = parts[3]
    if trace_id is None:
        trace_id = secrets.token_hex(16)
    span_id = secrets.token_hex(8)
    _trace_ctx.set((trace_id, span_id, flags))
    return trace_id


def getTraceId():
    """Return the trace id for the current request, or None."""
    ctx = _trace_ctx.get()
    return ctx[0] if ctx else None


def getTraceParent():
    """Return a W3C traceparent header value for outgoing requests made on
    behalf of the current request, or None if no trace context is set."""
    ctx = _trace_ctx.get()
    if ctx is None:
        return None
    trace_id, span_id, flags = ctx
    return f"00-{trace_id}-{span_id}-{flags}"


def _activeTaskCount():
    count = 0
    for task in asyncio.all_tasks():
        if not task.done():
            count += 1
    return count


def _isotime():
    now = time.time()
    ms = int(now * 1000) % 1000
    s = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now))
    return f"{s}.{ms:03d}Z"


def _timestamp():
    if config["timestamps"]:
        ts = _isotime() + " "
    else:
        ts = ""

    return ts


def _emit(level_name, msg, tag=None, extra=None):
    """Print one log line in the configured format.  tag replaces the level
    name in text format (used for the REQ/RSP access log lines); extra
    fields are only emitted in json format."""
    prefix = config["prefix"]
    trace_id = getTraceId()
    if config["log_format"] == "json":
        obj = {"time": _isotime(), "level": level_name}
        if prefix:
            obj["node"] = prefix.strip()
        if trace_id:
            obj["trace_id"] = trace_id
        if extra:
            obj.update(extra)
        obj["msg"] = msg
        print(json.dumps(obj, default=str))
    else:
        if tag is None:
            tag = level_name
        ts = _timestamp()
        trace = f"[{trace_id}] " if trace_id else ""
        print(f"{prefix}{ts}{tag}> {trace}{msg}")


def _logMsg(level, msg):
    if config["log_level"] > level:
        return  # ignore

    level_name = _getLevelName(level)
    _emit(level_name, msg)
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
    # adopt the caller's trace context (or start a new trace) so all log
    # lines emitted while handling this request carry the same trace id
    newTraceContext(req.headers.get("traceparent"))

    msg = f"{req.method}: {req.path}"
    extra = {"method": req.method, "path": req.path}
    if domain:
        msg += f" [{domain}]"
        extra["domain"] = domain
    _emit("INFO", msg, tag="REQ", extra=extra)

    INFO_METHODS = (
        "/about",
        "/register",
        "/info",
        "/nodeinfo",
        "/nodestate",
        "/register",
    )
    if req.path in INFO_METHODS:
        # always service these state requests regardless of node state and
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

    if config["log_level"] <= level:
        msg = f"<{code}> ({message}): {req.path}"
        extra = {"status": code, "reason": message, "path": req.path}
        _emit(_getLevelName(level), msg, tag="RSP", extra=extra)
