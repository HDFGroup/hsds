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
from aiohttp.web_exceptions import HTTPServiceUnavailable
from .util.domainUtil import getDomainFromRequest

req_count = {"GET": 0, "POST": 0, "PUT": 0, "DELETE": 0, "num_tasks": 0}
log_count = {"DEBUG": 0, "INFO": 0, "WARN": 0, "ERROR": 0}
# the following defaults will be adjusted by the app
config = {"log_level": "DEBUG", "prefix": ""}


def debug(msg):
	if config["log_level"] == "DEBUG":
		print(config["prefix"] + "DEBUG> " + msg)
		log_count["DEBUG"] += 1

def info(msg):
	if config["log_level"] not in  ("ERROR", "WARNING", "WARN"):
		print(config["prefix"] + "INFO> " + msg)
		log_count["INFO"] += 1

def warn(msg):
	if config.get("log_level") != "ERROR":
		print(config["prefix"] + "WARN> " + msg)
		log_count["WARN"] += 1

def warning(msg):
	if config.get("log_level") != "ERROR":
		print(config["prefix"] + "WARN> " + msg)
		log_count["WARN"] += 1

def error(msg):
	print(config["prefix"] + "ERROR> " + msg)
	log_count["ERROR"] += 1

def request(req):
	app = req.app
	domain = getDomainFromRequest(req, validate=False)
	if domain is None:
		print("REQ> {}: {}".format(req.method, req.path))
	else:
		print("REQ> {}: {} [{}]".format(req.method, req.path, domain))
	if req.path in ("/about", "/register", "/info", "/nodeinfo", "/nodestate", "/register"):
		# always service these state requests regardles of node state and task load
		return
	node_state = app["node_state"] if "node_state" in app else None
	if node_state != "READY":
		warning(f"returning 503 - node_state: {node_state}")
		raise HTTPServiceUnavailable()
	if req.method in ("GET", "POST", "PUT", "DELETE"):
		req_count[req.method] += 1
	num_tasks = len(asyncio.Task.all_tasks())
	active_tasks = len([task for task in asyncio.Task.all_tasks() if not task.done()])
	req_count["num_tasks"] = num_tasks
	if config["log_level"] == "DEBUG":
		debug(f"num tasks: {num_tasks} active tasks: {active_tasks}")

	max_task_count = app["max_task_count"]
	if app["node_type"] == "sn" and max_task_count and active_tasks > max_task_count:
		warning(f"more than {max_task_count} tasks, returning 503")
		raise HTTPServiceUnavailable()


def response(req, resp=None, code=None, message=None):
	level = "INFO"
	if code is None:
		# rsp needs to be set otherwise
		code = resp.status
	if message is None:
		message=resp.reason
	if code > 399:
		if  code < 500:
			level = "WARN"
		else:
			level = "ERROR"

	log_level = config["log_level"]
	prefix = config["prefix"]
	if log_level in ("DEBUG", "INFO") or (log_level == "WARN" and level != "INFO") or (log_level == "ERROR" and level == "ERROR"):
		print("{}{} RSP> <{}> ({}): {}".format(prefix, level, code, message, req.path))
