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
app = None # global app handle
log_level = "DEUBG"

def debug(msg):
	if log_level == "DEBUG":
		print("DEBUG> " + msg)
	if app:
		counter = app["log_count"]
		counter["DEBUG"] += 1

def info(msg):
	if log_level not in  ("ERROR", "WARNING", "WARN"):
		print("INFO> " + msg)
	if app:
		counter = app["log_count"]
		counter["INFO"] += 1

def warn(msg):
	if log_level != "ERROR":
		print("WARN> " + msg)
	if app:
		counter = app["log_count"]
		counter["WARN"] += 1

def warning(msg):
	if log_level != "ERROR":
		print("WARN> " + msg)
	if app:
		counter = app["log_count"]
		counter["WARN"] += 1

def error(msg):
	print("ERROR> " + msg)
	if app:
		counter = app["log_count"]
		counter["ERROR"] += 1