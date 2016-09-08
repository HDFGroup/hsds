#
# Simple looger for hsds
#
def info(msg):
	print("INFO> " + msg)

def warn(msg):
	print("WARN> " + msg)

def error(msg):
	print("ERROR> " + msg)

def request(req):
	print("REQ> {}: {}".format(req.method, req.path))

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
	
	print("{} RSP> <{}> ({}): {}".format(level, code, message, req.path))

