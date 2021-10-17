import aiohttp
import asyncio
import sys
import json
import random
import h5pyd
import base64
import logging
import time
import numpy as np
from config import Config

H5_PATH = "/data"
NUM_RETRIES = 10
SLEEP_TIME = 0.1
MAX_SLEEP_TIME = 10.0

cfg = Config()

def usage():
    print("usage: python append_1d_async.py [--loglevel=debug|info|warning|error] [--maxrows=n] [--tasks=n] domainpath")
    print(" domain must exist (use append_1d.py to initialize)")
    sys.exit(0)

def isBinary(rsp):
    """ return true if http response headers 
    indicate binary data """
    if 'Content-Type' not in rsp.headers:
        return False  # assume text
                    
    if rsp.headers['Content-Type'] == "application/octet-stream":
        return True
    else:
        return False
                     
   
class DataAppender:
    def __init__(self, app, max_tasks=10):
        self._app = app
          
        logging.info(f"DataAppender.__init__  {self.domain}")
        self._app = app
        self._max_tasks = max_tasks
        self._q = asyncio.Queue()
        # assign one sensor id per task
        for i in range(max_tasks):
            self._q.put_nowait(i+1)

    def getHeaders(self, format="json"):
        """Get default request headers for domain"""
        username = self.username
        password = self.password  
        headers = {}
        if username and password:
            auth_string = username + ':' + password
            auth_string = auth_string.encode('utf-8')
            auth_string = base64.b64encode(auth_string)
            auth_string = "Basic " + auth_string.decode("utf-8")
            headers['Authorization'] = auth_string

        if format == "binary":
            headers["accept"] = "application/octet-stream"
            headers["content-type"] = "application/octet-stream"
        elif format == "json":
            headers["accept"] = "application/json"
            headers["content-type"] = "application/json"
        else:
            logging.error("unnkown format")

        return headers

    @property
    def domain(self):
        return self._app["domain"]

    @property
    def dsettype(self):
        return self._app["dsettype"]

    @property
    def verbose(self):
        return self._app["verbose"]

    @property
    def max_rows(self):
        return self._app["max_rows"]

    @property
    def rows_added(self):
        return self._rows_added

    @property
    def username(self):
        return self._app["hs_username"]
           
    @property
    def password(self):
        return self._app["hs_password"]
         
    @property
    def endpoint(self):
        return self._app["hs_endpoint"]

    @property
    def result(self):
        return self._app["result"]

    @property
    def retries(self):
        return self._app["retries"]
         

    async def append(self):
        workers = [asyncio.Task(self.work())
            for _ in range(self._max_tasks)]
        # When all work is done, exit.
        msg = f"DataFetcher max_tasks {self._max_tasks} = await queue.join "
        logging.info(msg)
        await self._q.join()
        msg = "DataFetcher - join complete"
        logging.info(msg)

        for w in workers:
            w.cancel()
        logging.debug("DataFetcher - workers canceled")

    async def work(self):
        max_retries = self.retries
        async with aiohttp.ClientSession() as session:
            sensor_id = await self._q.get()
            logging.info(f"work init, sensor_id: {sensor_id}")
            seq_num = 1
            dsetid = await self.getDatasetId(session)
            logging.info(f"got dsetid: {dsetid}")
            retry_count = 0
            sleep_time = SLEEP_TIME
            while True:
                start_ts = time.time()
                status_code = None
                self._app["request_count"] += 1
                try:
                    status_code = await self.addrow(session, dsetid, sensor_id, seq_num)
                except IOError as ioe:
                    logging.error(f"got IOError: {ioe}")
                elapsed = time.time() - start_ts
                msg = f"DataAppender - task {sensor_id} start: {start_ts:.3f} "
                msg += f"elapsed: {elapsed:.3f}"
                logging.info(msg)

                if status_code == 200:
                    self._app["success_count"] += 1
                    self._app["rows_added"] += 1
                    seq_num += 1
                    if self.max_rows > 0 and self._app["rows_added"] >= self.max_rows:
                        logging.info(f"no more work for task! {sensor_id}")
                        self._q.task_done()

                        break
                    retry_count = 0
                    sleep_time = SLEEP_TIME

                elif status_code == 503:
                    logging.warning(f"server too busy, sleeping for {sleep_time}")
                    self._app["error_count"] += 1
                    await asyncio.sleep(sleep_time)
                    sleep_time *= 2.0  # wait twice as long next time
                    if sleep_time > MAX_SLEEP_TIME:
                        sleep_time = MAX_SLEEP_TIME
                else:
                    logging.error(f"got status code: {status_code} retry: {retry_count}")
                    self._app["error_count"] += 1
                    retry_count += 1
                    if retry_count > max_retries:
                        # move on to another block
                        self._q.task_done()
                        retry_count = 0
                        sleep_time = SLEEP_TIME

    async def getDatasetId(self, session):
        headers = self.getHeaders()
        dsetid = None
        req = f"{self.endpoint}/datasets/"
        
        params = {"h5path": H5_PATH}
        params["domain"] = self.domain
        logging.debug(f"get dataset by path: sending req: {req} {params}")
        status_code = 500
        self._app["request_count"] += 1
        async with session.get(req, headers=headers, params=params) as rsp:
            if rsp.status == 200:
                body = await rsp.text()
                body_json = json.loads(body)
                if "id" not in body_json:
                    msg = "expected 'id' key in response"
                    logging.error(msg)
                    raise IOError(msg)
                dsetid = body_json['id']
                self._app["success_count"] += 1
            else:
                msg = f"got bad status code for get dataset: {rsp.status}"
                logging.error(msg)
                self._app["error_count"] += 1
                raise IOError(msg)
        return dsetid


    async def addrow(self, session, dsetid, sensor_id, seq_num):
        # [('timestamp', np.float64), ('sensor', np.int16), ('seq', np.int32), ('mtype', np.int16), ('value', np.float32)])
        now = time.time()
        mtype = random.randrange(0, 4)
        value = random.random() # range 0 - 1
        row = (now, sensor_id, seq_num, mtype, value)

        headers = self.getHeaders()
        req = f"{self.endpoint}/datasets/{dsetid}/value"
        payload = {'value': row, 'append': 1}
        
        params = {}
        params["domain"] = self.domain
        logging.debug(f"append task: {sensor_id}: sending req: {req}")
        logging.info(f"append task: {sensor_id}, seq_num: {seq_num}")
        status_code = 500
        async with session.put(req, headers=headers, params=params, json=payload) as rsp:
            status_code = rsp.status
        return status_code
        
       

# parse command line args
domain = None
max_rows = 0
log_level = logging.INFO
max_tasks = 1
for narg in range(1, len(sys.argv)):
    arg = sys.argv[narg]
    if arg in ("-h", "--help"):
        usage()
    elif arg.startswith("--tasks="):
        max_tasks = int(arg[len("--tasks="):])
    elif arg.startswith("--loglevel="):
        level= arg[len("--loglevel="):]
        if level == "debug":
            log_level = logging.DEBUG
        elif level == "info":
            log_level = logging.INFO
        elif level == "warning":
            log_level = logging.WARNING
        elif level == "error":
            log_level = logging.ERROR
        else:
            print("unexpected log level:", log_level)
            sys.exit(1)
    elif arg.startswith("--maxrows="):
        max_rows = int(arg[len("--maxrows="):])
    elif arg.startswith("--tasks="):
        max_tasks = int(arg[len("--tasks="):])
    elif arg.startswith("-"):
        usage()
    else:
        domain = arg

if not domain:
    usage()
 
logging.basicConfig(format='%(asctime)s %(message)s', level=log_level)
    
# init app dictionary
cfg["domain"] = domain
cfg["max_rows"] = max_rows
cfg["retries"] =  NUM_RETRIES
cfg["request_count"] = 0
cfg["success_count"] = 0
cfg["error_count"] = 0
cfg["rows_added"] = 0

start_ts = time.time() 
data_appender = DataAppender(cfg, max_tasks=max_tasks)
loop = asyncio.get_event_loop()
loop.run_until_complete(data_appender.append())
end_ts = time.time()
print("num requests:", cfg["request_count"])
print("num success:", cfg["success_count"])
print("num failures:", cfg["error_count"])
count = cfg["rows_added"]
print("rows added:", count)
print(f"added {count} rows in {(end_ts-start_ts):8.4f} seconds")
print(f"{count/(end_ts-start_ts):5.4f} rows/sec")

 





