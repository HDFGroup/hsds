import aiohttp
import asyncio
import sys
import json
import random
import base64
import logging
import time
from config import Config

H5_PATH = "/data"
NUM_RETRIES = 10
SLEEP_TIME = 0.1
MAX_SLEEP_TIME = 10.0

cfg = Config()


def usage():
    msg = "usage: python append_1d_async.py [--loglevel=debug|info|warning|error] "
    msg += "[--maxrows=n] [--tasks=n] domainpath"
    print(msg)
    print(" domain must exist (use append_1d.py to initialize)")
    sys.exit(0)


def isBinary(rsp):
    """return true if http response headers
    indicate binary data"""
    if "Content-Type" not in rsp.headers:
        return False  # assume text

    if rsp.headers["Content-Type"] == "application/octet-stream":
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
            self._q.put_nowait(i + 1)

    def getHeaders(self, format="json"):
        """Get default request headers for domain"""
        username = self.username
        password = self.password
        headers = {}
        if username and password:
            auth_string = username + ":" + password
            auth_string = auth_string.encode("utf-8")
            auth_string = base64.b64encode(auth_string)
            auth_string = "Basic " + auth_string.decode("utf-8")
            headers["Authorization"] = auth_string

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
        """startup append workers"""
        workers = [asyncio.Task(self.work()) for _ in range(self._max_tasks)]
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
        async with aiohttp.ClientSession() as session:
            sensor_id = await self._q.get()
            logging.info(f"work init, sensor_id: {sensor_id}")
            seq_num = 1
            dsetid = None

            retry_count = 0
            sleep_time = SLEEP_TIME
            while True:
                start_ts = time.time()
                status_code = None
                self._app["request_count"] += 1
                try:
                    if not dsetid:
                        # get the dataset id before we we start adding rows
                        status_code, dsetid = await self.getDatasetId(session)
                        if status_code == 200:
                            logging.info(f"task: {sensor_id}: got dsetid: {dsetid}")
                            self._app["success_count"] += 1
                    else:
                        # call addrow and update stats
                        status_code = await self.addrow(
                            session, dsetid, sensor_id, seq_num
                        )
                        self._app["success_count"] += 1
                        self._app["rows_added"] += 1
                        seq_num += 1
                        if all(
                            (self.max_rows > 0),
                            (self._app["rows_added"] >= self.max_rows),
                        ):
                            logging.info(f"task: {sensor_id}: no more work to do")
                            self._q.task_done()
                            break
                except Exception as e:
                    logging.error(f"task: {sensor_id}: got Exception: {e}")

                elapsed = time.time() - start_ts
                msg = f"DataAppender - task {sensor_id} start: {start_ts:.3f} "
                msg += f"elapsed: {elapsed:.3f}"
                logging.info(msg)

                if status_code == 200:
                    # success!, reset retry count and sleep time
                    retry_count = 0
                    sleep_time = SLEEP_TIME

                elif status_code == 503:
                    # server overloaded - sleep for a bit
                    logging.warning(f"server too busy, sleeping for {sleep_time}")
                    self._app["error_count"] += 1
                    await asyncio.sleep(sleep_time)
                    sleep_time *= 2.0  # wait twice as long next time
                    if sleep_time > MAX_SLEEP_TIME:
                        sleep_time = MAX_SLEEP_TIME
                else:
                    # unexpected error - abort this task
                    logging.error(
                        f"task: {sensor_id}: got status code: {status_code} retry: {retry_count}"
                    )
                    self._app["error_count"] += 1
                    retry_count += 1
                    if retry_count > self.retries:
                        # quit this task
                        logging.error(f"task: {sensor_id}: max retries exceeded")
                        self._q.task_done()
                        break
        logging.info(f"task: {sensor_id}: exiting")

    async def getDatasetId(self, session):
        """Get the datset id for the dataset (hopefully) at H5_PATH"""
        headers = self.getHeaders()
        dsetid = None
        req = f"{self.endpoint}/datasets/"

        params = {"h5path": H5_PATH}
        params["domain"] = self.domain
        logging.debug(f"get dataset by path: sending req: {req} {params}")
        status_code = 500
        async with session.get(req, headers=headers, params=params) as rsp:
            status_code = rsp.status
            if status_code == 200:
                body = await rsp.text()
                body_json = json.loads(body)
                if "id" not in body_json:
                    msg = "expected 'id' key in response"
                    logging.error(msg)
                    raise IOError(msg)
                dsetid = body_json["id"]
        return status_code, dsetid

    async def addrow(self, session, dsetid, sensor_id, seq_num):
        """make up some random data and append to the dataset"""
        now = time.time()
        mtype = random.randrange(0, 4)
        value = random.random()  # range 0 - 1
        row = (now, sensor_id, seq_num, mtype, value)

        headers = self.getHeaders()
        req = f"{self.endpoint}/datasets/{dsetid}/value"
        # the 'append' param enables the row to be added to
        # the end of the datset without explictly extending the
        # dataspace
        payload = {"value": row, "append": 1}

        params = {}
        params["domain"] = self.domain
        logging.debug(f"append task: {sensor_id}: sending req: {req}")
        logging.info(f"append task: {sensor_id}, seq_num: {seq_num}")
        status_code = 500
        async with session.put(
            req, headers=headers, params=params, json=payload
        ) as rsp:
            status_code = rsp.status
        return status_code


#
# Main
#

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
        nlen = len("--tasks=")
        max_tasks = int(arg[nlen:])
    elif arg.startswith("--loglevel="):
        nlen = len("--loglevel=")
        level = arg[nlen:]
        if level == "debug":
            log_level = logging.DEBUG
        elif level == "info":
            log_level = logging.INFO
        elif level == "warning":
            log_level = logging.WARNING
        elif level == "error":
            log_level = logging.ERROR
        else:
            print(f"unexpected log level: {level}")
            sys.exit(1)
    elif arg.startswith("--maxrows="):
        nlen = len("--maxrows=")
        max_rows = int(arg[nlen:])
    elif arg.startswith("--tasks="):
        nlen = len("--tasks=")
        max_tasks = int(arg[nlen:])
    elif arg.startswith("-"):
        usage()
    else:
        domain = arg

if not domain:
    usage()

if domain.startswith("hdf5://"):
    # just need the path part for REST API
    nlen = len("hdf5:/")
    domain = domain[nlen:]

logging.basicConfig(format="%(asctime)s %(message)s", level=log_level)

# init app dictionary
cfg["domain"] = domain
cfg["max_rows"] = max_rows
cfg["retries"] = NUM_RETRIES
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
