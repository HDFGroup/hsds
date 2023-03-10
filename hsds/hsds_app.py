import os
import sys
from pathlib import Path
import site
import signal
import subprocess
import time
import queue
import threading
import logging
from shutil import which


def _enqueue_output(out, queue, loglevel):
    try:
        for line in iter(out.readline, b""):
            # filter lines by loglevel
            words = line.split()
            put_line = True

            if loglevel != logging.DEBUG:
                if len(words) >= 2:
                    # format should be "node_name log_level> msg"
                    level = words[1][:-1]
                    if loglevel == logging.INFO:
                        if level == "DEBUG":
                            put_line = False
                    elif loglevel == logging.WARN or loglevel == logging.WARNING:
                        if not level.startswith("WARN") and level != "ERROR":
                            put_line = False
                    elif loglevel == logging.ERROR:
                        if level != "ERROR":
                            put_line = False
            put_line = True
            if put_line:
                queue.put(line)
        logging.debug("_enqueue_output close()")
        out.close()
    except ValueError as ve:
        logging.warn(f"_enqueue_output - ValueError (handle closed?): {ve}")
    except Exception as e:
        logging.error(f"_enqueue_output - Unexpected exception {type(e)}: {e}")


def get_cmd_dir():
    """Return directory where hsds console shortcuts are."""
    hsds_shortcut = "hsds-servicenode"

    user_bin_dir = os.path.join(site.getuserbase(), "bin")
    if os.path.isdir(user_bin_dir):
        logging.debug(f"userbase bin_dir: {user_bin_dir}")
        if os.path.isfile(os.path.join(user_bin_dir, hsds_shortcut)):
            logging.info(f"using cmd_dir: {user_bin_dir}")
            return user_bin_dir

    logging.debug(f"looking for {hsds_shortcut} in PATH env var folders")
    cmd = which(hsds_shortcut, mode=os.F_OK | os.R_OK)
    if cmd is not None:
        cmd_dir = os.path.dirname(cmd)
        logging.info(f"using cmd_dir: {cmd_dir}")
        return cmd_dir

    sys_bin_dir = os.path.join(sys.exec_prefix, "bin")
    if os.path.isdir(sys_bin_dir):
        logging.debug(f"sys bin_dir: {sys_bin_dir}")
        if os.path.isfile(os.path.join(sys_bin_dir, hsds_shortcut)):
            logging.info(f"using cmd_dir: {sys_bin_dir}")
            return sys_bin_dir

    # fall back to just use __file__.parent
    bin_dir = Path(__file__).parent
    logging.info(f"no userbase or syspath found - using: {bin_dir}")
    return bin_dir


class HsdsApp:
    """
    Class to initiate and manage sub-process HSDS service
    """

    def __init__(
        self,
        username=None,
        password=None,
        password_file=None,
        logger=None,
        log_level=None,
        dn_count=1,
        logfile=None,
        root_dir=None,
        socket_dir=None,
        host=None,
        sn_port=None,
        config_dir=None,
        readonly=False,
        islambda=False,
    ):
        """
        Initializer for class
        """

        self._dn_urls = []
        self._socket_paths = []
        self._processes = {}
        self._queues = []
        self._threads = []
        self._dn_count = dn_count
        self._username = username
        self._password = password
        self._password_file = password_file
        self._logfile = logfile
        self._loglevel = log_level
        self._readonly = readonly
        self._islambda = islambda
        self._ready = False
        self._config_dir = config_dir
        self._cmd_dir = get_cmd_dir()

        if logger is None:
            self.log = logging
        else:
            self.log = logger

        # create a random dirname if one is not supplied
        if not socket_dir and not host:
            raise ValueError("socket_dir or host needs to be set")
        if host and not sn_port:
            raise ValueError("sn_port not set")

        if socket_dir is not None and not os.path.isdir(socket_dir):
            os.mkdir(socket_dir)

        if root_dir:
            if not os.path.isdir(root_dir):
                raise FileNotFoundError(f"storage directory: '{root_dir}' not found")
            self._root_dir = os.path.abspath(root_dir)
        else:
            self._root_dir = None

        # url-encode any slashed in the socket dir
        if socket_dir:
            if not socket_dir.endswith(os.path.sep):
                socket_dir += os.path.sep
            self.log.debug(f"HsdsApp init - Using socketdir: {socket_dir}")
            socket_url = ""
            for ch in socket_dir:
                if ch == "/" or ch == "\\":
                    socket_url += "%2F"
                else:
                    socket_url += ch
            sn_url = f"http+unix://{socket_url}sn_1.sock"

            for i in range(dn_count):
                socket_name = f"dn_{(i+1)}.sock"
                dn_url = f"http+unix://{socket_url}{socket_name}"
                self._dn_urls.append(dn_url)
                self._socket_paths.append(f"{socket_dir}{socket_name}")
            self._socket_paths.append(f"{socket_dir}sn_1.sock")
            self._socket_paths.append(f"{socket_dir}rangeget.sock")
            rangeget_url = f"http+unix://{socket_url}rangeget.sock"
        else:
            # setup TCP/IP endpoints
            sn_url = f"http://{host}:{sn_port}"
            dn_port = 6101  # TBD: pull this from config
            for i in range(dn_count):
                dn_url = f"http://{host}:{dn_port+i}"
                self._dn_urls.append(dn_url)
            rangeget_port = 6900  # TBD: pull from config
            rangeget_url = f"http://{host}:{rangeget_port}"

        # sort the ports so that node_number can be determined based on dn_url
        self._dn_urls.sort()
        self._endpoint = sn_url
        self._rangeget_url = rangeget_url

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def ready(self):
        return self._ready

    def print_process_output(self):
        """print any queue output from sub-processes"""
        if self._logfile:
            f = open(self._logfile, "a")
        else:
            f = sys.stdout

        while True:
            got_output = False
            for q in self._queues:
                try:
                    line = q.get_nowait()  # or q.get(timeout=.1)
                except queue.Empty:
                    pass  # no output on this queue yet
                else:
                    if isinstance(line, bytes):
                        # self.log.debug(line.decode("utf-8").strip())
                        f.write(line.decode("utf-8"))
                    else:
                        f.write(line)
                    got_output = True
            if not got_output:
                break  # all queues empty for now
        if self._logfile:
            f.close()

    def check_processes(self):
        # self.log.debug("check processes")
        self.print_process_output()
        for pname in self._processes:
            p = self._processes[pname]
            if p.poll() is not None:
                result = p.communicate()
                msg = f"process {pname} ended, result: {result}"
                self.log.warn(msg)
                # TBD - restart failed process

    def run(self):
        """startup hsds processes"""
        if self._processes:
            # just check process state and restart if necessary
            self.check_processes()
            return

        dn_urls_arg = ""
        for dn_url in self._dn_urls:
            if dn_urls_arg:
                dn_urls_arg += ","
            dn_urls_arg += dn_url

        pout = subprocess.PIPE  # will pipe to parent
        # create processes for count dn nodes, sn node, and rangeget node
        count = self._dn_count + 2  # plus 2 for rangeget proxy and sn
        # set PYTHONUNBUFFERED so we can get any output immediately
        os.environ["PYTHONUNBUFFERED"] = "1"
        # TODO: don't modify parent process env, use os.environ.copy(), set, and popen(env=)

        common_args = [
            "--standalone",
        ]
        common_args.append(f"--dn_urls={dn_urls_arg}")
        common_args.append(f"--rangeget_url={self._rangeget_url}")
        common_args.append(f"--hsds_endpoint={self._endpoint}")
        if self._islambda:
            # base boto packages installed in AWS image conflicting with aiobotocore
            # see: https://github.com/aio-libs/aiobotocore/issues/862
            # This command line argument will tell the sub-processes to remove
            # sitepackage libs from their path before importing aiobotocore
            common_args.append("--removesitepackages")
        # common_args.append("--server_name=Direct Connect (HSDS)")
        if len(self._socket_paths) > 0:
            common_args.append("--use_socket")
        if self._readonly:
            common_args.append("--readonly")
        if self._config_dir:
            common_args.append(f"--config_dir={self._config_dir}")
        if self._root_dir:
            common_args.append(f"--root_dir={self._root_dir}")
        if self._loglevel:
            common_args.append(f"--log_level={self._loglevel}")

        py_exe = sys.executable
        cmd_path = os.path.join(self._cmd_dir, "hsds-node")
        if not os.path.isfile(cmd_path):
            # search corresponding location for windows installs
            cmd_path = os.path.join(sys.exec_prefix, "Scripts")
            cmd_path = os.path.join(cmd_path, "hsds-node-script.py")
            if not os.path.isfile(cmd_path):
                raise FileNotFoundError("can't find hsds-node executable")

        for i in range(count):
            if i == 0:
                # args for service node
                pname = "sn"
                pargs = [py_exe, cmd_path, "--node_type=sn", "--log_prefix=sn "]
                if self._username:
                    pargs.append(f"--hs_username={self._username}")
                if self._password:
                    pargs.append(f"--hs_password={self._password}")
                if self._password_file:
                    pargs.append(f"--password_file={self._password_file}")
                else:
                    pargs.append("--password_file=")

                pargs.append(f"--sn_url={self._endpoint}")
                pargs.append("--logfile=sn1.log")
            elif i == 1:
                # args for rangeget node
                pname = "rg"
                pargs = [py_exe, cmd_path, "--node_type=rn", "--log_prefix=rg "]
            else:
                node_number = i - 2  # start with 0
                pname = f"dn{node_number}"
                pargs = [
                    py_exe,
                    cmd_path,
                    "--node_type=dn",
                    f"--log_prefix=dn{node_number+1} ",
                ]
                pargs.append(f"--dn_urls={dn_urls_arg}")
                pargs.append(f"--node_number={node_number}")
            # logging.info(f"starting {pargs[0]}")
            pargs.extend(common_args)
            p = subprocess.Popen(
                pargs, bufsize=1, universal_newlines=True, shell=False, stdout=pout
            )
            self._processes[pname] = p
            # setup queue so we can check on process output without blocking
            q = queue.Queue()
            loglevel = self.log.root.level
            t = threading.Thread(target=_enqueue_output, args=(p.stdout, q, loglevel))
            self._queues.append(q)
            t.daemon = True  # thread dies with the program
            t.start()
            self._threads.append(t)

        # wait to sockets are initialized
        start_ts = time.time()
        SLEEP_TIME = 1  # time to sleep between checking on socket connection
        MAX_INIT_TIME = 10.0  # max time to wait for socket to be initialized

        while True:
            ready = 0
            if len(self._socket_paths) > 0:
                for socket_path in self._socket_paths:
                    if os.path.exists(socket_path):
                        ready += 1
            else:
                if time.time() > start_ts + 5:
                    # TBD - put a real ready check here
                    ready = count
            if ready == count:
                self.log.info("all processes ready!")
                break
            else:
                self.log.debug(f"{ready}/{count} ready")
                self.log.debug(f"sleeping for {SLEEP_TIME}")
                time.sleep(SLEEP_TIME)
                if time.time() > start_ts + MAX_INIT_TIME:
                    msg = f"failed to initialize after {MAX_INIT_TIME} seconds"
                    self.log.error(msg)
                    raise IOError(msg)

        self.log.info(f"Ready after: {(time.time()-start_ts):4.2f} s")
        self._ready = True

    def stop(self):
        """terminate hsds processes"""
        if not self._processes:
            return

        now = time.time()
        logging.info(f"hsds app stop at {now}")
        
        for pname in self._processes:
            p = self._processes[pname]
            logging.info(f"terminating sub-process: {pname}")
            p.terminate()

        # wait for sub-proccesses to exit
        SLEEP_TIME = 0.1  # time to sleep between checking on process state
        MAX_WAIT_TIME = 10.0  # max time to wait for sub-process to terminate
        start_ts = time.time()
        while True:
            is_alive_cnt = 0
            for pname in self._processes:
                p = self._processes[pname]
                if p.poll() is None:
                    self.log.debug(f"process {pname} still alive")
                    is_alive_cnt += 1
                else:
                    self.log.debug(f"process {pname} has exited")

            if is_alive_cnt > 0:
                logging.debug(f"stop - {is_alive_cnt} processes still alive, sleep {SLEEP_TIME}")
                time.sleep(SLEEP_TIME)
            else:
                logging.debug("all subprocesses exited")
                break
            if time.time() > start_ts + MAX_WAIT_TIME:
                msg = f"failed to terminate after {MAX_WAIT_TIME} seconds"
                self.log.error(msg)
                break

        # kill any reluctant to die processes
        for pname in self._processes:
            p = self._processes[pname]
            if p.poll():
                logging.info(f"terminating process {pname}")
                p.terminate()
        self._processes = {}  # reset
        for t in self._threads:
            del t
        self._threads = []

    def __del__(self):
        """cleanup class resources"""
        self.stop()
        # self._tempdir.cleanup()
