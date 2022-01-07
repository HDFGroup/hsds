import os
import sys
import signal
import subprocess
import time
import uuid
import queue
import threading
import logging


def _enqueue_output(out, queue, loglevel):
    for line in iter(out.readline, b''):
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

        if put_line:
            queue.put(line)
    logging.debug("enqueu_output close()")
    out.close()


class HsdsApp:
    """
    Class to initiate and manage sub-process HSDS service
    """

    def __init__(self, username=None, password=None, logger=None, log_level=None, dn_count=1, logfile=None, socket_dir=None, config_dir=None):
        """
        Initializer for class
        """

        """
        # Using tempdir is causing a unicode exception
        # See: https://bugs.python.org/issue32958
        self._tempdir = tempfile.TemporaryDirectory()
        tmp_dir = self._tempdir.name
        """
        # create a random dirname if one is not supplied
        if socket_dir:
            if socket_dir[-1] != '/':
                socket_dir += '/'
        else:
            tmp_dir = "/tmp"  # TBD: will this work on windows?
            rand_name = uuid.uuid4().hex[:8]
            socket_dir = f"{tmp_dir}/hs{rand_name}/"  # TBD: use temp dir
        self._dn_urls = []
        self._socket_paths = []
        self._processes = []
        self._queues = []
        self._threads = []
        self._dn_count = dn_count
        self._username = username
        self._password = password
        self._logfile = logfile
        self._loglevel = log_level
        self._ready = False
        self._config_dir = config_dir

        if logger is None:
            self.log = logging
        else:
            self.log = logger

        if not os.path.isdir(socket_dir):
            os.mkdir(socket_dir)

        self.log.debug(f"HsdsApp init - Using socketdir: {socket_dir}")

        # url-encode any slashed in the socket dir
        socket_url = ""
        for ch in socket_dir:
            if ch == '/':
                socket_url += "%2F"
            else:
                socket_url += ch

        for i in range(dn_count):
            socket_name = f"dn_{(i+1)}.sock"
            dn_url = f"http+unix://{socket_url}{socket_name}"
            self._dn_urls.append(dn_url)
            self._socket_paths.append(f"{socket_dir}{socket_name}")

        # sort the ports so that node_number can be determined based on dn_url
        self._dn_urls.sort()
        self._endpoint = f"http+unix://{socket_url}sn_1.sock"
        self._socket_paths.append(f"{socket_dir}sn_1.sock")
        self._rangeget_url = f"http+unix://{socket_url}rangeget.sock"
        self._socket_paths.append(f"{socket_dir}rangeget.sock")

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def ready(self):
        return self._ready

    def print_process_output(self):
        """ print any queue output from sub-processes
        """
        # print("print_process_output")
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
        for p in self._processes:
            if p.poll() is not None:
                result = p.communicate()
                msg = f"process {p.args[0]} ended, result: {result}"
                self.log.warn(msg)
                # TBD - restart failed process

    def run(self):
        """ startup hsds processes
        """
        if self._processes:
            # just check process state and restart if necessary
            self.check_processes()
            return

        dn_urls_arg = ""
        for dn_url in self._dn_urls:
            if dn_urls_arg:
                dn_urls_arg += ','
            dn_urls_arg += dn_url

        pout = subprocess.PIPE   # will pipe to parent
        # create processes for count dn nodes, sn node, and rangeget node
        count = self._dn_count + 2  # plus 2 for rangeget proxy and sn
        # set PYTHONUNBUFFERED so we can get any output immediately
        os.environ["PYTHONUNBUFFERED"] = "1"
        # TODO: don't modify parent process env, use os.environ.copy(), set, and popen(env=)

        common_args = ["--standalone", ]
        # print("setting log_level to:", args.loglevel)
        # common_args.append(f"--log_level={args.loglevel}")
        common_args.append(f"--dn_urls={dn_urls_arg}")
        common_args.append(f"--rangeget_url={self._rangeget_url}")
        common_args.append(f"--hsds_endpoint={self._endpoint}")
        #common_args.append("--server_name=Direct Connect (HSDS)")
        common_args.append("--password_file=")
        common_args.append("--use_socket")
        if self._config_dir:
            common_args.append(f"--config-dir={self._config_dir}")
        if self._loglevel:
            common_args.append(f"--log_level={self._loglevel}")

        py_exe = sys.executable
        cmd_dir = os.path.join(sys.exec_prefix, "bin")
        for i in range(count):
            if i == 0:
                # args for service node
                pargs = [py_exe,
                         os.path.join(cmd_dir, "hsds-servicenode"),
                         "--log_prefix=sn "]
                if self._username:
                    pargs.append(f"--hs_username={self._username}")
                if self._password:
                    pargs.append(f"--hs_password={self._password}")
                pargs.append(f"--sn_url={self._endpoint}")
                pargs.append("--logfile=sn1.log")
            elif i == 1:
                # args for rangeget node
                pargs = [py_exe,
                         os.path.join(cmd_dir, "hsds-rangeget"),
                         "--log_prefix=rg "]
            else:
                node_number = i - 2  # start with 0
                pargs = [py_exe,
                         os.path.join(cmd_dir, "hsds-datanode"),
                         f"--log_prefix=dn{node_number+1} "]
                pargs.append(f"--dn_urls={dn_urls_arg}")
                pargs.append(f"--node_number={node_number}")
            # logging.info(f"starting {pargs[0]}")
            pargs.extend(common_args)
            p = subprocess.Popen(pargs, bufsize=1, universal_newlines=True, shell=False, stdout=pout)
            self._processes.append(p)
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
        SLEEP_TIME = 0.1  # time to sleep between checking on socket connection
        MAX_INIT_TIME = 10.0  # max time to wait for socket to be initialized

        while True:
            ready = 0
            for socket_path in self._socket_paths:
                if os.path.exists(socket_path):
                    ready += 1
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
        """ terminate hsds processes
        """
        if not self._processes:
            return
        now = time.time()
        logging.info(f"hsds app stop at {now}")
        for p in self._processes:
            logging.info(f"sending SIGINT to {p.args[0]}")
            p.send_signal(signal.SIGINT)
        # wait for sub-proccesses to exit
        SLEEP_TIME = 0.1  # time to sleep between checking on process state
        MAX_WAIT_TIME = 10.0  # max time to wait for sub-process to terminate
        start_ts = time.time()
        while True:
            is_alive = False
            for p in self._processes:
                if p.poll() is None:
                    is_alive = True
            if is_alive:
                logging.debug(f"still alive, sleep {SLEEP_TIME}")
                time.sleep(SLEEP_TIME)
            else:
                logging.debug("all subprocesses exited")
                break
            if time.time() > start_ts + MAX_WAIT_TIME:
                msg = f"failed to terminate after {MAX_WAIT_TIME} seconds"
                self.log.error(msg)
                break

        # kill any reluctant to die processes
        for p in self._processes:
            if p.poll():
                logging.info(f"terminating {p.args[0]}")
                p.terminate()
        self._processes = []
        for t in self._threads:
            del t
        self._threads = []

    def __del__(self):
        """ cleanup class resources """
        self.stop()
        # self._tempdir.cleanup()
