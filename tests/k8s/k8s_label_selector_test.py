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
from os import getpid
from pathlib import Path
from tempfile import NamedTemporaryFile

import re
from time import sleep
import requests
import subprocess
import unittest


TEST_DIR = Path(__file__).parent
CONFIG_FILE = TEST_DIR.parents[1] / "admin" / "config" / "config.yml"
assert CONFIG_FILE.is_file(), f"Wrong config.yml path {CONFIG_FILE}"


def run(args):
    ret = subprocess.run(args)
    ret.check_returncode()


class K8sListDnTest(unittest.TestCase):
    def setUp(self):
        # Usa a unique temporary K8s namespace to isolate the test environment:
        self.k8s_namespace = f"{__class__.__name__}-{getpid()}".lower()

        try:
            run(["kubectl", "create", "namespace", self.k8s_namespace])
        except FileNotFoundError as e:
            raise unittest.SkipTest("Missing kubectl") from e

        self.kubectl(["create", "configmap", "hsds-config", "--from-file", CONFIG_FILE])

    def tearDown(self):
        run(["kubectl", "delete", "namespaces", self.k8s_namespace])

    def kubectl(self, args, **kwargs):
        args = ["kubectl", "--namespace", self.k8s_namespace] + args

        if not kwargs:
            run(args)
        else:
            return subprocess.Popen(args, **kwargs)

    def create_pods(self, template_file, vars):
        vars = {f"${{HSDS_K8S_{k.upper()}}}": v for k, v in vars.items()}
        vars["${HSDS_K8S_NAMESPACE}"] = self.k8s_namespace

        with NamedTemporaryFile("wt") as file, open(template_file) as template:
            for line in template:
                for key, value in vars.items():
                    line = line.replace(key, value)
                file.write(line)

            file.flush()
            self.kubectl(["apply", "--filename", file.name])

    def forward_port(self, sn_pod_name):
        proc = self.kubectl(["port-forward", sn_pod_name, ":6101"],
                            stdout=subprocess.PIPE, universal_newlines=True)

        class Proc:
            def __init__(self, proc):
                self.proc = proc

            def __getattr__(self, name):
                return getattr(self.proc, name)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                self.proc.stdout.close()
                self.proc.kill()
                self.proc.wait(1)  # Collect the zombie process to avoid ResourceWarning

        proc = Proc(proc)

        line = proc.stdout.readline()

        pattern = re.compile(r".*:(\d+) -> 6101")
        port = pattern.match(line)
        if not port:
            msg = f"Failed parsing forwarded port from kubectl port-forward output '{line}'"
            raise RuntimeError(msg)

        proc.port = port.group(1)
        return proc

    def testK8sListDn(self):
        # Items pattern: { name: dn_replicas }
        ENVS = {"dev": 2, "prod": 3}

        template_file = TEST_DIR / "k8s_label_selector.yml.template"
        for env, dn_replicas in ENVS.items():
            vars = {"env": env, "dn_replicas": str(dn_replicas)}
            self.create_pods(template_file, vars)

        sleep(75)  # Give the pods time to start and HSDS to initialize

        for env, dn_replicas in ENVS.items():
            with self.forward_port(f"hsds-sn-{env}") as proc:
                resp = requests.get(f"http://localhost:{proc.port}/info")

                self.assertEqual(resp.status_code, requests.codes.ok,
                                 f"env={env}, reason={resp.reason}, body={resp.text}")

                self.assertEqual(dn_replicas, resp.json()["node"]["node_count"],
                                 f"env={env}")

    def testK8sAppLabel(self):
        """Check backward compatibility with old "k8s_app_label" config entry"""

        dn_replicas = 3

        template_file = TEST_DIR / "k8s_app_label.yml.template"
        vars = {"dn_replicas": str(dn_replicas)}
        self.create_pods(template_file, vars)

        sleep(60)  # Give the pods time to start and HSDS to initialize

        with self.forward_port("hsds-sn") as proc:
            resp = requests.get(f"http://localhost:{proc.port}/info")

            self.assertEqual(resp.status_code, requests.codes.ok,
                             f"reason={resp.reason}, body={resp.text}")

            self.assertEqual(dn_replicas, resp.json()["node"]["node_count"])


if __name__ == "__main__":
    unittest.main()
