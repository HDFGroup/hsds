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
# Kubernetes utility functions 
#

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
import urllib3
from .. import hsds_logger as log

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def getPodIps(k8s_app_label, k8s_namespace=None):
    # Return list of IPs of all pods in the cluster with given app label
    # (and namespace if set)
    k8s_config.load_incluster_config() #get the config from within the cluster and set it as the default config for all new clients
    c=k8s_client.Configuration() #go and get a copy of the default config
    c.verify_ssl=False #set verify_ssl to false in that config
    k8s_client.Configuration.set_default(c) #make that config the default for all new clients
    v1 = k8s_client.CoreV1Api()
    if k8s_namespace:
        # get pods for given namespace
        log.info(f"getting pods for namespace: {k8s_namespace}")
        ret = v1.list_namespaced_pod(namespace=k8s_namespace)
    else:
        log.info("getting pods for all namespaces")
        ret = v1.list_pod_for_all_namespaces(watch=False)
    pod_ips = []
    for i in ret.items:
        pod_ip = i.status.pod_ip
        if not pod_ip:
            continue
        labels = i.metadata.labels
        if labels and "app" in labels and labels["app"] == k8s_app_label:
            log.info(f"found hsds pod with app label: {k8s_app_label} - ip: {pod_ip}")
            pod_ips.append(pod_ip)
    if not pod_ips:
        log.error("Expected to find at least one hsds pod")
    pod_ips.sort()  # for assigning node numbers
    return pod_ips
  


