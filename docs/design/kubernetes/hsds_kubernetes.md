# HSDS Kubernetes Design



------

Running HSDS on Kubernetes would enable scaling (in terms of CPU and network bandwidth) beyond what is possible with a single machine.  This document will outline how HSDS can support Kubernetes with minimal changes to existing code.

------

## 0. Introduction

As a container based architecture, in principle in shouild be fairly easy to move from a Docker based deployment to a Kubernetes (K8S) Cluster deployment.  The main challenges are in how DN nodes will assign node ids to themselves (to form a partition of the S3 space) and how SN nodes will communicate with specific DN nodes (typically in Kubernetes clients connect to a service and thee actual container the client connects to is determined by the Kubernetes runtime).

To deal with this, we'll take an approach similiar to that used for OpenIO:

1. Eliminate head node, SN and DN nodes will use K8S API to discover other nodes
2. Assign node ids based on ordering of internal IPs (similar to how it works with OpenIO)
3. Manage health checks via SN nodes pinging DN nodes (again similar to how OpenIO works) and deal with pods dynamically being created, deleted, or moved
4. Use K8S load balancing rather than nginx
5. Use K8S secrets for AWS keys rather than passing environment variables
6. Logging



## 1. Node discovery

Using the python package for Kubernetes, a container can query for other nodes running in the same kubernetes namespace.  Unlike with clients running outside the cluster, contiainers don't need passwords.   Here is an example of how this works: https://github.com/kubernetes-client/python/blob/master/examples/in_cluster_config.py.  In experimenting with this code, we did see 503 errors, so the RBAC will need to be configured to get around this security wall.  (Not clear if this a change to the cluster itself or just the HSDS deployment)

## 2. Assign node ids for DN nodes

Each DN pod will query the K8S runtime to discover the IP/ports of the other DN pods (including itself).  This list will be sorted and then node ids assigned by the order in the list.  This action will be peformed periodically to catch any changes in the number of pods.  TBD: deal with invalidating the cache and flushing in-flight writes when this changes.  

SN nodes will also perform this action so that they have a map by node id of the DN nodes to communicate with.

## 3. Health checks

Each SN pod will ping each DN node and set the cluster state to READY if all DN nodes are healthy.  It will be assumed that any non-responsive DN node is a temporary state (i.e. not in endless crash loop) and the DN node will either respond or not show up on the least (say the pod is being deleted).

## 4. Load Balancing

TBD 

## 5. Secrets

Managing secrets (e.g. AWS authentication tokens) is the preferred method in K8s for handling sensitive information.  (In the Docker version we use environment variables).

## 6. Logging

K8s manages logs (basically just stdout from the containers) much like Docker does.

TBD: research methods for cluster-based log management (e.g. search and rotation)