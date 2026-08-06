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

To simplify deployment and scaling, one SN container and one DN container will be bundled into a K8s "Pod".  In K8s, pods are the minimal deployable unit.  Since scaling happens at the pod level, the number of SN and DN containers will always be the same (which if fine for most use cases).



## 1. Node discovery

Using the python package for Kubernetes, a container can query for other pods running in the same kubernetes namespace.  Unlike with clients running outside the cluster, contiainers don't need passwords.   Here is an example of how this works: <https://github.com/kubernetes-client/python/blob/master/examples/in_cluster_config.py>.  In experimenting with this code, we do see 503 errors, so the RBAC will need to be configured to get around this security wall.  (Not clear if this a change to the cluster itself or just the HSDS deployment).

## 2. Assign node ids for DN nodes

Each DN node will query the K8S runtime to discover the IP/ports of the other HSDS pods (including itself).  This list will be sorted and then node ids assigned by the order in the list.  This action will be peformed periodically to catch any changes in the number of pods.  When a pods node id changes (e.g. as a result of a scaling event), any in-flight are pending S3 writes will be written before updating the node id.  In anycase, the meta-data and chunk caches will be invalidating (since these objects are specific to the partition for that node id).

SN nodes will also perform this action so that they have a map by node id of the DN nodes to communicate with.

Any given node will only set its state to "READY" when the node id from each pod in the cluster (via sending an /info request) matches what is expected based on the pod IP sorting.

## 3. Health checks

Each SN pod will ping each DN node and set the cluster state to READY if all DN nodes are healthy.  It will be assumed that any non-responsive DN node is a temporary state (i.e. not in endless crash loop) and the DN node will either respond or not show up on the least (say the pod is being deleted).

## 4. Node State

The following node states will be used by HSDS for K8s deployments:

1. INITIALIZING: This is the initial state of the node
2. READY: This state will be set when the node ids for each pod is consistent with the IP ordering of pods
3. WAITING: When a node's id changes, but it has pending writes, the node will go to the WAITING state till the writes are written to S3 (this only applies to DN nodes)
4. When a node's id changes but not all nodes are in a consistent state, the state will be set to SCALING
5. When a pod is being shutdown by K8S, a preStop request will be sent to the node, and the node state will be sent to TERMINATING.  Any pending writes will be written to S3 before the node returns from the preStop request.  Kubernetes will wait for the response from preStop before proceeding with the termination (as long as the request can be complete in the timeout window).

The following diagram illustrates the state transitions for ndoes:

![Node State transitions](state_diagram.png)

## 5. Load Balancing and External Access

If external access to the HSDS is not needed (example it will only be using by other K8s applications),
load balancing between the different SN nodes will be via the K8S ClusterIP internal load balancer.

If external access is desired, expose the `hsds` service through an Ingress or the Gateway API. This is also where you terminate TLS and restrict the internal endpoints (`/metrics`, `/info`, `/about`) so they return `403` for outside callers while staying reachable in-cluster. See [Restricting external access](../../prometheus_metrics.md#restricting-external-access-to-metrics-and-info) and the example manifests `admin/kubernetes/k8s_ingress_nginx.yml` (ingress-nginx) and `admin/kubernetes/k8s_gateway_envoy.yml` (Gateway API + Envoy Gateway).

## 6. Secrets

Managing secrets (e.g. AWS authentication tokens) is the preferred method in K8s for handling sensitive information.  (In the Docker version we use environment variables).

## 7. Logging

K8s manages logs (basically just stdout from the containers) much like Docker does.

TBD: research methods for cluster-based log management (e.g. search and rotation).

## 8. Monitoring

Providers such as <https://uptime.com/> are avaiable that can monitor external endpoints and provide an alert if the service is down.

Additional AWS Cloudwatch can be configured to provide alarms based on specified criteria.  (TBD - provide more detail)

## 8. Cluster AutoScaling

For Kubernetes cluster using AWS EKS, the cluster can be configured with auto scaling: <https://docs.aws.amazon.com/eks/latest/userguide/cluster-autoscaler.html>.  For self-manged clusters using KOPS, and add on is avialble to provide auto scaling: <https://github.com/kubernetes/kops/tree/master/addons/cluster-autoscaler>.

When enabled Cluster Autoscaling (CA) has the effect that more VMs will be launched when pods cannote be scheduled due to lack of hardware resources.  Conversely when there is exccess capacity, the cluster can be down-scaled to save cost.

## 9. Horizontal Autoscaling

Cluster Autoscaling by itself will not scale up the number of HSDS pods when either the number of clients is excessively high, or a few number of clients are triggering a significant amount of work (e.g. selection requests that span large number of chunks).  To resolve this issue we need to setup Horizontal Autoscaling (HA) that scales up or down the number of HSDS pods based on a specific criteria.

Common metrics used with HA are CPU utilization or memory usage, however for HSDS a better criteria is when 503 (Server too busy) http responses are returned to the client.  Each HSDS node is configured to handle a specific number of inflight requests (`max_task_count`, defaulting to 100).  When this number is exceeded, a 503 response is returned.  Client libraries such as h5pyd know to use this as a signal to scale back the amount of requests being sent to HSDS.  Task saturation (`hsds_tasks_active / hsds_tasks_max`) is the same signal one step earlier, before any 503 is returned.

Both signals are now exported as Prometheus metrics (see [prometheus_metrics.md](../../prometheus_metrics.md)): `hsds_http_requests_total{status="503"}` and `hsds_tasks_active` / `hsds_tasks_max`.  This means the custom 503 autoscaler this section originally called for is **no longer needed** — a standard HPA or KEDA can consume these metrics directly.  Two approaches, simplest first:

### 9.1 HPA with metrics-server (+ prometheus-adapter)

**metrics-server** feeds CPU/memory to a plain `HorizontalPodAutoscaler`.  This is a coarse starting point (CPU rises under load) that needs no Prometheus:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: hsds
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: hsds
  minReplicas: 2
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

**prometheus-adapter** exposes the HSDS metrics through the `custom.metrics.k8s.io` API so the same HPA can scale on the real signal — the per-pod 503 rate:

```yaml
# prometheus-adapter rule (values.yaml): surface the 503 rate per pod
rules:
  - seriesQuery: 'hsds_http_requests_total{status="503"}'
    resources: {overrides: {pod: {resource: pod}}}
    name: {as: "hsds_http_503_rate"}
    metricsQuery: 'sum(rate(hsds_http_requests_total{status="503"}[2m])) by (pod)'
```

```yaml
# HPA metric block scaling on the custom metric
metrics:
  - type: Pods
    pods:
      metric: {name: hsds_http_503_rate}
      target:
        type: AverageValue
        averageValue: "1"   # scale up when 503s exceed ~1/s/pod
```

Good for clusters that already run metrics-server and want HPA without extra operators.  Limits: combining more than one metric in a single HPA is awkward, scale-down tuning is coarse, and prometheus-adapter rules are cluster-global config.

### 9.2 KEDA ScaledObject (more robust)

[KEDA](https://keda.sh) adds a Prometheus scaler and richer scaling behaviour (multiple triggers, independent up/down cooldowns, replica floors) and manages the underlying HPA for you:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: hsds
spec:
  scaleTargetRef:
    name: hsds            # the hsds Deployment
  minReplicaCount: 2
  maxReplicaCount: 20
  cooldownPeriod: 300     # wait 5m of calm before scaling down
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleDown:
          stabilizationWindowSeconds: 300
  triggers:
    # 1) shed-load signal: cluster-wide 503 rate
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.monitoring.svc.cluster.local:9090
        query: sum(rate(hsds_http_requests_total{status="503"}[2m]))
        threshold: "1"
    # 2) leading signal: task saturation before 503s appear
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.monitoring.svc.cluster.local:9090
        query: max(hsds_tasks_active / hsds_tasks_max)
        threshold: "0.8"
```

Good for production: KEDA scales on *either* trigger, its explicit scale-down stabilization avoids flapping on HSDS's bursty selection workloads, and it needs no cluster-global adapter config.

In both approaches, scale-up events that outrun cluster capacity leave pods in the pending state, which triggers Cluster Autoscaling (section 8) to add machines.
