# Prometheus Metrics

Every HSDS node (SN and DN) serves Prometheus metrics at `GET /metrics` in the
standard text exposition format, implemented with the official
[prometheus_client](https://github.com/prometheus/client_python) library.
The endpoint is always served, even when the node is not in the `READY` state,
so an unhealthy node can still be observed.

## Why these metrics

The set maps the four golden signals (traffic, errors, latency, saturation)
onto what actually degrades an HSDS deployment:

- **Traffic / errors / latency** — `hsds_http_requests_total` and
  `hsds_http_request_duration_seconds` are recorded by an aiohttp middleware
  on every request, including 4xx/5xx responses raised as exceptions. SN
  nodes return 503 when overloaded or when DNs are unreachable, so a rising
  `status="503"` rate is the primary "service is unhealthy" signal.
- **Cluster membership** — `hsds_node_ready` and `hsds_active_dn_count`
  detect nodes stuck outside `READY` and SNs that lost sight of DNs
  (`hsds_active_dn_count` dropping below the expected DN count means reduced
  capacity even while requests still succeed).
- **Saturation** — `hsds_tasks_active` vs `hsds_tasks_max`: when active
  asyncio tasks exceed `max_task_count`, SNs shed load with 503s. Watching
  the ratio tells you *before* it happens. Cache gauges
  (`hsds_cache_mem_used_bytes` vs `hsds_cache_mem_target_bytes`) expose
  chunk/meta/domain cache pressure on DNs.
- **Durability risk** — `hsds_cache_dirty_items` counts objects modified in
  memory but not yet flushed to storage. A persistently growing value means
  writes are outpacing storage flushes; those bytes are lost if the pod dies.
- **Storage backend** — `hsds_storage_errors_total`,
  `hsds_storage_bytes_read_total`, `hsds_storage_bytes_written_total`
  (labelled with `backend="s3"|"azure"|"file"`) surface failures and
  throughput of the object store, HSDS's hardest dependency.
- **Log-level counters** — `hsds_log_events_total{level="WARN"|"ERROR"}` is a
  cheap catch-all: alerting on ERROR rate catches problems that have no
  dedicated metric yet.

The default registry also provides `process_*` and `python_gc_*` metrics
(CPU, RSS, FDs, GC) at no extra cost.

## Metric reference

| Metric | Type | Labels | Source |
|---|---|---|---|
| `hsds_info` | gauge | `node_type`, `node_id`, `version` | node identity, useful for version rollout tracking |
| `hsds_node_ready` | gauge | | 1 when node state is `READY` |
| `hsds_start_time_seconds` | gauge | | node start time (restart detection) |
| `hsds_active_dn_count` | gauge | | DN urls this node currently knows about |
| `hsds_tasks_active` | gauge | | current asyncio task count |
| `hsds_tasks_max` | gauge | | task count above which SNs return 503 |
| `hsds_http_requests_total` | counter | `method`, `status` | all HTTP requests, via middleware |
| `hsds_http_request_duration_seconds` | histogram | | request latency, via middleware |
| `hsds_log_events_total` | counter | `level` | WARN/ERROR log line counts |
| `hsds_cache_items` | gauge | `cache` (`meta`/`chunk`/`domain`) | objects held in cache |
| `hsds_cache_dirty_items` | gauge | `cache` | objects awaiting flush to storage |
| `hsds_cache_mem_used_bytes` | gauge | `cache` | cache memory used |
| `hsds_cache_mem_target_bytes` | gauge | `cache` | configured cache memory target |
| `hsds_storage_errors_total` | counter | `backend` | storage client errors |
| `hsds_storage_bytes_read_total` | counter | `backend` | bytes read from storage |
| `hsds_storage_bytes_written_total` | counter | `backend` | bytes written to storage |

## Scraping on Kubernetes

The deployment manifests in `admin/kubernetes/` set the conventional pod
annotations (`prometheus.io/scrape: "true"`, `prometheus.io/port: "5101"`),
which scrape the SN container. Annotation-based discovery only supports one
port per pod; to also scrape the DN container use a PodMonitor
(Prometheus Operator) targeting both named container ports:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: hsds
spec:
  selector:
    matchLabels:
      app: hsds
  podMetricsEndpoints:
    - port: sn
      path: /metrics
    - port: dn
      path: /metrics
```

## Restricting external access to `/metrics` and `/info`

`/metrics` and `/info` (and `/about`) expose operational detail — node topology,
versions, cache and storage internals — that should be reachable by in-cluster scrapers
but not from the public internet.

These endpoints stay fully usable **inside the cluster**: a Prometheus Operator
`PodMonitor` scrapes pod IPs directly, and any in-cluster client can reach the SN
service at `hsds.<namespace>.svc.cluster.local:5101`. Neither path traverses the
Ingress/Gateway, so locking down the public edge doesn't affect them. The DN container
(`:6101`) is never placed in a Service, so it is only ever reachable in-cluster.

Only the **north-south edge** (Ingress or Gateway) needs to block these paths and return
`403` for external callers. Two example manifests route the public API (`/`,
`/datasets/...`, etc.) to the `hsds` service while returning `403` for
`^/(metrics|info|about)$`:

- ingress-nginx + Ingress: [`admin/kubernetes/k8s_ingress_nginx.yml`](../admin/kubernetes/k8s_ingress_nginx.yml)
- Gateway API + Envoy Gateway: [`admin/kubernetes/k8s_gateway_envoy.yml`](../admin/kubernetes/k8s_gateway_envoy.yml)

## Suggested alerts

```yaml
groups:
  - name: hsds
    rules:
      - alert: HsdsNodeNotReady
        expr: hsds_node_ready == 0
        for: 5m
      - alert: HsdsHighErrorRate
        expr: >
          sum(rate(hsds_http_requests_total{status=~"5.."}[5m]))
          / sum(rate(hsds_http_requests_total[5m])) > 0.05
        for: 10m
      - alert: HsdsSlowRequests
        expr: >
          histogram_quantile(0.99,
            rate(hsds_http_request_duration_seconds_bucket[5m])) > 5
        for: 10m
      - alert: HsdsTaskSaturation
        expr: hsds_tasks_active / hsds_tasks_max > 0.8
        for: 5m
      - alert: HsdsDirtyCacheGrowing
        expr: sum by (pod) (hsds_cache_dirty_items) > 100
        for: 15m
      - alert: HsdsStorageErrors
        expr: rate(hsds_storage_errors_total[5m]) > 0
        for: 5m
```
