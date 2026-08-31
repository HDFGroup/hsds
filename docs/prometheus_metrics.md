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
- **Cluster membership** — `hsds_node_ready` and `hsds_active_dns`
  detect nodes stuck outside `READY` and SNs that lost sight of DNs
  (`hsds_active_dns` dropping below the expected DN count means reduced
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
- **Async internals** — the HTTP middleware only sees top-level requests, so
  three things it can't see are instrumented directly:
  `hsds_internal_requests_total` / `hsds_internal_request_duration_seconds`
  measure SN→DN fan-out (one client request can spawn hundreds of internal
  calls), telling a slow DN apart from a slow SN; `hsds_crawler_queue_depth` /
  `hsds_crawler_active_workers` expose crawler saturation (a growing queue with
  workers pegged is overload *before* any 503); and
  `hsds_housekeeping_last_success_timestamp_seconds` catches a stuck or dead
  `healthCheck` loop.

The default registry also provides `process_*` and `python_gc_*` metrics
(CPU, RSS, FDs, GC) at no extra cost.

## Metric reference

| Metric | Type | Labels | Source |
|---|---|---|---|
| `hsds_info` | gauge | `node_type`, `node_id`, `version` | node identity, useful for version rollout tracking |
| `hsds_node_ready` | gauge | | 1 when node state is `READY` |
| `hsds_start_time_seconds` | gauge | | node start time (restart detection) |
| `hsds_active_dns` | gauge | | DN urls this node currently knows about |
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
| `hsds_internal_requests_total` | counter | `method`, `status` | internal SN→DN requests, via ClientSession TraceConfig |
| `hsds_internal_request_duration_seconds` | histogram | | internal SN→DN request latency |
| `hsds_crawler_queue_depth` | gauge | `crawler` (`chunk`/`domain`/`folder`) | items waiting in crawler queues |
| `hsds_crawler_active_workers` | gauge | `crawler` | crawler workers currently processing an item |
| `hsds_housekeeping_last_success_timestamp_seconds` | gauge | `task` | unix time a housekeeping task last succeeded |
| `hsds_housekeeping_duration_seconds` | histogram | `task` | housekeeping task run time |

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

Both discovery methods attach an `endpoint="sn"|"dn"` target label — the PodMonitor derives
it from the port name. The Grafana dashboard and several example queries split by that label,
so a scrape that does not set it (e.g. the bare `prometheus.io/scrape` annotation, which also
only reaches the SN) will leave those panels empty.

## Local testing with Docker Compose

`admin/docker/docker-compose.metrics.yml` adds Prometheus and Grafana to any HSDS compose
stack for local experimentation:

```sh
./build.sh --no-lint   # build the image from source (the published image may predate /metrics)
export ROOT_DIR=/tmp/hsds-data BUCKET_NAME=hsds.test HSDS_ENDPOINT=http://localhost:5101
mkdir -p $ROOT_DIR/$BUCKET_NAME
docker compose -f admin/docker/docker-compose.posix.yml \
               -f admin/docker/docker-compose.metrics.yml up -d --scale sn=1 --scale dn=2
```

Prometheus (<http://localhost:9090>) scrapes both node types via
`admin/prometheus/prometheus.yml`, which relabels each job to the same `endpoint` label the
PodMonitor produces. Grafana (<http://localhost:3000>, anonymous admin) ships with the
Prometheus datasource pre-wired.

## Grafana dashboard

`admin/grafana/hsds-dashboard.json` is an "HSDS Internals" dashboard covering readiness,
request rate/latency/errors, task-pool saturation, cache pressure, and storage throughput.
It is exported in Grafana **schema v2** and requires **Grafana 12+**; import it through the UI
(Dashboards → New → Import), as v2 dashboards cannot be loaded via file provisioning. It relies
on the `endpoint` label described above.

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

These rules are also shipped as `admin/prometheus/alert.rules.yml` (validated with
`promtool check rules`):

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
      # SN->DN fan-out failing: a different plane than HsdsHighErrorRate (client
      # facing) or HsdsStorageErrors (object store). Catches the SN losing contact
      # with DNs, or DNs shedding load internally, before it fully becomes client 503s.
      - alert: HsdsInternalErrors
        expr: >
          sum(rate(hsds_internal_requests_total{status=~"error|5.."}[5m]))
          / sum(rate(hsds_internal_requests_total[5m])) > 0.05
        for: 10m
      # healthCheck loop wedged/dead: the node still answers HTTP but has stopped
      # reconciling DN membership. last_success only advances on a clean run, so a
      # stuck loop shows up as a growing gap. The loop runs every node_sleep_time
      # (default 10s), so >180s is ~18 missed cycles. The metric is absent until the
      # first successful run (and in standalone mode, which has no health loop), so
      # this complements HsdsNodeNotReady rather than replacing it.
      - alert: HsdsHousekeepingStale
        expr: time() - hsds_housekeeping_last_success_timestamp_seconds > 180
        for: 1m
```

The remaining new metrics are diagnostic (dashboard) rather than alerting signals:

- `hsds_crawler_queue_depth` / `hsds_crawler_active_workers` — a deep queue is
  normal for a large request (e.g. a selection spanning thousands of chunks), so a
  threshold alert would be noisy. Sustained crawler saturation already surfaces via
  `HsdsTaskSaturation` (crawler workers are asyncio tasks). Use these on a dashboard
  to see *which* crawler is the bottleneck and whether workers are pegged at their
  `max_tasks` ceiling.
- `hsds_internal_request_duration_seconds` — slow DNs already trip
  `HsdsSlowRequests`; this histogram tells you *why* by separating internal fan-out
  latency from SN-side processing time.
