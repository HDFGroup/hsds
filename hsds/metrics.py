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
# Prometheus metrics exporter - serves /metrics in text exposition format
#
import asyncio
import contextlib
import time

from aiohttp import web
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

from . import hsds_logger as log

# request duration histogram bucket upper bounds, in seconds
BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)

_CACHES = ("meta_cache", "chunk_cache", "domain_cache")
_STORAGE_STATS = ("s3_stats", "azure_stats", "file_stats")


def _make_metrics():
    """create the counters/gauges/histograms held in the default registry"""
    return {
        "http_requests": Counter(
            "hsds_http_requests",
            "HTTP requests by method and status",
            ["method", "status"],
        ),
        "http_duration": Histogram(
            "hsds_http_request_duration_seconds",
            "HTTP request latency",
            buckets=BUCKETS,
        ),
        "internal_requests": Counter(
            "hsds_internal_requests",
            "Internal (SN->DN) requests by method and status",
            ["method", "status"],
        ),
        "internal_duration": Histogram(
            "hsds_internal_request_duration_seconds",
            "Internal (SN->DN) request latency",
            buckets=BUCKETS,
        ),
        "crawler_queue": Gauge(
            "hsds_crawler_queue_depth",
            "Items waiting in crawler queues",
            ["crawler"],
        ),
        "crawler_workers": Gauge(
            "hsds_crawler_active_workers",
            "Crawler worker tasks currently processing an item",
            ["crawler"],
        ),
        "housekeeping_last_success": Gauge(
            "hsds_housekeeping_last_success_timestamp_seconds",
            "Unix time a housekeeping task last completed successfully",
            ["task"],
        ),
        "housekeeping_duration": Histogram(
            "hsds_housekeeping_duration_seconds",
            "Housekeeping task run time",
            ["task"],
            buckets=BUCKETS,
        ),
    }


_METRICS = _make_metrics()


def reset():
    """re-create metrics with zeroed counts (used by tests)"""
    global _METRICS
    for collector in _METRICS.values():
        REGISTRY.unregister(collector)
    _METRICS = _make_metrics()


def observe_request(method, status, duration):
    """record one completed http request"""
    _METRICS["http_requests"].labels(method=method, status=str(int(status))).inc()
    _METRICS["http_duration"].observe(duration)


def observe_internal_request(method, status, duration):
    """record one completed internal SN->DN request"""
    _METRICS["internal_requests"].labels(method=method, status=str(status)).inc()
    _METRICS["internal_duration"].observe(duration)


def make_trace_config():
    """aiohttp TraceConfig that records outgoing (SN->DN) request metrics.

    Attach to every ClientSession so internal fan-out is measured without touching
    each call site.
    """
    from aiohttp import TraceConfig

    async def on_start(session, ctx, params):
        ctx.start = asyncio.get_event_loop().time()

    async def on_end(session, ctx, params):
        elapsed = asyncio.get_event_loop().time() - ctx.start
        observe_internal_request(params.method, params.response.status, elapsed)

    async def on_exception(session, ctx, params):
        elapsed = asyncio.get_event_loop().time() - ctx.start
        observe_internal_request(params.method, "error", elapsed)

    trace_config = TraceConfig()
    trace_config.on_request_start.append(on_start)
    trace_config.on_request_end.append(on_end)
    trace_config.on_request_exception.append(on_exception)
    return trace_config


def crawler_enqueued(crawler, n=1):
    """record n items added to a crawler's queue"""
    _METRICS["crawler_queue"].labels(crawler=crawler).inc(n)


@contextlib.contextmanager
def crawler_task(crawler):
    """wrap processing of one dequeued item: queue depth down, worker busy for the
    duration"""
    _METRICS["crawler_queue"].labels(crawler=crawler).dec()
    workers = _METRICS["crawler_workers"].labels(crawler=crawler)
    workers.inc()
    try:
        yield
    finally:
        workers.dec()


@contextlib.contextmanager
def housekeeping(task):
    """time an out-of-band task; advance its last-success timestamp only on clean
    completion (a stuck/failing loop then shows up as a stale timestamp)"""
    start = time.time()
    try:
        yield
        _METRICS["housekeeping_last_success"].labels(task=task).set(time.time())
    finally:
        _METRICS["housekeeping_duration"].labels(task=task).observe(time.time() - start)


@web.middleware
async def metrics_middleware(request, handler):
    """count every request and its latency, including error responses"""
    loop = asyncio.get_event_loop()
    start = loop.time()
    status = 500
    try:
        resp = await handler(request)
        status = resp.status
        return resp
    except web.HTTPException as he:
        status = he.status
        raise
    finally:
        observe_request(request.method, status, loop.time() - start)


class AppStateCollector:
    """expose state the app already tracks (node, caches, storage counters)
    as Prometheus metrics, read at scrape time"""

    def __init__(self, app):
        self._app = app

    def collect(self):
        from .basenode import getVersion  # deferred to avoid circular import

        app = self._app

        info = GaugeMetricFamily(
            "hsds_info",
            "Node identity: type, id and hsds version",
            labels=["node_type", "node_id", "version"],
        )
        node_type = app.get("node_type", "")
        info.add_metric([node_type, app.get("id", ""), getVersion()], 1)
        yield info

        ready = 1 if app.get("node_state") == "READY" else 0
        yield GaugeMetricFamily(
            "hsds_node_ready", "1 if node state is READY, 0 otherwise", value=ready
        )

        if "start_time" in app:
            yield GaugeMetricFamily(
                "hsds_start_time_seconds",
                "Unix time the node started",
                value=app["start_time"],
            )

        if "dn_urls" in app:
            yield GaugeMetricFamily(
                "hsds_active_dn_count",
                "Data nodes this node currently knows about",
                value=len(app["dn_urls"]),
            )

        try:
            num_tasks = len(asyncio.all_tasks())
        except RuntimeError:
            num_tasks = 0  # no running event loop
        yield GaugeMetricFamily(
            "hsds_tasks_active", "Current asyncio task count", value=num_tasks
        )

        if app.get("max_task_count"):
            yield GaugeMetricFamily(
                "hsds_tasks_max",
                "Task count above which SN nodes return 503",
                value=app["max_task_count"],
            )

        log_events = CounterMetricFamily(
            "hsds_log_events", "Log events by level", labels=["level"]
        )
        for level in ("WARN", "ERROR"):
            log_events.add_metric([level], log.log_count.get(level, 0))
        yield log_events

        caches = [(key[: -len("_cache")], app[key]) for key in _CACHES if key in app]
        if caches:
            items = GaugeMetricFamily(
                "hsds_cache_items", "Objects held in cache", labels=["cache"]
            )
            dirty = GaugeMetricFamily(
                "hsds_cache_dirty_items",
                "Cache objects not yet flushed to storage",
                labels=["cache"],
            )
            mem_used = GaugeMetricFamily(
                "hsds_cache_mem_used_bytes", "Cache memory used", labels=["cache"]
            )
            mem_target = GaugeMetricFamily(
                "hsds_cache_mem_target_bytes", "Cache memory target", labels=["cache"]
            )
            for name, cache in caches:
                items.add_metric([name], len(cache))
                dirty.add_metric([name], cache.dirtyCount)
                mem_used.add_metric([name], cache.memUsed)
                mem_target.add_metric([name], cache.memTarget)
            yield items
            yield dirty
            yield mem_used
            yield mem_target

        for key in _STORAGE_STATS:
            if key not in app:
                continue
            backend = key[: -len("_stats")]
            stats = app[key]
            errors = CounterMetricFamily(
                "hsds_storage_errors", "Storage operation errors", labels=["backend"]
            )
            errors.add_metric([backend], stats.get("error_count", 0))
            yield errors
            bytes_read = CounterMetricFamily(
                "hsds_storage_bytes_read", "Bytes read from storage", labels=["backend"]
            )
            bytes_read.add_metric([backend], stats.get("bytes_in", 0))
            yield bytes_read
            bytes_written = CounterMetricFamily(
                "hsds_storage_bytes_written",
                "Bytes written to storage",
                labels=["backend"],
            )
            bytes_written.add_metric([backend], stats.get("bytes_out", 0))
            yield bytes_written
            break  # only one storage backend is active per node


def render(app):
    """render app state plus process/http metrics as Prometheus text format"""
    app_registry = CollectorRegistry()
    app_registry.register(AppStateCollector(app))
    text = generate_latest(app_registry).decode("utf-8")
    text += generate_latest(REGISTRY).decode("utf-8")
    return text


async def metrics_handler(request):
    """HTTP GET handler for /metrics - Prometheus scrape endpoint"""
    text = render(request.app)
    headers = {"Content-Type": CONTENT_TYPE_LATEST}
    return web.Response(body=text.encode("utf-8"), headers=headers)
