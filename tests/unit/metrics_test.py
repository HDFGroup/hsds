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
import asyncio
import sys
import unittest

from aiohttp import web
from aiohttp.test_utils import make_mocked_request

sys.path.append("../..")
from hsds import metrics
from hsds.util.fileClient import FileClient
from hsds.util.lruCache import LruCache


def parse(text):
    """parse Prometheus exposition text into a {series: value} dict"""
    samples = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        key, value = line.rsplit(" ", 1)
        samples[key] = float(value)
    return samples


class FileClientStatsTest(unittest.TestCase):
    """storage metrics depend on the file_stats counters actually counting"""

    def testStatsIncrement(self):
        client = FileClient.__new__(FileClient)  # skip __init__, needs root_dir config
        client._app = {}
        client._file_stats_increment("bytes_out", inc=42)
        client._file_stats_increment("bytes_out", inc=8)
        client._file_stats_increment("error_count")
        stats = client._app["file_stats"]
        self.assertEqual(stats["bytes_out"], 50)
        self.assertEqual(stats["error_count"], 1)


class MetricsTest(unittest.TestCase):
    def setUp(self):
        metrics.reset()

    def testHttpRequestMetrics(self):
        metrics.observe_request("GET", 200, 0.05)
        metrics.observe_request("GET", 200, 0.2)
        metrics.observe_request("PUT", 503, 2.0)
        text = metrics.render({})
        samples = parse(text)
        self.assertEqual(samples['hsds_http_requests_total{method="GET",status="200"}'], 2)
        self.assertEqual(samples['hsds_http_requests_total{method="PUT",status="503"}'], 1)
        self.assertEqual(samples["hsds_http_request_duration_seconds_count"], 3)
        self.assertAlmostEqual(samples["hsds_http_request_duration_seconds_sum"], 2.25)
        # histogram buckets are cumulative
        self.assertEqual(samples['hsds_http_request_duration_seconds_bucket{le="0.1"}'], 1)
        self.assertEqual(samples['hsds_http_request_duration_seconds_bucket{le="0.25"}'], 2)
        self.assertEqual(samples['hsds_http_request_duration_seconds_bucket{le="+Inf"}'], 3)

    def testNodeMetrics(self):
        app = {
            "node_state": "READY",
            "node_type": "sn",
            "id": "sn-001",
            "start_time": 1234567890,
            "dn_urls": ["http://dn1:6101", "http://dn2:6101"],
            "max_task_count": 100,
        }
        text = metrics.render(app)
        samples = parse(text)
        self.assertEqual(samples["hsds_node_ready"], 1)
        self.assertEqual(samples["hsds_start_time_seconds"], 1234567890)
        self.assertEqual(samples["hsds_active_dn_count"], 2)
        self.assertEqual(samples["hsds_tasks_max"], 100)
        self.assertIn('node_type="sn"', text)
        self.assertIn("hsds_tasks_active", text)
        self.assertIn('hsds_log_events_total{level="ERROR"}', text)
        self.assertIn('hsds_log_events_total{level="WARN"}', text)

        app["node_state"] = "TERMINATING"
        samples = parse(metrics.render(app))
        self.assertEqual(samples["hsds_node_ready"], 0)

    def testCacheMetrics(self):
        app = {"chunk_cache": LruCache(mem_target=1000)}
        samples = parse(metrics.render(app))
        self.assertEqual(samples['hsds_cache_items{cache="chunk"}'], 0)
        self.assertEqual(samples['hsds_cache_dirty_items{cache="chunk"}'], 0)
        self.assertEqual(samples['hsds_cache_mem_used_bytes{cache="chunk"}'], 0)
        self.assertEqual(samples['hsds_cache_mem_target_bytes{cache="chunk"}'], 1000)

    def testStorageMetrics(self):
        s3_stats = {"error_count": 1, "bytes_in": 100, "bytes_out": 50}
        app = {"s3_stats": s3_stats}
        samples = parse(metrics.render(app))
        self.assertEqual(samples['hsds_storage_errors_total{backend="s3"}'], 1)
        self.assertEqual(samples['hsds_storage_bytes_read_total{backend="s3"}'], 100)
        self.assertEqual(samples['hsds_storage_bytes_written_total{backend="s3"}'], 50)

    def testMiddleware(self):
        async def run():
            async def ok_handler(request):
                return web.Response(status=204)

            async def err_handler(request):
                raise web.HTTPServiceUnavailable()

            req = make_mocked_request("GET", "/domains")
            resp = await metrics.metrics_middleware(req, ok_handler)
            self.assertEqual(resp.status, 204)

            req = make_mocked_request("PUT", "/domains")
            with self.assertRaises(web.HTTPServiceUnavailable):
                await metrics.metrics_middleware(req, err_handler)

        asyncio.run(run())
        samples = parse(metrics.render({}))
        self.assertEqual(samples['hsds_http_requests_total{method="GET",status="204"}'], 1)
        self.assertEqual(samples['hsds_http_requests_total{method="PUT",status="503"}'], 1)

    def testMetricsHandler(self):
        async def run():
            app = {"node_state": "READY"}
            req = make_mocked_request("GET", "/metrics", app=app)
            resp = await metrics.metrics_handler(req)
            self.assertEqual(resp.status, 200)
            self.assertTrue(resp.headers["Content-Type"].startswith("text/plain"))
            self.assertIn(b"hsds_node_ready 1", resp.body)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
