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
import contextvars
import io
import json
import re
import sys
import unittest
from contextlib import redirect_stdout

sys.path.append("../..")
from hsds import hsds_logger as log

ISO_TS = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z"
HEX32 = "0123456789abcdef01234567890abcde"
HEX16 = "0123456789abcdef"


def capture(func, *args, **kwargs):
    """Run func in a copied context (so trace context doesn't leak between
    tests) and return whatever it printed to stdout."""
    buf = io.StringIO()
    ctx = contextvars.copy_context()
    with redirect_stdout(buf):
        ctx.run(func, *args, **kwargs)
    return buf.getvalue()


class LoggerTest(unittest.TestCase):
    def setUp(self):
        log.setLogConfig("DEBUG", prefix="", timestamps=False, log_format="text")

    def testTextFormat(self):
        out = capture(log.info, "hello")
        self.assertEqual(out, "INFO> hello\n")
        out = capture(log.warn, "uh oh")
        self.assertEqual(out, "WARN> uh oh\n")

    def testTextPrefixAndTimestamp(self):
        log.setLogConfig("INFO", prefix="sn1 ", timestamps=True)
        out = capture(log.info, "hello")
        self.assertTrue(re.fullmatch(f"sn1 {ISO_TS} INFO> hello\n", out), out)

    def testLevelFiltering(self):
        log.setLogConfig("WARNING")
        self.assertEqual(capture(log.debug, "nope"), "")
        self.assertEqual(capture(log.info, "nope"), "")
        self.assertEqual(capture(log.error, "yep"), "ERROR> yep\n")

    def testJsonFormat(self):
        log.setLogConfig("INFO", log_format="json")
        out = capture(log.warning, "watch out")
        obj = json.loads(out)
        self.assertEqual(obj["level"], "WARN")
        self.assertEqual(obj["msg"], "watch out")
        self.assertTrue(re.fullmatch(ISO_TS, obj["time"]), obj)
        self.assertNotIn("trace_id", obj)

    def testJsonNodePrefix(self):
        log.setLogConfig("INFO", prefix="dn2 ", log_format="json")
        obj = json.loads(capture(log.info, "hi"))
        self.assertEqual(obj["node"], "dn2")

    def testBadConfig(self):
        with self.assertRaises(ValueError):
            log.setLogConfig("VERBOSE")
        with self.assertRaises(ValueError):
            log.setLogConfig("INFO", log_format="xml")

    def testNewTraceContextFromHeader(self):
        def check():
            traceparent = f"00-{HEX32}-{HEX16}-01"
            trace_id = log.newTraceContext(traceparent)
            self.assertEqual(trace_id, HEX32)
            self.assertEqual(log.getTraceId(), HEX32)
            outgoing = log.getTraceParent()
            # same trace, but a new span id for this hop
            parts = outgoing.split("-")
            self.assertEqual(len(parts), 4)
            self.assertEqual(parts[0], "00")
            self.assertEqual(parts[1], HEX32)
            self.assertNotEqual(parts[2], HEX16)
            self.assertTrue(re.fullmatch(r"[0-9a-f]{16}", parts[2]))
            self.assertEqual(parts[3], "01")

        contextvars.copy_context().run(check)

    def testNewTraceContextGenerated(self):
        def check():
            trace_id = log.newTraceContext(None)
            self.assertTrue(re.fullmatch(r"[0-9a-f]{32}", trace_id))
            other = log.newTraceContext(None)
            self.assertNotEqual(trace_id, other)

        contextvars.copy_context().run(check)

    def testNewTraceContextInvalidHeader(self):
        def check():
            for bad in ("junk", "00-zz-yy-01", f"00-{'0' * 32}-{HEX16}-01"):
                trace_id = log.newTraceContext(bad)
                self.assertTrue(re.fullmatch(r"[0-9a-f]{32}", trace_id), bad)

        contextvars.copy_context().run(check)

    def testNoTraceContext(self):
        self.assertIsNone(log.getTraceId())
        self.assertIsNone(log.getTraceParent())

    def testTraceIdInTextOutput(self):
        def emit():
            log.newTraceContext(f"00-{HEX32}-{HEX16}-01")
            log.info("with trace")

        out = capture(emit)
        self.assertEqual(out, f"INFO> [{HEX32}] with trace\n")

    def testTraceIdInJsonOutput(self):
        log.setLogConfig("INFO", log_format="json")

        def emit():
            log.newTraceContext(f"00-{HEX32}-{HEX16}-01")
            log.info("with trace")

        obj = json.loads(capture(emit))
        self.assertEqual(obj["trace_id"], HEX32)

    def testLogCount(self):
        before = log.log_count["INFO"]
        capture(log.info, "counted")
        self.assertEqual(log.log_count["INFO"], before + 1)


class TracePropagationTest(unittest.IsolatedAsyncioTestCase):
    """The traceparent header is forwarded on all outgoing http_* requests
    (SN -> DN hops) when a trace context is set."""

    async def test_http_methods_send_traceparent(self):
        import aiohttp
        from aiohttp import web
        from aiohttp.test_utils import TestServer
        from hsds.util.httpUtil import http_get, http_post, http_put, http_delete

        log.setLogConfig("ERROR")  # quiet the http_* info logging
        seen = {}

        async def handler(request):
            seen[request.method] = request.headers.get("traceparent")
            return web.json_response({"ok": True})

        app = web.Application()
        app.router.add_route("*", "/", handler)
        server = TestServer(app)
        await server.start_server()
        try:
            async with aiohttp.ClientSession() as session:
                log.newTraceContext(None)
                url = str(server.make_url("/"))
                await http_get({}, url, client=session)
                await http_post({}, url, data={}, client=session)
                await http_put({}, url, data={}, client=session)
                await http_delete({}, url, client=session)
        finally:
            await server.close()

        expected = log.getTraceParent()
        self.assertIsNotNone(expected)
        for method in ("GET", "POST", "PUT", "DELETE"):
            self.assertEqual(seen.get(method), expected, method)


if __name__ == "__main__":
    unittest.main()
