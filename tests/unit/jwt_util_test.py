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
import base64
import sys
import time
import unittest
from unittest.mock import patch, MagicMock

import jwt
from aiohttp.web_exceptions import HTTPUnauthorized, HTTPServiceUnavailable
from cryptography.hazmat.primitives.asymmetric import rsa

sys.path.append("../..")
from hsds.util import jwtUtil  # noqa: E402

OPENID_URL = "https://idp.example/.well-known/openid-configuration"
JWKS_URL = "https://idp.example/jwks"
SETTINGS = {
    "openid_provider": "custom",
    "openid_url": OPENID_URL,
    "openid_audience": "hsds-test",
    "openid_claims": "unique_name,roles",
}


def _b64(n):
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


class FakeProvider:
    """Stands in for requests.get against an OpenID provider, counting fetches."""

    def __init__(self, key, kid):
        numbers = key.public_key().public_numbers()
        self.jwk = {"kid": kid, "kty": "RSA", "e": _b64(numbers.e), "n": _b64(numbers.n)}
        self.calls = 0
        self.down = False

    def get(self, url, timeout=None):
        self.calls += 1
        if self.down:
            raise jwtUtil.requests.exceptions.ConnectionError("provider down")
        rsp = MagicMock(status_code=200)
        if url == OPENID_URL:
            rsp.json.return_value = {"jwks_uri": JWKS_URL}
        else:
            rsp.json.return_value = {"keys": [self.jwk]}
        return rsp


class JwtUtilTest(unittest.TestCase):
    def setUp(self):
        jwtUtil._jwks_cache.clear()
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.provider = FakeProvider(self.key, "k1")
        patches = [
            patch.object(jwtUtil.config, "get", lambda k, default=None: SETTINGS.get(k, default)),
            patch.object(jwtUtil.requests, "get", self.provider.get),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def token(self, kid="k1", key=None, **claims):
        payload = {"unique_name": "alice", "aud": "hsds-test", "exp": time.time() + 600}
        payload.update(claims)
        headers = {"kid": kid} if kid else {}
        return jwt.encode(payload, key or self.key, algorithm="RS256", headers=headers)

    def testValidTokensShareCachedKeys(self):
        for name in ("alice", "bob", "carol"):
            username, exp, _ = jwtUtil.verifyBearerToken(None, self.token(unique_name=name))
            self.assertEqual(username, name)
        self.assertEqual(self.provider.calls, 2)  # one config fetch and one jwks fetch

    def testForgedTokensDontReachProvider(self):
        jwtUtil.verifyBearerToken(None, self.token())
        calls = self.provider.calls
        other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        bad_tokens = [
            self.token(key=other_key),  # bad signature, known kid
            self.token(kid=None),  # no kid
            "not-a-token",
            self.token(aud="someone-else"),
            self.token(exp=time.time() - 10),
            self.token(nbf=time.time() + 600),  # not yet valid
        ]
        for token in bad_tokens:
            with self.assertRaises(HTTPUnauthorized):
                jwtUtil.verifyBearerToken(None, token)
        self.assertEqual(self.provider.calls, calls)

        # unknown kids refetch at most once per JWKS_MIN_REFETCH
        jwtUtil._jwks_cache[OPENID_URL]["tried"] = 0
        for i in range(20):
            with self.assertRaises(HTTPUnauthorized):
                jwtUtil.verifyBearerToken(None, self.token(kid=f"forged-{i}"))
        self.assertEqual(self.provider.calls, calls + 2)

    def testProviderDown(self):
        self.provider.down = True
        with self.assertRaises(HTTPServiceUnavailable):
            jwtUtil.verifyBearerToken(None, self.token())
        calls = self.provider.calls
        with self.assertRaises(HTTPServiceUnavailable):
            jwtUtil.verifyBearerToken(None, self.token())
        self.assertEqual(self.provider.calls, calls)  # failed fetch isn't retried at once

        # with keys already cached, an outage doesn't stop known tokens
        self.provider.down = False
        jwtUtil._jwks_cache.clear()
        jwtUtil.verifyBearerToken(None, self.token())
        self.provider.down = True
        jwtUtil._jwks_cache[OPENID_URL]["fetched"] = 0  # stale
        username, _, _ = jwtUtil.verifyBearerToken(None, self.token())
        self.assertEqual(username, "alice")


if __name__ == "__main__":
    unittest.main()
