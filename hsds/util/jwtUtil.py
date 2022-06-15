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
import requests
import base64
from aiohttp.web_exceptions import HTTPUnauthorized, HTTPForbidden
from aiohttp.web_exceptions import HTTPNotFound, HTTPServiceUnavailable
from aiohttp.web_exceptions import HTTPInternalServerError
import jwt
from jwt.exceptions import InvalidAudienceError, InvalidSignatureError
from jwt.exceptions import ExpiredSignatureError, DecodeError
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers

from .. import hsds_logger as log
from .. import config

MSONLINE_OPENID_URL = (
    "https://login.microsoftonline.com/common/.well-known/openid-configuration"
)
GOOGLE_OPENID_URL = "https://accounts.google.com/.well-known/openid-configuration"


def verifyBearerToken(app, token):
    # Contact OpenID provider to validate bearer token.
    # if valid, return username, exp, and roles
    username = None
    provider = config.get("openid_provider")

    if not provider:
        log.warn("no OpenID provider configured")
        raise HTTPUnauthorized()
    # Resolve provider into an OpenID configuration.
    log.debug(f"Using OpenID provider: {provider}")
    if provider.lower() == "azure":
        openid_url = MSONLINE_OPENID_URL
    elif provider.lower() == "google":
        openid_url = GOOGLE_OPENID_URL
    else:
        openid_url = config.get("openid_url")
        if not openid_url:
            msg = f"OpenID provider: {provider} requires 'openid_url' "
            msg += "config to be set"
            log.warn(msg)
            raise HTTPUnauthorized()

    audience = config.get("openid_audience")
    claims = config.get("openid_claims").split(",")

    # Maintain Azure defaults for compatibility.
    if not audience:
        audience = config.get("azure_resource_id")

    # If we still don't have a provider and audience, abort.
    if not openid_url or not audience or not claims:
        log.warn("Bearer authorization, but no OpenID configuration.")
        raise HTTPUnauthorized()

    log.debug(f"Bearer authorization, using provider: {provider}")
    log.debug(f"Bearer authorization, using audience: {audience}")
    log.debug(f"Bearer authorization, using claims: {claims}")
    if provider not in ("azure", "google"):
        log.debug(f"Bearer authorization, using openid_url: {openid_url}")

    log.debug(f"token: {token}")

    try:
        token_header = jwt.get_unverified_header(token)
    except DecodeError as de:
        log.warn(f"Decode error in jwt get_unverified_header: {de}")
        raise HTTPUnauthorized()
    except ValueError as ve:
        log.warn(f"Value error in jwt get_unverified_header: {ve}")
        raise HTTPUnauthorized()
    except Exception as e:
        msg = f"Unexpected exception {e.__class__.__name__} "
        msg += f"in jwt get_unverified_header: {e}"
        log.warn(msg)
        raise HTTPUnauthorized()

    try:
        res = requests.get(openid_url, timeout=1.0)
    except requests.exceptions.ConnectionError:
        msg = "connection error for getting openid configuration "
        msg += f"from : {openid_url}"
        log.warn(msg)
        raise HTTPInternalServerError()
    if res.status_code != 200:
        log.warn("Bad response from {openid_url}: {res.status_code}")
        if res.status_code == 404:
            raise HTTPNotFound()
        elif res.status_code == 401:
            raise HTTPUnauthorized()
        elif res.status_code == 403:
            raise HTTPForbidden()
        elif res.status_code == 503:
            raise HTTPServiceUnavailable()
        else:
            raise HTTPInternalServerError()

    jwk_uri = res.json()["jwks_uri"]

    # TBD: cache responses by uri
    res = requests.get(jwk_uri)
    jwk_keys = res.json()
    x5c = None
    rsa = {}
    log.info("_verifyBearerToken")

    # Iterate JWK keys and extract matching x5c chain
    for key in jwk_keys["keys"]:
        if key["kid"] == token_header["kid"]:
            if "x5c" in key:
                x5c = key["x5c"]
            elif "e" in key and "n" in key:
                for field in ["e", "n"]:
                    val = key[field]
                    val = val + "=" * ((4 - len(val) % 4) % 4)
                    val = base64.urlsafe_b64decode(val.encode("utf-8"))
                    rsa[field] = int.from_bytes(val, "big")

    # Use the X5C chain to load a public key.
    if x5c:
        log.debug("using x5c public key")
        cert = "".join(
            [
                "-----BEGIN CERTIFICATE-----\n",
                x5c[0],
                "\n-----END CERTIFICATE-----\n",
            ]
        )
        x509 = load_pem_x509_certificate(cert.encode(), default_backend())
        public_key = x509.public_key()
        """
        public_key_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo)
        log.debug(f"got public key: {public_key_bytes.decode('utf-8')}")
        """

    # Use RSA numbers to load a public key.
    elif rsa:
        log.debug("using rsa public key")
        public_key = RSAPublicNumbers(**rsa).public_key(default_backend())

    # We cannot load a public key.
    else:
        log.error("Unable to extract x5c chain or RSA key from JWK keys")
        raise HTTPInternalServerError()

    # log.debug(f"bearer token - public_key: {public_key}")

    try:
        jwt_decode = jwt.decode(
            token,
            public_key,
            algorithms="RS256",
            audience=audience,
        )
    except InvalidAudienceError:
        log.warn(f"OpenID InvalidAudienceError with {audience}")
        raise HTTPUnauthorized()
    except InvalidSignatureError:
        log.warn("OpenID InvalidSignatureError")
        raise HTTPUnauthorized()
    except ExpiredSignatureError:
        log.warn("OpenID ExpiredSignatureError")
        raise HTTPUnauthorized()
    roles = None
    for name in claims:
        log.debug(f"looking at claim: {name}")
        if name in jwt_decode:
            value = jwt_decode[name]
            log.debug(f"got value: {value} for claim: {name}")
            if name == "unique_name":
                username = value
            elif name == "appid":
                pass  # tbd
            elif name == "roles":
                roles = value
            else:
                log.info(f"ignoring claim: {name} with value: {value}")
        else:
            log.debug(f"claim: {name} not found in bearer token")

    if not username:
        log.warn("unable to retreive username from bearer token")
        raise HTTPUnauthorized()

    exp = None
    log.debug(f"decoded token: {jwt_decode}")
    if "exp" in jwt_decode:
        exp = jwt_decode["exp"]
        log.info(f"decoded bearer token for user: {username}, expired: {exp}")
        if exp < 0:
            log.warn("invalid expire time")
            raise HTTPUnauthorized()
    else:
        log.info(f"decoded bearer token for user: {username}, no expiration")

    return username, exp, roles
