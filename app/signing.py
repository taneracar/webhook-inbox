"""HMAC-SHA256 webhook signatures.

Header format (same shape Stripe uses):  X-Signature: t=<unix seconds>,v1=<hex>
The signed string is "<t>.<raw body>". Signing the timestamp together with the
body means an attacker cannot replay an old request with a fresh timestamp.
More than one v1 is allowed so a sender can rotate secrets without downtime.
"""

import hashlib
import hmac

HEADER = "X-Signature"


class SignatureError(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def sign(secret: str, timestamp_s: int, body: bytes) -> str:
    mac = hmac.new(secret.encode(), f"{timestamp_s}.".encode() + body, hashlib.sha256)
    return f"t={timestamp_s},v1={mac.hexdigest()}"


def verify(header: str | None, body: bytes, secret: str, now_s: int, tolerance_s: int) -> int:
    """Return the signed timestamp, or raise SignatureError with a short reason."""
    if not header:
        raise SignatureError("missing signature")

    timestamp: int | None = None
    candidates: list[str] = []
    for part in header.split(","):
        key, _, value = part.strip().partition("=")
        if key == "t" and value.isdigit():
            timestamp = int(value)
        elif key == "v1" and value:
            candidates.append(value)
    if timestamp is None or not candidates:
        raise SignatureError("malformed signature")

    if abs(now_s - timestamp) > tolerance_s:
        raise SignatureError("timestamp outside tolerance")

    expected = sign(secret, timestamp, body).split("v1=", 1)[1]
    # compare_digest is constant-time, so timing does not leak how many characters matched
    if not any(hmac.compare_digest(expected, c) for c in candidates):
        raise SignatureError("signature mismatch")
    return timestamp
