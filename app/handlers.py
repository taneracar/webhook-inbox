"""What happens to an accepted event. Handlers raise to say how a failure
should be treated; the worker owns retries, so handlers stay simple."""

import json
from collections.abc import Callable
from dataclasses import dataclass, field

import httpx

from .store import Claimed


class TransientError(Exception):
    """Worth retrying: timeouts, 5xx, 429, connection errors."""

    def __init__(self, message: str, retry_after_s: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_s = retry_after_s


class PermanentError(Exception):
    """Retrying will not help: bad payload, 4xx from downstream."""


Handler = Callable[[Claimed], None]


@dataclass
class Registry:
    """Routes (source, type) to a handler. `fallback` covers everything else."""

    routes: dict[tuple[str, str], Handler] = field(default_factory=dict)
    fallback: Handler | None = None

    def on(self, source: str, type_: str) -> Callable[[Handler], Handler]:
        def deco(fn: Handler) -> Handler:
            self.routes[(source, type_)] = fn
            return fn

        return deco

    def resolve(self, source: str, type_: str) -> Handler | None:
        return self.routes.get((source, type_)) or self.fallback


def _retry_after(resp: httpx.Response) -> float | None:
    value = resp.headers.get("retry-after", "")
    try:
        return max(0.0, float(value))
    except ValueError:
        return None  # HTTP-date form: fall back to normal backoff


def forwarder(url: str, timeout_s: float, client: httpx.Client | None = None) -> Handler:
    """Forward the event body to a downstream service and classify the result."""
    http = client or httpx.Client(timeout=timeout_s)

    def handle(c: Claimed) -> None:
        try:
            resp = http.post(
                url,
                content=c.payload,
                headers={
                    "content-type": "application/json",
                    # lets the downstream dedupe too: same key on every retry
                    "idempotency-key": f"{c.source}:{c.event_id}",
                    "x-attempt": str(c.attempt),
                },
            )
        except httpx.TimeoutException as e:
            raise TransientError(f"timeout: {e}") from e
        except httpx.TransportError as e:
            raise TransientError(f"transport: {e}") from e

        if resp.status_code < 300:
            return
        if resp.status_code == 429 or resp.status_code == 408 or resp.status_code >= 500:
            raise TransientError(f"downstream {resp.status_code}", _retry_after(resp))
        raise PermanentError(f"downstream {resp.status_code}: {resp.text[:200]}")

    return handle


def default_registry(downstream_url: str | None, timeout_s: float) -> Registry:
    reg = Registry()

    @reg.on("ats", "candidate.placed")
    def placed(c: Claimed) -> None:
        body = json.loads(c.payload)
        data = body.get("data") or {}
        missing = [k for k in ("candidate_id", "job_order_id", "start_date") if not data.get(k)]
        if missing:
            raise PermanentError(f"candidate.placed missing {', '.join(missing)}")
        if downstream_url:
            forwarder(downstream_url, timeout_s)(c)

    if downstream_url:
        reg.fallback = forwarder(downstream_url, timeout_s)
    return reg
