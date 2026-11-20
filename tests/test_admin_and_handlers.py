import httpx
import pytest

from app.handlers import PermanentError, TransientError, default_registry, forwarder
from app.store import Claimed

from .conftest import event_body


def claimed(payload=None, attempt=1):
    return Claimed(1, "ats", "evt_1", "candidate.placed", (payload or event_body()).decode(), attempt, "tok")


# ---- admin API --------------------------------------------------------------


@pytest.mark.parametrize("path", ["/events", "/events/1", "/stats"])
def test_admin_endpoints_need_the_token(client, path):
    assert client.get(path).status_code == 401
    assert client.get(path, headers={"Authorization": "Bearer wrong"}).status_code == 401


def test_health_is_public(client):
    assert client.get("/health").json() == {"ok": True}


def test_list_filter_and_detail(client, post, admin_headers, worker, registry):
    post(event_body("a"))
    post(event_body("b"))
    registry.routes[("ats", "candidate.placed")] = lambda c: None
    worker.run_once()
    done = client.get("/events?status=done", headers=admin_headers).json()
    assert {e["event_id"] for e in done} == {"a", "b"}
    detail = client.get(f"/events/{done[0]['id']}", headers=admin_headers).json()
    assert detail["history"][0]["outcome"] == "ok"


def test_replay_a_dead_event(client, post, admin_headers, worker, registry, store):
    pk = post(event_body()).json()["id"]
    registry.fallback = lambda c: (_ for _ in ()).throw(PermanentError("bad"))
    worker.run_once()
    assert store.get(pk)["status"] == "dead"

    assert client.post(f"/events/{pk}/replay", headers=admin_headers).status_code == 200
    registry.fallback = lambda c: None
    worker.run_once()
    ev = store.get(pk)
    assert ev["status"] == "done"
    assert [h["outcome"] for h in ev["history"]] == ["dead", "ok"], "history survives the replay"


def test_replay_refuses_pending_events(client, post, admin_headers):
    pk = post(event_body()).json()["id"]
    assert client.post(f"/events/{pk}/replay", headers=admin_headers).status_code == 409
    assert client.post("/events/999/replay", headers=admin_headers).status_code == 404


def test_stats_report_counts_and_lag(client, post, admin_headers, clock):
    post(event_body("a"))
    post(event_body("b"))
    clock.advance(42)
    s = client.get("/stats", headers=admin_headers).json()
    assert s["by_status"]["pending"] == 2
    assert s["lag_s"] == 42


# ---- forwarder (no network: httpx MockTransport) ----------------------------


def fwd(handler):
    return forwarder("https://downstream.test/hook", 1, httpx.Client(transport=httpx.MockTransport(handler)))


def test_forwarder_sends_body_with_an_idempotency_key():
    got = {}

    def ok(req: httpx.Request):
        got.update(key=req.headers["idempotency-key"], attempt=req.headers["x-attempt"], body=req.content)
        return httpx.Response(204)

    fwd(ok)(claimed(attempt=3))
    assert got == {"key": "ats:evt_1", "attempt": "3", "body": event_body()}


@pytest.mark.parametrize("code", [500, 502, 503, 408, 429])
def test_forwarder_treats_server_errors_and_throttling_as_transient(code):
    with pytest.raises(TransientError):
        fwd(lambda r: httpx.Response(code))(claimed())


@pytest.mark.parametrize("code", [400, 401, 404, 422])
def test_forwarder_treats_client_errors_as_permanent(code):
    with pytest.raises(PermanentError):
        fwd(lambda r: httpx.Response(code, text="nope"))(claimed())


def test_forwarder_reads_retry_after():
    with pytest.raises(TransientError) as e:
        fwd(lambda r: httpx.Response(429, headers={"retry-after": "120"}))(claimed())
    assert e.value.retry_after_s == 120


def test_forwarder_timeouts_are_transient():
    def slow(req):
        raise httpx.ReadTimeout("slow", request=req)

    with pytest.raises(TransientError, match="timeout"):
        fwd(slow)(claimed())


def test_placed_handler_rejects_incomplete_payloads_permanently():
    reg = default_registry(None, 1)
    handler = reg.resolve("ats", "candidate.placed")
    handler(claimed())  # complete: fine
    with pytest.raises(PermanentError, match="start_date"):
        handler(claimed(event_body(candidate_id="c", job_order_id="j")))
