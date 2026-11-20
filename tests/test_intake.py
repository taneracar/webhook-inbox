from concurrent.futures import ThreadPoolExecutor

import pytest

from app.signing import sign

from .conftest import SECRET, event_body


def test_signed_event_is_accepted(post):
    r = post(event_body())
    assert r.status_code == 202
    assert r.json()["duplicate"] is False


@pytest.mark.parametrize(
    "kwargs, reason",
    [
        ({"header": ""}, "missing signature"),
        ({"header": "garbage"}, "malformed signature"),
        ({"header": "t=abc,v1=00"}, "malformed signature"),
        ({"secret": "wrong"}, "signature mismatch"),
    ],
)
def test_bad_signatures_are_rejected_with_a_reason(post, kwargs, reason):
    r = post(event_body(), **kwargs)
    assert r.status_code == 401
    assert r.json()["detail"] == reason


def test_tampered_body_fails_even_with_a_valid_looking_signature(post, clock):
    ts = clock.now_ms() // 1000
    good = sign(SECRET, ts, event_body())
    r = post(event_body(candidate_id="c999", job_order_id="j1", start_date="2026-12-01"), header=good)
    assert r.status_code == 401


@pytest.mark.parametrize("skew", [-301, 301])
def test_old_or_future_timestamps_are_rejected(post, clock, skew):
    r = post(event_body(), ts=clock.now_ms() // 1000 + skew)
    assert r.json()["detail"] == "timestamp outside tolerance"


def test_timestamp_at_the_edge_of_tolerance_is_fine(post, clock):
    assert post(event_body(), ts=clock.now_ms() // 1000 - 300).status_code == 202


def test_secret_rotation_accepts_either_signature(post, clock):
    ts = clock.now_ms() // 1000
    body = event_body()
    old = sign("old-secret", ts, body).split("v1=")[1]
    new = sign(SECRET, ts, body).split("v1=")[1]
    assert post(body, header=f"t={ts},v1={old},v1={new}").status_code == 202


def test_each_source_has_its_own_secret(post):
    # signed with the ats secret, sent to billing
    assert post(event_body(), source="billing").status_code == 401


def test_unknown_source_is_404(post):
    assert post(event_body(), source="nope").status_code == 404


def test_duplicate_delivery_is_acknowledged_and_stored_once(post, store):
    first = post(event_body())
    second = post(event_body())
    assert first.status_code == 202
    assert second.status_code == 200
    assert second.json() == {"id": first.json()["id"], "duplicate": True}
    assert len(store.list(None, None, 100)) == 1


def test_same_id_with_different_body_is_a_409(post):
    post(event_body("evt_x"))
    r = post(event_body("evt_x", candidate_id="other", job_order_id="j", start_date="d"))
    assert r.status_code == 409


def test_same_id_from_different_sources_are_different_events(post, clock):
    body = event_body("evt_shared")
    assert post(body).status_code == 202
    assert post(body, source="billing", secret="other").status_code == 202


def test_concurrent_duplicates_create_exactly_one_row(post, store):
    body = event_body("evt_race")
    with ThreadPoolExecutor(max_workers=8) as pool:
        codes = sorted(r.status_code for r in pool.map(lambda _: post(body), range(16)))
    assert codes.count(202) == 1
    assert codes.count(200) == 15
    assert len(store.list(None, None, 100)) == 1


@pytest.mark.parametrize(
    "body, detail",
    [
        (b"not json", "body is not JSON"),
        (b"[1,2]", "body needs string fields 'id' and 'type'"),
        (b'{"id": 5, "type": "x"}', "body needs string fields 'id' and 'type'"),
        (b'{"id": "", "type": "x"}', "'id' must be 1-200 characters"),
    ],
)
def test_malformed_bodies_are_400_after_the_signature_checks_out(post, body, detail):
    r = post(body)
    assert r.status_code == 400
    assert r.json()["detail"] == detail


def test_oversized_body_is_413(post, settings):
    big = event_body(blob="x" * (settings.max_body_bytes + 1))
    assert post(big).status_code == 413
