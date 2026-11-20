import random
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.handlers import PermanentError, TransientError
from app.retry import backoff_s
from app.worker import Worker

from .conftest import event_body


def accept(store, clock, n=1, prefix="evt"):
    return [
        store.accept("ats", f"{prefix}_{i}", "candidate.placed", event_body(f"{prefix}_{i}"), clock.now_ms()).pk
        for i in range(n)
    ]


def status(store, pk):
    return store.get(pk)["status"]


def test_successful_event_is_done_after_one_attempt(store, worker, registry, clock):
    seen = []
    registry.fallback = lambda c: seen.append(c.event_id)
    [pk] = accept(store, clock)
    assert worker.run_once() == 1
    ev = store.get(pk)
    assert (ev["status"], ev["attempts"], seen) == ("done", 1, ["evt_0"])
    assert [h["outcome"] for h in ev["history"]] == ["ok"]


def test_transient_failure_is_retried_later_not_immediately(store, worker, registry, clock):
    calls = []

    def flaky(c):
        calls.append(c.attempt)
        if c.attempt < 3:
            raise TransientError("downstream 503")

    registry.fallback = flaky
    [pk] = accept(store, clock)

    worker.run_once()
    assert status(store, pk) == "pending"
    assert worker.run_once() == 0, "not due yet: backoff must hold it back"

    for _ in range(10):
        clock.advance(60)
        worker.run_once()
    assert calls == [1, 2, 3]
    assert status(store, pk) == "done"
    assert [h["outcome"] for h in store.get(pk)["history"]] == ["retry", "retry", "ok"]


def test_permanent_failure_goes_straight_to_dead(store, worker, registry, clock):
    def bad(c):
        raise PermanentError("downstream 422: invalid start_date")

    registry.fallback = bad
    [pk] = accept(store, clock)
    worker.run_once()
    ev = store.get(pk)
    assert (ev["status"], ev["attempts"]) == ("dead", 1)
    assert "422" in ev["last_error"]


def test_gives_up_after_max_attempts(store, worker, registry, clock, settings):
    registry.fallback = lambda c: (_ for _ in ()).throw(TransientError("still down"))
    [pk] = accept(store, clock)
    for _ in range(20):
        worker.run_once()
        clock.advance(120)
    ev = store.get(pk)
    assert ev["status"] == "dead"
    assert ev["attempts"] == settings.max_attempts
    assert ev["last_error"].startswith(f"gave up after {settings.max_attempts} attempts")


def test_a_crashing_handler_is_retried_not_lost(store, worker, registry, clock):
    registry.fallback = lambda c: 1 / 0
    [pk] = accept(store, clock)
    worker.run_once()
    ev = store.get(pk)
    assert ev["status"] == "pending"
    assert "ZeroDivisionError" in ev["last_error"]


def test_retry_after_from_downstream_is_respected(store, worker, registry, clock):
    registry.fallback = lambda c: (_ for _ in ()).throw(TransientError("429", retry_after_s=300))
    [pk] = accept(store, clock)
    start = clock.now_ms()
    worker.run_once()
    assert store.get(pk)["next_attempt_at"] - start >= 300_000


def test_no_handler_means_acknowledged(store, worker, clock):
    [pk] = accept(store, clock)
    worker.run_once()
    assert status(store, pk) == "done"


def test_crashed_worker_lease_expires_and_event_is_reclaimed(store, worker, clock, settings):
    [pk] = accept(store, clock)
    [stuck] = store.claim(clock.now_ms(), settings.lease_s * 1000, 10)  # claimed, then the process "dies"
    assert worker.run_once() == 0, "still leased"
    clock.advance(settings.lease_s + 1)
    assert worker.run_once() == 1
    ev = store.get(pk)
    assert (ev["status"], ev["attempts"]) == ("done", 2)

    # the zombie wakes up and reports: it must not overwrite the newer result
    assert store.mark_dead(stuck, clock.now_ms(), clock.now_ms(), "zombie") is False
    ev = store.get(pk)
    assert ev["status"] == "done"
    assert [h["outcome"] for h in ev["history"]] == ["ok", "stale"]


def test_parallel_workers_never_claim_the_same_event(store, clock, settings):
    pks = accept(store, clock, n=60)
    claimed: list[int] = []

    def drain(_):
        got = []
        while batch := store.claim(clock.now_ms(), 30_000, 5):
            got += [c.pk for c in batch]
        return got

    with ThreadPoolExecutor(max_workers=6) as pool:
        for got in pool.map(drain, range(6)):
            claimed += got
    assert sorted(claimed) == sorted(pks), "every event claimed exactly once"


def test_events_are_processed_oldest_first(store, worker, registry, clock):
    order = []
    registry.fallback = lambda c: order.append(c.event_id)
    for i in range(3):
        accept(store, clock, prefix=f"t{i}")
        clock.advance(1)
    worker.run_once()
    assert order == ["t0_0", "t1_0", "t2_0"]


def test_worker_batch_size_is_honoured(store, registry, settings, clock):
    settings.worker_batch = 3
    accept(store, clock, n=7)
    w = Worker(store, registry, settings, clock, random.Random(1))
    assert [w.run_once() for _ in range(4)] == [3, 3, 1, 0]


@pytest.mark.parametrize("attempt, ceiling", [(1, 2), (2, 4), (3, 8), (6, 60), (20, 60)])
def test_backoff_stays_under_an_exponential_ceiling(attempt, ceiling):
    rng = random.Random(0)
    delays = [backoff_s(attempt, 2, 60, rng) for _ in range(500)]
    assert all(0 <= d <= ceiling for d in delays)
    assert max(delays) > ceiling * 0.9, "jitter should use the whole range"
