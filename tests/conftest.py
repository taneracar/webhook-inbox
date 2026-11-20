import json
import os
import random

import pytest
from fastapi.testclient import TestClient

from app.clock import FakeClock
from app.config import Settings
from app.db import create_schema, make_engine, metadata
from app.handlers import Registry
from app.main import create_app
from app.signing import sign
from app.store import Store
from app.worker import Worker

SECRET = "test-secret"
ADMIN = "test-admin"


@pytest.fixture
def engine(tmp_path):
    """SQLite by default; set TEST_DATABASE_URL to run the same tests on Postgres (CI does)."""
    url = os.environ.get("TEST_DATABASE_URL") or f"sqlite:///{tmp_path / 'test.db'}"
    eng = make_engine(url)
    metadata.drop_all(eng)
    create_schema(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def settings():
    return Settings(
        database_url="unused",
        webhook_secrets={"ats": SECRET, "billing": "other"},
        admin_token=ADMIN,
        max_attempts=4,
        backoff_base_s=2,
        backoff_cap_s=60,
        lease_s=30,
        worker_batch=10,
        downstream_url=None,
    )


@pytest.fixture
def store(engine):
    return Store(engine)


@pytest.fixture
def client(settings, clock, engine):
    return TestClient(create_app(settings, clock, engine))


@pytest.fixture
def registry():
    return Registry()


@pytest.fixture
def worker(store, registry, settings, clock):
    return Worker(store, registry, settings, clock, random.Random(7))


def event_body(event_id="evt_1", type_="candidate.placed", **data) -> bytes:
    data = data or {"candidate_id": "c1", "job_order_id": "j1", "start_date": "2026-12-01"}
    return json.dumps({"id": event_id, "type": type_, "data": data}).encode()


@pytest.fixture
def post(client, clock):
    """Send a correctly signed webhook (override pieces to test failures)."""

    def _post(body: bytes, source="ats", secret=SECRET, ts=None, header=None):
        ts = clock.now_ms() // 1000 if ts is None else ts
        sig = header if header is not None else sign(secret, ts, body)
        headers = {"content-type": "application/json"}
        if sig:
            headers["X-Signature"] = sig
        return client.post(f"/webhooks/{source}", content=body, headers=headers)

    return _post


@pytest.fixture
def admin_headers():
    return {"Authorization": f"Bearer {ADMIN}"}
