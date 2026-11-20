# webhook-inbox

A small Python service that receives webhooks safely and processes them reliably: **signature verification, idempotent intake, a durable queue, and a retrying worker with leases and a dead-letter state.** FastAPI + SQLAlchemy 2, on SQLite or Postgres.

Webhooks fail in boring, predictable ways: senders retry and deliver the same event twice, two copies arrive at the same moment, the downstream service is down for ten minutes, a worker dies halfway through, someone replays a captured request. Each of those has a test here.

## How it works

```
sender ──POST /webhooks/{source}──▶ verify HMAC + timestamp ──▶ INSERT … ON CONFLICT DO NOTHING ──▶ 202
                                                                         │ events table
worker ◀── UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED) RETURNING ┘   (pending → processing)
   │
   ├─ ok ─────────────▶ done
   ├─ transient ──────▶ pending, next_attempt_at = now + backoff (or Retry-After)
   ├─ permanent ──────▶ dead
   └─ too many tries ─▶ dead                       admin: list · detail + history · replay · stats
```

**Intake**
- `X-Signature: t=<unix>,v1=<hmac-sha256 hex>` over `"<t>.<raw body>"`, compared in constant time. The timestamp is inside the MAC, so an old request cannot be replayed with a new timestamp; anything outside ±5 minutes is rejected. Several `v1` values are accepted so a sender can rotate secrets with no downtime. Each source has its own secret.
- The signature is checked against the **raw bytes** before any JSON parsing.
- `(source, event_id)` is the idempotency key, enforced by a unique constraint and `ON CONFLICT DO NOTHING`, not by a "check, then insert" race. A duplicate gets **200**, because any error would make the sender retry again. The same id with a *different* body gets **409**: that is a sender bug and should be loud.

**Processing**
- Claiming is one `UPDATE … RETURNING` statement. On Postgres it uses `FOR UPDATE SKIP LOCKED`, so many workers can drain the queue in parallel without blocking each other or taking the same row.
- A claim is a **lease** (`locked_until` + `lease_token`). If a worker dies, the event becomes due again when the lease runs out. If the "dead" worker wakes up later and reports a result, the update is conditional on its token and is ignored; the attempt is logged as `stale`.
- Handlers raise `TransientError` (timeouts, 5xx, 408, 429) or `PermanentError` (bad payload, other 4xx). Retries use exponential backoff with full jitter, honour `Retry-After` up to a cap, and stop at `max_attempts`, after which the event is `dead`. An unexpected exception in a handler counts as transient: a bug should not silently drop data.
- The default forwarder sends an `idempotency-key` header so the downstream can dedupe too. Delivery is at-least-once; idempotency on both sides is what makes that safe.
- Every attempt is recorded (outcome, duration, error). `POST /events/{id}/replay` re-queues a done or dead event and keeps that history.
- `GET /stats` returns counts by status and `lag_s`, how long the most overdue event has been waiting, which is the number to alert on.

## Tests

`pytest` runs **58 tests** in under a second, with an injected clock, so retries and leases are tested without sleeping.

| Area | Examples |
|---|---|
| Signatures | missing, malformed, wrong secret, tampered body, stale and future timestamps, the exact tolerance edge, secret rotation, per-source secrets |
| Idempotency | duplicate → 200 and one row; same id with different body → 409; **16 concurrent copies of one event → exactly one row** |
| Worker | backoff holds an event back; transient → retry → done; permanent → dead; gives up at `max_attempts`; crashing handler is retried; `Retry-After` respected; oldest first; batch size |
| Leases | a crashed worker's event is reclaimed; the zombie's late result is rejected and logged `stale`; **6 parallel workers drain 60 events with no event claimed twice** |
| Admin + forwarder | auth on every admin route, replay rules, stats and lag; forwarder classification for 2xx/4xx/5xx/408/429/timeouts with `httpx.MockTransport` (no network) |

CI runs the suite on **SQLite and Postgres 17**, on Python 3.12 and 3.13, plus ruff lint/format and a Docker build that must answer `/health` as a non-root user.

Checked against deliberate breakage: removing the lease-token guard, the `ON CONFLICT` clause, the timestamp window, the attempt limit, or `Retry-After` handling each makes at least one test fail.

Also run by hand against real processes (uvicorn API, `python -m app.worker`, and a throwaway downstream that returns 503 once): the event was accepted (202), the duplicate acknowledged (200), a bad signature refused (401), and the worker delivered on the second attempt after the 503, with history `['retry', 'ok']`.

## Running it

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest

uvicorn --factory app.main:create_app            # API on :8000
python -m app.worker                             # in another terminal
python -m scripts.send                           # signed sample event
python -m scripts.send --id evt_1                # run twice: the second is a duplicate
curl -H "Authorization: Bearer dev-admin-token" localhost:8000/stats
```

Configuration is environment variables (see `app/config.py`): `DATABASE_URL` (e.g. `postgresql+psycopg://…`), `WEBHOOK_SECRETS` as JSON (`{"ats":"…"}`), `ADMIN_TOKEN`, `DOWNSTREAM_URL`, `MAX_ATTEMPTS`, `BACKOFF_BASE_S`, `BACKOFF_CAP_S`, `LEASE_S`.

Docker: `docker build -t webhook-inbox .` then `docker run -p 8000:8000 webhook-inbox` for the API, or append `python -m app.worker` for the worker.

## Limits

- Schema is created with `create_all` on start. A real deployment would use Alembic migrations.
- The parallel-claim test runs on SQLite locally, which serialises writers; the `SKIP LOCKED` path is exercised by the Postgres job in CI.
- The admin API uses one shared bearer token. Fine for an internal tool, not for multi-user access.
- No real third-party sender is wired up; the sample events and the downstream in the manual run are synthetic.
