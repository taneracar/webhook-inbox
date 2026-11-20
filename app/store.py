"""All database access. Every state change is a single conditional UPDATE, so
correctness does not depend on the caller holding a lock in Python."""

import hashlib
import uuid
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import Engine, and_, func, insert, or_, select, update
from sqlalchemy.dialects import postgresql, sqlite

from .db import attempts, events

Status = Literal["pending", "processing", "done", "dead"]


@dataclass(frozen=True)
class Claimed:
    pk: int
    source: str
    event_id: str
    type: str
    payload: str
    attempt: int
    lease_token: str


@dataclass(frozen=True)
class Accepted:
    pk: int
    duplicate: bool
    conflict: bool  # same event id, different body


def body_hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


class Store:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.dialect = engine.dialect.name

    # ---- intake -------------------------------------------------------------

    def accept(self, source: str, event_id: str, type_: str, body: bytes, now_ms: int) -> Accepted:
        """Insert once per (source, event_id). A concurrent duplicate cannot
        create a second row: the unique constraint decides, not a prior SELECT."""
        values = dict(
            source=source,
            event_id=event_id,
            type=type_,
            payload=body.decode(),
            body_sha256=body_hash(body),
            status="pending",
            attempts=0,
            next_attempt_at=now_ms,
            received_at=now_ms,
        )
        ins = (postgresql.insert if self.dialect == "postgresql" else sqlite.insert)(events)
        stmt = ins.values(**values).on_conflict_do_nothing(index_elements=["source", "event_id"])
        with self.engine.begin() as conn:
            res = conn.execute(stmt.returning(events.c.id))
            pk = res.scalar()
            if pk is not None:
                return Accepted(pk=pk, duplicate=False, conflict=False)
            row = conn.execute(
                select(events.c.id, events.c.body_sha256).where(
                    events.c.source == source, events.c.event_id == event_id
                )
            ).one()
            return Accepted(pk=row.id, duplicate=True, conflict=row.body_sha256 != values["body_sha256"])

    # ---- worker side ----------------------------------------------------------

    def claim(self, now_ms: int, lease_ms: int, limit: int) -> list[Claimed]:
        """Atomically take up to `limit` due events. On Postgres, SKIP LOCKED lets
        many workers claim in parallel without waiting on or double-claiming rows.
        An event whose lease ran out (worker crashed) is due again."""
        due = or_(
            and_(events.c.status == "pending", events.c.next_attempt_at <= now_ms),
            and_(events.c.status == "processing", events.c.locked_until < now_ms),
        )
        pick = select(events.c.id).where(due).order_by(events.c.next_attempt_at, events.c.id).limit(limit)
        if self.dialect == "postgresql":
            pick = pick.with_for_update(skip_locked=True)
        token = str(uuid.uuid4())
        stmt = (
            update(events)
            .where(events.c.id.in_(pick.scalar_subquery()), due)
            .values(
                status="processing", attempts=events.c.attempts + 1, locked_until=now_ms + lease_ms, lease_token=token
            )
            .returning(
                events.c.id, events.c.source, events.c.event_id, events.c.type, events.c.payload, events.c.attempts
            )
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).all()
        return [
            Claimed(r.id, r.source, r.event_id, r.type, r.payload, r.attempts, token)
            for r in sorted(rows, key=lambda r: r.id)
        ]

    def _finish(self, c: Claimed, values: dict, outcome: str, started_ms: int, now_ms: int, error: str | None) -> bool:
        """Apply the result only if this worker still holds the lease. A worker
        that stalled past its lease must not overwrite the newer attempt."""
        with self.engine.begin() as conn:
            res = conn.execute(
                update(events)
                .where(events.c.id == c.pk, events.c.status == "processing", events.c.lease_token == c.lease_token)
                .values(**values, locked_until=None, lease_token=None)
            )
            ok = res.rowcount == 1
            conn.execute(
                insert(attempts).values(
                    event_pk=c.pk,
                    attempt=c.attempt,
                    started_at=started_ms,
                    duration_ms=max(0, now_ms - started_ms),
                    outcome=outcome if ok else "stale",
                    error=error,
                )
            )
            return ok

    def mark_done(self, c: Claimed, started_ms: int, now_ms: int) -> bool:
        return self._finish(c, dict(status="done", finished_at=now_ms, last_error=None), "ok", started_ms, now_ms, None)

    def mark_retry(self, c: Claimed, started_ms: int, now_ms: int, next_at_ms: int, error: str) -> bool:
        return self._finish(
            c, dict(status="pending", next_attempt_at=next_at_ms, last_error=error), "retry", started_ms, now_ms, error
        )

    def mark_dead(self, c: Claimed, started_ms: int, now_ms: int, error: str) -> bool:
        return self._finish(
            c, dict(status="dead", finished_at=now_ms, last_error=error), "dead", started_ms, now_ms, error
        )

    # ---- admin --------------------------------------------------------------

    def replay(self, pk: int, now_ms: int) -> bool:
        """Send a finished or dead event through the pipeline again. The attempt
        counter restarts; the attempt history is kept."""
        with self.engine.begin() as conn:
            res = conn.execute(
                update(events)
                .where(events.c.id == pk, events.c.status.in_(["done", "dead"]))
                .values(status="pending", attempts=0, next_attempt_at=now_ms, finished_at=None, last_error=None)
            )
            return res.rowcount == 1

    def get(self, pk: int) -> dict | None:
        with self.engine.connect() as conn:
            row = conn.execute(select(events).where(events.c.id == pk)).mappings().first()
            if row is None:
                return None
            hist = (
                conn.execute(select(attempts).where(attempts.c.event_pk == pk).order_by(attempts.c.id)).mappings().all()
            )
            return {**dict(row), "history": [dict(h) for h in hist]}

    def list(self, status: str | None, source: str | None, limit: int) -> list[dict]:
        q = select(
            events.c.id,
            events.c.source,
            events.c.event_id,
            events.c.type,
            events.c.status,
            events.c.attempts,
            events.c.next_attempt_at,
            events.c.last_error,
            events.c.received_at,
            events.c.finished_at,
        )
        if status:
            q = q.where(events.c.status == status)
        if source:
            q = q.where(events.c.source == source)
        with self.engine.connect() as conn:
            return [dict(r) for r in conn.execute(q.order_by(events.c.id.desc()).limit(limit)).mappings()]

    def stats(self, now_ms: int) -> dict:
        with self.engine.connect() as conn:
            counts = dict(conn.execute(select(events.c.status, func.count()).group_by(events.c.status)).all())
            oldest = conn.execute(
                select(func.min(events.c.next_attempt_at)).where(
                    events.c.status == "pending", events.c.next_attempt_at <= now_ms
                )
            ).scalar()
        return {
            "by_status": {s: counts.get(s, 0) for s in ("pending", "processing", "done", "dead")},
            # how long the most overdue event has been waiting: the number to alert on
            "lag_s": round((now_ms - oldest) / 1000, 3) if oldest is not None else 0,
        }
