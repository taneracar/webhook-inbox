from sqlalchemy import (
    BigInteger,
    Column,
    Engine,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    event,
)

metadata = MetaData()

# Times are epoch milliseconds (BigInteger): identical behaviour on SQLite and
# Postgres, and trivially comparable in SQL.
events = Table(
    "events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source", String(64), nullable=False),
    Column("event_id", String(200), nullable=False),
    Column("type", String(200), nullable=False),
    Column("payload", Text, nullable=False),
    Column("body_sha256", String(64), nullable=False),
    # pending -> processing -> done | pending (retry) | dead
    Column("status", String(16), nullable=False, default="pending"),
    Column("attempts", Integer, nullable=False, default=0),
    Column("next_attempt_at", BigInteger, nullable=False),
    Column("locked_until", BigInteger),
    Column("lease_token", String(36)),
    Column("last_error", Text),
    Column("received_at", BigInteger, nullable=False),
    Column("finished_at", BigInteger),
    # The idempotency key: a sender retrying the same event lands here.
    UniqueConstraint("source", "event_id", name="uq_events_source_event"),
)
Index("ix_events_due", events.c.status, events.c.next_attempt_at)

attempts = Table(
    "attempts",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("event_pk", Integer, ForeignKey("events.id", ondelete="CASCADE"), nullable=False),
    Column("attempt", Integer, nullable=False),
    Column("started_at", BigInteger, nullable=False),
    Column("duration_ms", Integer, nullable=False),
    Column("outcome", String(16), nullable=False),  # ok | retry | dead | stale
    Column("error", Text),
)
Index("ix_attempts_event", attempts.c.event_pk)


def make_engine(url: str) -> Engine:
    if url.startswith("sqlite"):
        engine = create_engine(url, connect_args={"check_same_thread": False, "timeout": 30})

        @event.listens_for(engine, "connect")
        def _sqlite_pragmas(conn, _):  # pragma: no cover - trivial
            cur = conn.cursor()
            cur.execute("pragma journal_mode=wal")
            cur.execute("pragma foreign_keys=on")
            cur.close()

        return engine
    return create_engine(url, pool_pre_ping=True)


def create_schema(engine: Engine) -> None:
    metadata.create_all(engine)
