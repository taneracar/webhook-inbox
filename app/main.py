import hmac
import json
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy import Engine, text

from .clock import Clock, SystemClock
from .config import Settings
from .db import create_schema, make_engine
from .signing import HEADER, SignatureError, verify
from .store import Store


def create_app(settings: Settings | None = None, clock: Clock | None = None, engine: Engine | None = None) -> FastAPI:
    s = settings or Settings()
    clk = clock or SystemClock()
    eng = engine or make_engine(s.database_url)
    create_schema(eng)
    store = Store(eng)

    app = FastAPI(
        title="webhook-inbox", version="0.1.0", description="Signed, idempotent webhook intake with a retrying worker."
    )
    app.state.store = store

    def admin(authorization: Annotated[str | None, Header()] = None) -> None:
        token = (authorization or "").removeprefix("Bearer ").strip()
        if not hmac.compare_digest(token.encode(), s.admin_token.encode()):
            raise HTTPException(401, "admin token required")

    @app.post("/webhooks/{source}", status_code=202)
    async def receive(source: str, request: Request):
        secret = s.webhook_secrets.get(source)
        if secret is None:
            raise HTTPException(404, "unknown source")

        # Read the raw bytes: the signature covers exactly what was sent, so
        # parsing and re-serialising JSON before verifying would break it.
        length = int(request.headers.get("content-length") or 0)
        if length > s.max_body_bytes:
            raise HTTPException(413, "body too large")
        body = await request.body()
        if len(body) > s.max_body_bytes:
            raise HTTPException(413, "body too large")

        try:
            verify(request.headers.get(HEADER), body, secret, clk.now_ms() // 1000, s.signature_tolerance_s)
        except SignatureError as e:
            raise HTTPException(401, e.reason) from e

        try:
            data = json.loads(body)
        except ValueError as e:
            raise HTTPException(400, "body is not JSON") from e
        if not isinstance(data, dict) or not isinstance(data.get("id"), str) or not isinstance(data.get("type"), str):
            raise HTTPException(400, "body needs string fields 'id' and 'type'")
        if not data["id"] or len(data["id"]) > 200:
            raise HTTPException(400, "'id' must be 1-200 characters")

        acc = store.accept(source, data["id"], data["type"], body, clk.now_ms())
        if acc.conflict:
            # Same id, different content: the sender has a bug. Refuse loudly
            # instead of silently keeping either version.
            return JSONResponse(
                status_code=409, content={"detail": "event id already used with a different body", "id": acc.pk}
            )
        # A duplicate gets 200, not an error: the sender just retried, and
        # anything other than 2xx would make it retry again.
        return JSONResponse(
            status_code=200 if acc.duplicate else 202, content={"id": acc.pk, "duplicate": acc.duplicate}
        )

    @app.get("/events", dependencies=[Depends(admin)])
    def list_events(
        status: Literal["pending", "processing", "done", "dead"] | None = None,
        source: str | None = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
    ):
        return store.list(status, source, limit)

    @app.get("/events/{pk}", dependencies=[Depends(admin)])
    def get_event(pk: int):
        ev = store.get(pk)
        if ev is None:
            raise HTTPException(404, "no such event")
        return ev

    @app.post("/events/{pk}/replay", dependencies=[Depends(admin)])
    def replay(pk: int):
        if store.get(pk) is None:
            raise HTTPException(404, "no such event")
        if not store.replay(pk, clk.now_ms()):
            raise HTTPException(409, "only done or dead events can be replayed")
        return {"id": pk, "status": "pending"}

    @app.get("/stats", dependencies=[Depends(admin)])
    def stats():
        return store.stats(clk.now_ms())

    @app.get("/health")
    def health():
        with eng.connect() as conn:
            conn.execute(text("select 1"))
        return {"ok": True}

    return app


# Run with: uvicorn --factory app.main:create_app
