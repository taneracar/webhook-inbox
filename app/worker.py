import logging
import random
import signal
import time

from .clock import Clock, SystemClock
from .config import Settings
from .db import create_schema, make_engine
from .handlers import PermanentError, Registry, TransientError, default_registry
from .retry import backoff_s
from .store import Store

log = logging.getLogger("webhook_inbox.worker")


class Worker:
    def __init__(
        self,
        store: Store,
        registry: Registry,
        settings: Settings,
        clock: Clock | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.store = store
        self.registry = registry
        self.s = settings
        self.clock = clock or SystemClock()
        self.rng = rng or random.Random()
        self._stop = False

    def run_once(self) -> int:
        """Claim one batch and process it. Returns how many events were handled."""
        batch = self.store.claim(self.clock.now_ms(), self.s.lease_s * 1000, self.s.worker_batch)
        for c in batch:
            started = self.clock.now_ms()
            handler = self.registry.resolve(c.source, c.type)
            try:
                if handler is None:
                    # no route and no fallback: acknowledge and move on, but say so
                    log.info("no handler for %s/%s, marking done", c.source, c.type)
                else:
                    handler(c)
            except PermanentError as e:
                self.store.mark_dead(c, started, self.clock.now_ms(), str(e))
                log.warning("dead %s/%s: %s", c.source, c.event_id, e)
                continue
            except TransientError as e:
                self._retry_or_die(c, started, str(e), e.retry_after_s)
                continue
            except Exception as e:  # a bug in a handler is treated as transient
                self._retry_or_die(c, started, f"{type(e).__name__}: {e}", None)
                continue
            self.store.mark_done(c, started, self.clock.now_ms())
        return len(batch)

    def _retry_or_die(self, c, started: int, error: str, retry_after_s: float | None) -> None:
        now = self.clock.now_ms()
        if c.attempt >= self.s.max_attempts:
            self.store.mark_dead(c, started, now, f"gave up after {c.attempt} attempts: {error}")
            log.warning("dead %s/%s after %d attempts", c.source, c.event_id, c.attempt)
            return
        delay = backoff_s(c.attempt, self.s.backoff_base_s, self.s.backoff_cap_s, self.rng)
        if retry_after_s is not None:
            # the downstream told us when; respect it, within reason
            delay = max(delay, min(retry_after_s, self.s.retry_after_cap_s))
        self.store.mark_retry(c, started, now, now + int(delay * 1000), error)

    def run_forever(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.stop())
        signal.signal(signal.SIGINT, lambda *_: self.stop())
        log.info("worker started")
        while not self._stop:
            if self.run_once() == 0:
                time.sleep(self.s.worker_idle_sleep_s)
        log.info("worker stopped")  # the current batch always finishes first

    def stop(self) -> None:
        self._stop = True


def main() -> None:  # pragma: no cover - process entry point
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    s = Settings()
    engine = make_engine(s.database_url)
    create_schema(engine)
    Worker(Store(engine), default_registry(s.downstream_url, s.downstream_timeout_s), s).run_forever()


if __name__ == "__main__":  # pragma: no cover
    main()
