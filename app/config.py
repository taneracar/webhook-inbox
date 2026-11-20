from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All knobs come from the environment (or .env). Defaults suit local dev."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "sqlite:///./inbox.db"
    # {"source-name": "shared-secret"}. A source not listed here gets 404.
    webhook_secrets: dict[str, str] = Field(default_factory=lambda: {"ats": "dev-secret"})
    admin_token: str = "dev-admin-token"

    signature_tolerance_s: int = 300  # reject signatures older/newer than this
    max_body_bytes: int = 256 * 1024

    max_attempts: int = 6
    backoff_base_s: float = 2.0
    backoff_cap_s: float = 600.0
    retry_after_cap_s: float = 3600.0
    lease_s: int = 60  # a claimed event is reclaimable after this

    downstream_url: str | None = None  # where the default handler forwards events
    downstream_timeout_s: float = 5.0

    worker_batch: int = 10
    worker_idle_sleep_s: float = 1.0
