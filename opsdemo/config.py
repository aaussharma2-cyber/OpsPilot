import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
INSTANCE_DIR = BASE_DIR / "instance"
INSTANCE_DIR.mkdir(exist_ok=True)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _fix_db_url(url: str) -> str:
    # SQLAlchemy 2 dropped the legacy "postgres://" scheme.
    # Supabase and Heroku-style services still emit it — normalise here.
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


_IS_DEV = _env_bool("FLASK_DEBUG", False)

# Detect production: DATABASE_URL is set and points to a real Postgres server
_DB_URL_RAW = os.environ.get("DATABASE_URL", "")
_IS_PROD = bool(_DB_URL_RAW) and not _DB_URL_RAW.startswith("sqlite")


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY", "change-this-in-production")
    CREDENTIAL_ENCRYPTION_KEY = os.environ.get("CREDENTIAL_ENCRYPTION_KEY", "")

    # ── Database ──────────────────────────────────────────────────────────────
    SQLALCHEMY_DATABASE_URI = _fix_db_url(
        _DB_URL_RAW or f"sqlite:///{INSTANCE_DIR / 'opsdemo.db'}"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    # pool_pre_ping avoids "SSL connection has been closed unexpectedly" on Supabase.
    # pool_recycle drops connections before Supabase's 5-minute idle timeout.
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_pre_ping": True,
        "pool_recycle": 280,
        **({"pool_size": 3, "max_overflow": 2,
            "connect_args": {"sslmode": "require"}} if _IS_PROD else {}),
    }

    # ── Cookies / session ─────────────────────────────────────────────────────
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"
    # Auto-enable secure cookie when running behind HTTPS (Render always uses HTTPS).
    SESSION_COOKIE_SECURE = _env_bool("SESSION_COOKIE_SECURE", _IS_PROD)
    REMEMBER_COOKIE_HTTPONLY = True
    REMEMBER_COOKIE_SAMESITE = "Lax"
    REMEMBER_COOKIE_SECURE = SESSION_COOKIE_SECURE
    PERMANENT_SESSION_LIFETIME = int(os.environ.get("PERMANENT_SESSION_LIFETIME_SECONDS", "28800"))

    # ── App ───────────────────────────────────────────────────────────────────
    APP_TITLE = "OpsPilot Local"
    ALERT_LOOKAHEAD_DAYS = int(os.environ.get("ALERT_LOOKAHEAD_DAYS", "30"))

    # Static file cache: no-cache in dev (instant reload), 1-hour in prod.
    # The static_ver() mtime cache-buster in base.html handles URL invalidation.
    SEND_FILE_MAX_AGE_DEFAULT = 0 if _IS_DEV else 3600

    # ── Shopify ───────────────────────────────────────────────────────────────
    SHOPIFY_API_VERSION = os.environ.get("SHOPIFY_API_VERSION", "2026-04")
    # Shopify OAuth / public URL — required for callback-based OAuth.
    # For local testing use ngrok or Cloudflare Tunnel, or deploy to a public URL.
    # The direct access-token (Custom App) flow works without these.
    SHOPIFY_API_KEY = os.environ.get("SHOPIFY_API_KEY", "")
    SHOPIFY_API_SECRET = os.environ.get("SHOPIFY_API_SECRET", "")
    SHOPIFY_REDIRECT_URI = os.environ.get("SHOPIFY_REDIRECT_URI", "")
    PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "")

    # ── Auth ──────────────────────────────────────────────────────────────────
    DEMO_USERNAME = os.environ.get("DEMO_USERNAME", "admin")
    DEMO_PASSWORD = os.environ.get("DEMO_PASSWORD", "ChangeMe123!")
