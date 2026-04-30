from __future__ import annotations

import os
import re
import secrets
import hmac
from functools import wraps

from flask import Flask, abort, g, redirect, request, session, url_for

from .config import Config
from .models import db, User, Organization

# ── Rate limiter (shared instance, configured in create_app) ─────────────────
try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
    limiter = Limiter(key_func=get_remote_address, default_limits=[], storage_uri="memory://")
    _limiter_available = True
except ImportError:
    limiter = None  # type: ignore
    _limiter_available = False


def _run_migrations(app: Flask) -> None:
    """Safe, idempotent schema migrations — adds columns missing from older DBs."""
    engine = db.engine
    with engine.connect() as conn:
        _safe_add = lambda sql: _try_exec(conn, sql)

        # Rename reserved-word table "user" → app_user (idempotent: fails silently if already done)
        _safe_add('ALTER TABLE "user" RENAME TO app_user')

        # Legacy columns
        _safe_add("ALTER TABLE task ADD COLUMN sprint_id INTEGER REFERENCES sprint(id)")
        _safe_add("ALTER TABLE dashboard_widget ADD COLUMN report_id INTEGER REFERENCES dashboard_report(id) ON DELETE CASCADE")
        _safe_add("ALTER TABLE renewal ADD COLUMN contact_name VARCHAR(120)")
        _safe_add("ALTER TABLE renewal ADD COLUMN contact_email VARCHAR(120)")

        # Multi-tenancy: add org_id to all data tables
        for tbl in [
            "app_user", "sprint", "task", "contact", "vendor", "asset",
            "inventory_item", "invoice", "renewal", "sale",
            "board_column", "field_definition", "workflow", "alert_log",
            "dashboard_widget", "dashboard_report", "integration_config",
            "sync_log", "audit_log",
        ]:
            _safe_add(f"ALTER TABLE {tbl} ADD COLUMN org_id INTEGER REFERENCES organization(id)")

        # Organization brand columns
        _safe_add("ALTER TABLE organization ADD COLUMN logo_path VARCHAR(255)")
        _safe_add("ALTER TABLE organization ADD COLUMN pdf_header_text VARCHAR(200)")
        _safe_add("ALTER TABLE organization ADD COLUMN pdf_footer_text VARCHAR(200)")
        _safe_add("ALTER TABLE organization ADD COLUMN email_from_name VARCHAR(120)")

        # User model new columns
        _safe_add("ALTER TABLE app_user ADD COLUMN email VARCHAR(120)")
        _safe_add("ALTER TABLE app_user ADD COLUMN is_active BOOLEAN DEFAULT TRUE")

        # TaskHistory: changed_by
        _safe_add("ALTER TABLE task_history ADD COLUMN changed_by VARCHAR(80)")

        # Migrate existing admin role → org_admin
        _safe_add("UPDATE app_user SET role='org_admin' WHERE role='admin'")

        # Create default organization and assign existing users/data to it
        _ensure_default_org(conn)


def _try_exec(conn, sql: str) -> None:
    try:
        conn.execute(db.text(sql))
        conn.commit()
    except Exception:
        pass


def _ensure_default_org(conn) -> None:
    """Create a 'Default' org and assign all un-homed data to it."""
    try:
        result = conn.execute(db.text("SELECT id FROM organization WHERE slug='default' LIMIT 1"))
        row = result.fetchone()
        if row:
            org_id = row[0]
        else:
            conn.execute(db.text(
                "INSERT INTO organization (name, slug, plan, is_active, created_at) "
                "VALUES ('Default Organisation', 'default', 'free', TRUE, CURRENT_TIMESTAMP)"
            ))
            conn.commit()
            result = conn.execute(db.text("SELECT id FROM organization WHERE slug='default' LIMIT 1"))
            row = result.fetchone()
            if not row:
                return
            org_id = row[0]

        # Assign all users/data without an org
        for tbl in [
            "app_user", "sprint", "task", "contact", "vendor", "asset",
            "inventory_item", "invoice", "renewal", "sale",
            "board_column", "field_definition", "workflow", "alert_log",
            "dashboard_widget", "dashboard_report", "integration_config",
            "sync_log", "audit_log",
        ]:
            try:
                conn.execute(db.text(f"UPDATE {tbl} SET org_id={org_id} WHERE org_id IS NULL"))
                conn.commit()
            except Exception:
                pass
        # Upgrade org_admin role
        try:
            conn.execute(db.text("UPDATE app_user SET role='org_admin' WHERE role='admin'"))
            conn.commit()
        except Exception:
            pass
    except Exception:
        pass


_SECURITY_ALERT_SPECS = [
    (
        "system:security:secret_key",
        lambda cfg: cfg.get("SECRET_KEY") in (
            "change-this-in-production", "change-me-in-production", "dev-secret-key", "secret"
        ),
        "warning",
        "Insecure SECRET_KEY in use",
        "SECRET_KEY is set to a default value. Set a random SECRET_KEY environment variable before exposing this app on a network.",
    ),
    (
        "system:security:demo_password",
        lambda cfg: cfg.get("DEMO_PASSWORD", "") in (
            "ChangeMe123!", "admin", "password", "demo", "admin123", "changeme"
        ),
        "warning",
        "Default admin password still in use",
        "Default admin password is still in use. Change it via Settings → Users or set DEMO_PASSWORD in the environment.",
    ),
    (
        "system:security:no_encryption_key",
        lambda cfg: not cfg.get("CREDENTIAL_ENCRYPTION_KEY"),
        "warning",
        "CREDENTIAL_ENCRYPTION_KEY not set",
        "CREDENTIAL_ENCRYPTION_KEY is not set. Integration tokens are encrypted with SECRET_KEY; use a separate stable encryption key before production use.",
    ),
    (
        "system:security:session_cookie_insecure",
        lambda cfg: not cfg.get("SESSION_COOKIE_SECURE"),
        "warning",
        "SESSION_COOKIE_SECURE disabled",
        "SESSION_COOKIE_SECURE is disabled. Enable it when serving this app over HTTPS.",
    ),
]


def _sync_security_alerts(app: Flask) -> None:
    from .models import AlertLog, db
    cfg = app.config
    for source, condition_fn, severity, title, detail in _SECURITY_ALERT_SPECS:
        try:
            active = condition_fn(cfg)
        except Exception:
            continue
        if active:
            if not AlertLog.query.filter_by(source=source).first():
                db.session.add(AlertLog(severity=severity, title=title, detail=detail,
                                        source=source, is_read=False))
        else:
            AlertLog.query.filter_by(source=source).delete()
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()


def create_app(test_config: dict | None = None) -> Flask:
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(Config)
    if test_config:
        app.config.update(test_config)

    db.init_app(app)

    # Rate limiter
    if _limiter_available:
        limiter.init_app(app)

    with app.app_context():
        db.create_all()
        _run_migrations(app)
        ensure_default_admin(app)
        from .services import seed_board_columns
        seed_board_columns()
        _sync_security_alerts(app)

    @app.before_request
    def load_current_user() -> None:
        g.user = None
        user_id = session.get("user_id")
        if user_id:
            u = db.session.get(User, user_id)
            if u and u.is_active:
                g.user = u

    @app.context_processor
    def inject_globals():
        from .services import get_integration_config

        def _theme():
            try:
                theme = get_integration_config("ui").get("theme", "dark")
                return theme if theme in {"dark", "light", "ocean", "warm"} else "dark"
            except Exception:
                return "dark"

        def _unread_notifications():
            try:
                from .models import AlertLog
                q = AlertLog.query.filter_by(is_read=False)
                if g.user and g.user.org_id:
                    q = q.filter_by(org_id=g.user.org_id)
                return q.count()
            except Exception:
                return 0

        return {
            "csrf_token": get_csrf_token,
            "ui_theme": _theme(),
            "unread_notifications": _unread_notifications(),
        }

    @app.after_request
    def set_security_headers(response):
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "img-src 'self' data:; "
            "style-src 'self' 'unsafe-inline'; "
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
            "connect-src 'self';"
        )
        if app.config.get("SESSION_COOKIE_SECURE"):
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        if "text/html" in response.content_type:
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

    import json as _json

    @app.template_filter("fromjson")
    def fromjson_filter(s):
        try:
            return _json.loads(s) if s else {}
        except Exception:
            return {}

    @app.template_filter("fmt_money")
    def fmt_money_filter(value):
        try:
            return f"${float(value):,.2f}"
        except Exception:
            return str(value)

    @app.template_filter("fmt_num")
    def fmt_num_filter(value):
        try:
            v = float(value)
            return f"{v:,.0f}" if v == int(v) else f"{v:,.2f}"
        except Exception:
            return str(value)

    def _static_ver(filename: str) -> str:
        try:
            return str(int(os.path.getmtime(os.path.join(app.static_folder, filename))))
        except OSError:
            return "0"

    app.jinja_env.globals["static_ver"] = _static_ver

    @app.template_filter("tojson")
    def tojson_filter(value):
        import json as _j
        return _j.dumps(value)

    @app.template_filter("count_org_users")
    def count_org_users_filter(org_id):
        try:
            return User.query.filter_by(org_id=org_id, is_active=True).count()
        except Exception:
            return 0

    from .routes import bp as main_bp
    app.register_blueprint(main_bp)

    @app.route("/health")
    def health():
        return {"status": "ok"}

    @app.cli.command("shopify-sync")
    def shopify_sync_command():
        """Run Shopify customer and order syncs without a browser session."""
        import click
        from .services import (
            get_integration_config,
            sync_shopify_customers,
            sync_shopify_orders,
        )
        cfg = get_integration_config("shopify")
        shop_domain = cfg.get("shop_domain", "")
        access_token = cfg.get("access_token", "")
        if not shop_domain or not access_token:
            raise click.ClickException("Shopify credentials are not configured.")
        customer_result = sync_shopify_customers(shop_domain, access_token)
        order_result = sync_shopify_orders(shop_domain, access_token)
        click.echo(f"Customers: {customer_result}")
        click.echo(f"Orders: {order_result}")

    return app


def ensure_default_admin(app: Flask) -> None:
    from werkzeug.security import generate_password_hash

    username = app.config["DEMO_USERNAME"]
    password = app.config["DEMO_PASSWORD"]
    existing = User.query.filter_by(username=username).first()
    if not existing:
        # Ensure default org exists
        org = Organization.query.filter_by(slug="default").first()
        if not org:
            org = Organization(name="Default Organisation", slug="default", plan="free")
            db.session.add(org)
            db.session.flush()
        user = User(username=username, role="org_admin", org_id=org.id, is_active=True)
        user.password_hash = generate_password_hash(password)
        db.session.add(user)
        db.session.commit()


# ── Auth decorators ──────────────────────────────────────────────────────────

def login_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if g.user is None:
            return redirect(url_for("main.login", next=request.path))
        return view_func(*args, **kwargs)
    return wrapped


def admin_required(view_func):
    """Org-admin or higher."""
    @wraps(view_func)
    @login_required
    def wrapped(*args, **kwargs):
        if not g.user.is_org_admin:
            abort(403)
        return view_func(*args, **kwargs)
    return wrapped


def super_admin_required(view_func):
    """Platform super-admin only."""
    @wraps(view_func)
    @login_required
    def wrapped(*args, **kwargs):
        if not g.user.is_super_admin:
            abort(403)
        return view_func(*args, **kwargs)
    return wrapped


SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


def get_csrf_token() -> str:
    token = session.get("csrf_token")
    if not token:
        token = secrets.token_hex(16)
        session["csrf_token"] = token
    return token


def verify_csrf() -> None:
    if request.method in SAFE_METHODS:
        return
    sent_token = request.form.get("csrf_token") or request.headers.get("X-CSRF-Token")
    session_token = session.get("csrf_token")
    if not sent_token or not session_token or not hmac.compare_digest(sent_token, session_token):
        abort(400, description="Invalid CSRF token")


# ── Tier enforcement ─────────────────────────────────────────────────────────

def check_record_limit(model_class, org_id) -> tuple[bool, int, int]:
    """Returns (allowed, current_count, limit). Allowed=True if under limit."""
    if org_id is None:
        return True, 0, 9999
    org = db.session.get(Organization, org_id)
    if not org:
        return True, 0, 9999
    if org.plan != "free":
        return True, 0, org.max_records
    current = model_class.query.filter_by(org_id=org_id).count()
    return current < org.max_records, current, org.max_records


def check_user_limit(org_id) -> tuple[bool, int, int]:
    """Returns (allowed, current_count, limit)."""
    if org_id is None:
        return True, 0, 9999
    org = db.session.get(Organization, org_id)
    if not org:
        return True, 0, 9999
    current = User.query.filter_by(org_id=org_id, is_active=True).count()
    return current < org.max_users, current, org.max_users
