from __future__ import annotations

import os
import secrets
import hmac
from functools import wraps

from flask import Flask, abort, g, redirect, request, session, url_for

from .config import Config
from .models import db, User


def _run_migrations(app: Flask) -> None:
    """Safe, idempotent schema migrations for SQLite."""
    with app.app_context():
        engine = db.engine
        with engine.connect() as conn:
            # Add sprint_id column to task table if missing
            try:
                conn.execute(db.text("ALTER TABLE task ADD COLUMN sprint_id INTEGER REFERENCES sprint(id)"))
                conn.commit()
            except Exception:
                pass  # Column already exists
            # Add report_id column to dashboard_widget if missing
            try:
                conn.execute(db.text("ALTER TABLE dashboard_widget ADD COLUMN report_id INTEGER REFERENCES dashboard_report(id) ON DELETE CASCADE"))
                conn.commit()
            except Exception:
                pass
            # Add contact fields to renewal if missing
            try:
                conn.execute(db.text("ALTER TABLE renewal ADD COLUMN contact_name VARCHAR(120)"))
                conn.commit()
            except Exception:
                pass
            try:
                conn.execute(db.text("ALTER TABLE renewal ADD COLUMN contact_email VARCHAR(120)"))
                conn.commit()
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
    """Create or remove AlertLog entries for active security conditions."""
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
            g.user = db.session.get(User, user_id)

    @app.context_processor
    def inject_globals():
        from .services import get_integration_config
        from werkzeug.security import check_password_hash

        def _theme():
            try:
                theme = get_integration_config("ui").get("theme", "dark")
                return theme if theme in {"dark", "light", "ocean", "warm"} else "dark"
            except Exception:
                return "dark"

        def _unread_notifications():
            try:
                from .models import AlertLog
                return AlertLog.query.filter_by(is_read=False).count()
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
        response.headers[
            "Content-Security-Policy"
        ] = "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline';"
        if app.config.get("SESSION_COOKIE_SECURE"):
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        # Prevent HTML pages from being cached by the browser
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
        """Return file mtime as a cache-bust token — changes automatically when the file is saved."""
        try:
            return str(int(os.path.getmtime(os.path.join(app.static_folder, filename))))
        except OSError:
            return "0"

    app.jinja_env.globals["static_ver"] = _static_ver

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
        user = User(username=username, role="admin")
        user.password_hash = generate_password_hash(password)
        db.session.add(user)
        db.session.commit()


def login_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if g.user is None:
            return redirect(url_for("main.login", next=request.path))
        return view_func(*args, **kwargs)

    return wrapped


def admin_required(view_func):
    @wraps(view_func)
    @login_required
    def wrapped(*args, **kwargs):
        if g.user.role != "admin":
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
