from __future__ import annotations

import os
import re
import secrets
import hmac
from functools import wraps

from flask import Flask, abort, g, redirect, request, session, url_for

from .config import Config
from .models import db, User, Organization

_TENANT_GUARDS_INSTALLED = False

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
            "task_history", "board_column", "field_definition", "field_value",
            "workflow", "workflow_run", "alert_log", "dashboard_widget",
            "dashboard_report", "integration_config", "sync_log", "audit_log",
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
        _safe_add("ALTER TABLE app_user ADD COLUMN reset_token_hash VARCHAR(255)")
        _safe_add("ALTER TABLE app_user ADD COLUMN reset_token_expires_at TIMESTAMP")
        _safe_add("CREATE INDEX IF NOT EXISTS ix_app_user_reset_token_hash ON app_user (reset_token_hash)")

        # TaskHistory: changed_by
        _safe_add("ALTER TABLE task_history ADD COLUMN changed_by VARCHAR(80)")

        # Migrate existing admin role → org_admin
        _safe_add("UPDATE app_user SET role='org_admin' WHERE role='admin'")

        # Promote DEMO_USERNAME to super_admin (platform owner — no org)
        demo_username = app.config.get("DEMO_USERNAME", "admin")
        try:
            conn.execute(
                db.text(
                    "UPDATE app_user SET role='super_admin', org_id=NULL "
                    "WHERE username=:u AND role != 'super_admin'"
                ).bindparams(u=demo_username)
            )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

        # Create default organization and assign existing users/data to it
        _ensure_default_org(conn)
        _migrate_tenant_unique_constraints(conn)


def _try_exec(conn, sql: str) -> None:
    try:
        conn.execute(db.text(sql))
        conn.commit()
    except Exception:
        try:
            conn.rollback()
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
            "task_history", "board_column", "field_definition", "field_value",
            "workflow", "workflow_run", "alert_log", "dashboard_widget",
            "dashboard_report", "integration_config", "sync_log", "audit_log",
        ]:
            try:
                conn.execute(db.text(f"UPDATE {tbl} SET org_id={org_id} WHERE org_id IS NULL"))
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
        # Upgrade org_admin role
        try:
            conn.execute(db.text("UPDATE app_user SET role='org_admin' WHERE role='admin'"))
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def _migrate_tenant_unique_constraints(conn) -> None:
    """Replace old global unique constraints with org-aware uniqueness."""
    if conn.dialect.name == "sqlite":
        _sqlite_rebuild_tenant_unique_tables(conn)
    else:
        for sql in [
            "ALTER TABLE board_column DROP CONSTRAINT IF EXISTS board_column_name_key",
            "ALTER TABLE field_definition DROP CONSTRAINT IF EXISTS field_definition_entity_type_field_key_key",
            "ALTER TABLE field_definition DROP CONSTRAINT IF EXISTS uq_field_entity_key",
            "ALTER TABLE integration_config DROP CONSTRAINT IF EXISTS integration_config_integration_key_key",
            "ALTER TABLE integration_config DROP CONSTRAINT IF EXISTS uq_integration_key",
            "ALTER TABLE inventory_item DROP CONSTRAINT IF EXISTS inventory_item_sku_key",
            "ALTER TABLE invoice DROP CONSTRAINT IF EXISTS invoice_reference_key",
            "ALTER TABLE sale DROP CONSTRAINT IF EXISTS sale_order_ref_key",
            "ALTER TABLE field_value DROP CONSTRAINT IF EXISTS uq_field_value",
            "ALTER TABLE field_value DROP CONSTRAINT IF EXISTS field_value_entity_type_entity_id_field_def_id_key",
        ]:
            _try_exec(conn, sql)

    for sql in [
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_col_name_org_idx ON board_column (name, org_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_field_entity_key_org_idx ON field_definition (entity_type, field_key, org_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_integration_key_org_idx ON integration_config (integration, key, org_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_sku_org_idx ON inventory_item (sku, org_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_invoice_ref_org_idx ON invoice (reference, org_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_sale_ref_org_idx ON sale (order_ref, org_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_field_value_org_idx ON field_value (entity_type, entity_id, field_def_id, org_id)",
    ]:
        _try_exec(conn, sql)


def _sqlite_unique_columns(conn, table: str) -> set[tuple[str, ...]]:
    uniques: set[tuple[str, ...]] = set()
    try:
        indexes = conn.execute(db.text(f"PRAGMA index_list('{table}')")).fetchall()
        for idx in indexes:
            if not idx[2]:
                continue
            cols = conn.execute(db.text(f"PRAGMA index_info('{idx[1]}')")).fetchall()
            uniques.add(tuple(col[2] for col in cols))
    except Exception:
        pass
    return uniques


def _sqlite_rebuild_tenant_unique_tables(conn) -> None:
    rebuilds = {
        "board_column": {
            "legacy": {("name",)},
            "create": """
                CREATE TABLE board_column__new (
                    id INTEGER NOT NULL PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    position INTEGER NOT NULL,
                    color VARCHAR(20),
                    org_id INTEGER REFERENCES organization(id) ON DELETE CASCADE
                )
            """,
            "columns": ["id", "name", "position", "color", "org_id"],
            "indexes": [
                "CREATE INDEX IF NOT EXISTS ix_board_column_org_id ON board_column (org_id)",
            ],
        },
        "field_definition": {
            "legacy": {("entity_type", "field_key")},
            "create": """
                CREATE TABLE field_definition__new (
                    id INTEGER NOT NULL PRIMARY KEY,
                    entity_type VARCHAR(30) NOT NULL,
                    name VARCHAR(60) NOT NULL,
                    field_key VARCHAR(60) NOT NULL,
                    field_type VARCHAR(20) NOT NULL,
                    options TEXT,
                    position INTEGER NOT NULL,
                    required BOOLEAN NOT NULL,
                    org_id INTEGER REFERENCES organization(id) ON DELETE CASCADE
                )
            """,
            "columns": ["id", "entity_type", "name", "field_key", "field_type", "options", "position", "required", "org_id"],
            "indexes": [
                "CREATE INDEX IF NOT EXISTS ix_field_definition_entity_type ON field_definition (entity_type)",
                "CREATE INDEX IF NOT EXISTS ix_field_definition_org_id ON field_definition (org_id)",
            ],
        },
        "integration_config": {
            "legacy": {("integration", "key")},
            "create": """
                CREATE TABLE integration_config__new (
                    id INTEGER NOT NULL PRIMARY KEY,
                    integration VARCHAR(40) NOT NULL,
                    key VARCHAR(80) NOT NULL,
                    value TEXT,
                    org_id INTEGER REFERENCES organization(id) ON DELETE CASCADE
                )
            """,
            "columns": ["id", "integration", "key", "value", "org_id"],
            "indexes": [
                "CREATE INDEX IF NOT EXISTS ix_integration_config_integration ON integration_config (integration)",
                "CREATE INDEX IF NOT EXISTS ix_integration_config_org_id ON integration_config (org_id)",
            ],
        },
        "inventory_item": {
            "legacy": {("sku",)},
            "create": """
                CREATE TABLE inventory_item__new (
                    id INTEGER NOT NULL PRIMARY KEY,
                    sku VARCHAR(60) NOT NULL,
                    name VARCHAR(120) NOT NULL,
                    category VARCHAR(80),
                    warehouse VARCHAR(80) NOT NULL,
                    qty_on_hand INTEGER NOT NULL,
                    reorder_level INTEGER NOT NULL,
                    unit_cost NUMERIC(12, 2) NOT NULL,
                    sale_price NUMERIC(12, 2) NOT NULL,
                    expiry_date DATE,
                    notes TEXT,
                    org_id INTEGER REFERENCES organization(id) ON DELETE CASCADE,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
            """,
            "columns": ["id", "sku", "name", "category", "warehouse", "qty_on_hand", "reorder_level", "unit_cost", "sale_price", "expiry_date", "notes", "org_id", "created_at", "updated_at"],
            "indexes": [
                "CREATE INDEX IF NOT EXISTS ix_inventory_item_sku ON inventory_item (sku)",
                "CREATE INDEX IF NOT EXISTS ix_inventory_item_expiry_date ON inventory_item (expiry_date)",
                "CREATE INDEX IF NOT EXISTS ix_inventory_item_org_id ON inventory_item (org_id)",
            ],
        },
        "invoice": {
            "legacy": {("reference",)},
            "create": """
                CREATE TABLE invoice__new (
                    id INTEGER NOT NULL PRIMARY KEY,
                    kind VARCHAR(20) NOT NULL,
                    party_name VARCHAR(120) NOT NULL,
                    reference VARCHAR(80) NOT NULL,
                    amount NUMERIC(12, 2) NOT NULL,
                    due_date DATE NOT NULL,
                    status VARCHAR(20) NOT NULL,
                    paid_on DATE,
                    notes TEXT,
                    org_id INTEGER REFERENCES organization(id) ON DELETE CASCADE,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
            """,
            "columns": ["id", "kind", "party_name", "reference", "amount", "due_date", "status", "paid_on", "notes", "org_id", "created_at", "updated_at"],
            "indexes": [
                "CREATE INDEX IF NOT EXISTS ix_invoice_kind ON invoice (kind)",
                "CREATE INDEX IF NOT EXISTS ix_invoice_due_date ON invoice (due_date)",
                "CREATE INDEX IF NOT EXISTS ix_invoice_status ON invoice (status)",
                "CREATE INDEX IF NOT EXISTS ix_invoice_org_id ON invoice (org_id)",
            ],
        },
        "sale": {
            "legacy": {("order_ref",)},
            "create": """
                CREATE TABLE sale__new (
                    id INTEGER NOT NULL PRIMARY KEY,
                    order_ref VARCHAR(80) NOT NULL,
                    customer_name VARCHAR(120) NOT NULL,
                    order_date DATE NOT NULL,
                    channel VARCHAR(80),
                    revenue NUMERIC(12, 2) NOT NULL,
                    cost NUMERIC(12, 2) NOT NULL,
                    quantity INTEGER NOT NULL,
                    org_id INTEGER REFERENCES organization(id) ON DELETE CASCADE,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
            """,
            "columns": ["id", "order_ref", "customer_name", "order_date", "channel", "revenue", "cost", "quantity", "org_id", "created_at", "updated_at"],
            "indexes": [
                "CREATE INDEX IF NOT EXISTS ix_sale_order_date ON sale (order_date)",
                "CREATE INDEX IF NOT EXISTS ix_sale_org_id ON sale (org_id)",
            ],
        },
        "field_value": {
            "legacy": {("entity_type", "entity_id", "field_def_id")},
            "create": """
                CREATE TABLE field_value__new (
                    id INTEGER NOT NULL PRIMARY KEY,
                    entity_type VARCHAR(30) NOT NULL,
                    entity_id INTEGER NOT NULL,
                    field_def_id INTEGER NOT NULL REFERENCES field_definition(id) ON DELETE CASCADE,
                    value TEXT,
                    org_id INTEGER REFERENCES organization(id) ON DELETE CASCADE
                )
            """,
            "columns": ["id", "entity_type", "entity_id", "field_def_id", "value", "org_id"],
            "indexes": [
                "CREATE INDEX IF NOT EXISTS ix_field_value_entity_type ON field_value (entity_type)",
                "CREATE INDEX IF NOT EXISTS ix_field_value_entity_id ON field_value (entity_id)",
                "CREATE INDEX IF NOT EXISTS ix_field_value_org_id ON field_value (org_id)",
            ],
        },
    }

    for table, spec in rebuilds.items():
        if _sqlite_unique_columns(conn, table).isdisjoint(spec["legacy"]):
            continue
        tmp = f"{table}__new"
        columns = ", ".join(spec["columns"])
        try:
            conn.execute(db.text("PRAGMA foreign_keys=OFF"))
            conn.execute(db.text(f"DROP TABLE IF EXISTS {tmp}"))
            conn.execute(db.text(spec["create"]))
            conn.execute(db.text(f"INSERT INTO {tmp} ({columns}) SELECT {columns} FROM {table}"))
            conn.execute(db.text(f"DROP TABLE {table}"))
            conn.execute(db.text(f"ALTER TABLE {tmp} RENAME TO {table}"))
            for index_sql in spec["indexes"]:
                conn.execute(db.text(index_sql))
            conn.execute(db.text("PRAGMA foreign_keys=ON"))
            conn.commit()
        except Exception:
            try:
                conn.rollback()
                conn.execute(db.text("PRAGMA foreign_keys=ON"))
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


def _install_tenant_guards() -> None:
    """Install ORM-level tenant filters for normal web requests."""
    global _TENANT_GUARDS_INSTALLED
    if _TENANT_GUARDS_INSTALLED:
        return

    from flask import has_request_context
    from sqlalchemy import event
    from sqlalchemy.orm import Session as ORMSession, with_loader_criteria

    from .models import (
        AlertLog, Asset, BoardColumn, Contact, DashboardReport, DashboardWidget,
        FieldDefinition, FieldValue, IntegrationConfig, InventoryItem, Invoice,
        Renewal, Sale, Sprint, SyncLog, Task, TaskHistory, Vendor, Workflow,
        WorkflowRun, AuditLog,
    )

    tenant_models = (
        Sprint, Task, Contact, Vendor, Asset, InventoryItem, Invoice, Renewal,
        Sale, TaskHistory, BoardColumn, FieldDefinition, FieldValue, Workflow,
        WorkflowRun, AlertLog, DashboardWidget, DashboardReport,
        IntegrationConfig, SyncLog, AuditLog,
    )

    def _request_org_id() -> int | None:
        if not has_request_context():
            return None
        role = session.get("role")
        org_id = session.get("org_id")
        if role is None:
            user = getattr(g, "user", None)
            state = getattr(user, "__dict__", {}) if user else {}
            role = state.get("role")
            org_id = state.get("org_id")
        if role == "super_admin":
            return None
        return org_id

    @event.listens_for(ORMSession, "do_orm_execute")
    def _scope_tenant_selects(execute_state):
        if not execute_state.is_select or execute_state.execution_options.get("skip_tenant_scope"):
            return
        org_id = _request_org_id()
        if org_id is None:
            return
        statement = execute_state.statement
        for model in tenant_models:
            statement = statement.options(
                with_loader_criteria(
                    model,
                    lambda cls: cls.org_id == org_id,
                    include_aliases=True,
                )
            )
        execute_state.statement = statement

    @event.listens_for(ORMSession, "before_flush")
    def _stamp_new_tenant_rows(session, flush_context, instances):
        org_id = _request_org_id()
        if org_id is None:
            return
        for obj in session.new:
            if isinstance(obj, tenant_models) and getattr(obj, "org_id", None) is None:
                obj.org_id = org_id

    _TENANT_GUARDS_INSTALLED = True


def create_app(test_config: dict | None = None) -> Flask:
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(Config)
    if test_config:
        app.config.update(test_config)

    db.init_app(app)
    _install_tenant_guards()

    # Rate limiter
    if _limiter_available:
        limiter.init_app(app)

    with app.app_context():
        db.create_all()
        _run_migrations(app)
        ensure_default_admin(app)
        from .services import seed_board_columns
        default_org = Organization.query.filter_by(slug="default").first()
        seed_board_columns(default_org.id if default_org else None)
        _sync_security_alerts(app)

    @app.before_request
    def load_current_user() -> None:
        g.user = None
        user_id = session.get("user_id")
        if user_id:
            u = db.session.get(User, user_id)
            if u and u.is_active:
                g.user = u
                session["role"] = u.role
                session["org_id"] = u.org_id

    @app.before_request
    def enforce_tenant_boundaries() -> None:
        if g.user is None:
            return
        endpoint = request.endpoint or ""
        allowed_for_super_admin = {
            "health",
            "static",
            "main.index",
            "main.login",
            "main.signup",
            "main.logout",
            "main.platform_admin",
            "main.platform_org_toggle",
            "main.platform_org_plan",
            "main.platform_user_toggle",
            "main.platform_user_role",
            "main.platform_user_password",
            "main.platform_billing_settings",
            "main.platform_email_settings",
        }
        if g.user.is_super_admin:
            if endpoint not in allowed_for_super_admin:
                abort(403)
            return
        if endpoint.startswith("main.") and not g.user.org_id:
            abort(403)

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
        user = User(username=username, role="super_admin", org_id=None, is_active=True)
        user.password_hash = generate_password_hash(password)
        db.session.add(user)
        db.session.commit()
    elif existing.role != "super_admin":
        existing.role = "super_admin"
        existing.org_id = None
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
    current = model_class.query.filter_by(org_id=org_id).count()
    if org.plan == "enterprise":
        return True, current, org.max_records
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
