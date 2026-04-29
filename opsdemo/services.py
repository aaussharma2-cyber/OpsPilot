from __future__ import annotations

import base64
import hashlib
import json
import re
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from flask import current_app

from .models import (
    AlertLog, Asset, BoardColumn, Contact, FieldDefinition, FieldValue,
    IntegrationConfig, InventoryItem, Invoice, Renewal, Sale, Sprint,
    SyncLog, Task, Vendor, Workflow, db,
)


# ── Static option lists ─────────────────────────────────────────────────────

DEFAULT_TASK_COLUMNS = ["Backlog", "In Progress", "Blocked", "Review", "Done"]
PRIORITIES = ["Low", "Medium", "High", "Critical"]
SPRINT_STATUSES = ["Planning", "Active", "Completed"]
CONTACT_KINDS = ["lead", "customer"]
INVOICE_KINDS = ["sales", "purchase"]
FIELD_TYPES = ["text", "number", "date", "select", "checkbox"]

ENTITY_TYPES = ["task", "contact", "vendor", "asset", "inventory", "invoice", "renewal", "sale"]
ENTITY_LABELS = {
    "task": "Task",
    "contact": "CRM Contact",
    "vendor": "Vendor",
    "asset": "Asset",
    "inventory": "Inventory Item",
    "invoice": "Invoice",
    "renewal": "Renewal",
    "sale": "Sale",
}

# (event_key, label, needs_value)
WORKFLOW_TRIGGER_EVENTS = {
    "task":      [("created", "is created", False), ("status_changed", "status changes to →", True)],
    "contact":   [("created", "is created", False), ("stage_changed", "stage changes to →", True)],
    "invoice":   [("created", "is created", False), ("paid", "is marked paid", False)],
    "renewal":   [("created", "is created", False), ("rolled_forward", "is rolled forward", False)],
    "inventory": [("low_stock", "goes below reorder level", False)],
    "vendor":    [("created", "is created", False)],
    "asset":     [("created", "is created", False)],
    "sale":      [("created", "is created", False)],
}

WORKFLOW_ACTIONS = [
    ("create_task", "Create a task"),
    ("update_task_status", "Move linked tasks to a status"),
    ("send_alert", "Send an in-app alert"),
]

# (field_key, label, field_type)  — used to build flexible condition UI
ENTITY_FIELDS: dict[str, list[tuple[str, str, str]]] = {
    "task": [
        ("status",    "Status",    "text"),
        ("priority",  "Priority",  "text"),
        ("due_date",  "Due Date",  "date"),
        ("owner",     "Owner",     "text"),
        ("title",     "Title",     "text"),
    ],
    "contact": [
        ("stage",    "Stage",    "text"),
        ("name",     "Name",     "text"),
        ("company",  "Company",  "text"),
        ("email",    "Email",    "text"),
    ],
    "invoice": [
        ("status",     "Status",     "text"),
        ("amount",     "Amount",     "number"),
        ("due_date",   "Due Date",   "date"),
        ("party_name", "Party Name", "text"),
        ("reference",  "Reference",  "text"),
    ],
    "renewal": [
        ("renew_on",  "Renewal Date", "date"),
        ("status",    "Status",       "text"),
        ("cost",      "Cost",         "number"),
        ("provider",  "Provider",     "text"),
        ("category",  "Category",     "text"),
    ],
    "vendor": [
        ("rating",        "Rating",       "number"),
        ("contract_end",  "Contract End", "date"),
        ("category",      "Category",     "text"),
        ("name",          "Name",         "text"),
    ],
    "asset": [
        ("status",       "Status",      "text"),
        ("expiry_date",  "Expiry Date", "date"),
        ("category",     "Category",    "text"),
        ("name",         "Name",        "text"),
    ],
    "inventory": [
        ("qty_on_hand",    "Qty On Hand",    "number"),
        ("reorder_level",  "Reorder Level",  "number"),
        ("sku",            "SKU",            "text"),
        ("name",           "Name",           "text"),
    ],
    "sale": [
        ("revenue",        "Revenue",       "number"),
        ("channel",        "Channel",       "text"),
        ("customer_name",  "Customer",      "text"),
        ("order_ref",      "Order Ref",     "text"),
    ],
}

# Operators by field type shown in the condition builder
CONDITION_OPS: dict[str, list[tuple[str, str]]] = {
    "text":   [("eq", "equals"), ("neq", "not equals"), ("contains", "contains")],
    "number": [("eq", "equals"), ("neq", "not equals"), ("gt", "greater than"), ("lt", "less than"), ("gte", "≥"), ("lte", "≤")],
    "date":   [("within_days", "is within next N days"), ("older_than_days", "is more than N days ago"), ("eq", "equals (YYYY-MM-DD)"), ("before", "is before"), ("after", "is after")],
}


# ── Dashboard widgets ────────────────────────────────────────────────────────

WIDGET_TYPES: dict[str, dict] = {
    "metric_revenue":     {"label": "Revenue",               "zone": "metric"},
    "metric_margin":      {"label": "Margin",                "zone": "metric"},
    "metric_invoice_due": {"label": "Open Invoice Exposure", "zone": "metric"},
    "metric_overdue":     {"label": "Overdue Invoices",      "zone": "metric"},
    "metric_low_stock":   {"label": "Low Stock Items",       "zone": "metric"},
    "metric_renewals":    {"label": "Renewals Due",          "zone": "metric"},
    "metric_expiries":    {"label": "Asset Expiries",        "zone": "metric"},
    "metric_tasks":       {"label": "Open Tasks",            "zone": "metric"},
    "sales_chart":        {"label": "Sales Trend",           "zone": "panel"},
    "alerts":             {"label": "Priority Alerts",       "zone": "panel"},
    "low_stock_table":    {"label": "Low-Stock Watchlist",   "zone": "panel"},
    "invoices_table":     {"label": "Recent Invoices",       "zone": "panel"},
    "tasks_table":        {"label": "Open Tasks Table",      "zone": "panel"},
    "contacts_table":     {"label": "Recent Contacts",       "zone": "panel"},
    "saved_report":       {"label": "Pinned Report",         "zone": "panel"},
}

DEFAULT_WIDGET_TYPES = [
    "metric_revenue", "metric_margin", "metric_invoice_due", "metric_overdue",
    "metric_low_stock", "metric_renewals", "metric_expiries", "metric_tasks",
    "sales_chart", "alerts", "low_stock_table",
]

# ── Reports ──────────────────────────────────────────────────────────────────

REPORT_ENTITIES: dict[str, dict] = {
    "sales": {
        "label": "Sales",
        "groups": [("month", "Month"), ("channel", "Channel"), ("customer", "Customer")],
        "metrics": [("count", "Count"), ("sum_revenue", "Total Revenue"), ("sum_cost", "Total Cost"), ("sum_margin", "Total Margin")],
    },
    "invoices": {
        "label": "Invoices",
        "groups": [("status", "Status"), ("kind", "Kind"), ("party", "Party")],
        "metrics": [("count", "Count"), ("sum_amount", "Total Amount")],
    },
    "inventory": {
        "label": "Inventory",
        "groups": [("category", "Category"), ("warehouse", "Warehouse")],
        "metrics": [("count", "Item Count"), ("sum_qty", "Total Qty on Hand")],
    },
    "assets": {
        "label": "Assets",
        "groups": [("category", "Category"), ("status", "Status")],
        "metrics": [("count", "Count"), ("sum_value", "Total Value")],
    },
    "renewals": {
        "label": "Renewals",
        "groups": [("category", "Category"), ("status", "Status")],
        "metrics": [("count", "Count"), ("sum_cost", "Total Cost")],
    },
    "contacts": {
        "label": "Contacts",
        "groups": [("kind", "Type"), ("stage", "Stage")],
        "metrics": [("count", "Count")],
    },
    "tasks": {
        "label": "Tasks",
        "groups": [("status", "Status"), ("priority", "Priority")],
        "metrics": [("count", "Count")],
    },
}


def get_report_data(report) -> list[dict]:
    """Execute a saved report and return rows as [{label, value}]."""
    from sqlalchemy import func

    entity = report.entity
    group_by = report.group_by or ""
    metric = report.metric or "count"
    rows = []

    def _q(model, group_field, value_expr, order_asc=False):
        q = db.session.query(group_field.label("label"), value_expr.label("value")).group_by(group_field)
        q = q.order_by(group_field.asc() if order_asc else value_expr.desc())
        return q.limit(20).all()

    if entity == "sales":
        if group_by == "month":
            gf = func.strftime("%Y-%m", Sale.order_date)
            order_asc = True
        elif group_by == "channel":
            gf = func.coalesce(Sale.channel, "Unknown")
            order_asc = False
        else:
            gf = Sale.customer_name
            order_asc = False
        if metric == "sum_revenue":
            vf = func.sum(Sale.revenue)
        elif metric == "sum_cost":
            vf = func.sum(Sale.cost)
        elif metric == "sum_margin":
            vf = func.sum(Sale.revenue - Sale.cost)
        else:
            vf = func.count()
        rows = _q(Sale, gf, vf, order_asc)

    elif entity == "invoices":
        gf = {"status": Invoice.status, "kind": Invoice.kind}.get(group_by, Invoice.party_name)
        vf = func.sum(Invoice.amount) if metric == "sum_amount" else func.count()
        rows = _q(Invoice, gf, vf)

    elif entity == "inventory":
        gf = func.coalesce(InventoryItem.category, "Unknown") if group_by == "category" else InventoryItem.warehouse
        vf = func.sum(InventoryItem.qty_on_hand) if metric == "sum_qty" else func.count()
        rows = _q(InventoryItem, gf, vf)

    elif entity == "assets":
        gf = func.coalesce(Asset.category, "Unknown") if group_by == "category" else Asset.status
        vf = func.sum(Asset.current_value) if metric == "sum_value" else func.count()
        rows = _q(Asset, gf, vf)

    elif entity == "renewals":
        gf = func.coalesce(Renewal.category, "Unknown") if group_by == "category" else Renewal.status
        vf = func.sum(Renewal.cost) if metric == "sum_cost" else func.count()
        rows = _q(Renewal, gf, vf)

    elif entity == "contacts":
        gf = Contact.kind if group_by == "kind" else Contact.stage
        rows = _q(Contact, gf, func.count())

    elif entity == "tasks":
        gf = Task.priority if group_by == "priority" else Task.status
        rows = _q(Task, gf, func.count())

    return [{"label": str(r.label or "Unknown"), "value": float(r.value or 0)} for r in rows]


# ── Board columns ────────────────────────────────────────────────────────────

def get_task_columns() -> list[str]:
    cols = BoardColumn.query.order_by(BoardColumn.position.asc(), BoardColumn.id.asc()).all()
    return [c.name for c in cols] if cols else DEFAULT_TASK_COLUMNS


def seed_board_columns() -> None:
    if BoardColumn.query.count():
        return
    for pos, name in enumerate(DEFAULT_TASK_COLUMNS):
        db.session.add(BoardColumn(name=name, position=pos))
    db.session.commit()


# ── Custom fields ────────────────────────────────────────────────────────────

def get_field_defs(entity_type: str) -> list[FieldDefinition]:
    return (
        FieldDefinition.query
        .filter_by(entity_type=entity_type)
        .order_by(FieldDefinition.position.asc(), FieldDefinition.id.asc())
        .all()
    )


def get_field_values_map(entity_type: str, entity_ids: list[int]) -> dict[int, dict[int, str]]:
    """Returns {entity_id: {field_def_id: value}}"""
    if not entity_ids:
        return {}
    rows = FieldValue.query.filter(
        FieldValue.entity_type == entity_type,
        FieldValue.entity_id.in_(entity_ids),
    ).all()
    result: dict[int, dict[int, str]] = {}
    for v in rows:
        result.setdefault(v.entity_id, {})[v.field_def_id] = v.value or ""
    return result


def save_field_values(entity_type: str, entity_id: int, field_defs: list[FieldDefinition], form_data) -> None:
    for fd in field_defs:
        key = f"cf_{fd.field_key}"
        if fd.field_type == "checkbox":
            value = "yes" if form_data.get(key) else None
        else:
            value = (form_data.get(key, "") or "").strip() or None
        existing = FieldValue.query.filter_by(
            entity_type=entity_type,
            entity_id=entity_id,
            field_def_id=fd.id,
        ).first()
        if existing:
            existing.value = value
        else:
            db.session.add(FieldValue(
                entity_type=entity_type,
                entity_id=entity_id,
                field_def_id=fd.id,
                value=value,
            ))


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


# ── Dashboard & alerts ───────────────────────────────────────────────────────

class ValidationError(Exception):
    pass


class EmptySeedError(Exception):
    pass


def parse_date(value: str | None):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_decimal(value: str | None, *, default: str = "0") -> Decimal:
    raw = value if value not in (None, "") else default
    try:
        return Decimal(raw)
    except Exception as exc:
        raise ValidationError(f"Invalid number: {raw}") from exc


def parse_int(value: str | None, *, default: int = 0) -> int:
    raw = value if value not in (None, "") else default
    try:
        return int(raw)
    except Exception as exc:
        raise ValidationError(f"Invalid integer: {raw}") from exc


def dashboard_snapshot():
    today = date.today()
    lookahead = current_app.config.get("ALERT_LOOKAHEAD_DAYS", 30)
    alert_cutoff = today + timedelta(days=lookahead)

    revenue_total = sum(float(s.revenue) for s in Sale.query.all())
    cost_total = sum(float(s.cost) for s in Sale.query.all())
    invoice_due = sum(
        float(i.amount)
        for i in Invoice.query.filter(Invoice.status != "Paid").all()
    )
    overdue_invoices = Invoice.query.filter(
        Invoice.status != "Paid", Invoice.due_date < today
    ).count()
    low_stock_items = InventoryItem.query.filter(
        InventoryItem.qty_on_hand <= InventoryItem.reorder_level
    ).count()
    renewals_due = Renewal.query.filter(
        Renewal.status == "Active", Renewal.renew_on <= alert_cutoff
    ).count()
    asset_expiries = Asset.query.filter(
        Asset.status == "Active", Asset.expiry_date.isnot(None), Asset.expiry_date <= alert_cutoff
    ).count()
    sales_count = Sale.query.count()
    task_open = Task.query.filter(Task.status != "Done").count()

    sales_by_month = defaultdict(float)
    for sale in Sale.query.order_by(Sale.order_date.asc()).all():
        key = sale.order_date.strftime("%Y-%m")
        sales_by_month[key] += float(sale.revenue)

    top_items = []
    for item in InventoryItem.query.order_by(InventoryItem.qty_on_hand.asc()).limit(5).all():
        top_items.append({
            "sku": item.sku,
            "name": item.name,
            "qty_on_hand": item.qty_on_hand,
            "reorder_level": item.reorder_level,
        })

    alerts = build_alerts(today=today, cutoff=alert_cutoff)

    return {
        "metrics": {
            "revenue_total": round(revenue_total, 2),
            "margin_total": round(revenue_total - cost_total, 2),
            "invoice_due": round(invoice_due, 2),
            "overdue_invoices": overdue_invoices,
            "low_stock_items": low_stock_items,
            "renewals_due": renewals_due,
            "asset_expiries": asset_expiries,
            "sales_count": sales_count,
            "task_open": task_open,
        },
        "sales_by_month": dict(sales_by_month),
        "alerts": alerts,
        "top_items": top_items,
    }


def build_alerts(today: date | None = None, cutoff: date | None = None):
    today = today or date.today()
    cutoff = cutoff or (today + timedelta(days=current_app.config.get("ALERT_LOOKAHEAD_DAYS", 30)))
    alerts = []

    for invoice in Invoice.query.filter(Invoice.status != "Paid", Invoice.due_date <= cutoff).order_by(Invoice.due_date.asc()).all():
        severity = "danger" if invoice.due_date < today else "warning"
        label = "Overdue invoice" if invoice.due_date < today else "Invoice due soon"
        alerts.append({
            "severity": severity,
            "label": label,
            "title": invoice.reference,
            "detail": f"{invoice.party_name} | due {invoice.due_date.isoformat()} | ${float(invoice.amount):,.2f}",
        })

    for renewal in Renewal.query.filter(Renewal.status == "Active", Renewal.renew_on <= cutoff).order_by(Renewal.renew_on.asc()).all():
        severity = "danger" if renewal.renew_on < today else "warning"
        alerts.append({
            "severity": severity,
            "label": "Renewal reminder",
            "title": renewal.title,
            "detail": f"{renewal.provider or 'Provider N/A'} | renews {renewal.renew_on.isoformat()} | ${float(renewal.cost):,.2f}",
        })

    for asset in Asset.query.filter(Asset.status == "Active", Asset.expiry_date.isnot(None), Asset.expiry_date <= cutoff).order_by(Asset.expiry_date.asc()).all():
        severity = "danger" if asset.expiry_date < today else "warning"
        alerts.append({
            "severity": severity,
            "label": "Asset expiry",
            "title": asset.name,
            "detail": f"{asset.category or 'Asset'} | expires {asset.expiry_date.isoformat()}",
        })

    for item in InventoryItem.query.filter(InventoryItem.qty_on_hand <= InventoryItem.reorder_level).order_by(InventoryItem.qty_on_hand.asc()).all():
        alerts.append({
            "severity": "warning",
            "label": "Low stock",
            "title": item.name,
            "detail": f"{item.sku} | {item.qty_on_hand} on hand vs reorder {item.reorder_level}",
        })

    return alerts[:20]


def seed_demo_data(force: bool = False) -> None:
    existing_count = sum(
        model.query.count()
        for model in [Task, Contact, Vendor, Asset, InventoryItem, Invoice, Renewal, Sale]
    )
    if existing_count and not force:
        raise EmptySeedError("Data already exists")

    if force and existing_count:
        for model in [Task, Contact, Vendor, Asset, InventoryItem, Invoice, Renewal, Sale]:
            model.query.delete()
        db.session.commit()

    today = date.today()

    tasks = [
        Task(title="Audit supplier contracts", status="Backlog", priority="High", due_date=today + timedelta(days=4), owner="Ops", related_type="vendor", related_name="Pacific Wholesale"),
        Task(title="Review Q2 promo calendar", status="In Progress", priority="Medium", due_date=today + timedelta(days=7), owner="Marketing", related_type="sales", related_name="Campaign Board"),
        Task(title="Approve invoice INV-2026-0007", status="Review", priority="Critical", due_date=today + timedelta(days=1), owner="Finance", related_type="invoice", related_name="INV-2026-0007"),
        Task(title="Close old lead pipeline", status="Done", priority="Low", due_date=today - timedelta(days=1), owner="Sales", related_type="crm", related_name="Dormant leads"),
    ]

    contacts = [
        Contact(kind="lead", name="Anika Rao", company="Northwind Retail", email="anika@northwind.test", phone="+61 400 111 000", stage="Qualified", notes="Interested in private-label bundle"),
        Contact(kind="customer", name="Luca Chen", company="Harbor Lane", email="luca@harborlane.test", phone="+61 400 222 000", stage="Won", notes="Top wholesale account"),
    ]

    vendors = [
        Vendor(name="Pacific Wholesale", category="Packaging", contact_name="Mina", email="mina@pacific.test", phone="+61 400 333 000", contract_end=today + timedelta(days=22), rating=4, notes="Primary mailer supplier"),
        Vendor(name="Aurora Freight", category="Logistics", contact_name="Dylan", email="ops@aurora.test", phone="+61 400 444 000", contract_end=today + timedelta(days=61), rating=5, notes="Interstate freight partner"),
    ]

    assets = [
        Asset(name="Warehouse Printer A", category="Hardware", serial_number="WH-PR-001", owner="Warehouse", status="Active", purchase_cost=Decimal("840.00"), current_value=Decimal("420.00"), expiry_date=today + timedelta(days=15), notes="Service agreement expires soon"),
        Asset(name="Adobe Creative Cloud", category="Software", serial_number="LIC-ACC-12", owner="Design", status="Active", purchase_cost=Decimal("960.00"), current_value=Decimal("480.00"), expiry_date=today + timedelta(days=44), notes="Annual license"),
    ]

    inventory = [
        InventoryItem(sku="SKU-1001", name="Travel Bottle Set", category="Accessories", warehouse="Main", qty_on_hand=18, reorder_level=20, unit_cost=Decimal("4.50"), sale_price=Decimal("14.95"), notes="Core bestseller"),
        InventoryItem(sku="SKU-1002", name="Refill Pouch", category="Consumables", warehouse="Main", qty_on_hand=74, reorder_level=30, unit_cost=Decimal("2.20"), sale_price=Decimal("8.95"), expiry_date=today + timedelta(days=80), notes="High repeat purchase"),
        InventoryItem(sku="SKU-1003", name="Gift Box Large", category="Packaging", warehouse="Overflow", qty_on_hand=9, reorder_level=25, unit_cost=Decimal("1.70"), sale_price=Decimal("5.50"), notes="Seasonal"),
    ]

    invoices = [
        Invoice(kind="sales", party_name="Harbor Lane", reference="INV-2026-0007", amount=Decimal("2450.00"), due_date=today - timedelta(days=2), status="Unpaid", notes="Wholesale order"),
        Invoice(kind="purchase", party_name="Pacific Wholesale", reference="BILL-2026-011", amount=Decimal("860.00"), due_date=today + timedelta(days=8), status="Unpaid", notes="Mailer cartons"),
        Invoice(kind="sales", party_name="Northwind Retail", reference="INV-2026-0008", amount=Decimal("1790.00"), due_date=today + timedelta(days=12), status="Paid", paid_on=today - timedelta(days=1), notes="Starter range"),
    ]

    renewals = [
        Renewal(title="Domain renewal", category="IT", provider="NameHost", renew_on=today + timedelta(days=11), cost=Decimal("39.00"), auto_renew=True, status="Active", notes="ops.localdemo.test"),
        Renewal(title="Warehouse insurance", category="Compliance", provider="Shield Mutual", renew_on=today + timedelta(days=28), cost=Decimal("1490.00"), auto_renew=False, status="Active", notes="Annual policy"),
    ]

    sales = [
        Sale(order_ref="SO-001", customer_name="Harbor Lane", order_date=today - timedelta(days=50), channel="Wholesale", revenue=Decimal("920.00"), cost=Decimal("420.00"), quantity=45),
        Sale(order_ref="SO-002", customer_name="Shopify Store", order_date=today - timedelta(days=35), channel="Direct", revenue=Decimal("1340.00"), cost=Decimal("510.00"), quantity=82),
        Sale(order_ref="SO-003", customer_name="Shopify Store", order_date=today - timedelta(days=20), channel="Direct", revenue=Decimal("1680.00"), cost=Decimal("700.00"), quantity=97),
        Sale(order_ref="SO-004", customer_name="Northwind Retail", order_date=today - timedelta(days=9), channel="Wholesale", revenue=Decimal("2225.00"), cost=Decimal("1120.00"), quantity=110),
        Sale(order_ref="SO-005", customer_name="Marketplace", order_date=today - timedelta(days=2), channel="Marketplace", revenue=Decimal("1045.00"), cost=Decimal("500.00"), quantity=51),
    ]

    for collection in [tasks, contacts, vendors, assets, inventory, invoices, renewals, sales]:
        db.session.add_all(collection)
    db.session.commit()


# ── Integration helpers ──────────────────────────────────────────────────────

# Only allow real Shopify store domains — blocks SSRF attempts
_SHOPIFY_DOMAIN_RE = re.compile(
    r"^[a-z0-9][a-z0-9\-]*\.myshopify\.com$", re.IGNORECASE
)

# Guard against accidentally syncing enormous catalogues in one go
_MAX_SYNC_RECORDS = 5_000

DEFAULT_SHOPIFY_API_VERSION = "2026-04"
_SHOPIFY_PAGE_SIZE = 100


def validate_shopify_domain(domain: str) -> bool:
    """Return True only if domain is a valid *.myshopify.com hostname."""
    return bool(_SHOPIFY_DOMAIN_RE.match(domain.strip()))


def normalize_shopify_domain(raw_domain: str) -> str:
    """Normalize common pasted Shopify URLs to a bare myshopify.com hostname."""
    domain = (raw_domain or "").strip().lower()
    for prefix in ("https://", "http://"):
        if domain.startswith(prefix):
            domain = domain[len(prefix):]
            break
    domain = domain.split("/", 1)[0].split("?", 1)[0].rstrip(".")
    return domain


def _safe_str(value: object, max_len: int = 120) -> str:
    """Sanitise and truncate any value coming from an external API."""
    if value is None:
        return ""
    return str(value).strip()[:max_len]


def _safe_decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except InvalidOperation:
        return Decimal("0")


def _classify_shopify_error(exc: Exception) -> str:
    """Return a user-safe error message without leaking internal details."""
    if isinstance(exc, urllib.error.HTTPError):
        codes = {
            401: "Authentication failed — check your access token.",
            402: "Shopify payment required — your plan may not support API access.",
            403: "Access forbidden — check your app's API scopes (read_customers, read_orders).",
            404: "Store not found — verify your shop domain.",
            422: "Shopify rejected the request — check your credentials.",
            429: "Shopify rate limit reached — try again in a few seconds.",
        }
        return codes.get(exc.code, f"Shopify API error (HTTP {exc.code}).")
    if isinstance(exc, urllib.error.URLError):
        return "Could not reach Shopify — check your shop domain and internet connection."
    if isinstance(exc, ShopifyGraphQLError):
        message = str(exc).lower()
        if "access denied" in message or "requires" in message or "scope" in message:
            return "Access forbidden — check your app's API scopes (read_customers, read_orders)."
        if "throttle" in message or "rate" in message:
            return "Shopify rate limit reached — try again in a few seconds."
        return "Shopify rejected the request — check your credentials and scopes."
    return "Unexpected sync error — check your credentials and try again."


_ENCRYPTED_CONFIG_KEYS = {"access_token", "password", "token", "api_key"}


def _get_fernet():
    secret = current_app.config.get("CREDENTIAL_ENCRYPTION_KEY") or current_app.config.get("SECRET_KEY", "fallback")
    return _fernet_for_secret(secret)


def _fernet_for_secret(secret: str):
    from cryptography.fernet import Fernet
    derived = hashlib.sha256(secret.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(derived))


def _encrypt_value(value: str) -> str:
    return _get_fernet().encrypt(value.encode()).decode()


def _decrypt_value(value: str) -> str:
    secrets_to_try = [
        current_app.config.get("CREDENTIAL_ENCRYPTION_KEY"),
        current_app.config.get("SECRET_KEY", "fallback"),
    ]
    seen = set()
    for secret in secrets_to_try:
        if not secret or secret in seen:
            continue
        seen.add(secret)
        try:
            return _fernet_for_secret(secret).decrypt(value.encode()).decode()
        except Exception:
            continue
    return value  # legacy plaintext - still works, re-save to encrypt


def get_integration_config(name: str) -> dict[str, str]:
    rows = IntegrationConfig.query.filter_by(integration=name).all()
    result = {}
    for r in rows:
        if r.key in _ENCRYPTED_CONFIG_KEYS and r.value:
            result[r.key] = _decrypt_value(r.value)
        else:
            result[r.key] = r.value
    return result


def set_integration_config(name: str, data: dict[str, str]) -> None:
    for key, value in data.items():
        stored = _encrypt_value(value) if key in _ENCRYPTED_CONFIG_KEYS and value else value
        row = IntegrationConfig.query.filter_by(integration=name, key=key).first()
        if row:
            row.value = stored
        else:
            db.session.add(IntegrationConfig(integration=name, key=key, value=stored))
    db.session.commit()


class ShopifyGraphQLError(Exception):
    """Raised when Shopify returns GraphQL errors in a successful HTTP response."""


def _shopify_api_version() -> str:
    return current_app.config.get("SHOPIFY_API_VERSION", DEFAULT_SHOPIFY_API_VERSION)


def _shopify_graphql(
    shop_domain: str,
    access_token: str,
    query: str,
    variables: dict | None = None,
    max_retries: int = 3,
) -> tuple[dict, str]:
    """
    Call the Shopify Admin GraphQL API.

    Returns (json_body, served_api_version). Shopify includes the served version
    in X-Shopify-API-Version, which helps spot version fall-forward.
    """
    version = _shopify_api_version()
    url = f"https://{shop_domain}/admin/api/{version}/graphql.json"
    payload = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "OpsPilot-Local/1.0",
        },
        method="POST",
    )
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode("utf-8"))
                if body.get("errors"):
                    raise ShopifyGraphQLError("; ".join(_graphql_error_messages(body["errors"])))
                return body, resp.getheader("X-Shopify-API-Version", version)
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt < max_retries - 1:
                retry_after = float(exc.headers.get("Retry-After", "2"))
                time.sleep(min(retry_after, 10))
                continue
            raise
    raise RuntimeError("Shopify: max retries exceeded")


def _graphql_error_messages(errors: object) -> list[str]:
    if not isinstance(errors, list):
        return ["Unknown GraphQL error"]
    messages = []
    for err in errors:
        if isinstance(err, dict):
            messages.append(_safe_str(err.get("message"), 240) or "Unknown GraphQL error")
        else:
            messages.append(_safe_str(err, 240))
    return messages or ["Unknown GraphQL error"]


def _money_amount(money_set: dict | None) -> Decimal:
    if not isinstance(money_set, dict):
        return Decimal("0")
    shop_money = money_set.get("shopMoney") or {}
    return _safe_decimal(shop_money.get("amount"))


def _customer_name(customer: dict | None, fallback: str = "Unknown") -> str:
    customer = customer or {}
    name = f"{customer.get('firstName') or ''} {customer.get('lastName') or ''}".strip()
    return _safe_str(name or customer.get("email") or fallback, 120)


def _customer_company(customer: dict | None) -> str:
    customer = customer or {}
    default_address = customer.get("defaultAddress") or {}
    return _safe_str(default_address.get("company"), 120)


def _customer_phone(customer: dict | None) -> str:
    customer = customer or {}
    default_address = customer.get("defaultAddress") or {}
    return _safe_str(customer.get("phone") or default_address.get("phone"), 50)


def _find_contact_by_shopify_id(shopify_id: str, email: str) -> Contact | None:
    if email:
        existing = Contact.query.filter_by(email=email).first()
        if existing:
            return existing
    if shopify_id:
        return Contact.query.filter(Contact.notes.like(f"%Shopify id={shopify_id}%")).first()
    return None


def _line_item_quantity(line_item: dict) -> int:
    value = line_item.get("currentQuantity")
    if value is None:
        value = line_item.get("quantity")
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def test_shopify_connection(shop_domain: str, access_token: str) -> dict:
    """Lightweight credential check using the Admin GraphQL API."""
    shop_domain = normalize_shopify_domain(shop_domain)
    if not validate_shopify_domain(shop_domain):
        return {"ok": False, "error": "Invalid shop domain — must end in .myshopify.com"}
    try:
        query = "query ShopProbe { shop { name myshopifyDomain } }"
        body, served_version = _shopify_graphql(shop_domain, access_token, query)
        shop = body.get("data", {}).get("shop", {})
        shop_name = _safe_str(shop.get("name", "Unknown"), 80)
        served_domain = _safe_str(shop.get("myshopifyDomain", shop_domain), 120)
        return {
            "ok": True,
            "shop_name": shop_name,
            "shop_domain": served_domain,
            "api_version": served_version,
        }
    except Exception as exc:
        return {"ok": False, "error": _classify_shopify_error(exc)}


def sync_shopify_customers(shop_domain: str, access_token: str) -> dict:
    """
    Pull customers from Shopify and upsert into Contact.
    Uses Admin GraphQL cursor pagination and matches by email or Shopify id.
    """
    shop_domain = normalize_shopify_domain(shop_domain)
    if not validate_shopify_domain(shop_domain):
        return {"status": "error", "error": "Invalid shop domain.", "created": 0, "updated": 0}

    created = updated = total_fetched = 0
    after: str | None = None

    query = """
    query OpsPilotCustomers($first: Int!, $after: String) {
      customers(first: $first, after: $after) {
        edges {
          cursor
          node {
            id
            firstName
            lastName
            email
            phone
            defaultAddress {
              company
              phone
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """

    while True:
        try:
            body, _served_version = _shopify_graphql(
                shop_domain,
                access_token,
                query,
                {"first": _SHOPIFY_PAGE_SIZE, "after": after},
            )
        except Exception as exc:
            _log_sync("shopify", "customers", created + updated, status="error", error=_classify_shopify_error(exc))
            return {"status": "error", "error": _classify_shopify_error(exc), "created": created, "updated": updated}

        connection = body.get("data", {}).get("customers", {})
        edges = connection.get("edges", [])
        total_fetched += len(edges)

        for edge in edges:
            c = edge.get("node") or {}
            shopify_id = _safe_str(c.get("id"), 160)
            email = _safe_str(c.get("email"), 120).lower()
            name = _customer_name(c, fallback=email or f"Shopify {shopify_id[-8:] or 'customer'}")
            existing = _find_contact_by_shopify_id(shopify_id, email)
            if existing:
                existing.name = name
                existing.company = _customer_company(c) or existing.company
                existing.phone = _customer_phone(c) or existing.phone
                existing.kind = "customer"
                existing.stage = existing.stage or "Active"
                updated += 1
            else:
                db.session.add(Contact(
                    kind="customer",
                    name=name,
                    company=_customer_company(c),
                    email=email,
                    phone=_customer_phone(c),
                    stage="Active",
                    notes=f"Synced from Shopify id={shopify_id}",
                ))
                created += 1

        db.session.commit()

        if total_fetched >= _MAX_SYNC_RECORDS:
            break

        page_info = connection.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")
        if not after:
            break

    _log_sync("shopify", "customers", created + updated)
    return {"status": "ok", "created": created, "updated": updated, "total_fetched": total_fetched}


def sync_shopify_orders(shop_domain: str, access_token: str) -> dict:
    """
    Pull orders from Shopify into Sale and Invoice records.
    Uses Admin GraphQL cursor pagination.
    """
    shop_domain = normalize_shopify_domain(shop_domain)
    if not validate_shopify_domain(shop_domain):
        return {"status": "error", "error": "Invalid shop domain.", "created_sales": 0, "created_invoices": 0}

    created_sales = updated_sales = created_inv = total_fetched = 0
    after: str | None = None
    today = date.today()

    query = """
    query OpsPilotOrders($first: Int!, $after: String) {
      orders(first: $first, after: $after, sortKey: CREATED_AT, reverse: true) {
        edges {
          cursor
          node {
            id
            name
            createdAt
            sourceName
            displayFinancialStatus
            customer {
              firstName
              lastName
              email
              phone
              defaultAddress {
                company
                phone
              }
            }
            currentTotalPriceSet {
              shopMoney {
                amount
                currencyCode
              }
            }
            totalPriceSet {
              shopMoney {
                amount
                currencyCode
              }
            }
            lineItems(first: 100) {
              nodes {
                quantity
                currentQuantity
              }
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """

    while True:
        try:
            body, _served_version = _shopify_graphql(
                shop_domain,
                access_token,
                query,
                {"first": _SHOPIFY_PAGE_SIZE, "after": after},
            )
        except Exception as exc:
            _log_sync("shopify", "orders", created_sales + updated_sales, status="error", error=_classify_shopify_error(exc))
            return {"status": "error", "error": _classify_shopify_error(exc), "created_sales": created_sales, "created_invoices": created_inv}

        connection = body.get("data", {}).get("orders", {})
        edges = connection.get("edges", [])
        total_fetched += len(edges)

        for edge in edges:
            o = edge.get("node") or {}
            order_ref = _safe_str(o.get("name") or f"#{o.get('id', '?')}", 80)
            cust = o.get("customer") or {}
            customer_name = _customer_name(cust)
            revenue = _money_amount(o.get("currentTotalPriceSet")) or _money_amount(o.get("totalPriceSet"))
            order_date_str = _safe_str(o.get("createdAt") or today.isoformat(), 10)[:10]
            try:
                order_date = date.fromisoformat(order_date_str)
            except ValueError:
                order_date = today
            channel = _safe_str((o.get("sourceName") or "Shopify").replace("_", " ").title(), 80)

            line_items = (o.get("lineItems") or {}).get("nodes") or []
            quantity = max(
                1,
                sum(_line_item_quantity(li) for li in line_items if isinstance(li, dict)),
            )

            existing_sale = Sale.query.filter_by(order_ref=order_ref).first()
            if existing_sale:
                existing_sale.revenue = revenue
                existing_sale.customer_name = customer_name
                existing_sale.channel = channel
                existing_sale.quantity = quantity
                updated_sales += 1
            else:
                db.session.add(Sale(
                    order_ref=order_ref,
                    customer_name=customer_name,
                    order_date=order_date,
                    channel=channel,
                    revenue=revenue,
                    cost=Decimal("0"),
                    quantity=quantity,
                ))
                created_sales += 1

            fin_status = _safe_str(o.get("displayFinancialStatus"), 30).upper()
            if fin_status in ("PENDING", "PARTIALLY_PAID", "AUTHORIZED", "UNPAID"):
                ref = _safe_str(f"SHP-{order_ref}", 80)
                due_raw = _safe_str(o.get("createdAt") or today.isoformat(), 10)[:10]
                try:
                    due_date = date.fromisoformat(due_raw)
                except ValueError:
                    due_date = today
                if not Invoice.query.filter_by(reference=ref).first():
                    db.session.add(Invoice(
                        kind="sales",
                        party_name=customer_name,
                        reference=ref,
                        amount=revenue,
                        due_date=due_date,
                        status="Unpaid",
                        notes=f"Synced from Shopify order {order_ref}",
                    ))
                    created_inv += 1

        db.session.commit()

        if total_fetched >= _MAX_SYNC_RECORDS:
            break

        page_info = connection.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")
        if not after:
            break

    _log_sync("shopify", "orders", created_sales + updated_sales)
    return {
        "status": "ok",
        "created_sales": created_sales,
        "updated_sales": updated_sales,
        "created_invoices": created_inv,
        "total_fetched": total_fetched,
    }


def _log_sync(integration: str, entity: str, count: int, status: str = "ok", error: str | None = None) -> None:
    db.session.add(SyncLog(integration=integration, entity=entity, synced_count=count, status=status, error=error))
    db.session.commit()


def get_sync_logs(integration: str, limit: int = 20) -> list:
    return SyncLog.query.filter_by(integration=integration).order_by(SyncLog.synced_at.desc()).limit(limit).all()


# ── Audit logging ────────────────────────────────────────────────────────────

def log_audit(
    action: str,
    module: str,
    status: str = "ok",
    message: str | None = None,
    related_record: str | None = None,
) -> None:
    """Write a row to AuditLog. Never raises — app flow is not interrupted on failure.
    Automatically resolves the current user from Flask's g object."""
    from .models import AuditLog
    try:
        from flask import g
        user_obj = getattr(g, "user", None)
        user_str = user_obj.username if user_obj else "system"
    except RuntimeError:
        user_str = "system"
    try:
        entry = AuditLog(
            user=user_str,
            action=action,
            module=module,
            status=status,
            message=message,
            related_record=related_record,
        )
        db.session.add(entry)
        db.session.commit()
    except Exception:
        db.session.rollback()


# ── Email / SMTP ─────────────────────────────────────────────────────────────

def get_smtp_config() -> dict[str, str]:
    return get_integration_config("smtp")


def create_invoice_from_renewal(renewal) -> Invoice:
    """Create an unpaid Invoice record from a Renewal. Does not commit."""
    today = date.today()
    base_ref = f"RNW-{renewal.id}-{today.strftime('%Y%m%d')}"
    ref = base_ref
    n = 0
    while Invoice.query.filter_by(reference=ref).first():
        n += 1
        ref = f"{base_ref}-{n}"
    return Invoice(
        kind="sales",
        party_name=renewal.contact_name or renewal.provider or renewal.title,
        reference=ref,
        amount=renewal.cost,
        due_date=renewal.renew_on,
        status="Unpaid",
        notes=f"Invoice for renewal: {renewal.title}",
    )


def _send_via_smtp(to_email: str, subject: str, html_body: str, cfg: dict) -> dict:
    """Send via smtplib. Raises ValueError on any failure."""
    import smtplib
    import socket
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    host = (cfg.get("host") or "").strip()
    port = int(cfg.get("port") or 587)
    username = (cfg.get("username") or "").strip()
    password = (cfg.get("password") or "").strip()
    from_addr = (cfg.get("from_addr") or username).strip()
    use_tls = str(cfg.get("use_tls", "true")).lower() != "false"

    if not host:
        raise ValueError("SMTP host is not configured. Go to Settings → Email.")
    if not from_addr:
        raise ValueError("SMTP 'From' address is not configured. Set it in Settings → Email.")

    provider_info = f"{host}:{port} (TLS={use_tls})"
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_email
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP(host, port, timeout=20) as smtp:
            smtp.ehlo()
            if use_tls:
                smtp.starttls()
                smtp.ehlo()
            if username and password:
                smtp.login(username, password)
            refused = smtp.sendmail(from_addr, [to_email], msg.as_string())
    except smtplib.SMTPAuthenticationError:
        raise ValueError("SMTP authentication failed — check username and password in Settings → Email.")
    except smtplib.SMTPSenderRefused:
        raise ValueError(
            f"SMTP server rejected the sender address '{from_addr}'. "
            "Check the 'From address' field in Settings → Email.")
    except smtplib.SMTPRecipientsRefused as exc:
        raise ValueError(f"SMTP server refused delivery to: {', '.join(exc.recipients.keys())}")
    except smtplib.SMTPConnectError:
        raise ValueError(f"Could not connect to SMTP server {host}:{port}. Check host and port.")
    except smtplib.SMTPException as exc:
        raise ValueError(f"SMTP error ({type(exc).__name__}): {exc}") from exc
    except (socket.gaierror, socket.timeout):
        raise ValueError(f"Network error reaching '{host}:{port}' — check hostname and connection.")
    except (OSError, TimeoutError) as exc:
        raise ValueError(f"Connection to '{host}:{port}' failed ({type(exc).__name__}).") from exc

    refused_addrs = list(refused.keys()) if refused else []
    if refused_addrs:
        raise ValueError(f"SMTP accepted message but refused delivery to: {', '.join(refused_addrs)}")
    return {"provider": provider_info, "refused": refused_addrs}


def _send_via_sendgrid(to_email: str, subject: str, html_body: str, cfg: dict) -> dict:
    """Send via SendGrid Web API v3. Raises ValueError on failure."""
    api_key = (cfg.get("api_key") or "").strip()
    from_addr = (cfg.get("from_addr") or "").strip()
    if not api_key:
        raise ValueError("SendGrid API key is not configured. Go to Settings → Email.")
    if not from_addr:
        raise ValueError("'From' address is not configured. Set it in Settings → Email.")

    payload = json.dumps({
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": from_addr},
        "subject": subject,
        "content": [{"type": "text/html", "value": html_body}],
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=payload,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status in (200, 202):
                return {"provider": "SendGrid API", "refused": []}
            raise ValueError(f"SendGrid returned unexpected status {resp.status}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:300]
        if exc.code == 401:
            raise ValueError("SendGrid API key is invalid or expired.")
        if exc.code == 403:
            raise ValueError("SendGrid API key lacks 'Mail Send' permission.")
        raise ValueError(f"SendGrid API error ({exc.code}): {body}")
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        raise ValueError(f"Could not reach SendGrid API: {exc}")


def _send_via_resend(to_email: str, subject: str, html_body: str, cfg: dict) -> dict:
    """Send via Resend API. Raises ValueError on failure."""
    api_key = (cfg.get("api_key") or "").strip()
    from_addr = (cfg.get("from_addr") or "").strip()
    if not api_key:
        raise ValueError("Resend API key is not configured. Go to Settings → Email.")
    if not from_addr:
        raise ValueError("'From' address is not configured. Set it in Settings → Email.")

    payload = json.dumps({
        "from": from_addr,
        "to": [to_email],
        "subject": subject,
        "html": html_body,
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=payload,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30):
            return {"provider": "Resend API", "refused": []}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:300]
        if exc.code == 401:
            raise ValueError("Resend API key is invalid.")
        if exc.code == 422:
            raise ValueError(f"Resend rejected the request — verify your 'from' domain is set up in Resend: {body}")
        raise ValueError(f"Resend API error ({exc.code}): {body}")
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        raise ValueError(f"Could not reach Resend API: {exc}")


def send_invoice_email(to_email: str, subject: str, html_body: str, smtp_config: dict) -> dict:
    """Send email via whichever provider is active in config.
    Returns {"provider": str, "refused": []}. Raises ValueError on failure."""
    provider = (smtp_config.get("provider") or "smtp").lower()
    if provider == "sendgrid":
        return _send_via_sendgrid(to_email, subject, html_body, smtp_config)
    if provider == "resend":
        return _send_via_resend(to_email, subject, html_body, smtp_config)
    return _send_via_smtp(to_email, subject, html_body, smtp_config)


def send_test_email(to_email: str, smtp_config: dict) -> dict:
    """Send a test email using the active provider to verify end-to-end delivery."""
    subject = "OpsPilot — Email delivery test"
    body = (
        "<p>This is a test email from <strong>OpsPilot Local</strong>.</p>"
        "<p>If you received this, your email configuration is working correctly.</p>"
    )
    return send_invoice_email(to_email, subject, body, smtp_config)


def test_api_connection(cfg: dict) -> dict:
    """Verify an API-based email provider key without sending email.
    Returns {"ok": True, "provider": str}. Raises ValueError on failure."""
    provider = (cfg.get("provider") or "smtp").lower()
    api_key = (cfg.get("api_key") or "").strip()
    if not api_key:
        raise ValueError("API key is not configured.")

    if provider == "sendgrid":
        req = urllib.request.Request(
            "https://api.sendgrid.com/v3/scopes",
            headers={"Authorization": f"Bearer {api_key}"},
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=10):
                return {"ok": True, "provider": "SendGrid API"}
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                raise ValueError("SendGrid API key is invalid or expired.")
            raise ValueError(f"SendGrid API error ({exc.code})")
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            raise ValueError(f"Could not reach SendGrid: {exc}")

    if provider == "resend":
        req = urllib.request.Request(
            "https://api.resend.com/domains",
            headers={"Authorization": f"Bearer {api_key}"},
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=10):
                return {"ok": True, "provider": "Resend API"}
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                raise ValueError("Resend API key is invalid.")
            raise ValueError(f"Resend API error ({exc.code})")
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            raise ValueError(f"Could not reach Resend: {exc}")

    raise ValueError(f"Unknown provider '{provider}' — use sendgrid or resend.")


# GitHub integration removed — use standard git tooling to push to GitHub.
# See README.md for deployment notes.
