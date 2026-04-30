from __future__ import annotations

import json as _json
from datetime import date, datetime, timezone


def utcnow_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)

from flask_sqlalchemy import SQLAlchemy


db = SQLAlchemy()


class TimestampMixin:
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow_naive)
    updated_at = db.Column(
        db.DateTime, nullable=False, default=utcnow_naive, onupdate=utcnow_naive
    )


class Organization(db.Model):
    __tablename__ = "organization"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    slug = db.Column(db.String(60), unique=True, nullable=False, index=True)
    plan = db.Column(db.String(20), nullable=False, default="free")  # free / pro / enterprise
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow_naive)

    # Brand / document settings
    logo_path = db.Column(db.String(255), nullable=True)       # relative path under static/uploads/logos/
    pdf_header_text = db.Column(db.String(200), nullable=True)  # custom header in PDF exports
    pdf_footer_text = db.Column(db.String(200), nullable=True)  # custom footer in PDF exports
    email_from_name = db.Column(db.String(120), nullable=True)  # sender display name for outgoing emails

    # Plan limits
    _PLAN_USER_LIMITS = {"free": 2, "pro": 25, "enterprise": 9999}
    _PLAN_RECORD_LIMITS = {"free": 5, "pro": 9999, "enterprise": 9999}

    @property
    def max_users(self) -> int:
        return self._PLAN_USER_LIMITS.get(self.plan, 2)

    @property
    def max_records(self) -> int:
        return self._PLAN_RECORD_LIMITS.get(self.plan, 5)

    @property
    def plan_label(self) -> str:
        return self.plan.capitalize()


class User(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    email = db.Column(db.String(120), unique=True, nullable=True, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    # Roles: super_admin > org_admin > member > viewer
    # Legacy: "admin" treated as org_admin, "manager" as member
    role = db.Column(db.String(30), nullable=False, default="org_admin")
    org_id = db.Column(
        db.Integer,
        db.ForeignKey("organization.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    org = db.relationship("Organization", foreign_keys=[org_id])

    @property
    def display_role(self) -> str:
        mapping = {
            "super_admin": "Super Admin",
            "org_admin": "Admin",
            "admin": "Admin",
            "member": "Member",
            "manager": "Member",
            "viewer": "Viewer",
        }
        return mapping.get(self.role, self.role.capitalize())

    @property
    def is_org_admin(self) -> bool:
        return self.role in ("org_admin", "admin", "super_admin")

    @property
    def is_super_admin(self) -> bool:
        return self.role == "super_admin"

    @property
    def can_write(self) -> bool:
        return self.role not in ("viewer",)


class Sprint(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    goal = db.Column(db.Text, nullable=True)
    start_date = db.Column(db.Date, nullable=True)
    end_date = db.Column(db.Date, nullable=True)
    status = db.Column(db.String(20), nullable=False, default="Planning")  # Planning / Active / Completed
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow_naive)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)


class Task(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(120), nullable=False)
    description = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(30), nullable=False, default="Backlog", index=True)
    priority = db.Column(db.String(20), nullable=False, default="Medium")
    due_date = db.Column(db.Date, nullable=True)
    owner = db.Column(db.String(120), nullable=True)
    related_type = db.Column(db.String(40), nullable=True)
    related_name = db.Column(db.String(120), nullable=True)
    sprint_id = db.Column(db.Integer, db.ForeignKey('sprint.id', ondelete='SET NULL'), nullable=True, index=True)
    sprint = db.relationship('Sprint', backref='tasks', foreign_keys=[sprint_id])
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)


class Contact(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    kind = db.Column(db.String(20), nullable=False, default="lead", index=True)
    name = db.Column(db.String(120), nullable=False)
    company = db.Column(db.String(120), nullable=True)
    email = db.Column(db.String(120), nullable=True)
    phone = db.Column(db.String(50), nullable=True)
    stage = db.Column(db.String(30), nullable=False, default="New")
    notes = db.Column(db.Text, nullable=True)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)


class Vendor(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    category = db.Column(db.String(80), nullable=True)
    contact_name = db.Column(db.String(120), nullable=True)
    email = db.Column(db.String(120), nullable=True)
    phone = db.Column(db.String(50), nullable=True)
    contract_end = db.Column(db.Date, nullable=True, index=True)
    rating = db.Column(db.Integer, nullable=False, default=3)
    notes = db.Column(db.Text, nullable=True)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)


class Asset(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    category = db.Column(db.String(80), nullable=True)
    serial_number = db.Column(db.String(120), nullable=True)
    owner = db.Column(db.String(120), nullable=True)
    status = db.Column(db.String(30), nullable=False, default="Active")
    purchase_cost = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    current_value = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    expiry_date = db.Column(db.Date, nullable=True, index=True)
    notes = db.Column(db.Text, nullable=True)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)


class InventoryItem(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sku = db.Column(db.String(60), nullable=False, index=True)
    name = db.Column(db.String(120), nullable=False)
    category = db.Column(db.String(80), nullable=True)
    warehouse = db.Column(db.String(80), nullable=False, default="Main")
    qty_on_hand = db.Column(db.Integer, nullable=False, default=0)
    reorder_level = db.Column(db.Integer, nullable=False, default=0)
    unit_cost = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    sale_price = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    expiry_date = db.Column(db.Date, nullable=True, index=True)
    notes = db.Column(db.Text, nullable=True)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)
    __table_args__ = (
        db.UniqueConstraint("sku", "org_id", name="uq_sku_org"),
    )


class Invoice(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    kind = db.Column(db.String(20), nullable=False, default="sales", index=True)
    party_name = db.Column(db.String(120), nullable=False)
    reference = db.Column(db.String(80), nullable=False)
    amount = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    due_date = db.Column(db.Date, nullable=False, index=True)
    status = db.Column(db.String(20), nullable=False, default="Unpaid", index=True)
    paid_on = db.Column(db.Date, nullable=True)
    notes = db.Column(db.Text, nullable=True)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)
    __table_args__ = (
        db.UniqueConstraint("reference", "org_id", name="uq_invoice_ref_org"),
    )


class Renewal(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(120), nullable=False)
    category = db.Column(db.String(80), nullable=True)
    provider = db.Column(db.String(120), nullable=True)
    renew_on = db.Column(db.Date, nullable=False, index=True)
    cost = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    auto_renew = db.Column(db.Boolean, nullable=False, default=False)
    status = db.Column(db.String(20), nullable=False, default="Active")
    notes = db.Column(db.Text, nullable=True)
    contact_name = db.Column(db.String(120), nullable=True)
    contact_email = db.Column(db.String(120), nullable=True)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)


class Sale(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    order_ref = db.Column(db.String(80), nullable=False)
    customer_name = db.Column(db.String(120), nullable=False)
    order_date = db.Column(db.Date, nullable=False, index=True, default=date.today)
    channel = db.Column(db.String(80), nullable=True)
    revenue = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    cost = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    quantity = db.Column(db.Integer, nullable=False, default=1)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)
    __table_args__ = (
        db.UniqueConstraint("order_ref", "org_id", name="uq_sale_ref_org"),
    )

    @property
    def margin(self):
        return float(self.revenue or 0) - float(self.cost or 0)


class TaskHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.Integer, db.ForeignKey('task.id', ondelete='CASCADE'), nullable=False, index=True)
    field = db.Column(db.String(40), nullable=False)
    old_value = db.Column(db.Text, nullable=True)
    new_value = db.Column(db.Text, nullable=True)
    changed_at = db.Column(db.DateTime, nullable=False, default=utcnow_naive)
    changed_by = db.Column(db.String(80), nullable=True)


# ── Customisation models ────────────────────────────────────────────────────

class BoardColumn(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)
    position = db.Column(db.Integer, nullable=False, default=0)
    color = db.Column(db.String(20), nullable=True)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)
    __table_args__ = (
        db.UniqueConstraint("name", "org_id", name="uq_col_name_org"),
    )


class FieldDefinition(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    entity_type = db.Column(db.String(30), nullable=False, index=True)
    name = db.Column(db.String(60), nullable=False)
    field_key = db.Column(db.String(60), nullable=False)
    field_type = db.Column(db.String(20), nullable=False, default='text')
    options = db.Column(db.Text, nullable=True)  # JSON array for select fields
    position = db.Column(db.Integer, nullable=False, default=0)
    required = db.Column(db.Boolean, nullable=False, default=False)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)
    __table_args__ = (
        db.UniqueConstraint('entity_type', 'field_key', 'org_id', name='uq_field_entity_key_org'),
    )

    @property
    def options_list(self):
        if self.options:
            try:
                return _json.loads(self.options)
            except Exception:
                return []
        return []


class FieldValue(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    entity_type = db.Column(db.String(30), nullable=False, index=True)
    entity_id = db.Column(db.Integer, nullable=False, index=True)
    field_def_id = db.Column(
        db.Integer,
        db.ForeignKey('field_definition.id', ondelete='CASCADE'),
        nullable=False,
    )
    value = db.Column(db.Text, nullable=True)
    field_def = db.relationship('FieldDefinition')
    __table_args__ = (
        db.UniqueConstraint('entity_type', 'entity_id', 'field_def_id', name='uq_field_value'),
    )


class Workflow(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    enabled = db.Column(db.Boolean, nullable=False, default=True)
    trigger_entity = db.Column(db.String(30), nullable=False)
    trigger_event = db.Column(db.String(30), nullable=False)
    trigger_condition = db.Column(db.Text, nullable=True)  # JSON
    action_type = db.Column(db.String(30), nullable=False)
    action_config = db.Column(db.Text, nullable=True)   # JSON
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)


class WorkflowRun(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    workflow_id = db.Column(
        db.Integer,
        db.ForeignKey('workflow.id', ondelete='CASCADE'),
        nullable=False,
    )
    triggered_at = db.Column(db.DateTime, nullable=False, default=utcnow_naive)
    entity_type = db.Column(db.String(30), nullable=False)
    entity_id = db.Column(db.Integer, nullable=False)
    status = db.Column(db.String(20), nullable=False, default='ok')
    detail = db.Column(db.Text, nullable=True)
    workflow = db.relationship('Workflow')


class AlertLog(db.Model):
    """In-app alerts created by workflow automation."""
    id = db.Column(db.Integer, primary_key=True)
    severity = db.Column(db.String(20), nullable=False, default='info')  # info / warning / danger
    title = db.Column(db.String(200), nullable=False)
    detail = db.Column(db.Text, nullable=True)
    source = db.Column(db.String(100), nullable=True)  # workflow name or system
    is_read = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow_naive)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)


class DashboardWidget(db.Model):
    """User-configured dashboard widget."""
    id = db.Column(db.Integer, primary_key=True)
    widget_type = db.Column(db.String(40), nullable=False)
    title = db.Column(db.String(100), nullable=True)
    position = db.Column(db.Integer, nullable=False, default=0)
    report_id = db.Column(db.Integer, db.ForeignKey('dashboard_report.id', ondelete='CASCADE'), nullable=True)
    report = db.relationship('DashboardReport', foreign_keys=[report_id])
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow_naive)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)


class DashboardReport(db.Model):
    """Saved report definition."""
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(120), nullable=False)
    entity = db.Column(db.String(30), nullable=False)
    group_by = db.Column(db.String(40), nullable=True)
    metric = db.Column(db.String(40), nullable=False, default='count')
    chart_type = db.Column(db.String(20), nullable=False, default='bar')
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow_naive)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)


class IntegrationConfig(db.Model):
    """Key-value store for third-party integration credentials."""
    id = db.Column(db.Integer, primary_key=True)
    integration = db.Column(db.String(40), nullable=False, index=True)
    key = db.Column(db.String(80), nullable=False)
    value = db.Column(db.Text, nullable=True)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)
    __table_args__ = (
        db.UniqueConstraint('integration', 'key', 'org_id', name='uq_integration_key_org'),
    )


class SyncLog(db.Model):
    """Record of each sync operation from external integrations."""
    id = db.Column(db.Integer, primary_key=True)
    integration = db.Column(db.String(40), nullable=False, index=True)
    entity = db.Column(db.String(40), nullable=False)
    synced_count = db.Column(db.Integer, nullable=False, default=0)
    status = db.Column(db.String(20), nullable=False, default='ok')
    error = db.Column(db.Text, nullable=True)
    synced_at = db.Column(db.DateTime, nullable=False, default=utcnow_naive)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="CASCADE"), nullable=True, index=True)


class AuditLog(db.Model):
    """System-wide audit trail for all important user and system actions."""
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, nullable=False, default=utcnow_naive, index=True)
    user = db.Column(db.String(80), nullable=True, index=True)
    action = db.Column(db.String(60), nullable=False, index=True)
    module = db.Column(db.String(40), nullable=False, index=True)
    status = db.Column(db.String(20), nullable=False, default='ok')  # ok / error / warning
    message = db.Column(db.Text, nullable=True)
    related_record = db.Column(db.String(120), nullable=True)
    org_id = db.Column(db.Integer, db.ForeignKey("organization.id", ondelete="SET NULL"), nullable=True, index=True)
