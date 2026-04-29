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


class User(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(30), nullable=False, default="admin")


class Sprint(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    goal = db.Column(db.Text, nullable=True)
    start_date = db.Column(db.Date, nullable=True)
    end_date = db.Column(db.Date, nullable=True)
    status = db.Column(db.String(20), nullable=False, default="Planning")  # Planning / Active / Completed
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow_naive)


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


class Contact(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    kind = db.Column(db.String(20), nullable=False, default="lead", index=True)
    name = db.Column(db.String(120), nullable=False)
    company = db.Column(db.String(120), nullable=True)
    email = db.Column(db.String(120), nullable=True)
    phone = db.Column(db.String(50), nullable=True)
    stage = db.Column(db.String(30), nullable=False, default="New")
    notes = db.Column(db.Text, nullable=True)


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


class InventoryItem(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sku = db.Column(db.String(60), nullable=False, unique=True, index=True)
    name = db.Column(db.String(120), nullable=False)
    category = db.Column(db.String(80), nullable=True)
    warehouse = db.Column(db.String(80), nullable=False, default="Main")
    qty_on_hand = db.Column(db.Integer, nullable=False, default=0)
    reorder_level = db.Column(db.Integer, nullable=False, default=0)
    unit_cost = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    sale_price = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    expiry_date = db.Column(db.Date, nullable=True, index=True)
    notes = db.Column(db.Text, nullable=True)


class Invoice(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    kind = db.Column(db.String(20), nullable=False, default="sales", index=True)
    party_name = db.Column(db.String(120), nullable=False)
    reference = db.Column(db.String(80), nullable=False, unique=True)
    amount = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    due_date = db.Column(db.Date, nullable=False, index=True)
    status = db.Column(db.String(20), nullable=False, default="Unpaid", index=True)
    paid_on = db.Column(db.Date, nullable=True)
    notes = db.Column(db.Text, nullable=True)


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


class Sale(TimestampMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    order_ref = db.Column(db.String(80), nullable=False, unique=True)
    customer_name = db.Column(db.String(120), nullable=False)
    order_date = db.Column(db.Date, nullable=False, index=True, default=date.today)
    channel = db.Column(db.String(80), nullable=True)
    revenue = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    cost = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    quantity = db.Column(db.Integer, nullable=False, default=1)

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


# ── Customisation models ────────────────────────────────────────────────────

class BoardColumn(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False, unique=True)
    position = db.Column(db.Integer, nullable=False, default=0)
    color = db.Column(db.String(20), nullable=True)


class FieldDefinition(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    entity_type = db.Column(db.String(30), nullable=False, index=True)
    name = db.Column(db.String(60), nullable=False)
    field_key = db.Column(db.String(60), nullable=False)
    field_type = db.Column(db.String(20), nullable=False, default='text')
    options = db.Column(db.Text, nullable=True)  # JSON array for select fields
    position = db.Column(db.Integer, nullable=False, default=0)
    required = db.Column(db.Boolean, nullable=False, default=False)
    __table_args__ = (
        db.UniqueConstraint('entity_type', 'field_key', name='uq_field_entity_key'),
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


class DashboardWidget(db.Model):
    """User-configured dashboard widget."""
    id = db.Column(db.Integer, primary_key=True)
    widget_type = db.Column(db.String(40), nullable=False)
    title = db.Column(db.String(100), nullable=True)
    position = db.Column(db.Integer, nullable=False, default=0)
    report_id = db.Column(db.Integer, db.ForeignKey('dashboard_report.id', ondelete='CASCADE'), nullable=True)
    report = db.relationship('DashboardReport', foreign_keys=[report_id])
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow_naive)


class DashboardReport(db.Model):
    """Saved report definition."""
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(120), nullable=False)
    entity = db.Column(db.String(30), nullable=False)
    group_by = db.Column(db.String(40), nullable=True)
    metric = db.Column(db.String(40), nullable=False, default='count')
    chart_type = db.Column(db.String(20), nullable=False, default='bar')
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow_naive)


class IntegrationConfig(db.Model):
    """Key-value store for third-party integration credentials."""
    id = db.Column(db.Integer, primary_key=True)
    integration = db.Column(db.String(40), nullable=False, index=True)
    key = db.Column(db.String(80), nullable=False)
    value = db.Column(db.Text, nullable=True)
    __table_args__ = (
        db.UniqueConstraint('integration', 'key', name='uq_integration_key'),
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
