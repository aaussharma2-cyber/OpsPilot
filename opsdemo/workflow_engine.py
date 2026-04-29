from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone

from .models import AlertLog, Task, WorkflowRun, Workflow, db


def _entity_display_name(entity) -> str:
    for attr in ("reference", "order_ref", "title", "name"):
        val = getattr(entity, attr, None)
        if val:
            return str(val)
    return str(getattr(entity, "id", ""))


def _interpolate(template: str, entity) -> str:
    def replace(m):
        attr = m.group(1)
        val = getattr(entity, attr, None)
        return str(val) if val is not None else ""
    return re.sub(r"\{(\w+)\}", replace, template)


def _parse_date_val(val) -> date | None:
    if isinstance(val, date):
        return val
    if isinstance(val, str) and val:
        try:
            return date.fromisoformat(val)
        except ValueError:
            pass
    return None


def _eval_condition(condition_json: str | None, entity, context: dict | None) -> bool:
    if not condition_json:
        return True
    try:
        cond = json.loads(condition_json)
        field = cond.get("field", "")
        op = cond.get("op", "eq")
        expected = str(cond.get("value", ""))
        today = date.today()

        # Pull value from context first, then from entity
        if context and field in context:
            raw_val = context[field]
        else:
            raw_val = getattr(entity, field, None)

        # Date operators
        if op in ("within_days", "older_than_days", "before", "after"):
            actual_date = _parse_date_val(raw_val)
            if actual_date is None:
                return False
            if op == "within_days":
                n = int(expected or 0)
                return today <= actual_date <= today + timedelta(days=n)
            if op == "older_than_days":
                n = int(expected or 0)
                return (today - actual_date).days >= n
            if op == "before":
                ref = _parse_date_val(expected)
                return ref is not None and actual_date < ref
            if op == "after":
                ref = _parse_date_val(expected)
                return ref is not None and actual_date > ref

        actual = str(raw_val or "")

        if op == "eq":
            return actual.lower() == expected.lower()
        if op == "neq":
            return actual.lower() != expected.lower()
        if op == "contains":
            return expected.lower() in actual.lower()
        if op == "gt":
            return float(actual or 0) > float(expected or 0)
        if op == "lt":
            return float(actual or 0) < float(expected or 0)
        if op == "gte":
            return float(actual or 0) >= float(expected or 0)
        if op == "lte":
            return float(actual or 0) <= float(expected or 0)
        return False
    except Exception:
        return False


def _execute_action(workflow: Workflow, entity, context: dict | None) -> str:
    from .services import get_task_columns

    config = json.loads(workflow.action_config or "{}")

    if workflow.action_type == "create_task":
        columns = get_task_columns()
        status = config.get("status", columns[0] if columns else "Backlog")
        if status not in columns:
            status = columns[0] if columns else "Backlog"
        title = _interpolate(config.get("title", "Task from workflow"), entity)
        task = Task(
            title=title or "Task from workflow",
            description=_interpolate(config.get("description", ""), entity) or None,
            status=status,
            priority=config.get("priority", "Medium"),
            owner=config.get("owner", "") or None,
            related_type=config.get("related_type", "") or None,
            related_name=_interpolate(config.get("related_name", ""), entity) or None,
        )
        db.session.add(task)
        db.session.flush()
        return f"Created task #{task.id}: {task.title}"

    if workflow.action_type == "update_task_status":
        columns = get_task_columns()
        new_status = config.get("status", "")
        if new_status not in columns:
            return f"Invalid status '{new_status}'"
        match_name = _interpolate(config.get("related_name", ""), entity) or _entity_display_name(entity)
        tasks = Task.query.filter_by(related_name=match_name).all()
        for t in tasks:
            t.status = new_status
        db.session.flush()
        return f"Moved {len(tasks)} task(s) linked to '{match_name}' → {new_status}"

    if workflow.action_type == "send_alert":
        title = _interpolate(config.get("title", "Workflow alert"), entity)
        detail = _interpolate(config.get("detail", ""), entity)
        severity = config.get("severity", "warning")
        alert = AlertLog(
            severity=severity,
            title=title or "Workflow alert",
            detail=detail or None,
            source=workflow.name,
        )
        db.session.add(alert)
        db.session.flush()
        return f"Alert created: {alert.title}"

    return f"Unknown action: {workflow.action_type}"


def fire_event(entity_type: str, event: str, entity, context: dict | None = None) -> None:
    """Fire after a DB operation. Never raises — workflow errors are logged, not re-raised."""
    try:
        workflows = Workflow.query.filter_by(
            enabled=True,
            trigger_entity=entity_type,
            trigger_event=event,
        ).all()

        for wf in workflows:
            if not _eval_condition(wf.trigger_condition, entity, context):
                continue
            try:
                detail = _execute_action(wf, entity, context)
                run_status = "ok"
            except Exception as exc:
                detail = str(exc)
                run_status = "error"

            db.session.add(WorkflowRun(
                workflow_id=wf.id,
                entity_type=entity_type,
                entity_id=entity.id,
                status=run_status,
                detail=detail,
            ))

        db.session.commit()
    except Exception:
        pass  # Never let workflow errors break the main request
