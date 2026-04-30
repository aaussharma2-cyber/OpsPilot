from __future__ import annotations

from flask import g, has_request_context, session


def current_org_id() -> int | None:
    """Return the current tenant org id for normal users."""
    if not has_request_context():
        return None
    role = session.get("role")
    if role == "super_admin":
        return None
    org_id = session.get("org_id")
    if org_id is not None:
        return org_id
    user = getattr(g, "user", None)
    if not user or user.is_super_admin:
        return None
    return user.org_id


def scoped_query(model, org_id: int | None = None):
    """Return a model query constrained to an organisation when possible."""
    org_id = current_org_id() if org_id is None else org_id
    query = model.query
    if org_id is not None and hasattr(model, "org_id"):
        query = query.filter(model.org_id == org_id)
    return query


def stamp_org(obj, org_id: int | None = None):
    """Attach an organisation id to a tenant-owned object."""
    org_id = current_org_id() if org_id is None else org_id
    if org_id is not None and hasattr(obj, "org_id") and getattr(obj, "org_id", None) is None:
        obj.org_id = org_id
    return obj
