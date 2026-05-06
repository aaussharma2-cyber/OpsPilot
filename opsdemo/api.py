"""
REST JSON API blueprint — consumed by the OpsPilot Android native app.
Authentication: Bearer JWT token obtained from POST /api/auth/login.
All data endpoints are org-scoped via the existing SQLAlchemy tenant guard.
"""
from __future__ import annotations

import datetime
import decimal
from functools import wraps

from flask import Blueprint, g, jsonify, request
from werkzeug.security import check_password_hash

try:
    import jwt as _pyjwt
    _JWT_OK = True
except ImportError:
    _pyjwt = None  # type: ignore
    _JWT_OK = False

from . import db
from .models import (
    AlertLog, Asset, Contact, InventoryItem, Invoice,
    Organization, Renewal, Sale, Sprint, Task, User, Vendor,
)

api_bp = Blueprint("api", __name__, url_prefix="/api")


# ── Serialisers ────────────────────────────────────────────────────────────────

def _v(val):
    if isinstance(val, (datetime.date, datetime.datetime)):
        return val.isoformat()
    if isinstance(val, decimal.Decimal):
        return float(val)
    return val


def _user(u: User) -> dict:
    org = None
    if u.org:
        org = {"id": u.org.id, "name": u.org.name, "slug": u.org.slug, "plan": u.org.plan}
    return {"id": u.id, "username": u.username, "email": u.email, "role": u.role, "org": org}


def _task(t: Task) -> dict:
    return {
        "id": t.id, "title": t.title, "description": t.description,
        "status": t.status, "priority": t.priority,
        "due_date": _v(t.due_date), "owner": t.owner, "sprint_id": t.sprint_id,
        "created_at": _v(t.created_at), "updated_at": _v(t.updated_at),
    }


def _sprint(s: Sprint) -> dict:
    return {
        "id": s.id, "name": s.name, "status": s.status,
        "start_date": _v(s.start_date), "end_date": _v(s.end_date),
        "goal": s.goal, "created_at": _v(s.created_at),
    }


def _contact(c: Contact) -> dict:
    return {
        "id": c.id, "name": c.name, "kind": c.kind, "stage": c.stage,
        "company": c.company, "email": c.email, "phone": c.phone,
        "notes": c.notes, "created_at": _v(c.created_at),
    }


def _vendor(v: Vendor) -> dict:
    return {
        "id": v.id, "name": v.name, "category": v.category,
        "contact_name": v.contact_name, "email": v.email, "phone": v.phone,
        "contract_end": _v(v.contract_end), "rating": v.rating,
        "notes": v.notes, "created_at": _v(v.created_at),
    }


def _asset(a: Asset) -> dict:
    return {
        "id": a.id, "name": a.name, "category": a.category,
        "serial_no": a.serial_no, "owner": a.owner, "status": a.status,
        "purchase_cost": _v(a.purchase_cost), "current_value": _v(a.current_value),
        "expiry": _v(a.expiry), "notes": a.notes, "created_at": _v(a.created_at),
    }


def _inventory(i: InventoryItem) -> dict:
    return {
        "id": i.id, "name": i.name, "sku": i.sku,
        "qty_on_hand": i.qty_on_hand, "reorder_level": i.reorder_level,
        "unit_cost": _v(i.unit_cost), "sale_price": _v(i.sale_price),
        "expiry": _v(i.expiry), "notes": i.notes, "created_at": _v(i.created_at),
    }


def _invoice(inv: Invoice) -> dict:
    return {
        "id": inv.id, "reference": inv.reference, "kind": inv.kind,
        "party_name": inv.party_name, "amount": _v(inv.amount),
        "due_date": _v(inv.due_date), "status": inv.status,
        "paid_on": _v(inv.paid_on), "notes": inv.notes,
        "created_at": _v(inv.created_at),
    }


def _renewal(r: Renewal) -> dict:
    return {
        "id": r.id, "title": r.title, "category": r.category,
        "provider": r.provider, "renew_on": _v(r.renew_on),
        "cost": _v(r.cost), "auto_renew": r.auto_renew,
        "contact_name": r.contact_name, "contact_email": r.contact_email,
        "notes": r.notes, "created_at": _v(r.created_at),
    }


def _sale(s: Sale) -> dict:
    return {
        "id": s.id, "order_ref": s.order_ref, "customer_name": s.customer_name,
        "order_date": _v(s.order_date), "channel": s.channel,
        "revenue": _v(s.revenue), "cost": _v(s.cost), "quantity": s.quantity,
        "notes": s.notes, "created_at": _v(s.created_at),
    }


def _alert(a: AlertLog) -> dict:
    return {
        "id": a.id, "severity": a.severity, "title": a.title,
        "detail": a.detail, "is_read": a.is_read, "created_at": _v(a.created_at),
    }


# ── JWT helpers ────────────────────────────────────────────────────────────────

def _secret() -> str:
    from flask import current_app
    return current_app.config.get("JWT_SECRET_KEY") or current_app.config["SECRET_KEY"]


def _make_token(user_id: int) -> str:
    payload = {
        "sub": user_id,
        "iat": datetime.datetime.utcnow(),
        "exp": datetime.datetime.utcnow() + datetime.timedelta(days=30),
    }
    token = _pyjwt.encode(payload, _secret(), algorithm="HS256")
    return token if isinstance(token, str) else token.decode()


def _read_token(token: str) -> int | None:
    try:
        payload = _pyjwt.decode(token, _secret(), algorithms=["HS256"])
        return int(payload["sub"])
    except Exception:
        return None


# ── Auth hooks & decorators ────────────────────────────────────────────────────

@api_bp.before_request
def _load_api_user() -> None:
    """Load g.user from Bearer JWT for every API request."""
    if not _JWT_OK:
        return
    # Session already loaded a user (web client hitting /api — fine to reuse).
    if getattr(g, "user", None) is not None:
        return
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return
    user_id = _read_token(auth[7:])
    if user_id:
        u = db.session.get(User, user_id)
        if u and u.is_active:
            g.user = u


def api_required(f):
    @wraps(f)
    def wrap(*a, **kw):
        if not getattr(g, "user", None):
            return jsonify(error="Unauthorized"), 401
        return f(*a, **kw)
    return wrap


def org_required(f):
    """Require an authenticated user that belongs to an organisation."""
    @wraps(f)
    def wrap(*a, **kw):
        user = getattr(g, "user", None)
        if not user:
            return jsonify(error="Unauthorized"), 401
        if not user.org_id:
            return jsonify(error="No organisation associated with this account"), 403
        return f(*a, **kw)
    return wrap


# ── Auth ──────────────────────────────────────────────────────────────────────

@api_bp.route("/auth/login", methods=["POST"])
def api_login():
    if not _JWT_OK:
        return jsonify(error="PyJWT not installed on server"), 503
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    if not username or not password:
        return jsonify(error="username and password are required"), 400

    u = User.query.filter_by(username=username).first()
    if not u or not u.is_active:
        return jsonify(error="Invalid credentials"), 401
    if not check_password_hash(u.password_hash, password):
        return jsonify(error="Invalid credentials"), 401
    if u.org and not u.org.is_active:
        return jsonify(error="Your organisation is suspended"), 403

    return jsonify(token=_make_token(u.id), user=_user(u))


@api_bp.route("/auth/me")
@api_required
def api_me():
    return jsonify(_user(g.user))


# ── Dashboard ─────────────────────────────────────────────────────────────────

@api_bp.route("/dashboard")
@org_required
def api_dashboard():
    today = datetime.date.today()
    open_tasks = Task.query.filter(Task.status != "Done").count()
    overdue_invoices = Invoice.query.filter(
        Invoice.status != "Paid", Invoice.due_date < today
    ).count()
    upcoming_renewals = Renewal.query.filter(
        Renewal.renew_on >= today,
        Renewal.renew_on <= today + datetime.timedelta(days=30),
    ).count()
    low_stock = InventoryItem.query.filter(
        InventoryItem.qty_on_hand <= InventoryItem.reorder_level
    ).count()
    unread_alerts = AlertLog.query.filter_by(is_read=False).count()
    return jsonify({
        "open_tasks": open_tasks,
        "overdue_invoices": overdue_invoices,
        "upcoming_renewals": upcoming_renewals,
        "low_stock_items": low_stock,
        "unread_alerts": unread_alerts,
    })


# ── Tasks ─────────────────────────────────────────────────────────────────────

@api_bp.route("/tasks", methods=["GET", "POST"])
@org_required
def api_tasks():
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        t = Task(
            title=body.get("title") or "New Task",
            description=body.get("description", ""),
            status=body.get("status", "To Do"),
            priority=body.get("priority", "Medium"),
            owner=body.get("owner", ""),
            sprint_id=body.get("sprint_id") or None,
        )
        if body.get("due_date"):
            try:
                t.due_date = datetime.date.fromisoformat(body["due_date"])
            except ValueError:
                pass
        db.session.add(t)
        db.session.commit()
        return jsonify(_task(t)), 201
    status_f = request.args.get("status")
    q = Task.query.order_by(Task.created_at.desc())
    if status_f:
        q = q.filter_by(status=status_f)
    return jsonify([_task(t) for t in q.limit(200).all()])


@api_bp.route("/tasks/<int:tid>", methods=["GET", "PUT", "DELETE"])
@org_required
def api_task(tid):
    t = Task.query.get_or_404(tid)
    if request.method == "DELETE":
        db.session.delete(t)
        db.session.commit()
        return "", 204
    if request.method == "PUT":
        body = request.get_json(silent=True) or {}
        for f in ("title", "description", "status", "priority", "owner", "sprint_id"):
            if f in body:
                setattr(t, f, body[f] or None if f == "sprint_id" else body[f])
        if "due_date" in body:
            try:
                t.due_date = datetime.date.fromisoformat(body["due_date"]) if body["due_date"] else None
            except ValueError:
                pass
        db.session.commit()
        return jsonify(_task(t))
    return jsonify(_task(t))


# ── Sprints ───────────────────────────────────────────────────────────────────

@api_bp.route("/sprints", methods=["GET", "POST"])
@org_required
def api_sprints():
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        s = Sprint(name=body.get("name") or "Sprint", status="Planning", goal=body.get("goal", ""))
        for date_f in ("start_date", "end_date"):
            if body.get(date_f):
                try:
                    setattr(s, date_f, datetime.date.fromisoformat(body[date_f]))
                except ValueError:
                    pass
        db.session.add(s)
        db.session.commit()
        return jsonify(_sprint(s)), 201
    return jsonify([_sprint(s) for s in Sprint.query.order_by(Sprint.created_at.desc()).all()])


@api_bp.route("/sprints/<int:sid>")
@org_required
def api_sprint(sid):
    s = Sprint.query.get_or_404(sid)
    result = _sprint(s)
    result["tasks"] = [_task(t) for t in Task.query.filter_by(sprint_id=sid).all()]
    return jsonify(result)


# ── CRM ───────────────────────────────────────────────────────────────────────

@api_bp.route("/crm", methods=["GET", "POST"])
@org_required
def api_crm():
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        c = Contact(
            name=body.get("name", ""), kind=body.get("kind", "lead"),
            stage=body.get("stage", "New"), company=body.get("company", ""),
            email=body.get("email", ""), phone=body.get("phone", ""),
            notes=body.get("notes", ""),
        )
        db.session.add(c)
        db.session.commit()
        return jsonify(_contact(c)), 201
    q = Contact.query.order_by(Contact.created_at.desc())
    if request.args.get("kind"):
        q = q.filter_by(kind=request.args["kind"])
    return jsonify([_contact(c) for c in q.limit(200).all()])


@api_bp.route("/crm/<int:cid>", methods=["GET", "PUT", "DELETE"])
@org_required
def api_contact(cid):
    c = Contact.query.get_or_404(cid)
    if request.method == "DELETE":
        db.session.delete(c)
        db.session.commit()
        return "", 204
    if request.method == "PUT":
        body = request.get_json(silent=True) or {}
        for f in ("name", "kind", "stage", "company", "email", "phone", "notes"):
            if f in body:
                setattr(c, f, body[f])
        db.session.commit()
        return jsonify(_contact(c))
    return jsonify(_contact(c))


# ── Vendors ───────────────────────────────────────────────────────────────────

@api_bp.route("/vendors", methods=["GET", "POST"])
@org_required
def api_vendors():
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        v = Vendor(
            name=body.get("name", ""), category=body.get("category", ""),
            contact_name=body.get("contact_name", ""), email=body.get("email", ""),
            phone=body.get("phone", ""), rating=body.get("rating"),
            notes=body.get("notes", ""),
        )
        if body.get("contract_end"):
            try:
                v.contract_end = datetime.date.fromisoformat(body["contract_end"])
            except ValueError:
                pass
        db.session.add(v)
        db.session.commit()
        return jsonify(_vendor(v)), 201
    return jsonify([_vendor(v) for v in Vendor.query.order_by(Vendor.created_at.desc()).limit(200).all()])


@api_bp.route("/vendors/<int:vid>", methods=["PUT", "DELETE"])
@org_required
def api_vendor(vid):
    v = Vendor.query.get_or_404(vid)
    if request.method == "DELETE":
        db.session.delete(v)
        db.session.commit()
        return "", 204
    body = request.get_json(silent=True) or {}
    for f in ("name", "category", "contact_name", "email", "phone", "rating", "notes"):
        if f in body:
            setattr(v, f, body[f])
    db.session.commit()
    return jsonify(_vendor(v))


# ── Assets ────────────────────────────────────────────────────────────────────

@api_bp.route("/assets", methods=["GET", "POST"])
@org_required
def api_assets():
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        a = Asset(
            name=body.get("name", ""), category=body.get("category", ""),
            serial_no=body.get("serial_no", ""), owner=body.get("owner", ""),
            status=body.get("status", "Active"),
            purchase_cost=body.get("purchase_cost"), current_value=body.get("current_value"),
            notes=body.get("notes", ""),
        )
        if body.get("expiry"):
            try:
                a.expiry = datetime.date.fromisoformat(body["expiry"])
            except ValueError:
                pass
        db.session.add(a)
        db.session.commit()
        return jsonify(_asset(a)), 201
    return jsonify([_asset(a) for a in Asset.query.order_by(Asset.created_at.desc()).limit(200).all()])


@api_bp.route("/assets/<int:aid>", methods=["PUT", "DELETE"])
@org_required
def api_asset(aid):
    a = Asset.query.get_or_404(aid)
    if request.method == "DELETE":
        db.session.delete(a)
        db.session.commit()
        return "", 204
    body = request.get_json(silent=True) or {}
    for f in ("name", "category", "serial_no", "owner", "status", "purchase_cost", "current_value", "notes"):
        if f in body:
            setattr(a, f, body[f])
    db.session.commit()
    return jsonify(_asset(a))


# ── Inventory ─────────────────────────────────────────────────────────────────

@api_bp.route("/inventory", methods=["GET", "POST"])
@org_required
def api_inventory():
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        item = InventoryItem(
            name=body.get("name", ""), sku=body.get("sku", ""),
            qty_on_hand=int(body.get("qty_on_hand", 0)),
            reorder_level=int(body.get("reorder_level", 0)),
            unit_cost=body.get("unit_cost"), sale_price=body.get("sale_price"),
            notes=body.get("notes", ""),
        )
        if body.get("expiry"):
            try:
                item.expiry = datetime.date.fromisoformat(body["expiry"])
            except ValueError:
                pass
        db.session.add(item)
        db.session.commit()
        return jsonify(_inventory(item)), 201
    return jsonify([_inventory(i) for i in InventoryItem.query.order_by(InventoryItem.qty_on_hand.asc()).limit(200).all()])


@api_bp.route("/inventory/<int:iid>/adjust", methods=["POST"])
@org_required
def api_inventory_adjust(iid):
    item = InventoryItem.query.get_or_404(iid)
    body = request.get_json(silent=True) or {}
    delta = int(body.get("delta", 0))
    item.qty_on_hand = max(0, item.qty_on_hand + delta)
    db.session.commit()
    return jsonify(_inventory(item))


@api_bp.route("/inventory/<int:iid>", methods=["DELETE"])
@org_required
def api_inventory_item(iid):
    item = InventoryItem.query.get_or_404(iid)
    db.session.delete(item)
    db.session.commit()
    return "", 204


# ── Invoices ──────────────────────────────────────────────────────────────────

@api_bp.route("/invoices", methods=["GET", "POST"])
@org_required
def api_invoices():
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        inv = Invoice(
            reference=body.get("reference", ""), kind=body.get("kind", "sales"),
            party_name=body.get("party_name", ""), amount=body.get("amount", 0),
            status=body.get("status", "Draft"), notes=body.get("notes", ""),
        )
        if body.get("due_date"):
            try:
                inv.due_date = datetime.date.fromisoformat(body["due_date"])
            except ValueError:
                pass
        db.session.add(inv)
        db.session.commit()
        return jsonify(_invoice(inv)), 201
    return jsonify([_invoice(i) for i in Invoice.query.order_by(Invoice.due_date.asc()).limit(200).all()])


@api_bp.route("/invoices/<int:inv_id>/mark_paid", methods=["POST"])
@org_required
def api_invoice_mark_paid(inv_id):
    inv = Invoice.query.get_or_404(inv_id)
    inv.status = "Paid"
    inv.paid_on = datetime.date.today()
    db.session.commit()
    return jsonify(_invoice(inv))


@api_bp.route("/invoices/<int:inv_id>", methods=["DELETE"])
@org_required
def api_invoice(inv_id):
    inv = Invoice.query.get_or_404(inv_id)
    db.session.delete(inv)
    db.session.commit()
    return "", 204


# ── Renewals ──────────────────────────────────────────────────────────────────

@api_bp.route("/renewals", methods=["GET", "POST"])
@org_required
def api_renewals():
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        r = Renewal(
            title=body.get("title", ""), category=body.get("category", ""),
            provider=body.get("provider", ""), cost=body.get("cost"),
            auto_renew=bool(body.get("auto_renew", False)),
            contact_name=body.get("contact_name", ""),
            contact_email=body.get("contact_email", ""),
            notes=body.get("notes", ""),
        )
        if body.get("renew_on"):
            try:
                r.renew_on = datetime.date.fromisoformat(body["renew_on"])
            except ValueError:
                pass
        db.session.add(r)
        db.session.commit()
        return jsonify(_renewal(r)), 201
    return jsonify([_renewal(r) for r in Renewal.query.order_by(Renewal.renew_on.asc()).limit(200).all()])


@api_bp.route("/renewals/<int:rid>", methods=["PUT", "DELETE"])
@org_required
def api_renewal(rid):
    r = Renewal.query.get_or_404(rid)
    if request.method == "DELETE":
        db.session.delete(r)
        db.session.commit()
        return "", 204
    body = request.get_json(silent=True) or {}
    for f in ("title", "category", "provider", "cost", "auto_renew", "contact_name", "contact_email", "notes"):
        if f in body:
            setattr(r, f, body[f])
    if body.get("renew_on"):
        try:
            r.renew_on = datetime.date.fromisoformat(body["renew_on"])
        except ValueError:
            pass
    db.session.commit()
    return jsonify(_renewal(r))


# ── Sales ─────────────────────────────────────────────────────────────────────

@api_bp.route("/sales", methods=["GET", "POST"])
@org_required
def api_sales():
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        s = Sale(
            order_ref=body.get("order_ref", ""), customer_name=body.get("customer_name", ""),
            channel=body.get("channel", ""), revenue=body.get("revenue", 0),
            cost=body.get("cost", 0), quantity=int(body.get("quantity", 1)),
            notes=body.get("notes", ""),
        )
        if body.get("order_date"):
            try:
                s.order_date = datetime.date.fromisoformat(body["order_date"])
            except ValueError:
                pass
        db.session.add(s)
        db.session.commit()
        return jsonify(_sale(s)), 201
    return jsonify([_sale(s) for s in Sale.query.order_by(Sale.order_date.desc()).limit(200).all()])


@api_bp.route("/sales/<int:sid>", methods=["DELETE"])
@org_required
def api_sale(sid):
    s = Sale.query.get_or_404(sid)
    db.session.delete(s)
    db.session.commit()
    return "", 204


# ── Notifications ─────────────────────────────────────────────────────────────

@api_bp.route("/notifications")
@org_required
def api_notifications():
    alerts = AlertLog.query.order_by(AlertLog.created_at.desc()).limit(100).all()
    return jsonify([_alert(a) for a in alerts])


@api_bp.route("/notifications/<int:nid>/read", methods=["POST"])
@org_required
def api_notification_read(nid):
    a = AlertLog.query.get_or_404(nid)
    a.is_read = True
    db.session.commit()
    return jsonify(_alert(a))


@api_bp.route("/notifications/read_all", methods=["POST"])
@org_required
def api_notifications_read_all():
    AlertLog.query.filter_by(is_read=False).update({"is_read": True})
    db.session.commit()
    return jsonify(ok=True)
