from __future__ import annotations

import csv
import io
import json
import secrets
from datetime import date, datetime, timedelta, timezone

from flask import (
    Blueprint,
    Response,
    current_app,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash

from . import admin_required, login_required, verify_csrf
from werkzeug.security import generate_password_hash

from .models import (
    AlertLog, AuditLog, Asset, BoardColumn, Contact, DashboardReport, DashboardWidget,
    FieldDefinition, FieldValue, IntegrationConfig, InventoryItem, Invoice,
    Renewal, Sale, Sprint, SyncLog, Task, TaskHistory, User, Vendor,
    Workflow, WorkflowRun, db,
)
from .services import (
    CONDITION_OPS,
    CONTACT_KINDS,
    DEFAULT_WIDGET_TYPES,
    ENTITY_FIELDS,
    ENTITY_LABELS,
    ENTITY_TYPES,
    FIELD_TYPES,
    INVOICE_KINDS,
    PRIORITIES,
    REPORT_ENTITIES,
    SPRINT_STATUSES,
    WIDGET_TYPES,
    WORKFLOW_ACTIONS,
    WORKFLOW_TRIGGER_EVENTS,
    ValidationError,
    create_invoice_from_renewal,
    dashboard_snapshot,
    get_field_defs,
    get_field_values_map,
    get_integration_config,
    get_report_data,
    get_smtp_config,
    get_sync_logs,
    get_task_columns,
    log_audit,
    normalize_shopify_domain,
    parse_date,
    parse_decimal,
    parse_int,
    save_field_values,
    seed_demo_data,
    send_invoice_email,
    send_test_email,
    test_api_connection,
    set_integration_config,
    slugify,
    sync_shopify_customers,
    sync_shopify_orders,
    test_shopify_connection,
    validate_shopify_domain,
)
from .workflow_engine import fire_event

bp = Blueprint("main", __name__)


# ── Error handlers ───────────────────────────────────────────────────────────

@bp.app_errorhandler(400)
def handle_bad_request(error):
    return render_template("error.html", code=400, message=str(error)), 400


@bp.app_errorhandler(403)
def handle_forbidden(error):
    return render_template("error.html", code=403, message="Forbidden"), 403


@bp.app_errorhandler(404)
def handle_not_found(error):
    return render_template("error.html", code=404, message="Not found"), 404


@bp.app_errorhandler(ValidationError)
def handle_validation_error(error):
    return render_template("error.html", code=400, message=str(error)), 400


# ── Auth ─────────────────────────────────────────────────────────────────────

@bp.route("/")
def index():
    if g.get("user"):
        return redirect(url_for("main.dashboard"))
    return redirect(url_for("main.login"))


@bp.route("/login", methods=["GET", "POST"])
def login():
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    lock_until_raw = session.get("login_lock_until")
    lock_until = datetime.fromisoformat(lock_until_raw) if lock_until_raw else None
    if request.method == "POST":
        verify_csrf()
        if lock_until and lock_until > now:
            flash(f"Too many failed attempts. Try again after {lock_until.strftime('%H:%M:%S')} UTC.", "danger")
            return render_template("login.html")

        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = User.query.filter_by(username=username).first()
        if user and check_password_hash(user.password_hash, password):
            session.clear()
            session.permanent = True
            session["user_id"] = user.id
            session["csrf_token"] = secrets.token_hex(16)
            g.user = user
            log_audit("login", "auth", status="ok", message=f"User '{username}' logged in")
            flash("Welcome back.", "success")
            return redirect(url_for("main.dashboard"))

        failures = int(session.get("login_failures", 0)) + 1
        session["login_failures"] = failures
        if failures >= 5:
            lock_until = now + timedelta(minutes=5)
            session["login_lock_until"] = lock_until.isoformat()
            log_audit("login_failed", "auth", status="error",
                      message=f"Login locked after 5 failures for username '{username}'")
            flash("Too many failed attempts. Login locked for 5 minutes.", "danger")
        else:
            log_audit("login_failed", "auth", status="error",
                      message=f"Failed login attempt for username '{username}'")
            flash("Invalid username or password.", "danger")
    return render_template("login.html")


@bp.route("/logout", methods=["POST"])
@login_required
def logout():
    verify_csrf()
    log_audit("logout", "auth", status="ok", message=f"User '{g.user.username}' logged out")
    session.clear()
    flash("Logged out.", "success")
    return redirect(url_for("main.login"))


@bp.route("/seed", methods=["POST"])
@admin_required
def seed():
    verify_csrf()
    force = request.form.get("force") == "1"
    try:
        seed_demo_data(force=force)
        flash("Demo data loaded.", "success")
    except Exception as exc:
        flash(str(exc), "warning")
    return redirect(url_for("main.dashboard"))


# ── Dashboard ────────────────────────────────────────────────────────────────

@bp.route("/dashboard")
@login_required
def dashboard():
    widgets = DashboardWidget.query.order_by(DashboardWidget.position.asc()).all()
    if not widgets:
        defaults = [
            DashboardWidget(widget_type=wt, position=i)
            for i, wt in enumerate(DEFAULT_WIDGET_TYPES)
        ]
        db.session.add_all(defaults)
        db.session.commit()
        widgets = DashboardWidget.query.order_by(DashboardWidget.position.asc()).all()

    snapshot = dashboard_snapshot()
    widget_types_present = {w.widget_type for w in widgets}

    extra: dict = {}
    if "invoices_table" in widget_types_present:
        extra["recent_invoices"] = (
            Invoice.query.filter(Invoice.status != "Paid")
            .order_by(Invoice.due_date.asc()).limit(8).all()
        )
    if "tasks_table" in widget_types_present:
        extra["open_tasks"] = (
            Task.query.filter(Task.status != "Done")
            .order_by(Task.due_date.asc()).limit(10).all()
        )
    if "contacts_table" in widget_types_present:
        extra["recent_contacts"] = (
            Contact.query.order_by(Contact.created_at.desc()).limit(8).all()
        )

    # Fetch report data for any pinned-report widgets
    report_widget_data: dict[int, list] = {}
    for w in widgets:
        if w.widget_type == "saved_report" and w.report_id:
            report = db.session.get(DashboardReport, w.report_id)
            if report:
                report_widget_data[w.id] = get_report_data(report)

    metric_widgets = [w for w in widgets if w.widget_type.startswith("metric_")]
    panel_widgets = [w for w in widgets if not w.widget_type.startswith("metric_")]

    return render_template(
        "dashboard.html",
        snapshot=snapshot,
        metric_widgets=metric_widgets,
        panel_widgets=panel_widgets,
        extra=extra,
        report_widget_data=report_widget_data,
        WIDGET_TYPES=WIDGET_TYPES,
        today=date.today(),
    )


@bp.route("/dashboard/widgets/add", methods=["POST"])
@login_required
def widget_add():
    verify_csrf()
    widget_type = request.form.get("widget_type", "").strip()
    report_id_raw = request.form.get("report_id", "").strip()
    if widget_type not in WIDGET_TYPES:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "error": "Unknown widget type"}), 400
        flash("Unknown widget type.", "danger")
        return redirect(url_for("main.dashboard"))
    last = DashboardWidget.query.order_by(DashboardWidget.position.desc()).first()
    position = (last.position + 1) if last else 0
    report_id = int(report_id_raw) if report_id_raw.isdigit() else None
    w = DashboardWidget(widget_type=widget_type, position=position, report_id=report_id)
    db.session.add(w)
    db.session.commit()
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True, "id": w.id})
    return redirect(url_for("main.dashboard"))


@bp.route("/dashboard/widgets/<int:wid>/delete", methods=["POST"])
@login_required
def widget_delete(wid):
    verify_csrf()
    w = db.session.get(DashboardWidget, wid)
    if not w:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "error": "Not found"}), 404
        return redirect(url_for("main.dashboard"))
    db.session.delete(w)
    db.session.commit()
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True})
    return redirect(url_for("main.dashboard"))


@bp.route("/dashboard/widgets/reorder", methods=["POST"])
@login_required
def widget_reorder():
    verify_csrf()
    ids = request.json.get("ids", [])
    for position, wid in enumerate(ids):
        DashboardWidget.query.filter_by(id=int(wid)).update({"position": position})
    db.session.commit()
    return jsonify({"ok": True})


# ── Reports ──────────────────────────────────────────────────────────────────

@bp.route("/reports")
@login_required
def reports():
    all_reports = DashboardReport.query.order_by(DashboardReport.created_at.desc()).all()
    return render_template(
        "reports.html",
        title="Reports",
        reports=all_reports,
        REPORT_ENTITIES=REPORT_ENTITIES,
    )


@bp.route("/reports/create", methods=["POST"])
@login_required
def report_create():
    verify_csrf()
    title = request.form.get("title", "").strip()
    entity = request.form.get("entity", "").strip()
    group_by = request.form.get("group_by", "").strip()
    metric = request.form.get("metric", "count").strip()
    chart_type = request.form.get("chart_type", "bar").strip()
    if not title:
        flash("Report title is required.", "danger")
        return redirect(url_for("main.reports"))
    if entity not in REPORT_ENTITIES:
        flash("Invalid data source.", "danger")
        return redirect(url_for("main.reports"))
    report = DashboardReport(title=title, entity=entity, group_by=group_by, metric=metric, chart_type=chart_type)
    db.session.add(report)
    db.session.commit()
    return redirect(url_for("main.report_detail", rid=report.id))


@bp.route("/reports/<int:rid>")
@login_required
def report_detail(rid):
    report = db.session.get(DashboardReport, rid)
    if not report:
        return render_template("error.html", code=404, message="Report not found"), 404
    data = get_report_data(report)
    entity_meta = REPORT_ENTITIES.get(report.entity, {})
    metric_label = next((lbl for key, lbl in entity_meta.get("metrics", []) if key == report.metric), report.metric)
    group_label = next((lbl for key, lbl in entity_meta.get("groups", []) if key == report.group_by), report.group_by)
    return render_template(
        "report_detail.html",
        title=report.title,
        report=report,
        data=data,
        metric_label=metric_label,
        group_label=group_label,
        REPORT_ENTITIES=REPORT_ENTITIES,
    )


@bp.route("/reports/<int:rid>/delete", methods=["POST"])
@login_required
def report_delete(rid):
    verify_csrf()
    report = db.session.get(DashboardReport, rid)
    if report:
        db.session.delete(report)
        db.session.commit()
        flash("Report deleted.", "success")
    return redirect(url_for("main.reports"))


@bp.route("/reports/<int:rid>/export.csv")
@login_required
def report_export(rid):
    report = db.session.get(DashboardReport, rid)
    if not report:
        return redirect(url_for("main.reports"))
    data = get_report_data(report)
    entity_meta = REPORT_ENTITIES.get(report.entity, {})
    group_label = next((lbl for key, lbl in entity_meta.get("groups", []) if key == report.group_by), report.group_by or "Group")
    metric_label = next((lbl for key, lbl in entity_meta.get("metrics", []) if key == report.metric), report.metric)
    total = sum(r["value"] for r in data)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([group_label, metric_label, "% of total"])
    for row in data:
        pct = f"{(row['value'] / total * 100):.1f}%" if total else "0%"
        w.writerow([row["label"], round(row["value"], 2), pct])
    w.writerow(["Total", round(total, 2), "100%"])

    filename = report.title.replace(" ", "_").lower() + ".csv"
    log_audit("export", "reports", related_record=report.title,
              message=f"Exported report '{report.title}' as CSV")
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@bp.route("/reports/<int:rid>/export.pdf")
@login_required
def report_export_pdf(rid):
    report = db.session.get(DashboardReport, rid)
    if not report:
        flash("Report not found.", "danger")
        return redirect(url_for("main.reports"))
    try:
        _pdf_rl()
    except ImportError:
        flash("PDF export requires reportlab. Run: pip install reportlab", "danger")
        return redirect(url_for("main.report_detail", rid=rid))

    data = get_report_data(report)
    entity_meta = REPORT_ENTITIES.get(report.entity, {})
    group_label = next((l for k, l in entity_meta.get("groups", []) if k == report.group_by), report.group_by or "Group")
    metric_label = next((l for k, l in entity_meta.get("metrics", []) if k == report.metric), report.metric)
    total = sum(r["value"] for r in data)
    is_money = report.metric in ("sum_revenue", "sum_cost", "sum_margin", "sum_amount", "sum_value")

    def _fmt(v):
        return f"${v:,.2f}" if is_money else f"{v:,.0f}" if v == int(v) else f"{v:,.2f}"

    rows = [[r["label"], _fmt(r["value"]),
             f"{(r['value'] / total * 100):.1f}%" if total else "0%"] for r in data]
    totals = ["Total", _fmt(total), "100%"]

    buf = _build_table_pdf(
        title=report.title,
        headers=[group_label, metric_label, "% of Total"],
        rows=rows,
        totals=totals,
        subtitle=f"{entity_meta.get('label', report.entity)}  ·  {len(data)} rows",
    )
    log_audit("export_pdf", "reports", related_record=report.title,
              message=f"Exported report '{report.title}' as PDF")
    return _pdf_dl(buf, report.title.replace(" ", "_").lower() + ".pdf")


@bp.route("/reports/<int:rid>/pin", methods=["POST"])
@login_required
def report_pin(rid):
    verify_csrf()
    report = db.session.get(DashboardReport, rid)
    if not report:
        flash("Report not found.", "danger")
        return redirect(url_for("main.reports"))
    # Check if already pinned
    existing = DashboardWidget.query.filter_by(widget_type="saved_report", report_id=rid).first()
    if existing:
        flash(f"'{report.title}' is already pinned to the dashboard.", "warning")
        return redirect(url_for("main.report_detail", rid=rid))
    last = DashboardWidget.query.order_by(DashboardWidget.position.desc()).first()
    position = (last.position + 1) if last else 0
    db.session.add(DashboardWidget(
        widget_type="saved_report",
        title=report.title,
        position=position,
        report_id=rid,
    ))
    db.session.commit()
    flash(f"'{report.title}' pinned to dashboard.", "success")
    return redirect(url_for("main.dashboard"))


# ── Settings — Integrations ───────────────────────────────────────────────────

@bp.route("/settings/integrations")
@admin_required
def settings_integrations():
    shopify_cfg = get_integration_config("shopify")
    shopify_connected = bool(shopify_cfg.get("shop_domain") and shopify_cfg.get("access_token"))
    shopify_setup_required = not bool(shopify_cfg.get("shop_domain"))
    recent_logs = SyncLog.query.order_by(SyncLog.synced_at.desc()).limit(10).all()
    return render_template(
        "integrations.html",
        title="Integrations",
        shopify_connected=shopify_connected,
        shopify_setup_required=shopify_setup_required,
        recent_logs=recent_logs,
    )


@bp.route("/settings/integrations/shopify", methods=["GET", "POST"])
@admin_required
def settings_integrations_shopify():
    cfg = get_integration_config("shopify")
    if request.method == "POST":
        verify_csrf()
        action = request.form.get("action", "save")
        if action == "save":
            shop_domain = normalize_shopify_domain(request.form.get("shop_domain", ""))
            access_token = request.form.get("access_token", "").strip()
            has_saved_token = bool(cfg.get("access_token"))
            if not shop_domain or (not access_token and not has_saved_token):
                flash("Shop domain and access token are required.", "danger")
            elif not validate_shopify_domain(shop_domain):
                flash("Invalid shop domain — must end in .myshopify.com (e.g. mystore.myshopify.com).", "danger")
            else:
                data = {"shop_domain": shop_domain}
                if access_token:
                    data["access_token"] = access_token
                set_integration_config("shopify", data)
                flash("Shopify credentials saved.", "success")
                cfg = get_integration_config("shopify")
        elif action == "disconnect":
            IntegrationConfig.query.filter_by(integration="shopify").delete()
            db.session.commit()
            flash("Shopify credentials removed.", "success")
            return redirect(url_for("main.settings_integrations_shopify"))
        elif action == "sync_customers":
            shop_domain = cfg.get("shop_domain", "")
            access_token = cfg.get("access_token", "")
            if not shop_domain or not access_token:
                flash("Configure Shopify credentials first.", "danger")
            else:
                log_audit("shopify_sync_attempt", "integrations", related_record=shop_domain,
                          message=f"Shopify customer sync started for {shop_domain}")
                result = sync_shopify_customers(shop_domain, access_token)
                if result["status"] == "ok":
                    log_audit("shopify_sync", "integrations", status="ok", related_record=shop_domain,
                              message=f"Shopify customer sync: {result['created']} created, {result['updated']} updated")
                    flash(f"Synced customers: {result['created']} created, {result['updated']} updated.", "success")
                else:
                    log_audit("shopify_sync", "integrations", status="error", related_record=shop_domain,
                              message=f"Shopify customer sync failed: {result['error']}")
                    flash(f"Sync failed: {result['error']}", "danger")
        elif action == "sync_orders":
            shop_domain = cfg.get("shop_domain", "")
            access_token = cfg.get("access_token", "")
            if not shop_domain or not access_token:
                flash("Configure Shopify credentials first.", "danger")
            else:
                log_audit("shopify_sync_attempt", "integrations", related_record=shop_domain,
                          message=f"Shopify order sync started for {shop_domain}")
                result = sync_shopify_orders(shop_domain, access_token)
                if result["status"] == "ok":
                    log_audit("shopify_sync", "integrations", status="ok", related_record=shop_domain,
                              message=f"Shopify order sync: {result['created_sales']} sales created, "
                                       f"{result['updated_sales']} updated, {result['created_invoices']} invoices created")
                    flash(
                        f"Synced orders: {result['created_sales']} sales created, "
                        f"{result['updated_sales']} updated, {result['created_invoices']} invoices created.",
                        "success",
                    )
                else:
                    log_audit("shopify_sync", "integrations", status="error", related_record=shop_domain,
                              message=f"Shopify order sync failed: {result['error']}")
                    flash(f"Sync failed: {result['error']}", "danger")
        return redirect(url_for("main.settings_integrations_shopify"))

    logs = get_sync_logs("shopify")
    public_cfg = dict(cfg)
    public_cfg.pop("access_token", None)
    return render_template(
        "integrations_shopify.html",
        title="Shopify Integration",
        cfg=public_cfg,
        token_saved=bool(cfg.get("access_token")),
        api_version=current_app.config.get("SHOPIFY_API_VERSION"),
        public_base_url=current_app.config.get("PUBLIC_BASE_URL", ""),
        logs=logs,
    )


@bp.route("/settings/integrations/shopify/test", methods=["POST"])
@admin_required
def settings_integrations_shopify_test():
    verify_csrf()
    cfg = get_integration_config("shopify")
    shop_domain = cfg.get("shop_domain", "")
    access_token = cfg.get("access_token", "")
    if not shop_domain or not access_token:
        return jsonify({"ok": False, "error": "Credentials not saved yet."})
    result = test_shopify_connection(shop_domain, access_token)
    if result.get("ok"):
        log_audit("shopify_test", "integrations", status="ok", related_record=shop_domain,
                  message=f"Shopify connection test succeeded for {shop_domain}")
    else:
        log_audit("shopify_test", "integrations", status="error", related_record=shop_domain,
                  message=f"Shopify connection test failed for {shop_domain}: {result.get('error', 'unknown')}")
    return jsonify(result)


# ── Settings — Theme ──────────────────────────────────────────────────────────

THEMES = {
    "dark":  {"label": "Dark",  "description": "Deep navy backgrounds — easy on the eyes at night."},
    "light": {"label": "Light", "description": "Clean white UI for bright environments."},
    "ocean": {"label": "Ocean", "description": "Rich blue tones with teal accents."},
    "warm":  {"label": "Warm",  "description": "Amber and slate — a warmer dark palette."},
}


@bp.route("/settings/theme", methods=["GET", "POST"])
@admin_required
def settings_theme():
    current_theme = get_integration_config("ui").get("theme", "dark")
    if request.method == "POST":
        verify_csrf()
        theme = request.form.get("theme", "dark")
        if theme not in THEMES:
            theme = "dark"
        set_integration_config("ui", {"theme": theme})
        flash(f"Theme changed to {THEMES[theme]['label']}.", "success")
        return redirect(url_for("main.settings_theme"))
    return render_template(
        "settings_theme.html",
        title="Theme",
        current_theme=current_theme,
        THEMES=THEMES,
    )


# ── Tasks / Kanban ───────────────────────────────────────────────────────────

@bp.route("/tasks", methods=["GET", "POST"])
@login_required
def tasks():
    columns = get_task_columns()
    field_defs = get_field_defs("task")
    if request.method == "POST":
        verify_csrf()
        title = request.form.get("title", "").strip()
        if not title:
            flash("Task title is required.", "danger")
            return redirect(url_for("main.tasks"))
        status = request.form.get("status", "Backlog")
        priority = request.form.get("priority", "Medium")
        if status not in columns:
            status = columns[0] if columns else "Backlog"
        if priority not in PRIORITIES:
            priority = "Medium"
        sprint_id_raw = request.form.get("sprint_id_new", "").strip()
        sprint_id = int(sprint_id_raw) if sprint_id_raw.isdigit() else None
        task = Task(
            title=title,
            description=request.form.get("description", "").strip() or None,
            status=status,
            priority=priority,
            due_date=parse_date(request.form.get("due_date")),
            owner=request.form.get("owner", "").strip() or None,
            related_type=request.form.get("related_type", "").strip() or None,
            related_name=request.form.get("related_name", "").strip() or None,
            sprint_id=sprint_id,
        )
        db.session.add(task)
        db.session.flush()
        save_field_values("task", task.id, field_defs, request.form)
        db.session.add(TaskHistory(task_id=task.id, field="created", new_value=task.title))
        db.session.commit()
        fire_event("task", "created", task)
        flash("Task created.", "success")
        return redirect(url_for("main.tasks"))

    today = date.today()
    active_sprint = Sprint.query.filter_by(status="Active").order_by(Sprint.id.desc()).first()
    planning_sprints = Sprint.query.filter_by(status="Planning").order_by(Sprint.id.desc()).all()
    all_sprints = Sprint.query.filter(Sprint.status != "Completed").order_by(Sprint.id.desc()).all()
    sprint_tasks = []

    if active_sprint:
        sprint_tasks = Task.query.filter_by(sprint_id=active_sprint.id).order_by(
            Task.due_date.is_(None), Task.due_date.asc(), Task.created_at.desc()
        ).all()
        grouped = {col: [] for col in columns}
        for t in sprint_tasks:
            col_key = t.status if t.status in grouped else columns[0]
            grouped[col_key].append(t)
    else:
        grouped = {}

    # Backlog: tasks with no sprint assigned (exclude Done/Completed status)
    done_cols = {"Done", "Completed"}
    backlog_tasks = Task.query.filter(
        Task.sprint_id.is_(None),
        ~Task.status.in_(done_cols),
    ).order_by(Task.due_date.is_(None), Task.due_date.asc(), Task.created_at.desc()).all()

    # Completed tasks (Done status, no sprint or any sprint)
    done_tasks = Task.query.filter(Task.status.in_(done_cols)).order_by(Task.created_at.desc()).limit(50).all()

    all_shown = sprint_tasks + backlog_tasks + done_tasks
    fv_map = get_field_values_map("task", [t.id for t in all_shown])
    return render_template(
        "tasks.html",
        grouped=grouped,
        task_columns=columns,
        priorities=PRIORITIES,
        today=today,
        field_defs=field_defs,
        fv_map=fv_map,
        active_sprint=active_sprint,
        planning_sprints=planning_sprints,
        all_sprints=all_sprints,
        backlog_tasks=backlog_tasks,
        done_tasks=done_tasks,
        sprint_tasks=sprint_tasks,
    )


@bp.route("/tasks/<int:task_id>/move", methods=["POST"])
@login_required
def task_move(task_id: int):
    verify_csrf()
    task = db.session.get(Task, task_id)
    if not task:
        return jsonify({"ok": False, "error": "Task not found"}), 404
    payload = request.get_json(silent=True) or request.form
    status = (payload.get("status") or "").strip()
    columns = get_task_columns()
    if status not in columns:
        return jsonify({"ok": False, "error": "Invalid status"}), 400
    old_status = task.status
    task.status = status
    if old_status != status:
        db.session.add(TaskHistory(task_id=task.id, field="status", old_value=old_status, new_value=status))
    db.session.commit()
    if old_status != status:
        fire_event("task", "status_changed", task, context={"new_status": status})
    return jsonify({"ok": True, "status": task.status})


@bp.route("/tasks/<int:task_id>/panel", methods=["GET"])
@login_required
def task_panel(task_id: int):
    task = db.session.get(Task, task_id)
    if not task:
        return "Not found", 404
    columns = get_task_columns()
    field_defs = get_field_defs("task")
    fv_map = get_field_values_map("task", [task_id])
    task_vals = fv_map.get(task_id, {})
    history = (
        TaskHistory.query
        .filter_by(task_id=task_id)
        .order_by(TaskHistory.changed_at.desc())
        .limit(30)
        .all()
    )
    return render_template(
        "_task_panel.html",
        task=task,
        task_columns=columns,
        priorities=PRIORITIES,
        field_defs=field_defs,
        task_vals=task_vals,
        history=history,
        today=date.today(),
    )


@bp.route("/tasks/<int:task_id>/edit", methods=["POST"])
@login_required
def task_edit(task_id: int):
    verify_csrf()
    task = db.session.get(Task, task_id)
    if not task:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "error": "Task not found"}), 404
        flash("Task not found.", "danger")
        return redirect(url_for("main.tasks"))
    title = request.form.get("title", "").strip()
    if not title:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "error": "Title required"}), 400
        flash("Task title is required.", "danger")
        return redirect(url_for("main.tasks"))
    columns = get_task_columns()
    status = request.form.get("status", task.status)
    priority = request.form.get("priority", task.priority)
    if status not in columns:
        status = task.status
    if priority not in PRIORITIES:
        priority = task.priority

    # Record history for changed fields
    watch = [
        ("title",       task.title,       title),
        ("status",      task.status,      status),
        ("priority",    task.priority,    priority),
        ("due_date",    str(task.due_date or ""),  request.form.get("due_date", "") or ""),
        ("owner",       task.owner or "", request.form.get("owner", "").strip()),
        ("description", task.description or "", request.form.get("description", "").strip()),
    ]
    for field, old, new in watch:
        if str(old) != str(new):
            db.session.add(TaskHistory(task_id=task_id, field=field,
                                       old_value=old or None, new_value=new or None))

    old_status = task.status
    task.title = title
    task.description = request.form.get("description", "").strip() or None
    task.status = status
    task.priority = priority
    task.due_date = parse_date(request.form.get("due_date"))
    task.owner = request.form.get("owner", "").strip() or None
    task.related_type = request.form.get("related_type", "").strip() or None
    task.related_name = request.form.get("related_name", "").strip() or None
    field_defs = get_field_defs("task")
    save_field_values("task", task.id, field_defs, request.form)
    db.session.commit()
    if old_status != task.status:
        fire_event("task", "status_changed", task, context={"new_status": task.status})

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({
            "ok": True,
            "task": {
                "id": task.id,
                "title": task.title,
                "status": task.status,
                "priority": task.priority,
                "due_date": str(task.due_date) if task.due_date else None,
                "owner": task.owner,
                "description": task.description,
            },
        })
    flash("Task updated.", "success")
    return redirect(url_for("main.tasks"))


@bp.route("/tasks/<int:task_id>/delete", methods=["POST"])
@login_required
def task_delete(task_id: int):
    verify_csrf()
    task = db.session.get(Task, task_id)
    if task:
        db.session.delete(task)
        db.session.commit()
        flash("Task deleted.", "success")
    return redirect(url_for("main.tasks"))


# ── Sprints ───────────────────────────────────────────────────────────────────

@bp.route("/sprints/create", methods=["POST"])
@login_required
def sprint_create():
    verify_csrf()
    name = request.form.get("name", "").strip()
    if not name:
        flash("Sprint name is required.", "danger")
        return redirect(url_for("main.tasks"))
    sprint = Sprint(
        name=name,
        goal=request.form.get("goal", "").strip() or None,
        start_date=parse_date(request.form.get("start_date")),
        end_date=parse_date(request.form.get("end_date")),
        status="Planning",
    )
    db.session.add(sprint)
    db.session.commit()
    flash(f"Sprint '{sprint.name}' created.", "success")
    return redirect(url_for("main.tasks"))


@bp.route("/sprints/<int:sprint_id>/start", methods=["POST"])
@login_required
def sprint_start(sprint_id: int):
    verify_csrf()
    sprint = db.session.get(Sprint, sprint_id)
    if not sprint or sprint.status != "Planning":
        flash("Sprint not found or cannot be started.", "danger")
        return redirect(url_for("main.tasks"))
    # Only one active sprint at a time
    if Sprint.query.filter_by(status="Active").first():
        flash("There is already an active sprint. Complete it first.", "warning")
        return redirect(url_for("main.tasks"))
    sprint.status = "Active"
    if not sprint.start_date:
        sprint.start_date = date.today()
    db.session.commit()
    flash(f"Sprint '{sprint.name}' is now active.", "success")
    return redirect(url_for("main.tasks"))


@bp.route("/sprints/<int:sprint_id>/complete", methods=["POST"])
@login_required
def sprint_complete(sprint_id: int):
    verify_csrf()
    sprint = db.session.get(Sprint, sprint_id)
    if not sprint or sprint.status != "Active":
        flash("Sprint not found or not active.", "danger")
        return redirect(url_for("main.tasks"))
    action = request.form.get("incomplete_action", "backlog")
    sprint.status = "Completed"
    if not sprint.end_date:
        sprint.end_date = date.today()
    # Handle unfinished tasks
    unfinished = Task.query.filter(
        Task.sprint_id == sprint_id,
        ~Task.status.in_(["Done", "Completed"]),
    ).all()
    for t in unfinished:
        if action == "backlog":
            t.sprint_id = None
        # else keep them in the completed sprint for review
    db.session.commit()
    done_count = Task.query.filter(
        Task.sprint_id == sprint_id,
        Task.status.in_(["Done", "Completed"]),
    ).count()
    flash(f"Sprint '{sprint.name}' completed! {done_count} tasks done, {len(unfinished)} moved to backlog.", "success")
    return redirect(url_for("main.tasks"))


@bp.route("/tasks/<int:task_id>/sprint", methods=["POST"])
@login_required
def task_set_sprint(task_id: int):
    verify_csrf()
    task = db.session.get(Task, task_id)
    if not task:
        return jsonify({"ok": False, "error": "Task not found"}), 404
    sprint_id_raw = request.form.get("sprint_id", "").strip()
    if not sprint_id_raw:
        task.sprint_id = None
    else:
        try:
            task.sprint_id = int(sprint_id_raw)
        except (ValueError, TypeError):
            task.sprint_id = None
    db.session.commit()
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True})
    return redirect(url_for("main.tasks"))


@bp.route("/tasks/bulk_sprint", methods=["POST"])
@login_required
def tasks_bulk_sprint():
    verify_csrf()
    task_ids = request.form.getlist("task_ids")
    sprint_id_raw = request.form.get("sprint_id", "").strip()
    sprint_id = int(sprint_id_raw) if sprint_id_raw.isdigit() else None
    updated = 0
    for tid in task_ids:
        try:
            task = db.session.get(Task, int(tid))
            if task:
                task.sprint_id = sprint_id
                updated += 1
        except (ValueError, TypeError):
            pass
    db.session.commit()
    flash(f"{updated} task{'s' if updated != 1 else ''} assigned to sprint.", "success")
    return redirect(url_for("main.tasks"))


# ── Sprint management page ────────────────────────────────────────────────────

@bp.route("/sprints")
@login_required
def sprint_list():
    today = date.today()
    all_sprints = Sprint.query.order_by(Sprint.status.asc(), Sprint.id.desc()).all()
    # Build stats per sprint
    sprint_stats = {}
    for s in all_sprints:
        tasks = Task.query.filter_by(sprint_id=s.id).all()
        total = len(tasks)
        done = sum(1 for t in tasks if t.status in ("Done", "Completed"))
        sprint_stats[s.id] = {
            "total": total,
            "done": done,
            "pct": round(done / total * 100) if total else 0,
            "by_status": {},
        }
        for t in tasks:
            sprint_stats[s.id]["by_status"][t.status] = sprint_stats[s.id]["by_status"].get(t.status, 0) + 1
    return render_template(
        "sprints.html",
        sprints=all_sprints,
        sprint_stats=sprint_stats,
        today=today,
        sprint_statuses=SPRINT_STATUSES,
    )


@bp.route("/sprints/<int:sprint_id>")
@login_required
def sprint_detail(sprint_id: int):
    sprint = db.session.get(Sprint, sprint_id)
    if not sprint:
        flash("Sprint not found.", "danger")
        return redirect(url_for("main.sprint_list"))
    today = date.today()
    tasks = Task.query.filter_by(sprint_id=sprint_id).order_by(Task.due_date.is_(None), Task.due_date.asc(), Task.created_at.desc()).all()
    columns = get_task_columns()
    by_status = {col: [] for col in columns}
    for t in tasks:
        key = t.status if t.status in by_status else columns[0]
        by_status[key].append(t)
    total = len(tasks)
    done = sum(1 for t in tasks if t.status in ("Done", "Completed"))
    field_defs = get_field_defs("task")
    fv_map = get_field_values_map("task", [t.id for t in tasks])
    return render_template(
        "sprint_detail.html",
        sprint=sprint,
        tasks=tasks,
        by_status=by_status,
        columns=columns,
        total=total,
        done=done,
        pct=round(done / total * 100) if total else 0,
        today=today,
        field_defs=field_defs,
        fv_map=fv_map,
        priorities=PRIORITIES,
    )


# ── CRM ──────────────────────────────────────────────────────────────────────

@bp.route("/crm", methods=["GET", "POST"])
@login_required
def crm():
    field_defs = get_field_defs("contact")
    if request.method == "POST":
        verify_csrf()
        name = request.form.get("name", "").strip()
        kind = request.form.get("kind", "lead")
        if not name:
            flash("Contact name is required.", "danger")
            return redirect(url_for("main.crm"))
        if kind not in CONTACT_KINDS:
            kind = "lead"
        record = Contact(
            kind=kind,
            name=name,
            company=request.form.get("company", "").strip() or None,
            email=request.form.get("email", "").strip() or None,
            phone=request.form.get("phone", "").strip() or None,
            stage=request.form.get("stage", "New").strip() or "New",
            notes=request.form.get("notes", "").strip() or None,
        )
        db.session.add(record)
        db.session.flush()
        save_field_values("contact", record.id, field_defs, request.form)
        db.session.commit()
        fire_event("contact", "created", record)
        log_audit("create", "crm", related_record=name, message=f"Created {kind} contact '{name}'")
        flash("CRM record created.", "success")
        return redirect(url_for("main.crm"))

    contacts = Contact.query.order_by(Contact.created_at.desc()).all()
    fv_map = get_field_values_map("contact", [c.id for c in contacts])
    return render_template(
        "crm.html",
        contacts=contacts,
        contact_kinds=CONTACT_KINDS,
        field_defs=field_defs,
        fv_map=fv_map,
    )


@bp.route("/crm/<int:contact_id>/edit", methods=["POST"])
@login_required
def crm_edit(contact_id: int):
    verify_csrf()
    record = db.session.get(Contact, contact_id)
    if not record:
        flash("Contact not found.", "danger")
        return redirect(url_for("main.crm"))
    old_stage = record.stage
    record.name = request.form.get("name", record.name).strip() or record.name
    record.kind = request.form.get("kind", record.kind)
    if record.kind not in CONTACT_KINDS:
        record.kind = "lead"
    record.company = request.form.get("company", "").strip() or None
    record.email = request.form.get("email", "").strip() or None
    record.phone = request.form.get("phone", "").strip() or None
    record.stage = request.form.get("stage", "New").strip() or "New"
    record.notes = request.form.get("notes", "").strip() or None
    field_defs = get_field_defs("contact")
    save_field_values("contact", record.id, field_defs, request.form)
    db.session.commit()
    if old_stage != record.stage:
        fire_event("contact", "stage_changed", record, context={"new_stage": record.stage})
    log_audit("update", "crm", related_record=record.name, message=f"Updated contact '{record.name}'")
    flash("Contact updated.", "success")
    return redirect(url_for("main.crm"))


@bp.route("/crm/<int:contact_id>/delete", methods=["POST"])
@login_required
def crm_delete(contact_id: int):
    verify_csrf()
    record = db.session.get(Contact, contact_id)
    if record:
        log_audit("delete", "crm", related_record=record.name, message=f"Deleted contact '{record.name}'")
        db.session.delete(record)
        db.session.commit()
        flash("CRM record deleted.", "success")
    return redirect(url_for("main.crm"))


# ── Vendors ──────────────────────────────────────────────────────────────────

@bp.route("/vendors", methods=["GET", "POST"])
@login_required
def vendors():
    field_defs = get_field_defs("vendor")
    if request.method == "POST":
        verify_csrf()
        name = request.form.get("name", "").strip()
        if not name:
            flash("Vendor name is required.", "danger")
            return redirect(url_for("main.vendors"))
        vendor = Vendor(
            name=name,
            category=request.form.get("category", "").strip() or None,
            contact_name=request.form.get("contact_name", "").strip() or None,
            email=request.form.get("email", "").strip() or None,
            phone=request.form.get("phone", "").strip() or None,
            contract_end=parse_date(request.form.get("contract_end")),
            rating=min(max(parse_int(request.form.get("rating"), default=3), 1), 5),
            notes=request.form.get("notes", "").strip() or None,
        )
        db.session.add(vendor)
        db.session.flush()
        save_field_values("vendor", vendor.id, field_defs, request.form)
        db.session.commit()
        fire_event("vendor", "created", vendor)
        flash("Vendor created.", "success")
        return redirect(url_for("main.vendors"))

    vendors_list = Vendor.query.order_by(Vendor.contract_end.is_(None), Vendor.contract_end.asc(), Vendor.name.asc()).all()
    fv_map = get_field_values_map("vendor", [v.id for v in vendors_list])
    return render_template("vendors.html", vendors=vendors_list, field_defs=field_defs, fv_map=fv_map)


@bp.route("/vendors/<int:vendor_id>/delete", methods=["POST"])
@login_required
def vendor_delete(vendor_id: int):
    verify_csrf()
    vendor = db.session.get(Vendor, vendor_id)
    if vendor:
        db.session.delete(vendor)
        db.session.commit()
        flash("Vendor deleted.", "success")
    return redirect(url_for("main.vendors"))


# ── Assets ───────────────────────────────────────────────────────────────────

@bp.route("/assets", methods=["GET", "POST"])
@login_required
def assets():
    field_defs = get_field_defs("asset")
    if request.method == "POST":
        verify_csrf()
        name = request.form.get("name", "").strip()
        if not name:
            flash("Asset name is required.", "danger")
            return redirect(url_for("main.assets"))
        asset = Asset(
            name=name,
            category=request.form.get("category", "").strip() or None,
            serial_number=request.form.get("serial_number", "").strip() or None,
            owner=request.form.get("owner", "").strip() or None,
            status=request.form.get("status", "Active").strip() or "Active",
            purchase_cost=parse_decimal(request.form.get("purchase_cost")),
            current_value=parse_decimal(request.form.get("current_value")),
            expiry_date=parse_date(request.form.get("expiry_date")),
            notes=request.form.get("notes", "").strip() or None,
        )
        db.session.add(asset)
        db.session.flush()
        save_field_values("asset", asset.id, field_defs, request.form)
        db.session.commit()
        fire_event("asset", "created", asset)
        flash("Asset created.", "success")
        return redirect(url_for("main.assets"))

    assets_list = Asset.query.order_by(Asset.expiry_date.is_(None), Asset.expiry_date.asc(), Asset.name.asc()).all()
    fv_map = get_field_values_map("asset", [a.id for a in assets_list])
    return render_template("assets.html", assets=assets_list, field_defs=field_defs, fv_map=fv_map)


@bp.route("/assets/<int:asset_id>/delete", methods=["POST"])
@login_required
def asset_delete(asset_id: int):
    verify_csrf()
    asset = db.session.get(Asset, asset_id)
    if asset:
        db.session.delete(asset)
        db.session.commit()
        flash("Asset deleted.", "success")
    return redirect(url_for("main.assets"))


@bp.route("/assets/export.pdf")
@login_required
def assets_export_pdf():
    try:
        _pdf_rl()
    except ImportError:
        flash("PDF export requires reportlab. Run: pip install reportlab", "danger")
        return redirect(url_for("main.assets"))

    assets_list = Asset.query.order_by(Asset.category.asc(), Asset.name.asc()).all()
    today = date.today()

    def _expiry(a):
        if not a.expiry_date:
            return "-"
        flag = " (!)" if a.expiry_date <= today + timedelta(days=30) else ""
        return a.expiry_date.strftime("%Y-%m-%d") + flag

    rows = [[a.name, a.category or "-", a.serial_number or "-", a.owner or "-",
             a.status, f"${float(a.purchase_cost):,.2f}",
             f"${float(a.current_value):,.2f}", _expiry(a)]
            for a in assets_list]
    total_purchase = sum(float(a.purchase_cost) for a in assets_list)
    total_value = sum(float(a.current_value) for a in assets_list)
    totals = ["Total", "", "", "", f"{len(assets_list)} items",
              f"${total_purchase:,.2f}", f"${total_value:,.2f}", ""]

    buf = _build_table_pdf(
        title="Assets Register",
        headers=["Name", "Category", "Serial #", "Owner", "Status",
                 "Purchase Cost", "Current Value", "Expiry"],
        rows=rows, totals=totals,
        subtitle=f"{len(assets_list)} assets  |  Total current value ${total_value:,.2f}",
    )
    log_audit("export_pdf", "assets", message=f"Exported {len(assets_list)} assets as PDF")
    return _pdf_dl(buf, f"assets_{date.today().isoformat()}.pdf")


# ── Inventory ────────────────────────────────────────────────────────────────

@bp.route("/inventory", methods=["GET", "POST"])
@login_required
def inventory():
    field_defs = get_field_defs("inventory")
    if request.method == "POST":
        verify_csrf()
        sku = request.form.get("sku", "").strip().upper()
        name = request.form.get("name", "").strip()
        if not sku or not name:
            flash("SKU and item name are required.", "danger")
            return redirect(url_for("main.inventory"))
        if InventoryItem.query.filter_by(sku=sku).first():
            flash("SKU already exists.", "danger")
            return redirect(url_for("main.inventory"))
        item = InventoryItem(
            sku=sku,
            name=name,
            category=request.form.get("category", "").strip() or None,
            warehouse=request.form.get("warehouse", "Main").strip() or "Main",
            qty_on_hand=parse_int(request.form.get("qty_on_hand")),
            reorder_level=parse_int(request.form.get("reorder_level")),
            unit_cost=parse_decimal(request.form.get("unit_cost")),
            sale_price=parse_decimal(request.form.get("sale_price")),
            expiry_date=parse_date(request.form.get("expiry_date")),
            notes=request.form.get("notes", "").strip() or None,
        )
        db.session.add(item)
        db.session.flush()
        save_field_values("inventory", item.id, field_defs, request.form)
        db.session.commit()
        if item.qty_on_hand <= item.reorder_level:
            fire_event("inventory", "low_stock", item)
        flash("Inventory item created.", "success")
        return redirect(url_for("main.inventory"))

    items = InventoryItem.query.order_by(InventoryItem.qty_on_hand.asc(), InventoryItem.name.asc()).all()
    fv_map = get_field_values_map("inventory", [i.id for i in items])
    return render_template("inventory.html", items=items, field_defs=field_defs, fv_map=fv_map)


@bp.route("/inventory/<int:item_id>/adjust", methods=["POST"])
@login_required
def inventory_adjust(item_id: int):
    verify_csrf()
    item = db.session.get(InventoryItem, item_id)
    if not item:
        flash("Inventory item not found.", "danger")
        return redirect(url_for("main.inventory"))
    delta = parse_int(request.form.get("delta"), default=0)
    old_qty = item.qty_on_hand
    item.qty_on_hand = max(item.qty_on_hand + delta, 0)
    db.session.commit()
    if item.qty_on_hand <= item.reorder_level:
        fire_event("inventory", "low_stock", item)
    log_audit("adjust", "inventory", related_record=item.sku,
              message=f"Stock adjusted for {item.sku}: {old_qty} → {item.qty_on_hand} (delta {delta:+d})")
    flash(f"Stock adjusted for {item.sku}.", "success")
    return redirect(url_for("main.inventory"))


@bp.route("/inventory/<int:item_id>/delete", methods=["POST"])
@login_required
def inventory_delete(item_id: int):
    verify_csrf()
    item = db.session.get(InventoryItem, item_id)
    if item:
        db.session.delete(item)
        db.session.commit()
        flash("Inventory item deleted.", "success")
    return redirect(url_for("main.inventory"))


# ── Invoices ─────────────────────────────────────────────────────────────────

@bp.route("/invoices", methods=["GET", "POST"])
@login_required
def invoices():
    field_defs = get_field_defs("invoice")
    if request.method == "POST":
        verify_csrf()
        reference = request.form.get("reference", "").strip().upper()
        party_name = request.form.get("party_name", "").strip()
        due_date = request.form.get("due_date")
        if not reference or not party_name or not due_date:
            flash("Reference, party name and due date are required.", "danger")
            return redirect(url_for("main.invoices"))
        kind = request.form.get("kind", "sales")
        if kind not in INVOICE_KINDS:
            kind = "sales"
        if Invoice.query.filter_by(reference=reference).first():
            flash("Invoice reference already exists.", "danger")
            return redirect(url_for("main.invoices"))
        invoice = Invoice(
            kind=kind,
            party_name=party_name,
            reference=reference,
            amount=parse_decimal(request.form.get("amount")),
            due_date=parse_date(due_date),
            status=request.form.get("status", "Unpaid").strip() or "Unpaid",
            paid_on=parse_date(request.form.get("paid_on")),
            notes=request.form.get("notes", "").strip() or None,
        )
        db.session.add(invoice)
        db.session.flush()
        save_field_values("invoice", invoice.id, field_defs, request.form)
        db.session.commit()
        fire_event("invoice", "created", invoice)
        log_audit("create", "invoices", related_record=reference,
                  message=f"Created invoice '{reference}' for {party_name} — ${invoice.amount}")
        flash("Invoice created.", "success")
        return redirect(url_for("main.invoices"))

    today = date.today()
    invoices_list = Invoice.query.order_by(Invoice.due_date.asc(), Invoice.created_at.desc()).all()
    fv_map = get_field_values_map("invoice", [i.id for i in invoices_list])
    active_renewals = Renewal.query.filter(Renewal.status == "Active").order_by(Renewal.renew_on.asc()).all()
    return render_template("invoices.html", invoices=invoices_list, today=today, field_defs=field_defs, fv_map=fv_map, active_renewals=active_renewals)


@bp.route("/invoices/<int:invoice_id>/mark_paid", methods=["POST"])
@login_required
def invoice_mark_paid(invoice_id: int):
    verify_csrf()
    invoice = db.session.get(Invoice, invoice_id)
    if invoice:
        invoice.status = "Paid"
        invoice.paid_on = date.today()
        db.session.commit()
        fire_event("invoice", "paid", invoice)
        log_audit("status_change", "invoices", related_record=invoice.reference,
                  message=f"Invoice '{invoice.reference}' marked as Paid")
        flash(f"{invoice.reference} marked as paid.", "success")
    return redirect(url_for("main.invoices"))


@bp.route("/invoices/<int:invoice_id>/delete", methods=["POST"])
@login_required
def invoice_delete(invoice_id: int):
    verify_csrf()
    invoice = db.session.get(Invoice, invoice_id)
    if invoice:
        log_audit("delete", "invoices", related_record=invoice.reference,
                  message=f"Deleted invoice '{invoice.reference}'")
        db.session.delete(invoice)
        db.session.commit()
        flash("Invoice deleted.", "success")
    return redirect(url_for("main.invoices"))


# ── PDF helpers ───────────────────────────────────────────────────────────────

def _pdf_rl():
    """Lazy-import reportlab modules. Raises ImportError if not installed."""
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_RIGHT, TA_CENTER, TA_LEFT
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable,
    )
    return A4, landscape, mm, colors, TA_RIGHT, TA_CENTER, TA_LEFT, ParagraphStyle, \
           SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable


def _build_table_pdf(title: str, headers: list, rows: list,
                     totals: list | None = None, subtitle: str = "") -> io.BytesIO:
    """Render a professional landscape table PDF. Returns a seeked BytesIO."""
    A4, landscape, mm, colors, TA_RIGHT, TA_CENTER, TA_LEFT, ParagraphStyle, \
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable = _pdf_rl()

    NAVY  = colors.HexColor("#1e3a5f")
    BLUE  = colors.HexColor("#3b82f6")
    LIGHT = colors.HexColor("#f8fafc")
    ALT   = colors.HexColor("#eef2f7")
    GREY  = colors.HexColor("#64748b")
    LINE  = colors.HexColor("#e2e8f0")
    WHITE = colors.white

    app_title = current_app.config.get("APP_TITLE", "OpsPilot Local")
    generated = datetime.now(timezone.utc).strftime("%d %B %Y, %H:%M UTC")

    buf = io.BytesIO()
    pagesize = landscape(A4)
    pw = pagesize[0] - 30 * mm  # usable width

    doc = SimpleDocTemplate(buf, pagesize=pagesize,
                             leftMargin=15*mm, rightMargin=15*mm,
                             topMargin=12*mm, bottomMargin=12*mm)

    def _p(text, font="Helvetica", size=9, color=colors.black, align=TA_LEFT):
        s = ParagraphStyle("x", fontName=font, fontSize=size, textColor=color,
                           alignment=align, leading=size * 1.35, spaceAfter=0)
        return Paragraph(str(text), s)

    elements = []

    # Header bar
    hdr = Table([[
        _p(f"<b>{app_title}</b>", size=13, color=WHITE),
        _p(f"<b>{title.upper()}</b>", size=15, color=WHITE, align=TA_RIGHT),
    ]], colWidths=[pw * 0.6, pw * 0.4])
    hdr.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), NAVY),
        ("TOPPADDING", (0, 0), (-1, -1), 8), ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (0, -1), 8), ("RIGHTPADDING", (-1, 0), (-1, -1), 8),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    elements.append(hdr)
    elements.append(Spacer(1, 2 * mm))

    if subtitle:
        sub_s = ParagraphStyle("sub", fontName="Helvetica", fontSize=8,
                                textColor=GREY, leading=10)
        elements.append(Paragraph(f"{subtitle}  ·  Generated {generated}", sub_s))
    else:
        sub_s = ParagraphStyle("sub", fontName="Helvetica", fontSize=8,
                                textColor=GREY, leading=10)
        elements.append(Paragraph(f"Generated {generated}", sub_s))
    elements.append(Spacer(1, 3 * mm))

    n = len(headers)
    col_w = [pw / n] * n

    tbl_data = [[_p(h, font="Helvetica-Bold", size=8, color=WHITE) for h in headers]]
    for i, row in enumerate(rows):
        tbl_data.append([_p(str(c) if c is not None else "", size=8) for c in row])
    if totals:
        tbl_data.append([_p(str(c) if c is not None else "", font="Helvetica-Bold", size=8)
                         for c in totals])

    row_styles = [("BACKGROUND", (0, 0), (-1, 0), BLUE)]
    for i in range(1, len(tbl_data)):
        if totals and i == len(tbl_data) - 1:
            row_styles.append(("BACKGROUND", (0, i), (-1, i), ALT))
            row_styles.append(("LINEABOVE", (0, i), (-1, i), 1, BLUE))
        else:
            row_styles.append(("BACKGROUND", (0, i), (-1, i), LIGHT if i % 2 == 0 else WHITE))

    tbl = Table(tbl_data, colWidths=col_w, repeatRows=1)
    tbl.setStyle(TableStyle([
        *row_styles,
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("GRID", (0, 0), (-1, -1), 0.3, LINE),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    elements.append(tbl)

    elements.append(Spacer(1, 3 * mm))
    elements.append(HRFlowable(width="100%", thickness=0.3, color=LINE))
    foot_s = ParagraphStyle("foot", fontName="Helvetica", fontSize=7,
                             textColor=GREY, alignment=TA_CENTER)
    elements.append(Paragraph(f"Generated by {app_title}", foot_s))

    doc.build(elements)
    buf.seek(0)
    return buf


def _pdf_dl(buf: io.BytesIO, filename: str) -> Response:
    return Response(buf.getvalue(), mimetype="application/pdf",
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@bp.route("/invoices/<int:invoice_id>/export.pdf")
@login_required
def invoice_export_pdf(invoice_id: int):
    invoice = db.session.get(Invoice, invoice_id)
    if not invoice:
        flash("Invoice not found.", "danger")
        return redirect(url_for("main.invoices"))

    try:
        A4, landscape, mm, colors, TA_RIGHT, TA_CENTER, TA_LEFT, ParagraphStyle, \
            SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable = _pdf_rl()
    except ImportError:
        flash("PDF export requires reportlab. Run: pip install reportlab", "danger")
        return redirect(url_for("main.invoices"))

    NAVY   = colors.HexColor("#1e3a5f")
    BLUE   = colors.HexColor("#3b82f6")
    LIGHT  = colors.HexColor("#f0f4f8")
    GREY   = colors.HexColor("#64748b")
    LINE   = colors.HexColor("#e2e8f0")
    RED    = colors.HexColor("#ef4444")
    GREEN  = colors.HexColor("#22c55e")
    WHITE  = colors.white

    app_title = current_app.config.get("APP_TITLE", "OpsPilot Local")
    today = date.today()
    is_overdue = invoice.status != "Paid" and invoice.due_date < today
    status_color = GREEN if invoice.status == "Paid" else (RED if is_overdue else BLUE)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                             leftMargin=20*mm, rightMargin=20*mm,
                             topMargin=15*mm, bottomMargin=15*mm)
    pw = A4[0] - 40 * mm  # usable width

    def _p(text, font="Helvetica", size=10, color=colors.black, align=TA_LEFT,
           space_before=0, space_after=2):
        s = ParagraphStyle("x", fontName=font, fontSize=size, textColor=color,
                           alignment=align, leading=size * 1.35,
                           spaceBefore=space_before, spaceAfter=space_after)
        return Paragraph(str(text), s)

    elements = []

    # ── Header bar ────────────────────────────────────────────────────────────
    hdr = Table([[
        _p(f"<b>{app_title}</b>", size=13, color=WHITE),
        _p("<b>INVOICE</b>", font="Helvetica-Bold", size=22, color=WHITE, align=TA_RIGHT),
    ]], colWidths=[pw * 0.55, pw * 0.45])
    hdr.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), NAVY),
        ("TOPPADDING", (0, 0), (-1, -1), 10), ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("LEFTPADDING", (0, 0), (0, -1), 8), ("RIGHTPADDING", (-1, 0), (-1, -1), 8),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    elements.append(hdr)
    elements.append(Spacer(1, 7 * mm))

    # ── Bill To + Invoice Details ─────────────────────────────────────────────
    bill_col = [
        _p("BILL TO", font="Helvetica-Bold", size=8, color=GREY, space_after=4),
        _p(f"<b>{invoice.party_name}</b>", size=14, space_after=0),
    ]
    det_col = [
        _p("INVOICE DETAILS", font="Helvetica-Bold", size=8, color=GREY, space_after=4),
        _p(f"<b>Reference:</b>  {invoice.reference}", size=10),
        _p(f"<b>Type:</b>  {invoice.kind.title()}", size=10),
        _p(f"<b>Due date:</b>  {invoice.due_date.strftime('%d %B %Y')}", size=10),
        _p(f"<b>Generated:</b>  {datetime.now(timezone.utc).strftime('%d %B %Y')}", size=10),
    ]
    meta = Table([[bill_col, det_col]], colWidths=[pw * 0.5, pw * 0.5])
    meta.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 0), ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ("LEFTPADDING", (0, 0), (0, -1), 0), ("RIGHTPADDING", (-1, 0), (-1, -1), 0),
    ]))
    elements.append(meta)
    elements.append(Spacer(1, 7 * mm))

    # ── Amount due box ────────────────────────────────────────────────────────
    amount_str = f"${float(invoice.amount):,.2f}"
    amt = Table([[
        _p("AMOUNT DUE", font="Helvetica-Bold", size=9, color=GREY),
        _p(f"<b>{amount_str}</b>", font="Helvetica-Bold", size=22, color=NAVY, align=TA_RIGHT),
    ]], colWidths=[pw * 0.5, pw * 0.5])
    amt.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT),
        ("TOPPADDING", (0, 0), (-1, -1), 8), ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (0, -1), 8), ("RIGHTPADDING", (-1, 0), (-1, -1), 8),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LINEABOVE", (0, 0), (-1, 0), 2.5, BLUE),
    ]))
    elements.append(amt)

    # Status row
    paid_text = (f"  ·  Paid on {invoice.paid_on.strftime('%d %B %Y')}"
                 if invoice.paid_on else "")
    status_row = Table([[
        _p(f"Status: <b>{invoice.status}</b>{paid_text}", size=10, color=status_color),
        _p("OVERDUE" if is_overdue else ("" if invoice.status == "Paid" else ""),
           font="Helvetica-Bold", size=9, color=RED, align=TA_RIGHT),
    ]], colWidths=[pw * 0.7, pw * 0.3])
    status_row.setStyle(TableStyle([
        ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 8), ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("LINEBELOW", (0, 0), (-1, -1), 0.5, LINE),
    ]))
    elements.append(status_row)

    # ── Notes ─────────────────────────────────────────────────────────────────
    if invoice.notes:
        elements.append(Spacer(1, 5 * mm))
        elements.append(_p("NOTES", font="Helvetica-Bold", size=8, color=GREY))
        elements.append(Spacer(1, 2 * mm))
        elements.append(_p(invoice.notes, size=10))

    # ── Footer ────────────────────────────────────────────────────────────────
    elements.append(Spacer(1, 10 * mm))
    elements.append(HRFlowable(width="100%", thickness=0.5, color=LINE))
    elements.append(Spacer(1, 2 * mm))
    foot_s = ParagraphStyle("foot", fontName="Helvetica", fontSize=8,
                             textColor=GREY, alignment=TA_CENTER)
    elements.append(Paragraph(
        f"Generated by {app_title}  ·  "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC",
        foot_s,
    ))

    doc.build(elements)
    log_audit("export_pdf", "invoices", related_record=invoice.reference,
              message=f"Exported invoice '{invoice.reference}' as PDF")
    filename = f"invoice_{invoice.reference.replace('/', '-')}.pdf"
    return _pdf_dl(buf, filename)


# ── Renewals ─────────────────────────────────────────────────────────────────

@bp.route("/renewals", methods=["GET", "POST"])
@login_required
def renewals():
    field_defs = get_field_defs("renewal")
    if request.method == "POST":
        verify_csrf()
        title = request.form.get("title", "").strip()
        renew_on = request.form.get("renew_on")
        if not title or not renew_on:
            flash("Title and renewal date are required.", "danger")
            return redirect(url_for("main.renewals"))
        record = Renewal(
            title=title,
            category=request.form.get("category", "").strip() or None,
            provider=request.form.get("provider", "").strip() or None,
            renew_on=parse_date(renew_on),
            cost=parse_decimal(request.form.get("cost")),
            auto_renew=request.form.get("auto_renew") == "on",
            status=request.form.get("status", "Active").strip() or "Active",
            notes=request.form.get("notes", "").strip() or None,
            contact_name=request.form.get("contact_name", "").strip() or None,
            contact_email=request.form.get("contact_email", "").strip() or None,
        )
        db.session.add(record)
        db.session.flush()
        save_field_values("renewal", record.id, field_defs, request.form)
        db.session.commit()
        fire_event("renewal", "created", record)
        flash("Renewal created.", "success")
        return redirect(url_for("main.renewals"))

    renewals_list = Renewal.query.order_by(Renewal.renew_on.asc(), Renewal.title.asc()).all()
    fv_map = get_field_values_map("renewal", [r.id for r in renewals_list])
    return render_template("renewals.html", renewals=renewals_list, field_defs=field_defs, fv_map=fv_map, today=date.today())


@bp.route("/renewals/<int:renewal_id>/complete", methods=["POST"])
@login_required
def renewal_complete(renewal_id: int):
    verify_csrf()
    renewal = db.session.get(Renewal, renewal_id)
    if renewal:
        old_date = renewal.renew_on
        renewal.renew_on = renewal.renew_on + timedelta(days=365)
        db.session.commit()
        fire_event("renewal", "rolled_forward", renewal)
        log_audit("renewal_action", "renewals", related_record=renewal.title,
                  message=f"Renewal '{renewal.title}' rolled forward from {old_date} to {renewal.renew_on}")
        flash(f"Renewal rolled forward for {renewal.title}.", "success")
    return redirect(url_for("main.renewals"))


@bp.route("/renewals/<int:renewal_id>/delete", methods=["POST"])
@login_required
def renewal_delete(renewal_id: int):
    verify_csrf()
    renewal = db.session.get(Renewal, renewal_id)
    if renewal:
        db.session.delete(renewal)
        db.session.commit()
        flash("Renewal deleted.", "success")
    return redirect(url_for("main.renewals"))


@bp.route("/renewals/<int:renewal_id>/edit", methods=["POST"])
@login_required
def renewal_edit(renewal_id: int):
    verify_csrf()
    renewal = db.session.get(Renewal, renewal_id)
    if not renewal:
        flash("Renewal not found.", "danger")
        return redirect(url_for("main.renewals"))
    renewal.title = request.form.get("title", renewal.title).strip() or renewal.title
    renewal.category = request.form.get("category", "").strip() or None
    renewal.provider = request.form.get("provider", "").strip() or None
    renew_on = request.form.get("renew_on")
    if renew_on:
        renewal.renew_on = parse_date(renew_on)
    renewal.cost = parse_decimal(request.form.get("cost", str(renewal.cost)))
    renewal.auto_renew = request.form.get("auto_renew") == "on"
    renewal.status = request.form.get("status", renewal.status).strip() or renewal.status
    renewal.notes = request.form.get("notes", "").strip() or None
    renewal.contact_name = request.form.get("contact_name", "").strip() or None
    renewal.contact_email = request.form.get("contact_email", "").strip() or None
    db.session.commit()
    flash(f"Renewal '{renewal.title}' updated.", "success")
    return redirect(url_for("main.renewals"))


@bp.route("/renewals/<int:renewal_id>/send_invoice", methods=["POST"])
@login_required
def renewal_send_invoice(renewal_id: int):
    verify_csrf()
    renewal = db.session.get(Renewal, renewal_id)
    if not renewal:
        flash("Renewal not found.", "danger")
        return redirect(url_for("main.renewals"))

    recipient_email = request.form.get("recipient_email", "").strip()
    recipient_name = request.form.get("recipient_name", "").strip()
    invoice_notes = request.form.get("invoice_notes", "").strip()
    do_send_email = request.form.get("send_email") == "1"

    # Persist contact details back to renewal if not already set
    if recipient_email and not renewal.contact_email:
        renewal.contact_email = recipient_email
    if recipient_name and not renewal.contact_name:
        renewal.contact_name = recipient_name

    invoice = create_invoice_from_renewal(renewal)
    if invoice_notes:
        invoice.notes = invoice_notes
    db.session.add(invoice)
    db.session.commit()
    fire_event("invoice", "created", invoice)

    log_audit("create", "renewals", related_record=renewal.title,
              message=f"Created invoice {invoice.reference} from renewal '{renewal.title}'")

    if do_send_email and recipient_email:
        smtp_cfg = get_smtp_config()
        if not smtp_cfg.get("host"):
            log_audit("email_send", "renewals", status="error", related_record=invoice.reference,
                      message=f"Email not sent for {invoice.reference} — SMTP not configured. "
                               f"Recipient: {recipient_email}, subject: 'Invoice {invoice.reference}'")
            flash(f"Invoice {invoice.reference} created, but email not sent — SMTP is not configured. "
                  "Configure it in Settings → Email.", "warning")
        else:
            try:
                html_body = render_template(
                    "_invoice_email.html",
                    invoice=invoice,
                    renewal=renewal,
                    recipient_name=recipient_name or renewal.contact_name or renewal.provider or "Valued Customer",
                )
                subject = f"Invoice {invoice.reference} — {renewal.title}"
                result = send_invoice_email(recipient_email, subject, html_body, smtp_cfg)
                log_audit(
                    "email_send", "renewals", status="ok",
                    related_record=invoice.reference,
                    message=(
                        f"Invoice email accepted by SMTP for delivery. "
                        f"Invoice: {invoice.reference}, recipient: {recipient_email}, "
                        f"subject: '{subject}', provider: {result.get('provider', 'unknown')}. "
                        f"Note: SMTP acceptance does not guarantee inbox delivery — "
                        f"check spam folder and verify SPF/DKIM if not received."
                    ),
                )
                flash(
                    f"Invoice {invoice.reference} created and submitted to email server for {recipient_email}. "
                    "Check spam if not received — also verify your From address matches your SMTP account.",
                    "success",
                )
            except Exception as exc:
                safe_msg = str(exc)
                log_audit(
                    "email_send", "renewals", status="error",
                    related_record=invoice.reference,
                    message=(
                        f"Email send FAILED for invoice {invoice.reference} to {recipient_email}. "
                        f"Error: {safe_msg}. "
                        f"SMTP host: {smtp_cfg.get('host', 'unknown')}:{smtp_cfg.get('port', '?')}"
                    ),
                )
                flash(f"Invoice {invoice.reference} created, but email failed: {safe_msg}", "warning")
    else:
        if do_send_email and not recipient_email:
            log_audit("email_send", "renewals", status="warning", related_record=invoice.reference,
                      message=f"Email send requested for {invoice.reference} but no recipient address provided")
        flash(f"Invoice {invoice.reference} created successfully.", "success")

    return redirect(url_for("main.invoices"))


@bp.route("/renewals/export.pdf")
@login_required
def renewals_export_pdf():
    try:
        _pdf_rl()
    except ImportError:
        flash("PDF export requires reportlab. Run: pip install reportlab", "danger")
        return redirect(url_for("main.renewals"))

    renewals_list = Renewal.query.order_by(Renewal.renew_on.asc()).all()
    rows = [[r.title, r.category or "-", r.provider or "-",
             r.renew_on.strftime("%Y-%m-%d"), f"${float(r.cost):,.2f}",
             r.status, "Yes" if r.auto_renew else "No"]
            for r in renewals_list]
    total_cost = sum(float(r.cost) for r in renewals_list)
    totals = ["Total", "", "", f"{len(renewals_list)} renewals",
              f"${total_cost:,.2f}", "", ""]

    buf = _build_table_pdf(
        title="Renewals",
        headers=["Title", "Category", "Provider", "Renew On", "Cost", "Status", "Auto-Renew"],
        rows=rows, totals=totals,
        subtitle=f"{len(renewals_list)} renewals  |  Total annual cost ${total_cost:,.2f}",
    )
    log_audit("export_pdf", "renewals", message=f"Exported {len(renewals_list)} renewals as PDF")
    return _pdf_dl(buf, f"renewals_{date.today().isoformat()}.pdf")


# ── Settings — Email ──────────────────────────────────────────────────────────

@bp.route("/settings/email", methods=["GET", "POST"])
@admin_required
def settings_email():
    cfg = get_smtp_config()
    if request.method == "POST":
        verify_csrf()
        action = request.form.get("action", "save")

        if action == "save":
            data: dict = {
                "provider":  "smtp",
                "host":      request.form.get("host", "").strip(),
                "port":      request.form.get("port", "587").strip() or "587",
                "username":  request.form.get("username", "").strip(),
                "from_addr": request.form.get("from_addr", "").strip(),
                "use_tls":   "true" if request.form.get("use_tls") else "false",
            }
            new_password = request.form.get("password", "").strip()
            if new_password:
                data["password"] = new_password
            set_integration_config("smtp", data)
            flash("SMTP settings saved.", "success")

        elif action == "save_api":
            provider = request.form.get("api_provider", "sendgrid").strip()
            data: dict = {
                "provider":  provider,
                "from_addr": request.form.get("api_from_addr", "").strip(),
            }
            new_key = request.form.get("api_key", "").strip()
            if new_key:
                data["api_key"] = new_key
            set_integration_config("smtp", data)
            flash(f"{provider.title()} API settings saved.", "success")

        elif action == "test":
            if not cfg.get("host"):
                flash("Save SMTP settings first.", "warning")
            else:
                try:
                    import smtplib
                    host = cfg.get("host", "")
                    port = int(cfg.get("port") or 587)
                    use_tls = str(cfg.get("use_tls", "true")).lower() != "false"
                    with smtplib.SMTP(host, port, timeout=10) as smtp:
                        smtp.ehlo()
                        if use_tls:
                            smtp.starttls()
                            smtp.ehlo()
                        u, p = cfg.get("username", ""), cfg.get("password", "")
                        if u and p:
                            smtp.login(u, p)
                    log_audit("smtp_test_connection", "settings", status="ok",
                              message=f"SMTP connection test passed. Host: {host}:{port} (TLS={use_tls})")
                    flash("SMTP connection successful.", "success")
                except Exception as exc:
                    log_audit("smtp_test_connection", "settings", status="error",
                              message=f"SMTP connection test failed. {exc}")
                    flash(f"SMTP connection failed: {exc}", "danger")

        elif action == "test_api":
            try:
                result = test_api_connection(cfg)
                log_audit("api_test_connection", "settings", status="ok",
                          message=f"{result['provider']} key verified successfully.")
                flash(f"{result['provider']} API key is valid.", "success")
            except ValueError as exc:
                log_audit("api_test_connection", "settings", status="error", message=str(exc))
                flash(f"API key check failed: {exc}", "danger")

        elif action == "send_test":
            to_email = request.form.get("test_recipient", "").strip()
            provider = cfg.get("provider", "smtp")
            if not to_email:
                flash("Enter a recipient email address.", "warning")
            elif provider == "smtp" and not cfg.get("host"):
                flash("Save SMTP settings first.", "warning")
            elif provider in ("sendgrid", "resend") and not cfg.get("api_key"):
                flash("Save API settings first.", "warning")
            else:
                try:
                    result = send_test_email(to_email, cfg)
                    log_audit("email_test_send", "settings", status="ok",
                              message=(f"Test email sent via {result.get('provider')} to {to_email}. "
                                       "SMTP acceptance does not guarantee inbox delivery."))
                    flash(f"Test email sent to {to_email} via {result.get('provider')}. "
                          "Check inbox and spam folder.", "success")
                except ValueError as exc:
                    log_audit("email_test_send", "settings", status="error", message=str(exc))
                    flash(f"Test email failed: {exc}", "danger")

        return redirect(url_for("main.settings_email"))

    public_cfg = {k: v for k, v in cfg.items() if k not in ("password", "api_key")}
    return render_template(
        "settings_email.html",
        title="Email Settings",
        cfg=public_cfg,
        password_saved=bool(cfg.get("password")),
        api_key_saved=bool(cfg.get("api_key")),
    )




# ── Notifications ─────────────────────────────────────────────────────────────

@bp.route("/notifications")
@login_required
def notifications():
    page = request.args.get("page", 1, type=int)
    per_page = 30
    severity_filter = request.args.get("severity", "").strip()
    query = AlertLog.query
    if severity_filter in ("info", "warning", "danger"):
        query = query.filter_by(severity=severity_filter)
    total = query.count()
    alerts = query.order_by(AlertLog.created_at.desc()).offset((page - 1) * per_page).limit(per_page).all()
    unread = AlertLog.query.filter_by(is_read=False).count()
    return render_template(
        "notifications.html",
        title="Notifications",
        alerts=alerts,
        unread=unread,
        page=page,
        per_page=per_page,
        total=total,
        severity_filter=severity_filter,
    )


@bp.route("/notifications/<int:alert_id>/read", methods=["POST"])
@login_required
def notification_mark_read(alert_id: int):
    verify_csrf()
    alert = db.session.get(AlertLog, alert_id)
    if alert:
        alert.is_read = True
        db.session.commit()
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True})
    return redirect(url_for("main.notifications"))


@bp.route("/notifications/read_all", methods=["POST"])
@login_required
def notifications_read_all():
    verify_csrf()
    AlertLog.query.filter_by(is_read=False).update({"is_read": True})
    db.session.commit()
    flash("All notifications marked as read.", "success")
    return redirect(url_for("main.notifications"))


@bp.route("/notifications/clear_read", methods=["POST"])
@login_required
def notifications_clear_read():
    verify_csrf()
    AlertLog.query.filter_by(is_read=True).delete()
    db.session.commit()
    flash("Read notifications cleared.", "success")
    return redirect(url_for("main.notifications"))


# ── Audit log ─────────────────────────────────────────────────────────────────

@bp.route("/audit-log")
@admin_required
def audit_log():
    page = request.args.get("page", 1, type=int)
    per_page = 50
    module_filter = request.args.get("module", "").strip()
    status_filter = request.args.get("status", "").strip()
    user_filter = request.args.get("user", "").strip()
    query = AuditLog.query
    if module_filter:
        query = query.filter_by(module=module_filter)
    if status_filter in ("ok", "error", "warning"):
        query = query.filter_by(status=status_filter)
    if user_filter:
        query = query.filter_by(user=user_filter)
    total = query.count()
    entries = query.order_by(AuditLog.timestamp.desc()).offset((page - 1) * per_page).limit(per_page).all()
    modules = [r[0] for r in db.session.query(AuditLog.module).distinct().order_by(AuditLog.module).all()]
    users = [r[0] for r in db.session.query(AuditLog.user).distinct().order_by(AuditLog.user).all()]
    return render_template(
        "audit_log.html",
        title="Audit Log",
        entries=entries,
        page=page,
        per_page=per_page,
        total=total,
        module_filter=module_filter,
        status_filter=status_filter,
        user_filter=user_filter,
        modules=modules,
        users=users,
    )


# ── Sales ────────────────────────────────────────────────────────────────────

@bp.route("/sales", methods=["GET", "POST"])
@login_required
def sales():
    field_defs = get_field_defs("sale")
    if request.method == "POST":
        verify_csrf()
        order_ref = request.form.get("order_ref", "").strip().upper()
        customer_name = request.form.get("customer_name", "").strip()
        order_date = request.form.get("order_date")
        if not order_ref or not customer_name or not order_date:
            flash("Order ref, customer and order date are required.", "danger")
            return redirect(url_for("main.sales"))
        if Sale.query.filter_by(order_ref=order_ref).first():
            flash("Order reference already exists.", "danger")
            return redirect(url_for("main.sales"))
        sale = Sale(
            order_ref=order_ref,
            customer_name=customer_name,
            order_date=parse_date(order_date),
            channel=request.form.get("channel", "").strip() or None,
            revenue=parse_decimal(request.form.get("revenue")),
            cost=parse_decimal(request.form.get("cost")),
            quantity=parse_int(request.form.get("quantity"), default=1),
        )
        db.session.add(sale)
        db.session.flush()
        save_field_values("sale", sale.id, field_defs, request.form)
        db.session.commit()
        fire_event("sale", "created", sale)
        flash("Sale created.", "success")
        return redirect(url_for("main.sales"))

    sales_list = Sale.query.order_by(Sale.order_date.desc(), Sale.created_at.desc()).all()
    fv_map = get_field_values_map("sale", [s.id for s in sales_list])
    monthly = dashboard_snapshot()["sales_by_month"]
    return render_template("sales.html", sales=sales_list, monthly=monthly, field_defs=field_defs, fv_map=fv_map)


@bp.route("/sales/<int:sale_id>/delete", methods=["POST"])
@login_required
def sales_delete(sale_id: int):
    verify_csrf()
    sale = db.session.get(Sale, sale_id)
    if sale:
        db.session.delete(sale)
        db.session.commit()
        flash("Sale deleted.", "success")
    return redirect(url_for("main.sales"))


@bp.route("/sales/export.pdf")
@login_required
def sales_export_pdf():
    try:
        _pdf_rl()
    except ImportError:
        flash("PDF export requires reportlab. Run: pip install reportlab", "danger")
        return redirect(url_for("main.sales"))

    sales_list = Sale.query.order_by(Sale.order_date.desc()).all()
    rows = [[s.order_ref, s.customer_name, s.order_date.strftime("%Y-%m-%d"),
             s.channel or "-", f"${float(s.revenue):,.2f}",
             f"${float(s.cost):,.2f}", f"${s.margin:,.2f}", str(s.quantity)]
            for s in sales_list]
    total_rev = sum(float(s.revenue) for s in sales_list)
    total_cost = sum(float(s.cost) for s in sales_list)
    total_margin = sum(s.margin for s in sales_list)
    total_qty = sum(s.quantity for s in sales_list)
    totals = ["Total", f"{len(sales_list)} orders", "", "",
              f"${total_rev:,.2f}", f"${total_cost:,.2f}",
              f"${total_margin:,.2f}", str(total_qty)]

    buf = _build_table_pdf(
        title="Sales",
        headers=["Order Ref", "Customer", "Date", "Channel",
                 "Revenue", "Cost", "Margin", "Qty"],
        rows=rows, totals=totals,
        subtitle=f"{len(sales_list)} orders  |  Revenue ${total_rev:,.2f}  |  Margin ${total_margin:,.2f}",
    )
    log_audit("export_pdf", "sales", message=f"Exported {len(sales_list)} sales as PDF")
    return _pdf_dl(buf, f"sales_{date.today().isoformat()}.pdf")


# ── Settings: Board ──────────────────────────────────────────────────────────

@bp.route("/settings")
@admin_required
def settings():
    workflow_count = Workflow.query.count()
    field_count = FieldDefinition.query.count()
    column_count = BoardColumn.query.count()
    user_count = User.query.count()
    sprint_count = Sprint.query.count()
    integration_count = IntegrationConfig.query.count()
    current_theme = get_integration_config("ui").get("theme", "dark")
    smtp_configured = bool(get_smtp_config().get("host"))
    audit_count = AuditLog.query.count()
    return render_template(
        "settings.html",
        workflow_count=workflow_count,
        field_count=field_count,
        column_count=column_count,
        user_count=user_count,
        sprint_count=sprint_count,
        integration_count=integration_count,
        current_theme=current_theme,
        smtp_configured=smtp_configured,
        audit_count=audit_count,
        THEMES=THEMES,
    )


@bp.route("/settings/board", methods=["GET", "POST"])
@admin_required
def settings_board():
    if request.method == "POST":
        verify_csrf()
        action = request.form.get("action")
        if action == "add":
            name = request.form.get("name", "").strip()
            color = request.form.get("color", "").strip()
            if not name:
                flash("Column name is required.", "danger")
            elif BoardColumn.query.filter_by(name=name).first():
                flash("A column with that name already exists.", "danger")
            else:
                pos = BoardColumn.query.count()
                db.session.add(BoardColumn(name=name, position=pos, color=color or None))
                db.session.commit()
                flash(f"Column '{name}' added.", "success")
        elif action == "rename":
            col_id = parse_int(request.form.get("col_id"))
            new_name = request.form.get("name", "").strip()
            col = db.session.get(BoardColumn, col_id)
            if col and new_name and new_name != col.name:
                if BoardColumn.query.filter_by(name=new_name).first():
                    flash("That name is already taken.", "danger")
                else:
                    Task.query.filter_by(status=col.name).update({"status": new_name})
                    col.name = new_name
                    col.color = request.form.get("color", "").strip() or None
                    db.session.commit()
                    flash(f"Column renamed to '{new_name}'.", "success")
            elif col:
                col.color = request.form.get("color", "").strip() or None
                db.session.commit()
                flash("Column updated.", "success")
        elif action == "delete":
            col_id = parse_int(request.form.get("col_id"))
            col = db.session.get(BoardColumn, col_id)
            if col:
                remaining = [c for c in get_task_columns() if c != col.name]
                fallback = remaining[0] if remaining else "Backlog"
                Task.query.filter_by(status=col.name).update({"status": fallback})
                db.session.delete(col)
                db.session.commit()
                flash(f"Column '{col.name}' deleted. Tasks moved to '{fallback}'.", "success")
        return redirect(url_for("main.settings_board"))

    columns = BoardColumn.query.order_by(BoardColumn.position.asc(), BoardColumn.id.asc()).all()
    column_colors = ["", "blue", "green", "yellow", "red", "purple"]
    return render_template("settings_board.html", columns=columns, column_colors=column_colors)


@bp.route("/settings/board/reorder", methods=["POST"])
@admin_required
def settings_board_reorder():
    verify_csrf()
    ids = (request.get_json(silent=True) or {}).get("ids", [])
    for pos, col_id in enumerate(ids):
        col = db.session.get(BoardColumn, int(col_id))
        if col:
            col.position = pos
    db.session.commit()
    return jsonify({"ok": True})


# ── Settings: Fields ─────────────────────────────────────────────────────────

@bp.route("/settings/fields", methods=["GET", "POST"])
@admin_required
def settings_fields():
    if request.method == "POST":
        verify_csrf()
        action = request.form.get("action")
        if action == "add":
            entity_type = request.form.get("entity_type", "").strip()
            name = request.form.get("name", "").strip()
            field_type = request.form.get("field_type", "text")
            options_raw = request.form.get("options", "").strip()
            required = request.form.get("required") == "on"
            if not entity_type or not name:
                flash("Entity type and field name are required.", "danger")
            elif entity_type not in ENTITY_TYPES:
                flash("Invalid entity type.", "danger")
            elif field_type not in FIELD_TYPES:
                flash("Invalid field type.", "danger")
            else:
                key = slugify(name)
                if FieldDefinition.query.filter_by(entity_type=entity_type, field_key=key).first():
                    flash(f"A field '{name}' already exists for that entity.", "danger")
                else:
                    pos = FieldDefinition.query.filter_by(entity_type=entity_type).count()
                    opts = None
                    if field_type == "select" and options_raw:
                        opts = json.dumps([o.strip() for o in options_raw.split(",") if o.strip()])
                    db.session.add(FieldDefinition(
                        entity_type=entity_type,
                        name=name,
                        field_key=key,
                        field_type=field_type,
                        options=opts,
                        position=pos,
                        required=required,
                    ))
                    db.session.commit()
                    flash(f"Field '{name}' added to {ENTITY_LABELS.get(entity_type, entity_type)}.", "success")
        elif action == "delete":
            fd_id = parse_int(request.form.get("fd_id"))
            fd = db.session.get(FieldDefinition, fd_id)
            if fd:
                db.session.delete(fd)
                db.session.commit()
                flash(f"Field '{fd.name}' deleted.", "success")
        return redirect(url_for("main.settings_fields"))

    selected = request.args.get("entity_type", ENTITY_TYPES[0])
    if selected not in ENTITY_TYPES:
        selected = ENTITY_TYPES[0]
    field_defs = get_field_defs(selected)
    return render_template(
        "settings_fields.html",
        entity_types=ENTITY_TYPES,
        entity_labels=ENTITY_LABELS,
        field_types=FIELD_TYPES,
        field_defs=field_defs,
        selected_entity=selected,
    )


# ── Settings: Workflows ──────────────────────────────────────────────────────

@bp.route("/settings/workflows", methods=["GET", "POST"])
@admin_required
def settings_workflows():
    columns = get_task_columns()
    if request.method == "POST":
        verify_csrf()
        action = request.form.get("action")
        if action == "add":
            name = request.form.get("name", "").strip()
            trigger_entity = request.form.get("trigger_entity", "")
            trigger_event = request.form.get("trigger_event", "")
            trigger_value = request.form.get("trigger_value", "").strip()
            action_type = request.form.get("action_type", "")
            if not name or not trigger_entity or not trigger_event or not action_type:
                flash("Name, trigger, and action are all required.", "danger")
            elif trigger_entity not in ENTITY_TYPES:
                flash("Invalid trigger entity.", "danger")
            else:
                # Support new flexible condition builder
                cond_field = request.form.get("cond_field", "").strip()
                cond_op = request.form.get("cond_op", "eq").strip()
                cond_value = request.form.get("cond_value", "").strip()
                condition = None
                if cond_field and cond_op and cond_value:
                    condition = json.dumps({"field": cond_field, "op": cond_op, "value": cond_value})
                elif trigger_value:
                    field_map = {"status_changed": "new_status", "stage_changed": "new_stage"}
                    cf = field_map.get(trigger_event, "value")
                    condition = json.dumps({"field": cf, "op": "eq", "value": trigger_value})

                cfg: dict = {}
                if action_type == "create_task":
                    cfg = {
                        "title": request.form.get("act_title", "").strip() or "Task from workflow",
                        "status": request.form.get("act_status", columns[0] if columns else "Backlog"),
                        "priority": request.form.get("act_priority", "Medium"),
                        "owner": request.form.get("act_owner", "").strip(),
                        "related_type": trigger_entity,
                        "related_name": request.form.get("act_related_name", "").strip(),
                        "description": request.form.get("act_description", "").strip(),
                    }
                elif action_type == "update_task_status":
                    cfg = {
                        "status": request.form.get("act_status", ""),
                        "related_name": request.form.get("act_related_name", "").strip(),
                    }
                elif action_type == "send_alert":
                    cfg = {
                        "title": request.form.get("act_alert_title", "").strip() or "Alert",
                        "detail": request.form.get("act_alert_detail", "").strip(),
                        "severity": request.form.get("act_alert_severity", "warning"),
                    }

                db.session.add(Workflow(
                    name=name,
                    enabled=True,
                    trigger_entity=trigger_entity,
                    trigger_event=trigger_event,
                    trigger_condition=condition,
                    action_type=action_type,
                    action_config=json.dumps(cfg),
                ))
                db.session.commit()
                flash(f"Workflow '{name}' created.", "success")
        elif action == "toggle":
            wf_id = parse_int(request.form.get("wf_id"))
            wf = db.session.get(Workflow, wf_id)
            if wf:
                wf.enabled = not wf.enabled
                db.session.commit()
                flash(f"Workflow '{wf.name}' {'enabled' if wf.enabled else 'disabled'}.", "success")
        elif action == "edit":
            wf_id = parse_int(request.form.get("wf_id"))
            wf = db.session.get(Workflow, wf_id)
            if not wf:
                flash("Workflow not found.", "danger")
            else:
                wf.name = request.form.get("name", wf.name).strip() or wf.name
                trigger_entity = request.form.get("trigger_entity", wf.trigger_entity)
                trigger_event = request.form.get("trigger_event", wf.trigger_event)
                if trigger_entity in ENTITY_TYPES:
                    wf.trigger_entity = trigger_entity
                wf.trigger_event = trigger_event
                action_type = request.form.get("action_type", wf.action_type)
                wf.action_type = action_type

                # Rebuild condition
                cond_field = request.form.get("cond_field", "").strip()
                cond_op = request.form.get("cond_op", "eq").strip()
                cond_value = request.form.get("cond_value", "").strip()
                trigger_value = request.form.get("trigger_value", "").strip()
                if cond_field and cond_op and cond_value:
                    wf.trigger_condition = json.dumps({"field": cond_field, "op": cond_op, "value": cond_value})
                elif trigger_value:
                    field_map = {"status_changed": "new_status", "stage_changed": "new_stage"}
                    cf = field_map.get(trigger_event, "value")
                    wf.trigger_condition = json.dumps({"field": cf, "op": "eq", "value": trigger_value})
                else:
                    wf.trigger_condition = None

                # Rebuild action config
                cfg: dict = {}
                if action_type == "create_task":
                    cfg = {
                        "title": request.form.get("act_title", "").strip() or "Task from workflow",
                        "status": request.form.get("act_status", columns[0] if columns else "Backlog"),
                        "priority": request.form.get("act_priority", "Medium"),
                        "owner": request.form.get("act_owner", "").strip(),
                        "related_type": wf.trigger_entity,
                        "related_name": request.form.get("act_related_name", "").strip(),
                        "description": request.form.get("act_description", "").strip(),
                    }
                elif action_type == "update_task_status":
                    cfg = {
                        "status": request.form.get("act_status", ""),
                        "related_name": request.form.get("act_related_name", "").strip(),
                    }
                elif action_type == "send_alert":
                    cfg = {
                        "title": request.form.get("act_alert_title", "").strip() or "Alert",
                        "detail": request.form.get("act_alert_detail", "").strip(),
                        "severity": request.form.get("act_alert_severity", "warning"),
                    }
                wf.action_config = json.dumps(cfg)
                db.session.commit()
                flash(f"Workflow '{wf.name}' updated.", "success")
        elif action == "delete":
            wf_id = parse_int(request.form.get("wf_id"))
            wf = db.session.get(Workflow, wf_id)
            if wf:
                db.session.delete(wf)
                db.session.commit()
                flash("Workflow deleted.", "success")
        return redirect(url_for("main.settings_workflows"))

    workflows = Workflow.query.order_by(Workflow.created_at.desc()).all()
    recent_runs = (
        WorkflowRun.query
        .order_by(WorkflowRun.triggered_at.desc())
        .limit(50)
        .all()
    )
    return render_template(
        "settings_workflows.html",
        workflows=workflows,
        recent_runs=recent_runs,
        entity_types=ENTITY_TYPES,
        entity_labels=ENTITY_LABELS,
        trigger_events=WORKFLOW_TRIGGER_EVENTS,
        workflow_actions=WORKFLOW_ACTIONS,
        entity_fields=ENTITY_FIELDS,
        condition_ops=CONDITION_OPS,
        priorities=PRIORITIES,
        columns=columns,
    )


# ── Settings: Users ──────────────────────────────────────────────────────────

@bp.route("/settings/users", methods=["GET", "POST"])
@admin_required
def settings_users():
    if request.method == "POST":
        verify_csrf()
        action = request.form.get("action")

        if action == "add":
            username = request.form.get("username", "").strip().lower()
            password = request.form.get("password", "")
            role = request.form.get("role", "viewer")
            if not username or not password:
                flash("Username and password are required.", "danger")
            elif len(password) < 10:
                flash("Password must be at least 10 characters.", "danger")
            elif User.query.filter_by(username=username).first():
                flash(f"Username '{username}' is already taken.", "danger")
            else:
                user = User(username=username, role=role)
                user.password_hash = generate_password_hash(password)
                db.session.add(user)
                db.session.commit()
                flash(f"User '{username}' created.", "success")

        elif action == "edit":
            user_id = parse_int(request.form.get("user_id"))
            user = db.session.get(User, user_id)
            if not user:
                flash("User not found.", "danger")
            else:
                new_role = request.form.get("role", user.role)
                user.role = new_role
                new_password = request.form.get("password", "").strip()
                if new_password:
                    if len(new_password) < 10:
                        flash("Password must be at least 10 characters.", "danger")
                        return redirect(url_for("main.settings_users"))
                    user.password_hash = generate_password_hash(new_password)
                db.session.commit()
                flash(f"User '{user.username}' updated.", "success")

        elif action == "delete":
            user_id = parse_int(request.form.get("user_id"))
            user = db.session.get(User, user_id)
            if not user:
                flash("User not found.", "danger")
            elif user.id == g.user.id:
                flash("You cannot delete your own account.", "danger")
            else:
                db.session.delete(user)
                db.session.commit()
                flash(f"User '{user.username}' deleted.", "success")

        return redirect(url_for("main.settings_users"))

    users = User.query.order_by(User.username.asc()).all()
    roles = ["admin", "manager", "viewer"]
    return render_template("settings_users.html", users=users, roles=roles)
