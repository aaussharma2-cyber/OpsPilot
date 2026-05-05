import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest
from werkzeug.security import generate_password_hash

from opsdemo import _try_exec, create_app
from opsdemo import routes as main_routes
from opsdemo import services
from opsdemo.models import (
    AlertLog,
    AuditLog,
    Contact,
    InventoryItem,
    Invoice,
    Organization,
    Renewal,
    Sale,
    Task,
    User,
    db,
)


@pytest.fixture()
def app():
    db_file = ROOT / "instance" / "test_opsdemo.db"
    for suffix in ("", "-journal", "-wal", "-shm"):
        path = Path(f"{db_file}{suffix}")
        if path.exists():
            path.unlink()
    app = create_app(
        {
            "TESTING": True,
            "SQLALCHEMY_DATABASE_URI": f"sqlite:///{db_file}",
            "DEMO_USERNAME": "admin",
            "DEMO_PASSWORD": "ChangeMe123!",
            "SECRET_KEY": "test-secret",
            "CREDENTIAL_ENCRYPTION_KEY": "test-credential-key",
        }
    )
    yield app
    with app.app_context():
        db.session.remove()
        db.engine.dispose()
    for suffix in ("", "-journal", "-wal", "-shm"):
        path = Path(f"{db_file}{suffix}")
        if path.exists():
            path.unlink()


@pytest.fixture()
def client(app):
    return app.test_client()


def extract_csrf(html: str) -> str:
    patterns = [
        r'name="csrf_token" value="([a-f0-9]+)"',
        r'meta name="csrf-token" content="([a-f0-9]+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, html)
        if match:
            return match.group(1)
    raise AssertionError("CSRF token not found")


def login(client, username="admin", password="ChangeMe123!", expect_dashboard=True):
    login_page = client.get("/login")
    csrf = extract_csrf(login_page.get_data(as_text=True))
    response = client.post(
        "/login",
        data={"username": username, "password": password, "csrf_token": csrf},
        follow_redirects=True,
    )
    assert response.status_code == 200
    if expect_dashboard:
        assert b"Dashboard" in response.data
    return response


def logout(client):
    page = client.get("/dashboard")
    csrf = extract_csrf(page.get_data(as_text=True))
    response = client.post("/logout", data={"csrf_token": csrf}, follow_redirects=True)
    assert response.status_code == 200
    return response


def signup(client, org_name="Tenant Two", username="tenant2", email="tenant2@example.test"):
    page = client.get("/signup")
    csrf = extract_csrf(page.get_data(as_text=True))
    response = client.post(
        "/signup",
        data={
            "csrf_token": csrf,
            "org_name": org_name,
            "username": username,
            "email": email,
            "password": "TenantPass123!",
            "password2": "TenantPass123!",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"Dashboard" in response.data
    return response


def default_org_id():
    org = Organization.query.filter_by(slug="default").first()
    assert org is not None
    return org.id


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json["status"] == "ok"


def test_shopify_domain_normalization():
    assert services.normalize_shopify_domain("https://Demo-Store.myshopify.com/admin") == "demo-store.myshopify.com"
    assert services.validate_shopify_domain("demo-store.myshopify.com")
    assert not services.validate_shopify_domain("demo-store.myshopify.com.evil.test")


def test_migration_helper_rolls_back_failed_statement():
    class FailingConn:
        rolled_back = False

        def execute(self, statement):
            raise RuntimeError("simulated failed DDL")

        def commit(self):
            raise AssertionError("commit should not run after failed execute")

        def rollback(self):
            self.rolled_back = True

    conn = FailingConn()
    _try_exec(conn, "ALTER TABLE missing ADD COLUMN example INTEGER")
    assert conn.rolled_back is True


def test_login_seed_dashboard(client):
    login(client)
    dashboard = client.get("/dashboard")
    csrf = extract_csrf(dashboard.get_data(as_text=True))
    response = client.post("/seed", data={"csrf_token": csrf}, follow_redirects=True)
    assert response.status_code == 200
    assert b"Demo data loaded" in response.data
    assert b"Priority Alerts" in response.data
    assert b"Low-Stock Watchlist" in response.data


def test_task_create_and_move(client, app):
    login(client)
    page = client.get("/tasks")
    csrf = extract_csrf(page.get_data(as_text=True))
    response = client.post(
        "/tasks",
        data={
            "csrf_token": csrf,
            "title": "Follow up with supplier",
            "status": "Backlog",
            "priority": "High",
        },
        follow_redirects=True,
    )
    assert b"Task created" in response.data
    with app.app_context():
        task = Task.query.filter_by(title="Follow up with supplier").first()
        assert task is not None
        move = client.post(
            f"/tasks/{task.id}/move",
            json={"status": "Done"},
            headers={"X-CSRF-Token": csrf},
        )
        assert move.status_code == 200
        db.session.refresh(task)
        assert task.status == "Done"


def test_crm_inventory_invoice_renewal_sales_flow(client, app):
    login(client)

    crm_page = client.get("/crm")
    csrf = extract_csrf(crm_page.get_data(as_text=True))
    crm_response = client.post(
        "/crm",
        data={
            "csrf_token": csrf,
            "kind": "customer",
            "name": "Taylor Morgan",
            "company": "Acorn Co",
            "stage": "Won",
        },
        follow_redirects=True,
    )
    assert b"Taylor Morgan" in crm_response.data

    inv_page = client.get("/inventory")
    csrf = extract_csrf(inv_page.get_data(as_text=True))
    item_response = client.post(
        "/inventory",
        data={
            "csrf_token": csrf,
            "sku": "sku-x1",
            "name": "Starter Kit",
            "qty_on_hand": "3",
            "reorder_level": "5",
            "unit_cost": "8.50",
            "sale_price": "19.95",
        },
        follow_redirects=True,
    )
    assert b"Starter Kit" in item_response.data

    with app.app_context():
        item = InventoryItem.query.filter_by(sku="SKU-X1").first()
        assert item is not None
        item_id = item.id
    adjust_response = client.post(
        f"/inventory/{item_id}/adjust",
        data={"csrf_token": csrf, "delta": "4"},
        follow_redirects=True,
    )
    assert b"Stock adjusted" in adjust_response.data
    with app.app_context():
        item = db.session.get(InventoryItem, item_id)
        assert item.qty_on_hand == 7

    invoice_page = client.get("/invoices")
    csrf = extract_csrf(invoice_page.get_data(as_text=True))
    invoice_response = client.post(
        "/invoices",
        data={
            "csrf_token": csrf,
            "reference": "INV-T1",
            "party_name": "Acorn Co",
            "amount": "120.00",
            "due_date": "2026-04-30",
            "status": "Unpaid",
        },
        follow_redirects=True,
    )
    assert b"INV-T1" in invoice_response.data
    with app.app_context():
        invoice = Invoice.query.filter_by(reference="INV-T1").first()
        assert invoice is not None
        invoice_id = invoice.id
    paid_response = client.post(
        f"/invoices/{invoice_id}/mark_paid",
        data={"csrf_token": csrf},
        follow_redirects=True,
    )
    assert b"marked as paid" in paid_response.data
    with app.app_context():
        invoice = db.session.get(Invoice, invoice_id)
        assert invoice.status == "Paid"

    renewal_page = client.get("/renewals")
    csrf = extract_csrf(renewal_page.get_data(as_text=True))
    renewal_response = client.post(
        "/renewals",
        data={
            "csrf_token": csrf,
            "title": "SSL certificate",
            "provider": "Trust CA",
            "renew_on": "2026-05-15",
            "cost": "99.00",
        },
        follow_redirects=True,
    )
    assert b"SSL certificate" in renewal_response.data
    with app.app_context():
        renewal = Renewal.query.filter_by(title="SSL certificate").first()
        assert renewal is not None
        renewal_id = renewal.id
        original = renewal.renew_on
    roll_response = client.post(
        f"/renewals/{renewal_id}/complete",
        data={"csrf_token": csrf},
        follow_redirects=True,
    )
    assert b"rolled forward" in roll_response.data
    with app.app_context():
        renewal = db.session.get(Renewal, renewal_id)
        assert renewal.renew_on.year == original.year + 1

    sales_page = client.get("/sales")
    csrf = extract_csrf(sales_page.get_data(as_text=True))
    sales_response = client.post(
        "/sales",
        data={
            "csrf_token": csrf,
            "order_ref": "SO-T1",
            "customer_name": "Acorn Co",
            "order_date": "2026-04-20",
            "channel": "Direct",
            "revenue": "240.00",
            "cost": "110.00",
            "quantity": "5",
        },
        follow_redirects=True,
    )
    assert b"SO-T1" in sales_response.data
    assert b"Monthly revenue" in sales_response.data


def test_shopify_credentials_do_not_render_saved_token(client):
    login(client)
    page = client.get("/settings/integrations/shopify")
    csrf = extract_csrf(page.get_data(as_text=True))
    response = client.post(
        "/settings/integrations/shopify",
        data={
            "csrf_token": csrf,
            "action": "save",
            "shop_domain": "https://demo-store.myshopify.com/admin",
            "access_token": "shpat_secret_token",
            "client_id": "shopify-client-id",
            "client_secret": "shopify-client-secret",
        },
        follow_redirects=True,
    )
    html = response.get_data(as_text=True)
    assert "demo-store.myshopify.com" in html
    assert "shopify-client-id" in html
    assert "shpat_secret_token" not in html
    assert "shopify-client-secret" not in html
    assert "Leave blank to keep saved token" in html
    assert "Leave blank to keep saved secret" in html


def test_shopify_graphql_sync_mapping(client, app, monkeypatch):
    login(client)

    def fake_graphql(shop_domain, access_token, query, variables=None, max_retries=3):
        assert shop_domain == "demo-store.myshopify.com"
        assert access_token == "token"
        if "ShopProbe" in query:
            return {
                "data": {"shop": {"name": "Demo Store", "myshopifyDomain": "demo-store.myshopify.com"}}
            }, "2026-04"
        if "OpsPilotCustomers" in query:
            return {
                "data": {
                    "customers": {
                        "edges": [{
                            "node": {
                                "id": "gid://shopify/Customer/1",
                                "firstName": "Riley",
                                "lastName": "Stone",
                                "email": "riley@example.test",
                                "phone": None,
                                "defaultAddress": {"company": "Stone Co", "phone": "+100"},
                            }
                        }],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            }, "2026-04"
        return {
            "data": {
                "orders": {
                    "edges": [{
                        "node": {
                            "id": "gid://shopify/Order/1",
                            "name": "#1001",
                            "createdAt": "2026-04-20T12:00:00Z",
                            "sourceName": "web",
                            "displayFinancialStatus": "PENDING",
                            "customer": {
                                "firstName": "Riley",
                                "lastName": "Stone",
                                "email": "riley@example.test",
                                "phone": None,
                                "defaultAddress": {"company": "Stone Co", "phone": "+100"},
                            },
                            "currentTotalPriceSet": {"shopMoney": {"amount": "42.50", "currencyCode": "USD"}},
                            "totalPriceSet": {"shopMoney": {"amount": "42.50", "currencyCode": "USD"}},
                            "lineItems": {"nodes": [{"quantity": 2, "currentQuantity": 2}]},
                        }
                    }],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        }, "2026-04"

    monkeypatch.setattr(services, "_shopify_graphql", fake_graphql)

    with app.app_context():
        probe = services.test_shopify_connection("demo-store.myshopify.com", "token")
        customers = services.sync_shopify_customers("demo-store.myshopify.com", "token")
        orders = services.sync_shopify_orders("demo-store.myshopify.com", "token")

        assert probe["ok"] is True
        assert customers["created"] == 1
        assert orders["created_sales"] == 1
        assert orders["created_invoices"] == 1
        assert Contact.query.filter_by(email="riley@example.test").first().company == "Stone Co"
        assert Sale.query.filter_by(order_ref="#1001").first().quantity == 2
        assert float(Invoice.query.filter_by(reference="SHP-#1001").first().amount) == 42.50


# ── New feature tests ─────────────────────────────────────────────────────────

def test_app_starts_successfully(client):
    """App health endpoint responds with ok."""
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json["status"] == "ok"


def test_settings_page_no_github(client):
    """Settings page loads and no longer contains a GitHub section."""
    login(client)
    r = client.get("/settings")
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    assert "GitHub" not in html
    assert "Board Columns" in html


def test_github_route_removed(client):
    """The /settings/github route no longer exists."""
    login(client)
    r = client.get("/settings/github")
    assert r.status_code == 404


def test_notification_center_loads(client):
    """Notifications page loads and returns 200."""
    login(client)
    r = client.get("/notifications")
    assert r.status_code == 200
    assert b"Notifications" in r.data


def test_notification_center_shows_alerts(client, app):
    """Alerts stored in AlertLog appear on the notification center page."""
    login(client)
    with app.app_context():
        alert = AlertLog(
            severity="warning",
            title="Test alert",
            detail="Unit test detail",
            org_id=default_org_id(),
        )
        db.session.add(alert)
        db.session.commit()

    r = client.get("/notifications")
    assert b"Test alert" in r.data


def test_notification_mark_read(client, app):
    """Mark-as-read updates is_read flag."""
    login(client)
    with app.app_context():
        alert = AlertLog(severity="info", title="Read me", is_read=False, org_id=default_org_id())
        db.session.add(alert)
        db.session.commit()
        alert_id = alert.id

    notif_page = client.get("/notifications")
    csrf = extract_csrf(notif_page.get_data(as_text=True))
    r = client.post(
        f"/notifications/{alert_id}/read",
        data={"csrf_token": csrf},
        follow_redirects=True,
    )
    assert r.status_code == 200
    with app.app_context():
        updated = db.session.get(AlertLog, alert_id)
        assert updated.is_read is True


def test_audit_log_page_loads(client):
    """Audit log page loads and returns 200."""
    login(client)
    r = client.get("/audit-log")
    assert r.status_code == 200
    assert b"Audit Log" in r.data


def test_audit_log_entry_created(client, app):
    """log_audit() writes a row to AuditLog."""
    with app.app_context():
        services.log_audit("test_action", "tests", status="ok",
                           message="unit test entry", related_record="test-ref")
        entry = AuditLog.query.filter_by(action="test_action").first()
        assert entry is not None
        assert entry.module == "tests"
        assert entry.status == "ok"
        assert entry.related_record == "test-ref"


def test_login_creates_audit_log(client, app):
    """A successful login creates an audit log entry."""
    r = login(client)
    with app.app_context():
        entry = AuditLog.query.filter_by(action="login").first()
        assert entry is not None
        assert entry.user == "admin"
        assert entry.status == "ok"


def test_failed_login_creates_audit_log(client, app):
    """A failed login attempt creates an audit log entry."""
    login_page = client.get("/login")
    csrf = extract_csrf(login_page.get_data(as_text=True))
    client.post(
        "/login",
        data={"username": "admin", "password": "wrongpassword!", "csrf_token": csrf},
        follow_redirects=True,
    )
    with app.app_context():
        entry = AuditLog.query.filter_by(action="login_failed").first()
        assert entry is not None
        assert entry.status == "error"


def test_password_reset_flow_updates_password(client, app, monkeypatch):
    """Forgot-password creates a one-hour token and reset-password updates the hash."""
    captured = {}

    def fake_send(user, reset_url):
        captured["url"] = reset_url
        return True

    monkeypatch.setattr(main_routes, "_send_password_reset_email", fake_send)
    with app.app_context():
        user = User.query.filter_by(username="admin").first()
        user.email = "admin@example.test"
        db.session.commit()

    forgot_page = client.get("/forgot-password")
    csrf = extract_csrf(forgot_page.get_data(as_text=True))
    response = client.post(
        "/forgot-password",
        data={"csrf_token": csrf, "identifier": "admin@example.test"},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert "reset link has been sent" in response.get_data(as_text=True)
    assert "/reset-password/" in captured["url"]

    token = captured["url"].rsplit("/", 1)[-1]
    reset_page = client.get(f"/reset-password/{token}")
    csrf = extract_csrf(reset_page.get_data(as_text=True))
    response = client.post(
        f"/reset-password/{token}",
        data={
            "csrf_token": csrf,
            "password": "NewPass12345!",
            "password2": "NewPass12345!",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert "Password updated" in response.get_data(as_text=True)
    login(client, password="NewPass12345!")


def test_email_failure_without_smtp_creates_audit_log(client, app):
    """Attempting to send a renewal email with no SMTP config logs an error audit entry."""
    login(client)

    # Create a renewal to send from
    renewals_page = client.get("/renewals")
    csrf = extract_csrf(renewals_page.get_data(as_text=True))
    client.post(
        "/renewals",
        data={"csrf_token": csrf, "title": "Email test renewal", "renew_on": "2026-12-31", "cost": "50.00"},
        follow_redirects=True,
    )
    with app.app_context():
        renewal = Renewal.query.filter_by(title="Email test renewal").first()
        assert renewal is not None
        renewal_id = renewal.id

    renewals_page = client.get("/renewals")
    csrf = extract_csrf(renewals_page.get_data(as_text=True))
    client.post(
        f"/renewals/{renewal_id}/send_invoice",
        data={
            "csrf_token": csrf,
            "recipient_email": "test@example.com",
            "recipient_name": "Test User",
            "send_email": "1",
        },
        follow_redirects=True,
    )
    with app.app_context():
        # Should find an email_send audit entry with error status (no SMTP configured)
        entry = AuditLog.query.filter_by(action="email_send", status="error").first()
        assert entry is not None
        assert "SMTP" in (entry.message or "")


def test_invoice_pdf_export(client, app):
    """Invoice PDF export returns a PDF response."""
    login(client)

    inv_page = client.get("/invoices")
    csrf = extract_csrf(inv_page.get_data(as_text=True))
    client.post(
        "/invoices",
        data={
            "csrf_token": csrf,
            "reference": "PDF-TEST-001",
            "party_name": "PDF Corp",
            "amount": "500.00",
            "due_date": "2026-06-30",
            "status": "Unpaid",
        },
        follow_redirects=True,
    )
    with app.app_context():
        invoice = Invoice.query.filter_by(reference="PDF-TEST-001").first()
        assert invoice is not None
        invoice_id = invoice.id

    r = client.get(f"/invoices/{invoice_id}/export.pdf")
    assert r.status_code == 200
    assert r.content_type == "application/pdf"
    assert r.data[:4] == b"%PDF"

    with app.app_context():
        audit = AuditLog.query.filter_by(action="export_pdf", related_record="PDF-TEST-001").first()
        assert audit is not None


def test_shopify_missing_config_shows_setup_message(client, app):
    """Shopify page shows local setup warning when PUBLIC_BASE_URL is not set."""
    login(client)
    r = client.get("/settings/integrations/shopify")
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    assert "Running locally" in html or "public callback URL" in html


def test_core_module_pages_load(client):
    """Smoke test every major module page for template/query regressions."""
    login(client)
    for path in [
        "/dashboard",
        "/tasks",
        "/sprints",
        "/crm",
        "/vendors",
        "/assets",
        "/inventory",
        "/invoices",
        "/renewals",
        "/sales",
        "/reports",
        "/notifications",
        "/settings",
        "/settings/integrations",
        "/settings/integrations/shopify",
        "/settings/theme",
        "/settings/email",
        "/settings/billing",
        "/settings/board",
        "/settings/fields",
        "/settings/workflows",
        "/settings/users",
        "/settings/brand",
        "/audit-log",
    ]:
        response = client.get(path)
        assert response.status_code == 200, path


def test_signup_works_after_legacy_board_column_unique_migration():
    """Existing DBs with globally unique board columns must still allow signup."""
    db_file = ROOT / "instance" / "legacy_signup.db"
    for suffix in ("", "-journal", "-wal", "-shm"):
        path = Path(f"{db_file}{suffix}")
        if path.exists():
            path.unlink()
    conn = sqlite3.connect(db_file)
    conn.execute(
        "CREATE TABLE board_column ("
        "id INTEGER PRIMARY KEY, "
        "name VARCHAR(50) NOT NULL UNIQUE, "
        "position INTEGER NOT NULL, "
        "color VARCHAR(20)"
        ")"
    )
    for pos, name in enumerate(["Backlog", "In Progress", "Blocked", "Review", "Done"]):
        conn.execute(
            "INSERT INTO board_column (name, position, color) VALUES (?, ?, NULL)",
            (name, pos),
        )
    conn.commit()
    conn.close()

    try:
        legacy_app = create_app(
            {
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{db_file}",
                "DEMO_USERNAME": "admin",
                "DEMO_PASSWORD": "ChangeMe123!",
                "SECRET_KEY": "test-secret",
                "CREDENTIAL_ENCRYPTION_KEY": "test-credential-key",
            }
        )
        client = legacy_app.test_client()
        signup(client, org_name="Legacy Signup", username="legacyadmin", email="legacy@example.test")
        with legacy_app.app_context():
            rows = db.session.execute(db.text("PRAGMA index_list('board_column')")).all()
            unique_sets = []
            for row in rows:
                if row[2]:
                    cols = db.session.execute(db.text(f"PRAGMA index_info('{row[1]}')")).all()
                    unique_sets.append(tuple(col[2] for col in cols))
            assert ("name",) not in unique_sets
            assert ("name", "org_id") in unique_sets
            db.session.remove()
            db.engine.dispose()
    finally:
        for suffix in ("", "-journal", "-wal", "-shm"):
            path = Path(f"{db_file}{suffix}")
            if path.exists():
                path.unlink()


def test_self_signup_org_is_isolated_from_default_data(client, app):
    """A self-signed-up org cannot see the default org's data or credentials."""
    login(client)

    shopify_page = client.get("/settings/integrations/shopify")
    csrf = extract_csrf(shopify_page.get_data(as_text=True))
    client.post(
        "/settings/integrations/shopify",
        data={
            "csrf_token": csrf,
            "action": "save",
            "shop_domain": "default-store.myshopify.com",
            "access_token": "default-secret-token",
        },
        follow_redirects=True,
    )

    invoice_page = client.get("/invoices")
    csrf = extract_csrf(invoice_page.get_data(as_text=True))
    client.post(
        "/invoices",
        data={
            "csrf_token": csrf,
            "reference": "ORG-A-ONLY",
            "party_name": "Default Tenant",
            "amount": "99.00",
            "due_date": "2026-05-01",
            "status": "Unpaid",
        },
        follow_redirects=True,
    )
    with app.app_context():
        invoice = Invoice.query.filter_by(reference="ORG-A-ONLY").first()
        assert invoice is not None
        invoice_id = invoice.id

    logout(client)
    signup(client, org_name="Fresh Tenant", username="freshadmin", email="fresh@example.test")

    html = client.get("/invoices").get_data(as_text=True)
    assert "ORG-A-ONLY" not in html

    blocked_pdf = client.get(f"/invoices/{invoice_id}/export.pdf", follow_redirects=True)
    assert "Invoice not found" in blocked_pdf.get_data(as_text=True)

    audit_html = client.get("/audit-log").get_data(as_text=True)
    assert "ORG-A-ONLY" not in audit_html

    shopify_html = client.get("/settings/integrations/shopify").get_data(as_text=True)
    assert "default-store.myshopify.com" not in shopify_html
    assert "default-secret-token" not in shopify_html


def test_cross_org_invoice_mutation_by_id_is_blocked(client, app):
    """Guessing another org's invoice id must not allow writes."""
    login(client)
    invoice_page = client.get("/invoices")
    csrf = extract_csrf(invoice_page.get_data(as_text=True))
    client.post(
        "/invoices",
        data={
            "csrf_token": csrf,
            "reference": "MUTATION-LOCK",
            "party_name": "Default Tenant",
            "amount": "49.00",
            "due_date": "2026-05-01",
            "status": "Unpaid",
        },
        follow_redirects=True,
    )
    with app.app_context():
        invoice = Invoice.query.filter_by(reference="MUTATION-LOCK").first()
        invoice_id = invoice.id

    logout(client)
    signup(client, org_name="Write Block Tenant", username="writeblock", email="writeblock@example.test")
    csrf = extract_csrf(client.get("/invoices").get_data(as_text=True))
    client.post(f"/invoices/{invoice_id}/mark_paid", data={"csrf_token": csrf}, follow_redirects=True)

    with app.app_context():
        invoice = db.session.get(Invoice, invoice_id)
        assert invoice.status == "Unpaid"


def test_super_admin_cannot_access_tenant_data_pages(client, app):
    """Platform super admins can manage platform metadata, not tenant records."""
    with app.app_context():
        user = User(username="super", email="super@example.test", role="super_admin", is_active=True)
        user.password_hash = generate_password_hash("SuperPass123!")
        db.session.add(user)
        db.session.commit()

    response = login(client, username="super", password="SuperPass123!", expect_dashboard=False)
    assert b"Platform Administration" in response.data
    assert client.get("/platform/admin").status_code == 200
    assert client.get("/invoices").status_code == 403


def test_super_admin_can_manage_platform_users(client, app):
    """The owner can see all signups and deactivate tenant users from platform admin."""
    with app.app_context():
        super_user = User(username="owner", email="owner@example.test", role="super_admin", is_active=True)
        super_user.password_hash = generate_password_hash("OwnerPass123!")
        db.session.add(super_user)
        db.session.commit()
        tenant_user = User.query.filter_by(username="admin").first()
        tenant_user_id = tenant_user.id

    response = login(client, username="owner", password="OwnerPass123!", expect_dashboard=False)
    html = response.get_data(as_text=True)
    assert "All Users" in html
    assert "admin" in html
    csrf = extract_csrf(html)

    response = client.post(
        f"/platform/admin/users/{tenant_user_id}/role",
        data={"csrf_token": csrf, "role": "viewer"},
        follow_redirects=True,
    )
    assert response.status_code == 200
    with app.app_context():
        assert db.session.get(User, tenant_user_id).role == "viewer"

    csrf = extract_csrf(response.get_data(as_text=True))
    client.post(
        f"/platform/admin/users/{tenant_user_id}/toggle",
        data={"csrf_token": csrf},
        follow_redirects=True,
    )
    with app.app_context():
        assert db.session.get(User, tenant_user_id).is_active is False


def test_platform_billing_links_are_visible_to_org_admin(client, app):
    """Platform owner payment/donation links show on org billing without exposing tenant data."""
    with app.app_context():
        services.set_platform_integration_config(
            "platform_billing",
            {
                "pro_price": "$5/month",
                "pro_payment_url": "https://billing.example.test/pro",
                "donation_url": "https://buymeacoffee.example.test/opspilot",
                "billing_contact_email": "billing@example.test",
                "provider_note": "Hosted links only.",
            },
        )
    login(client)
    response = client.get("/settings/billing")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "https://billing.example.test/pro" in html
    assert "https://buymeacoffee.example.test/opspilot" in html
    assert "billing@example.test" in html
