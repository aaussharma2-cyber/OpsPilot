# OpsPilot Local Demo

A local-first, single-tenant operations workspace for small e-commerce and back-office workflows.

## What is included

This demo includes:

- Login with hashed password storage
- CSRF protection on all mutating requests
- Session cookie hardening (`HttpOnly`, `SameSite=Lax`)
- Basic security headers (`X-Frame-Options`, `nosniff`, `Referrer-Policy`, CSP)
- Seedable demo data
- Dashboard with:
  - revenue and margin summary
  - open invoice exposure
  - overdue invoice count
  - low-stock count
  - renewals due
  - asset expiry count
  - open task count
  - monthly sales chart
  - alert panel
- Tasks page with drag-and-drop kanban columns
- CRM page for leads/customers
- Vendors register
- Assets register with expiry tracking
- Inventory register with stock adjustments and low-stock highlighting
- Invoices with mark-paid workflow
- Renewals with roll-forward action
- Sales register with monthly visualization
- Shopify Admin GraphQL sync for customers and orders

## Tech stack

- Python 3.12+
- Flask
- SQLAlchemy / SQLite
- Jinja templates
- Vanilla JavaScript for kanban interactions
- Docker / Docker Compose support

## Local run (no Docker)

```bash
cd local_ops_demo
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open:

```text
http://127.0.0.1:8000
```

Default demo login:

- Username: `admin`
- Password: `ChangeMe123!`

Change them before real use:

```bash
export DEMO_USERNAME='your-admin'
export DEMO_PASSWORD='replace-this'
export SECRET_KEY='replace-with-a-random-long-secret'
export CREDENTIAL_ENCRYPTION_KEY='replace-with-a-second-random-secret'
python app.py
```

## Docker run

```bash
cd local_ops_demo
cp .env.example .env
# edit .env and replace SECRET_KEY / credentials
docker compose up --build
```

Open:

```text
http://127.0.0.1:8000
```

The SQLite database is stored in:

```text
./instance/opsdemo.db
```

That folder is mounted as a volume in Docker Compose so your data survives rebuilds.

## Feature smoke test

Run the automated checks:

```bash
cd local_ops_demo
pytest -q
```

The included tests verify:

- health endpoint
- login flow
- demo seed load
- dashboard render
- task creation and kanban status move
- CRM record creation
- inventory creation and stock adjustment
- invoice creation and mark-paid flow
- renewal creation and annual roll-forward
- sales entry and visualization page render

## Security notes

This is a strong local demo, not a finished enterprise platform. Current protections include:

- hashed passwords with Werkzeug
- CSRF token validation for POST requests and JSON kanban moves
- duplicate checks on unique references (SKU, invoice ref, sales order ref)
- role foundation through user roles (`admin` by default)
- security headers on every response
- non-root Docker container
- Shopify token encryption at rest
- configurable Shopify Admin API version (`SHOPIFY_API_VERSION`, default `2026-04`)

Before using this beyond local/internal pilot use, add:

- proper RBAC by module and action
- audit logs for critical actions
- database backups and restore automation
- TLS / reverse proxy
- production WSGI server (Gunicorn or similar)
- rate limiting / IP throttling
- password reset and user management screens
- granular permissions and approval flows
- immutable stock movement ledger instead of direct quantity adjustments
- invoice line items, taxes, and reconciliation
- file/document attachments
- deeper external integrations (Amazon, Xero, carriers, email)

## Shopify integration

The Shopify integration uses the Admin GraphQL API. Create or install a Shopify custom app with `read_customers` and `read_orders`, then save:

- Store domain: `mystore.myshopify.com`
- Admin API access token: `shpat_...`

For scheduled syncs, use:

```bash
flask --app app shopify-sync
```

## Suggested next milestones

### Milestone 1
Turn this demo into a cleaner internal system:
- user management
- role permissions
- audit trail
- search and filters
- edit screens
- export/import CSV

### Milestone 2
Make it e-commerce operationally useful:
- products and variants
- purchase orders and receiving
- sales orders and fulfillment
- stock movement history
- returns / RMA
- alerts by email / webhook

### Milestone 3
Make it production-ready:
- Postgres instead of SQLite
- background jobs
- reverse proxy and TLS
- SSO / MFA
- backups, observability, structured logs

## Project structure

```text
local_ops_demo/
  app.py
  Dockerfile
  docker-compose.yml
  requirements.txt
  README.md
  opsdemo/
    __init__.py
    config.py
    models.py
    routes.py
    services.py
    static/
    templates/
  tests/
    test_app.py
```
