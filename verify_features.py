import urllib.request, urllib.parse, http.cookiejar, re

BASE = "http://127.0.0.1:8000"
jar = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

# Get login page for CSRF token
r = opener.open(BASE + "/login")
body = r.read().decode("utf-8", errors="replace")
m = re.search(r'name="csrf_token" value="([^"]+)"', body)
csrf = m.group(1) if m else ""
print("CSRF token found:", bool(csrf))

# Login
login_data = urllib.parse.urlencode({"username": "admin", "password": "ChangeMe123!", "csrf_token": csrf}).encode()
r2 = opener.open(BASE + "/login", login_data)
body2 = r2.read().decode("utf-8", errors="replace")
print("Logged in:", "Dashboard" in body2 or "OpsPilot" in body2, "| URL:", r2.url)

checks = {
    "Settings page": (BASE + "/settings", ["settings/email", "Email"]),
    "Email settings page": (BASE + "/settings/email", ["SMTP", "host", "port"]),
    "Renewals page": (BASE + "/renewals", ["contact_email", "open-invoice-modal", "Send Invoice", "open-edit-modal"]),
    "Invoices page": (BASE + "/invoices", ["renewal-prefill", "renewal-hint", "Link to renewal"]),
    "Dashboard": (BASE + "/", ["Dashboard", "OpsPilot"]),
}

all_pass = True
for name, (url, keywords) in checks.items():
    try:
        r = opener.open(url)
        body = r.read().decode("utf-8", errors="replace")
        found = {kw: kw in body for kw in keywords}
        all_ok = all(found.values())
        if not all_ok:
            all_pass = False
        status = "OK  " if all_ok else "FAIL"
        print(f"{status}: {name}")
        for kw, ok in found.items():
            if not ok:
                print(f"       MISSING: {kw!r}")
    except Exception as e:
        all_pass = False
        print(f"ERR : {name} -> {e}")

print()
print("ALL FEATURES VERIFIED" if all_pass else "SOME CHECKS FAILED — see above")
