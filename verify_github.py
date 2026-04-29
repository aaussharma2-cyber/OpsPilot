import urllib.request, urllib.parse, http.cookiejar, re

BASE = "http://127.0.0.1:8000"
jar = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

r = opener.open(BASE + "/login")
body = r.read().decode("utf-8", errors="replace")
m = re.search(r'name="csrf_token" value="([^"]+)"', body)
csrf = m.group(1) if m else ""
login_data = urllib.parse.urlencode({"username": "admin", "password": "ChangeMe123!", "csrf_token": csrf}).encode()
opener.open(BASE + "/login", login_data)

r = opener.open(BASE + "/settings/github")
body = r.read().decode("utf-8", errors="replace")
print("GitHub settings page status:", r.status)
print("Has PAT field:", "Personal Access Token" in body)
print("Has create_repo action:", "create_repo" in body)
print("Has push action:", "push" in body)
print("Has how-it-works panel:", "How it works" in body)
print("Has token help link:", "github.com/settings/tokens" in body)

r2 = opener.open(BASE + "/settings")
b2 = r2.read().decode("utf-8", errors="replace")
print("Settings page has GitHub card:", "GitHub" in b2 and "settings_github" in b2)
print("Settings page has octopus emoji:", "\U0001f419" in b2 or "github" in b2.lower())
