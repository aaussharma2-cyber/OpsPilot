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

r2 = opener.open(BASE + "/settings")
b2 = r2.read().decode("utf-8", errors="replace")
# Find GitHub-related lines
for line in b2.splitlines():
    if "github" in line.lower() or "GitHub" in line:
        print(repr(line.strip()))
