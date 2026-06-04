"""
Run this locally to get a fresh GMAIL_REFRESH_TOKEN.
Needs GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET set in your shell
(copy them from Railway env vars).

Usage:
  GMAIL_CLIENT_ID=xxx GMAIL_CLIENT_SECRET=yyy python reauth_gmail.py
"""
import os, urllib.parse, webbrowser, http.server, threading, requests

CLIENT_ID     = os.environ["GMAIL_CLIENT_ID"]
CLIENT_SECRET = os.environ["GMAIL_CLIENT_SECRET"]
REDIRECT_URI  = "http://localhost:8765/callback"
SCOPES        = "https://www.googleapis.com/auth/gmail.readonly"

auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode({
    "client_id":     CLIENT_ID,
    "redirect_uri":  REDIRECT_URI,
    "response_type": "code",
    "scope":         SCOPES,
    "access_type":   "offline",
    "prompt":        "consent",   # force consent so Google issues a fresh refresh token
})

code_holder = {}

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        if "code" in params:
            code_holder["code"] = params["code"][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"<h2>Got it! You can close this tab.</h2>")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"No code — try again.")
        threading.Thread(target=self.server.shutdown, daemon=True).start()

    def log_message(self, *args):
        pass

print(f"\nOpening browser for Google sign-in as mekala@gmail.com ...\n")
webbrowser.open(auth_url)

server = http.server.HTTPServer(("localhost", 8765), Handler)
server.serve_forever()

code = code_holder.get("code")
if not code:
    print("No code received — exiting.")
    raise SystemExit(1)

r = requests.post("https://oauth2.googleapis.com/token", data={
    "client_id":     CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "redirect_uri":  REDIRECT_URI,
    "grant_type":    "authorization_code",
    "code":          code,
})
r.raise_for_status()
tokens = r.json()

refresh_token = tokens.get("refresh_token")
if not refresh_token:
    print("ERROR: No refresh_token in response:", tokens)
    raise SystemExit(1)

print("\n" + "="*60)
print("SUCCESS — copy this into Railway as GMAIL_REFRESH_TOKEN:")
print("="*60)
print(refresh_token)
print("="*60 + "\n")
