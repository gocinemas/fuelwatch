"""
One-time Gmail OAuth setup.
Run:  python3 school_auth.py
Then copy the printed GMAIL_REFRESH_TOKEN into .env / Railway env vars.

You need a Google Cloud project with the Gmail API enabled and
OAuth 2.0 credentials (Desktop app type).
See: https://console.cloud.google.com/apis/credentials
"""

import json
import os
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer

CLIENT_ID     = os.environ.get("GMAIL_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("GMAIL_CLIENT_SECRET", "")
REDIRECT_URI  = "http://localhost:8765"
SCOPE         = "https://www.googleapis.com/auth/gmail.readonly"

auth_code = None


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        qs = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(qs)
        auth_code = params.get("code", [None])[0]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Auth complete - you can close this tab.")

    def log_message(self, *args):
        pass


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("Set GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET in your environment first.")
        print("  export GMAIL_CLIENT_ID=your_client_id")
        print("  export GMAIL_CLIENT_SECRET=your_client_secret")
        return

    url = (
        "https://accounts.google.com/o/oauth2/v2/auth?"
        + urllib.parse.urlencode({
            "client_id":     CLIENT_ID,
            "redirect_uri":  REDIRECT_URI,
            "response_type": "code",
            "scope":         SCOPE,
            "access_type":   "offline",
            "prompt":        "consent",
        })
    )
    print(f"Opening browser for Gmail authorisation…\n{url}\n")
    webbrowser.open(url)

    server = HTTPServer(("localhost", 8765), _Handler)
    server.handle_request()  # blocks until one request comes in

    if not auth_code:
        print("No auth code received.")
        return

    import urllib.request
    data = urllib.parse.urlencode({
        "code":          auth_code,
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri":  REDIRECT_URI,
        "grant_type":    "authorization_code",
    }).encode()
    req  = urllib.request.Request("https://oauth2.googleapis.com/token", data=data)
    resp = urllib.request.urlopen(req)
    tokens = json.loads(resp.read())

    refresh_token = tokens.get("refresh_token")
    if refresh_token:
        print("\n✅ Success! Add this to your .env / Railway environment:\n")
        print(f"GMAIL_REFRESH_TOKEN={refresh_token}")
    else:
        print("No refresh_token in response:", tokens)


if __name__ == "__main__":
    main()
