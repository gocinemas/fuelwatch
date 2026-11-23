"""Reminders for the Factory page (humanagency.co/factory).

Three kinds of reminder, two ways to get them:
  - Bins: the evening before collection (from 6pm UK time)
  - Fuel: when the cheapest price near a postcode drops by 1p or more (checked once a day, from 8am)
  - Trains: your usual train out and back, if it's 5+ minutes late or cancelled (in the hour before it leaves)
  - Sent on WhatsApp (people signed in on the Factory page) or as phone/desktop notifications (web push)

Endpoints
  GET  /api/factory/me/reminders      WhatsApp settings for the signed-in person
  POST /api/factory/me/reminders      {bins:{on,postcode,uprn,label}, fuel:{on,postcode,fuel},
                                       trains:{on,from,from_name,to,to_name,out:"07:42",back:"17:48",weekends}}
  GET  /api/factory/push/key          public key the browser needs to subscribe (503 if not set up)
  POST /api/factory/push/reminders    {subscription, bins, fuel}  (both off = forget this device)
  POST /cron/factory-reminders        run now. Header X-Cron-Secret. ?dry=1 shows what would go out

Storage: one ma_details row per person or device, type="factory_reminders".
  device_id "wa:+447..." for WhatsApp, "push:<hash>" for a browser.
Runs every 15 minutes from scheduler_init. Each reminder goes out at most once a day.

Setup on Railway
  pip install pywebpush (in requirements.txt)
  VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY: run `python factory_reminders.py --new-keys` once and paste both in.
  Optional VAPID_SUBJECT (defaults to https://humanagency.co).
"""
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import jsonify, request

UK = ZoneInfo("Europe/London")
TYPE = "factory_reminders"
FACTORY_ORIGIN = "https://humanagency.co"
BINS_FROM_HOUR = 18      # send bin reminders from 6pm
BINS_UNTIL_HOUR = 22     # and not after 10pm, so a late restart doesn't wake anyone
FUEL_FROM_HOUR = 8
FUEL_DROP_P = 1.0        # pence per litre
FACTORY_URL = "https://humanagency.co/factory/"

TRAIN_LATE_MIN = 5
TRAIN_WINDOW_MIN = 60     # start watching a train an hour before it leaves

_hooks = {"send_wa": None, "cheapest_fuel": None, "departures": None}


# ── storage ──────────────────────────────────────────────────────────────────

def _sb():
    import library
    return library._sb()


def _get(device_id):
    rows = (_sb().table("ma_details").select("id,device_id,data")
            .eq("type", TYPE).eq("device_id", device_id).limit(1).execute().data or [])
    return rows[0] if rows else None


def _save(device_id, data):
    row = _get(device_id)
    if row:
        _sb().table("ma_details").update({"data": data}).eq("id", row["id"]).execute()
    else:
        _sb().table("ma_details").insert({"device_id": device_id, "type": TYPE,
                                          "label": TYPE, "data": data}).execute()


def _delete(device_id):
    row = _get(device_id)
    if row:
        _sb().table("ma_details").delete().eq("id", row["id"]).execute()


def _all_rows():
    return _sb().table("ma_details").select("id,device_id,data").eq("type", TYPE).execute().data or []


# ── cleaning what the page sends ─────────────────────────────────────────────

def _pc(raw):
    s = "".join(str(raw or "").upper().split())
    return s[:-3] + " " + s[-3:] if 5 <= len(s) <= 8 else ""


def _clean(body, old):
    """Merge the page's settings into what we already hold, keeping our own bookkeeping fields."""
    old = old or {}
    out = {}
    b = body.get("bins") or {}
    ob = old.get("bins") or {}
    if b.get("on") and _pc(b.get("postcode")) and str(b.get("uprn") or "").strip():
        same = ob.get("uprn") == str(b["uprn"]).strip()
        out["bins"] = {"on": True, "postcode": _pc(b["postcode"]), "uprn": str(b["uprn"]).strip()[:40],
                       "label": str(b.get("label") or "")[:120],
                       "last_sent": ob.get("last_sent") if same else None}
    f = body.get("fuel") or {}
    of = old.get("fuel") or {}
    if f.get("on") and _pc(f.get("postcode")):
        fuel = "diesel" if f.get("fuel") == "diesel" else "petrol"
        same = of.get("postcode") == _pc(f["postcode"]) and of.get("fuel") == fuel
        out["fuel"] = {"on": True, "postcode": _pc(f["postcode"]), "fuel": fuel,
                       "last_price": of.get("last_price") if same else None,
                       "last_check": of.get("last_check") if same else None}
    t = body.get("trains") or {}
    ot = old.get("trains") or {}
    crs = lambda x: str(x or "").strip().upper()
    hhmm = lambda x: str(x or "").strip() if len(str(x or "").strip()) == 5 and str(x)[2] == ":" and str(x).replace(":", "").isdigit() else ""
    if t.get("on") and len(crs(t.get("from"))) == 3 and len(crs(t.get("to"))) == 3 and (hhmm(t.get("out")) or hhmm(t.get("back"))):
        out["trains"] = {"on": True, "from": crs(t["from"]), "to": crs(t["to"]),
                         "from_name": str(t.get("from_name") or crs(t["from"]))[:60],
                         "to_name": str(t.get("to_name") or crs(t["to"]))[:60],
                         "out": hhmm(t.get("out")), "back": hhmm(t.get("back")),
                         "weekends": bool(t.get("weekends")), "sent": ot.get("sent") or {}}
    return out


def _public(data):
    """What the page is allowed to see back."""
    b, f, t = (data or {}).get("bins"), (data or {}).get("fuel"), (data or {}).get("trains")
    return {"bins": {"on": True, "postcode": b["postcode"], "label": b.get("label", "")} if b else {"on": False},
            "fuel": {"on": True, "postcode": f["postcode"], "fuel": f["fuel"]} if f else {"on": False},
            "trains": {k: t[k] for k in ("on", "from", "from_name", "to", "to_name", "out", "back", "weekends")} if t else {"on": False}}


# ── sending ──────────────────────────────────────────────────────────────────

def _send_push(sub, title, body):
    """Returns True if sent, False if the subscription is gone (caller forgets it), None on other errors."""
    priv = os.environ.get("VAPID_PRIVATE_KEY", "")
    if not priv:
        return None
    try:
        from pywebpush import webpush, WebPushException
    except ImportError:
        print("[factory-reminders] pywebpush not installed")
        return None
    try:
        webpush(subscription_info=sub,
                data=json.dumps({"title": title, "body": body, "url": FACTORY_URL}),
                vapid_private_key=priv,
                vapid_claims={"sub": os.environ.get("VAPID_SUBJECT", "https://humanagency.co")},
                ttl=6 * 3600)
        return True
    except WebPushException as e:
        code = getattr(getattr(e, "response", None), "status_code", None)
        if code in (404, 410):
            return False
        print(f"[factory-reminders] push error {code}: {str(e)[:160]}")
        return None
    except Exception as e:
        print(f"[factory-reminders] push error: {str(e)[:160]}")
        return None


def _deliver(device_id, data, title, text, wa_text, dry, log):
    if dry:
        log.append({"to": device_id[:12] + "…", "title": title, "text": text})
        return True
    if device_id.startswith("wa:"):
        if not _hooks["send_wa"]:
            return False
        _hooks["send_wa"]("whatsapp:" + device_id[3:], wa_text)
        return True
    ok = _send_push(data.get("sub") or {}, title, text)
    if ok is False:
        _delete(device_id)
    return bool(ok)


# ── the checks ───────────────────────────────────────────────────────────────

def _bins_message(data, today, now):
    b = data.get("bins")
    if not b or not b.get("on") or b.get("last_sent") == today.isoformat():
        return None
    if not (BINS_FROM_HOUR <= now.hour < BINS_UNTIL_HOUR):
        return None
    import bins_lookup
    body, code = bins_lookup.dates_for(b["postcode"], b["uprn"])
    b["last_sent"] = today.isoformat()          # checked today, whatever the answer
    if code != 200:
        return None
    tomorrow = (today + timedelta(days=1)).isoformat()
    names = [c.get("bin", "") for c in (body.get("collections") or []) if c.get("date") == tomorrow and c.get("bin")]
    if not names:
        return None
    what = " and ".join(n.lower() for n in names)
    what = what[0].upper() + what[1:]
    day = (today + timedelta(days=1)).strftime("%A")
    text = f"{what} tomorrow ({day}). Put them out tonight."
    return ("🗑️ Bins tomorrow", text,
            f"🗑️ *Bins tomorrow ({day})*\n{what}. Put them out tonight.\n\nReply *STOP BINS* to turn these off.")


def _fuel_message(data, today, now):
    f = data.get("fuel")
    if not f or not f.get("on") or f.get("last_check") == today.isoformat() or now.hour < FUEL_FROM_HOUR:
        return None
    if not _hooks["cheapest_fuel"]:
        return None
    price, station = _hooks["cheapest_fuel"](f["postcode"].replace(" ", ""), f["fuel"])
    f["last_check"] = today.isoformat()
    if price is None:
        return None
    price = round(float(price), 1)
    last = f.get("last_price")
    f["last_price"] = price
    if last is None or price > float(last) - FUEL_DROP_P:
        return None
    label = "Diesel" if f["fuel"] == "diesel" else "Petrol"
    brand = (station or {}).get("brand", "") or "a station near you"
    drop = float(last) - price
    text = f"{label} is down {drop:.1f}p to {price:.1f}p at {brand}, near {f['postcode']}."
    return (f"⛽ {label} down {drop:.1f}p", text,
            f"⛽ *{label} down {drop:.1f}p near {f['postcode']}*\n{price:.1f}p at {brand}.\n\nReply *STOP FUEL* to turn these off.")


def _train_messages(data, today, now):
    """Messages for the usual train out (from -> to) and back (to -> from), if late or cancelled."""
    t = data.get("trains")
    if not t or not t.get("on") or not _hooks["departures"]:
        return []
    if now.weekday() >= 5 and not t.get("weekends"):
        return []
    sent = {k: v for k, v in (t.get("sent") or {}).items() if k.startswith(today.isoformat())}
    msgs = []
    for leg, frm, to, frm_name, to_name in (("out", t["from"], t["to"], t["from_name"], t["to_name"]),
                                             ("back", t["to"], t["from"], t["to_name"], t["from_name"])):
        when = t.get(leg)
        if not when:
            continue
        dep = now.replace(hour=int(when[:2]), minute=int(when[3:]), second=0, microsecond=0)
        until = (dep - now).total_seconds() / 60
        if not (-2 <= until <= TRAIN_WINDOW_MIN):
            continue
        trains = _hooks["departures"](frm, to)
        if not trains:
            continue
        mine = next((x for x in trains if x.get("scheduled") == when), None)
        if not mine:
            continue
        late = int(mine.get("late_min") or 0)
        if mine.get("cancelled"):
            status = "cancelled"
        elif late >= TRAIN_LATE_MIN:
            status = f"late:{late // 5 * 5}"      # only message again if it gets 5+ minutes worse
        else:
            continue
        key = f"{today.isoformat()} {leg} {when}"
        if sent.get(key) == status:
            continue
        sent[key] = status
        route = f"{when} {frm_name} → {to_name}"
        if status == "cancelled":
            alt = next((x for x in trains if x.get("scheduled", "") > when and not x.get("cancelled")), None)
            extra = f" Next: {alt['scheduled']} ({alt.get('expected', 'on time').lower()})." if alt else ""
            text = f"Your {route} is cancelled.{extra}"
            title = f"🚆 {when} cancelled"
        else:
            text = f"Your {route} is running {late} min late, expected {mine.get('expected_at', '')}."
            title = f"🚆 {when} running {late} min late"
        msgs.append((title, text, f"🚆 *{text}*\n\nReply *STOP TRAINS* to turn these off."))
    t["sent"] = sent
    return msgs


def run_reminders(now=None, dry=False):
    now = now or datetime.now(UK)
    today = now.date()
    sent, log = 0, []
    for row in _all_rows():
        device_id, data = row.get("device_id", ""), dict(row.get("data") or {})
        before = json.dumps(data, sort_keys=True)
        try:
            msgs = [m for m in (_bins_message(data, today, now), _fuel_message(data, today, now)) if m]
            msgs += _train_messages(data, today, now)
            for msg in msgs:
                if _deliver(device_id, data, msg[0], msg[1], msg[2], dry, log):
                    sent += 1
            if not dry and json.dumps(data, sort_keys=True) != before and _get(device_id):
                _sb().table("ma_details").update({"data": data}).eq("id", row["id"]).execute()
        except Exception as e:
            print(f"[factory-reminders] {device_id[:12]}: {str(e)[:200]}")
    return {"sent": sent, "checked": len(log) if dry else None, "preview": log if dry else None}


def run_scheduled():
    """Called by the scheduler every 15 minutes."""
    if not _hooks["send_wa"]:
        return
    try:
        out = run_reminders()
        if out["sent"]:
            print(f"[factory-reminders] sent {out['sent']}")
    except Exception as e:
        print(f"[factory-reminders] run failed: {e}")


def stop_whatsapp(from_number, what="all"):
    """Turn off WhatsApp reminders. what = 'bins', 'fuel', 'trains' or 'all'. Returns True if something was on."""
    phone = (from_number or "").replace("whatsapp:", "").strip()
    device_id = "wa:" + phone
    row = _get(device_id)
    if not row:
        return False
    data = dict(row.get("data") or {})
    had = bool(data.get(what)) if what != "all" else bool(data.get("bins") or data.get("fuel") or data.get("trains"))
    for k in (("bins", "fuel", "trains") if what == "all" else (what,)):
        data.pop(k, None)
    if data.get("bins") or data.get("fuel") or data.get("trains"):
        _save(device_id, data)
    else:
        _delete(device_id)
    return had


# ── routes ───────────────────────────────────────────────────────────────────

def _cors_push(resp):
    if request.path.startswith("/api/factory/push/") and request.headers.get("Origin", "") == FACTORY_ORIGIN:
        resp.headers["Access-Control-Allow-Origin"] = FACTORY_ORIGIN
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Vary"] = "Origin"
    return resp


def register_factory_reminders(app, send_wa, cheapest_fuel):
    _hooks["send_wa"] = send_wa
    _hooks["cheapest_fuel"] = cheapest_fuel

    def _departures(frm, to):
        """Trains from frm that go on to call at to, via Miru's own departures route (cached 30s)."""
        with app.test_client() as c:
            d = c.get(f"/api/train/departures?crs={frm}&calling_at={to}").get_json(silent=True) or {}
        return d.get("trains") if d.get("filtered") else None
    _hooks["departures"] = _departures
    app.after_request(_cors_push)
    from miru_auth import session_phone

    @app.route("/api/factory/me/reminders", methods=["GET", "POST"])
    def factory_me_reminders():
        phone = session_phone()
        if not phone:
            return jsonify({"error": "Sign in first"}), 401
        device_id = "wa:" + phone
        row = _get(device_id)
        if request.method == "GET":
            return jsonify(_public(row["data"] if row else {}))
        body = request.get_json(silent=True) or {}
        data = _clean(body, row["data"] if row else {})
        if data:
            _save(device_id, data)
        else:
            _delete(device_id)
        turned_on = [k for k in ("bins", "fuel", "trains") if data.get(k) and not (row and (row.get("data") or {}).get(k))]
        if turned_on:
            names = {"bins": "bin day reminders", "fuel": "fuel price drops", "trains": "alerts when your trains are late"}
            what = ", ".join(names[k] for k in turned_on[:-1]) + (" and " if len(turned_on) > 1 else "") + names[turned_on[-1]]
            send_wa("whatsapp:" + phone,
                    f"🔔 You're set up for {what} from the Factory.\n\n"
                    "Reply *STOP BINS*, *STOP FUEL*, *STOP TRAINS* or *STOP REMINDERS* any time.")
        return jsonify(_public(data))

    @app.route("/api/factory/push/key", methods=["GET"])
    def factory_push_key():
        key = os.environ.get("VAPID_PUBLIC_KEY", "")
        return (jsonify({"key": key}), 200) if key and os.environ.get("VAPID_PRIVATE_KEY") else (jsonify({"error": "Notifications aren't set up yet"}), 503)

    @app.route("/api/factory/push/reminders", methods=["POST"])
    def factory_push_reminders():
        body = request.get_json(silent=True) or {}
        sub = body.get("subscription") or {}
        endpoint = str(sub.get("endpoint") or "")
        keys = sub.get("keys") or {}
        if not endpoint.startswith("https://") or not keys.get("p256dh") or not keys.get("auth"):
            return jsonify({"error": "Missing or invalid subscription"}), 400
        device_id = "push:" + hashlib.sha256(endpoint.encode()).hexdigest()[:40]
        row = _get(device_id)
        data = _clean(body, row["data"] if row else {})
        if not data:
            _delete(device_id)
            return jsonify(_public({}))
        data["sub"] = {"endpoint": endpoint, "keys": {"p256dh": keys["p256dh"], "auth": keys["auth"]}}
        _save(device_id, data)
        if not row:
            _send_push(data["sub"], "🔔 Reminders are on", "We'll only send what you picked. Turn them off any time on the Factory page.")
        return jsonify(_public(data))

    @app.route("/cron/factory-reminders", methods=["POST"])
    def cron_factory_reminders():
        secret = os.environ.get("CRON_SECRET", "")
        if not secret or request.headers.get("X-Cron-Secret", "") != secret:
            return jsonify({"error": "Unauthorized"}), 401
        return jsonify(run_reminders(dry=request.args.get("dry") == "1"))


if __name__ == "__main__" and "--new-keys" in sys.argv:
    # One-off: make the key pair for notifications. Paste both values into Railway.
    import base64
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    k = ec.generate_private_key(ec.SECP256R1())
    priv = base64.urlsafe_b64encode(k.private_numbers().private_value.to_bytes(32, "big")).rstrip(b"=").decode()
    pub = base64.urlsafe_b64encode(k.public_key().public_bytes(
        serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)).rstrip(b"=").decode()
    print("VAPID_PUBLIC_KEY=" + pub)
    print("VAPID_PRIVATE_KEY=" + priv)
