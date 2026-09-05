"""
School Comms OAuth handlers for Gmail + WhatsApp integration
Phase 1: Bulletproof foundation for email + group message sync
"""

import os
import json
import requests
from urllib.parse import urlencode
from datetime import datetime, timedelta
from flask import request, redirect, jsonify, session
from functools import wraps
import hmac
import hashlib

# ============================================================================
# GMAIL OAUTH FLOW
# ============================================================================

def get_gmail_auth_url(state_token):
    """Generate Google OAuth authorization URL"""
    client_id = os.getenv('GMAIL_WEB_CLIENT_ID')
    redirect_uri = f"{os.getenv('MIRU_BASE_URL', 'http://localhost:5000')}/oauth/gmail/callback"

    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'scope': 'https://www.googleapis.com/auth/gmail.readonly email profile',
        'state': state_token,
        'access_type': 'offline',
        'prompt': 'consent'
    }

    return f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"


def exchange_gmail_code(code):
    """Exchange authorization code for access token"""
    client_id = os.getenv('GMAIL_WEB_CLIENT_ID')
    client_secret = os.getenv('GMAIL_WEB_CLIENT_SECRET')
    redirect_uri = f"{os.getenv('MIRU_BASE_URL', 'http://localhost:5000')}/oauth/gmail/callback"

    token_url = 'https://oauth2.googleapis.com/token'
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': redirect_uri,
        'grant_type': 'authorization_code',
        'code': code
    }

    response = requests.post(token_url, data=payload, timeout=10)
    response.raise_for_status()
    return response.json()


def refresh_gmail_token(refresh_token):
    """Refresh expired Gmail access token"""
    client_id = os.getenv('GMAIL_WEB_CLIENT_ID')
    client_secret = os.getenv('GMAIL_WEB_CLIENT_SECRET')

    token_url = 'https://oauth2.googleapis.com/token'
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token'
    }

    response = requests.post(token_url, data=payload, timeout=10)
    response.raise_for_status()
    data = response.json()
    data['refresh_token'] = refresh_token  # API doesn't return refresh token again
    return data


# ============================================================================
# WHATSAPP OAUTH FLOW
# ============================================================================

def get_whatsapp_auth_url(state_token, from_number):
    """Generate WhatsApp Business OAuth authorization URL"""
    # Note: This is WhatsApp Cloud API OAuth for Business Accounts
    # Users grant permission to read messages from WhatsApp groups

    client_id = os.getenv('WHATSAPP_BUSINESS_CLIENT_ID')
    redirect_uri = f"{os.getenv('MIRU_BASE_URL', 'http://localhost:5000')}/oauth/whatsapp/callback"

    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'state': f"{state_token}|{from_number}",  # Embed phone number in state
        'scope': 'whatsapp_business_messaging'  # Read permission for groups
    }

    return f"https://www.whatsapp.com/business/authorize?{urlencode(params)}"


def exchange_whatsapp_code(code):
    """Exchange authorization code for WhatsApp access token"""
    client_id = os.getenv('WHATSAPP_BUSINESS_CLIENT_ID')
    client_secret = os.getenv('WHATSAPP_BUSINESS_CLIENT_SECRET')
    redirect_uri = f"{os.getenv('MIRU_BASE_URL', 'http://localhost:5000')}/oauth/whatsapp/callback"

    token_url = 'https://graph.instagram.com/v18.0/oauth/access_token'  # WhatsApp uses Instagram graph
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': redirect_uri,
        'grant_type': 'authorization_code',
        'code': code
    }

    response = requests.post(token_url, data=payload, timeout=10)
    response.raise_for_status()
    return response.json()


# ============================================================================
# WEBHOOK VERIFICATION (for WhatsApp + Gmail push notifications)
# ============================================================================

def verify_whatsapp_webhook(token, signature, body):
    """
    Verify WhatsApp webhook signature
    Meta sends X-Hub-Signature header with HMAC-SHA256 of request body
    """
    app_secret = os.getenv('WHATSAPP_APP_SECRET')
    expected_signature = hmac.new(
        app_secret.encode(),
        body,
        hashlib.sha256
    ).hexdigest()

    received_signature = signature.replace('sha256=', '')
    return hmac.compare_digest(expected_signature, received_signature)


def verify_webhook_token(token):
    """Verify webhook verification token"""
    webhook_token = os.getenv('WHATSAPP_WEBHOOK_VERIFY_TOKEN')
    return token == webhook_token


# ============================================================================
# FLASK ROUTE HANDLERS (to be added to sms_service.py)
# ============================================================================

def register_oauth_routes(app, db):
    """Register OAuth routes with Flask app"""

    # ========================================
    # Gmail OAuth Routes
    # ========================================

    @app.route('/oauth/gmail/callback', methods=['GET'])
    def gmail_oauth_callback():
        """
        Gmail OAuth callback
        Receives: code, state
        """
        code = request.args.get('code')
        state_token = request.args.get('state')
        error = request.args.get('error')

        if error:
            return f"Gmail auth failed: {error}", 400

        if not code:
            return "Missing authorization code", 400

        try:
            # Exchange code for tokens
            token_data = exchange_gmail_code(code)
            access_token = token_data['access_token']
            refresh_token = token_data.get('refresh_token')
            expires_in = token_data.get('expires_in', 3600)

            # Get Gmail email from token
            headers = {'Authorization': f'Bearer {access_token}'}
            profile = requests.get(
                'https://www.googleapis.com/gmail/v1/users/me/profile',
                headers=headers,
                timeout=10
            ).json()
            gmail_email = profile['emailAddress']

            # Store in database (or session)
            expires_at = datetime.utcnow() + timedelta(seconds=expires_in)

            # Update user's Gmail connection
            # TODO: Link to user profile via state_token (user_id)
            db_result = db.table('user_profiles').update({
                'gmail_access_token': access_token,
                'gmail_refresh_token': refresh_token,
                'gmail_email': gmail_email,
                'gmail_token_expires': expires_at.isoformat(),
                'gmail_connected': True,
                'gmail_connected_at': datetime.utcnow().isoformat()
            }).eq('miru_token', state_token).execute()

            # Redirect back to onboarding with success flag
            return redirect(f"/onboarding_v2?gmail_connected=true&token={state_token}")

        except Exception as e:
            print(f"Gmail OAuth error: {e}")
            return f"Gmail auth error: {str(e)}", 500


    # ========================================
    # WhatsApp OAuth Routes
    # ========================================

    @app.route('/oauth/whatsapp/callback', methods=['GET'])
    def whatsapp_oauth_callback():
        """
        WhatsApp Business OAuth callback
        Receives: code, state (state includes user_id|phone_number)
        """
        code = request.args.get('code')
        state_parts = request.args.get('state', '|').split('|')
        state_token = state_parts[0] if state_parts else ''
        from_number = state_parts[1] if len(state_parts) > 1 else ''
        error = request.args.get('error')

        if error:
            return f"WhatsApp auth failed: {error}", 400

        if not code:
            return "Missing authorization code", 400

        try:
            # Exchange code for tokens
            token_data = exchange_whatsapp_code(code)
            access_token = token_data['access_token']
            # WhatsApp tokens typically don't have refresh; they're long-lived
            expires_in = token_data.get('expires_in', 5184000)  # 60 days default

            expires_at = datetime.utcnow() + timedelta(seconds=expires_in)

            # Store WhatsApp token
            db.table('school_wa_tokens').upsert({
                'from_number': from_number,
                'access_token': access_token,
                'refresh_token': '',  # WhatsApp tokens don't refresh
                'token_type': 'Bearer',
                'expires_at': expires_at.isoformat(),
                'connected_at': datetime.utcnow().isoformat()
            }).execute()

            # Update user profile
            db.table('user_profiles').update({
                'whatsapp_connected': True,
                'whatsapp_connected_at': datetime.utcnow().isoformat()
            }).eq('miru_token', state_token).execute()

            # Redirect back to onboarding with success flag
            return redirect(f"/onboarding_v2?whatsapp_connected=true&token={state_token}")

        except Exception as e:
            print(f"WhatsApp OAuth error: {e}")
            return f"WhatsApp auth error: {str(e)}", 500


    # ========================================
    # WhatsApp Webhook Routes
    # ========================================

    @app.route('/webhook/whatsapp', methods=['GET'])
    def whatsapp_webhook_verify():
        """
        WhatsApp webhook verification
        Meta sends: hub.mode, hub.challenge, hub.verify_token
        """
        mode = request.args.get('hub.mode')
        token = request.args.get('hub.verify_token')
        challenge = request.args.get('hub.challenge')

        if mode == 'subscribe' and verify_webhook_token(token):
            print("[WhatsApp] Webhook verified")
            return challenge, 200

        print("[WhatsApp] Webhook verification failed")
        return "Unauthorized", 403


    @app.route('/webhook/whatsapp', methods=['POST'])
    def whatsapp_webhook_receive():
        """
        WhatsApp webhook receiver
        Meta sends: messages, status updates, delivery confirmations
        """
        try:
            # Verify signature
            signature = request.headers.get('X-Hub-Signature', '')
            body = request.get_data()

            if not verify_whatsapp_webhook(os.getenv('WHATSAPP_WEBHOOK_VERIFY_TOKEN'), signature, body):
                return "Unauthorized", 403

            data = request.get_json()

            # Process messages
            if 'entry' in data:
                for entry in data['entry']:
                    if 'changes' in entry:
                        for change in entry['changes']:
                            if change['field'] == 'messages':
                                messages = change['value'].get('messages', [])
                                for msg in messages:
                                    process_whatsapp_message(msg, db)

            # Always respond with 200 to acknowledge receipt
            return jsonify({'status': 'ok'}), 200

        except Exception as e:
            print(f"WhatsApp webhook error: {e}")
            return jsonify({'error': str(e)}), 500


def process_whatsapp_message(msg, db):
    """
    Process incoming WhatsApp message
    Extract event, action, dates using NLP
    """
    from datetime import datetime

    msg_id = msg.get('id')
    from_number = msg.get('from')
    group_id = msg.get('group_id', '')
    text = msg.get('text', {}).get('body', '')
    timestamp = msg.get('timestamp')

    # Convert Unix timestamp to datetime
    msg_datetime = datetime.fromtimestamp(int(timestamp)) if timestamp else datetime.utcnow()

    # Store raw message
    db.table('school_wa_messages').insert({
        'from_number': from_number,
        'group_id': group_id,
        'message_text': text,
        'received_at': msg_datetime.isoformat(),
        'wa_message_id': msg_id,
        'category': None,  # Will be filled by NLP
        'confidence': 0.0
    }).execute()

    # Audit log
    db.table('school_audit_log').insert({
        'from_number': from_number,
        'operation': 'message_received',
        'details': {'msg_id': msg_id, 'group_id': group_id, 'text_preview': text[:100]},
        'status': 'success'
    }).execute()

    # TODO: Call NLP categorizer here
    # categorize_message(msg_id, text, db)


# ============================================================================
# API ENDPOINTS for frontend config
# ============================================================================

def register_config_routes(app):
    """Register API endpoints for OAuth client IDs (needed by frontend)"""

    @app.route('/api/config/gmail-client-id', methods=['GET'])
    def get_gmail_client_id():
        """Return Gmail OAuth client ID (public)"""
        return jsonify({
            'client_id': os.getenv('GMAIL_WEB_CLIENT_ID', '')
        })

    @app.route('/api/config/whatsapp-client-id', methods=['GET'])
    def get_whatsapp_client_id():
        """Return WhatsApp OAuth client ID (public)"""
        return jsonify({
            'client_id': os.getenv('WHATSAPP_BUSINESS_CLIENT_ID', '')
        })


# ============================================================================
# USAGE in sms_service.py:
# ============================================================================
# from school_oauth_handlers import register_oauth_routes, register_config_routes
#
# # In your Flask app initialization:
# register_oauth_routes(app, db)  # Register OAuth callbacks + webhook
# register_config_routes(app)     # Register config endpoints
