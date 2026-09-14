"""
Personal Messages Dashboard — Miru Module
==========================================

Interactive dashboard to view and manage:
- WhatsApp messages + Claude extraction
- TODOs (filtered by status, priority, category)
- School-specific action items
- Message search and history

Integrates with Supabase tables:
- messages, message_processing, todos, auto_responses
"""

import os
import json
from datetime import datetime, timedelta
from flask import Blueprint, render_template_string, request, jsonify
from supabase import create_client

# ─────────────────────────────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────────────────────────────

pm_dashboard_bp = Blueprint('personal_messages_dashboard', __name__)

def get_supabase():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY required")
    return create_client(url, key)

# ─────────────────────────────────────────────────────────────────────
# 1. DASHBOARD PAGE
# ─────────────────────────────────────────────────────────────────────

@pm_dashboard_bp.route('/personal-messages', methods=['GET'])
def personal_messages_dashboard():
    """Render the personal messages dashboard."""
    return render_template_string(DASHBOARD_HTML)

# ─────────────────────────────────────────────────────────────────────
# 2. API ENDPOINTS (Data)
# ─────────────────────────────────────────────────────────────────────

@pm_dashboard_bp.route('/api/personal-messages', methods=['GET'])
def get_messages():
    """Get paginated messages with Claude extractions."""
    try:
        sb = get_supabase()

        limit = int(request.args.get('limit', 20))
        offset = int(request.args.get('offset', 0))
        search = request.args.get('search', '')
        status = request.args.get('status', '')

        # Build query
        query = sb.table('messages').select('*')

        # Filter by status if provided
        if status:
            query = query.eq('status', status)

        # Search in message body
        if search:
            # Note: Supabase LIKE search requires special syntax
            query = query.ilike('body', f'%{search}%')

        # Order by date, paginate
        result = query.order('created_at', desc=True).range(offset, offset + limit - 1).execute()

        # Fetch processing details for each message
        messages = []
        for msg in result.data:
            proc_result = sb.table('message_processing').select('*').eq(
                'message_id', msg['id']
            ).execute()

            processing = proc_result.data[0] if proc_result.data else {}

            messages.append({
                'id': msg['id'],
                'body': msg['body'],
                'from': msg['from_number'],
                'created_at': msg['created_at'],
                'status': msg['status'],
                'category': processing.get('category', 'unknown'),
                'summary': processing.get('summary', ''),
                'school_related': processing.get('raw_response', {}).get('school_related', False),
                'needs_response': processing.get('needs_response', False)
            })

        return jsonify({
            'total': len(messages),
            'messages': messages,
            'limit': limit,
            'offset': offset
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@pm_dashboard_bp.route('/api/personal-todos', methods=['GET'])
def get_todos():
    """Get TODOs with filtering."""
    try:
        sb = get_supabase()

        status = request.args.get('status', 'open')  # Default: show open TODOs
        priority = request.args.get('priority', '')
        school_only = request.args.get('school', 'false').lower() == 'true'

        # Build query
        query = sb.table('todos').select('*')

        if status:
            query = query.eq('status', status)

        if priority:
            query = query.eq('priority', priority)

        # Order by due date, then priority
        result = query.order('due_date', desc=False).execute()

        todos = []
        for todo in result.data:
            # Get source message to check if school-related
            is_school = False
            if todo.get('source_message_id'):
                msg_result = sb.table('messages').select('*').eq(
                    'id', todo['source_message_id']
                ).execute()
                if msg_result.data:
                    proc_result = sb.table('message_processing').select('*').eq(
                        'message_id', todo['source_message_id']
                    ).execute()
                    if proc_result.data:
                        is_school = proc_result.data[0].get('raw_response', {}).get('school_related', False)

            if school_only and not is_school:
                continue

            todos.append({
                'id': todo['id'],
                'text': todo['text'],
                'due_date': todo['due_date'],
                'priority': todo['priority'],
                'status': todo['status'],
                'created_at': todo['created_at'],
                'school_related': is_school
            })

        return jsonify({'todos': todos, 'total': len(todos)})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@pm_dashboard_bp.route('/api/personal-todos/<todo_id>', methods=['PATCH'])
def update_todo(todo_id):
    """Update a TODO status."""
    try:
        sb = get_supabase()
        data = request.get_json()

        update_data = {}
        if 'status' in data:
            update_data['status'] = data['status']
            if data['status'] == 'done':
                update_data['completed_at'] = datetime.utcnow().isoformat()

        if 'priority' in data:
            update_data['priority'] = data['priority']

        update_data['updated_at'] = datetime.utcnow().isoformat()

        result = sb.table('todos').update(update_data).eq('id', todo_id).execute()

        if not result.data:
            return jsonify({'error': 'TODO not found'}), 404

        return jsonify(result.data[0])

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@pm_dashboard_bp.route('/api/personal-school-items', methods=['GET'])
def get_school_items():
    """Get school-specific action items (payments, events, deadlines)."""
    try:
        sb = get_supabase()

        # Get school-related TODOs
        todo_result = sb.table('todos').select('*').execute()

        school_items = []
        for todo in todo_result.data:
            # Check if related to school
            if todo.get('source_message_id'):
                proc_result = sb.table('message_processing').select('*').eq(
                    'message_id', todo['source_message_id']
                ).execute()
                if proc_result.data:
                    is_school = proc_result.data[0].get('raw_response', {}).get('school_related', False)
                    if is_school:
                        school_items.append({
                            'type': 'todo',
                            'text': todo['text'],
                            'due_date': todo['due_date'],
                            'priority': todo['priority'],
                            'status': todo['status']
                        })

        return jsonify({'school_items': school_items, 'total': len(school_items)})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@pm_dashboard_bp.route('/api/personal-stats', methods=['GET'])
def get_stats():
    """Get dashboard statistics."""
    try:
        sb = get_supabase()

        # Count messages by status
        msg_result = sb.table('messages').select('status').execute()
        messages_total = len(msg_result.data)

        # Count TODOs by status
        todo_result = sb.table('todos').select('status', 'priority').execute()
        todos_total = len(todo_result.data)
        todos_open = sum(1 for t in todo_result.data if t['status'] == 'open')
        todos_high = sum(1 for t in todo_result.data if t['priority'] == 'high')

        # Count overdue TODOs
        today = datetime.now().date().isoformat()
        todos_overdue = sum(1 for t in todo_result.data
                          if t.get('due_date') and t['due_date'] < today and t['status'] == 'open')

        return jsonify({
            'messages': {
                'total': messages_total,
                'processed': sum(1 for m in msg_result.data if m['status'] == 'processed')
            },
            'todos': {
                'total': todos_total,
                'open': todos_open,
                'high_priority': todos_high,
                'overdue': todos_overdue
            }
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─────────────────────────────────────────────────────────────────────
# 3. HTML TEMPLATE
# ─────────────────────────────────────────────────────────────────────

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Personal Messages — Miru</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f5f5;
            color: #333;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }

        header {
            background: white;
            border-bottom: 1px solid #e0e0e0;
            padding: 20px 0;
            margin-bottom: 30px;
        }

        header h1 {
            font-size: 28px;
            margin-bottom: 10px;
        }

        header p {
            color: #666;
            font-size: 14px;
        }

        .tabs {
            display: flex;
            gap: 20px;
            margin-bottom: 30px;
            border-bottom: 1px solid #e0e0e0;
        }

        .tab {
            padding: 10px 0;
            cursor: pointer;
            border-bottom: 3px solid transparent;
            color: #666;
            font-weight: 500;
        }

        .tab.active {
            color: #007bff;
            border-bottom-color: #007bff;
        }

        .tab-content {
            display: none;
        }

        .tab-content.active {
            display: block;
        }

        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }

        .stat-card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            border-left: 4px solid #007bff;
        }

        .stat-card.warning {
            border-left-color: #ff9800;
        }

        .stat-card.danger {
            border-left-color: #f44336;
        }

        .stat-value {
            font-size: 28px;
            font-weight: bold;
            color: #007bff;
        }

        .stat-card.warning .stat-value {
            color: #ff9800;
        }

        .stat-card.danger .stat-value {
            color: #f44336;
        }

        .stat-label {
            color: #666;
            font-size: 14px;
            margin-top: 5px;
        }

        .filter-bar {
            background: white;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 20px;
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }

        .filter-bar input,
        .filter-bar select {
            padding: 8px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
        }

        .message-item,
        .todo-item {
            background: white;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 15px;
            border-left: 4px solid #ddd;
        }

        .message-item {
            border-left-color: #2196f3;
        }

        .todo-item {
            border-left-color: #4caf50;
        }

        .todo-item.high {
            border-left-color: #f44336;
        }

        .message-body {
            font-size: 15px;
            margin-bottom: 10px;
        }

        .message-meta {
            display: flex;
            gap: 15px;
            font-size: 12px;
            color: #666;
            flex-wrap: wrap;
        }

        .badge {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 500;
            background: #f0f0f0;
            color: #333;
        }

        .badge.school {
            background: #e3f2fd;
            color: #1976d2;
        }

        .badge.high {
            background: #ffebee;
            color: #c62828;
        }

        .todo-text {
            font-size: 15px;
            margin-bottom: 8px;
        }

        .todo-actions {
            display: flex;
            gap: 10px;
            margin-top: 10px;
        }

        .todo-actions button {
            padding: 6px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            background: white;
            cursor: pointer;
            font-size: 12px;
        }

        .todo-actions button:hover {
            background: #f5f5f5;
        }

        .loading {
            text-align: center;
            padding: 40px;
            color: #666;
        }

        .error {
            background: #ffebee;
            color: #c62828;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 15px;
        }
    </style>
</head>
<body>
    <header>
        <div class="container">
            <h1>📱 Personal Messages</h1>
            <p>Manage messages, TODOs, and school items all in one place</p>
        </div>
    </header>

    <div class="container">
        <!-- Stats -->
        <div id="stats" class="stats"></div>

        <!-- Tabs -->
        <div class="tabs">
            <div class="tab active" onclick="switchTab('todos')">✓ TODOs</div>
            <div class="tab" onclick="switchTab('school')">🏫 School Items</div>
            <div class="tab" onclick="switchTab('messages')">💬 Messages</div>
        </div>

        <!-- TODOs Tab -->
        <div id="todos" class="tab-content active">
            <div class="filter-bar">
                <select id="status-filter" onchange="loadTodos()">
                    <option value="open">Open</option>
                    <option value="done">Done</option>
                    <option value="snoozed">Snoozed</option>
                </select>
                <select id="priority-filter" onchange="loadTodos()">
                    <option value="">All Priorities</option>
                    <option value="high">High</option>
                    <option value="medium">Medium</option>
                    <option value="low">Low</option>
                </select>
            </div>
            <div id="todos-list"></div>
        </div>

        <!-- School Items Tab -->
        <div id="school" class="tab-content">
            <div id="school-list"></div>
        </div>

        <!-- Messages Tab -->
        <div id="messages" class="tab-content">
            <div class="filter-bar">
                <input type="text" id="search-input" placeholder="Search messages..." onchange="loadMessages()">
                <select id="message-status-filter" onchange="loadMessages()">
                    <option value="">All Messages</option>
                    <option value="processed">Processed</option>
                    <option value="failed">Failed</option>
                </select>
            </div>
            <div id="messages-list"></div>
        </div>
    </div>

    <script>
        function switchTab(tab) {
            document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
            document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
            document.getElementById(tab).classList.add('active');
            event.target.classList.add('active');

            if (tab === 'todos') loadTodos();
            if (tab === 'messages') loadMessages();
            if (tab === 'school') loadSchoolItems();
        }

        async function loadStats() {
            try {
                const response = await fetch('/api/personal-stats');
                const data = await response.json();

                document.getElementById('stats').innerHTML = `
                    <div class="stat-card">
                        <div class="stat-value">${data.todos.open}</div>
                        <div class="stat-label">Open TODOs</div>
                    </div>
                    <div class="stat-card warning">
                        <div class="stat-value">${data.todos.high_priority}</div>
                        <div class="stat-label">High Priority</div>
                    </div>
                    <div class="stat-card danger">
                        <div class="stat-value">${data.todos.overdue}</div>
                        <div class="stat-label">Overdue</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">${data.messages.total}</div>
                        <div class="stat-label">Messages Processed</div>
                    </div>
                `;
            } catch (e) {
                console.error(e);
            }
        }

        async function loadTodos() {
            const status = document.getElementById('status-filter').value;
            const priority = document.getElementById('priority-filter').value;

            try {
                const url = `/api/personal-todos?status=${status}${priority ? '&priority=' + priority : ''}`;
                const response = await fetch(url);
                const data = await response.json();

                let html = '';
                for (const todo of data.todos) {
                    const daysLeft = todo.due_date ? Math.floor((new Date(todo.due_date) - new Date()) / 86400000) : null;
                    html += `
                        <div class="todo-item ${todo.priority === 'high' ? 'high' : ''}">
                            <div class="todo-text">${todo.text}</div>
                            <div class="message-meta">
                                ${todo.due_date ? `<span>📅 ${todo.due_date}${daysLeft !== null && daysLeft < 0 ? ' ⚠️ OVERDUE' : ''}</span>` : ''}
                                <span class="badge ${todo.priority === 'high' ? 'high' : ''}">${todo.priority}</span>
                                ${todo.school_related ? '<span class="badge school">School</span>' : ''}
                            </div>
                            <div class="todo-actions">
                                ${todo.status !== 'done' ? `<button onclick="updateTodo('${todo.id}', 'done')">✓ Done</button>` : ''}
                                <button onclick="updateTodo('${todo.id}', 'snoozed')">Snooze</button>
                            </div>
                        </div>
                    `;
                }
                document.getElementById('todos-list').innerHTML = html || '<p>No TODOs</p>';
            } catch (e) {
                console.error(e);
            }
        }

        async function loadMessages() {
            const search = document.getElementById('search-input').value;
            const status = document.getElementById('message-status-filter').value;

            try {
                const url = `/api/personal-messages?search=${search}${status ? '&status=' + status : ''}`;
                const response = await fetch(url);
                const data = await response.json();

                let html = '';
                for (const msg of data.messages) {
                    html += `
                        <div class="message-item">
                            <div class="message-body">${msg.body}</div>
                            <div class="message-meta">
                                <span>📱 ${msg.from}</span>
                                <span>⏰ ${new Date(msg.created_at).toLocaleString()}</span>
                                <span class="badge">${msg.category}</span>
                                ${msg.school_related ? '<span class="badge school">School</span>' : ''}
                                ${msg.summary ? `<span>${msg.summary}</span>` : ''}
                            </div>
                        </div>
                    `;
                }
                document.getElementById('messages-list').innerHTML = html || '<p>No messages</p>';
            } catch (e) {
                console.error(e);
            }
        }

        async function loadSchoolItems() {
            try {
                const response = await fetch('/api/personal-school-items');
                const data = await response.json();

                let html = '';
                for (const item of data.school_items) {
                    html += `
                        <div class="todo-item">
                            <div class="todo-text">${item.text}</div>
                            <div class="message-meta">
                                ${item.due_date ? `<span>📅 ${item.due_date}</span>` : ''}
                                <span class="badge high">${item.priority}</span>
                                <span class="badge school">School</span>
                            </div>
                        </div>
                    `;
                }
                document.getElementById('school-list').innerHTML = html || '<p>No school items</p>';
            } catch (e) {
                console.error(e);
            }
        }

        async function updateTodo(id, status) {
            try {
                await fetch(`/api/personal-todos/${id}`, {
                    method: 'PATCH',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({status})
                });
                loadTodos();
                loadStats();
            } catch (e) {
                console.error(e);
            }
        }

        // Initial load
        loadStats();
        loadTodos();
    </script>
</body>
</html>
"""

def register_personal_messages_endpoints(app):
    """Register dashboard blueprint with Flask app."""
    app.register_blueprint(pm_dashboard_bp)
    print("✅ Personal Messages Dashboard registered")
