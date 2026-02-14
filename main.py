import asyncio
import logging
import os
import shutil
import subprocess
import psutil
import re
import time
import threading
import random
import requests
import zipfile
import urllib.parse
import html
import sys
import json
import secrets
import traceback
from flask import Flask, request, render_template_string, jsonify, send_file
from datetime import datetime, timedelta
import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest

# --- Configuration ---
BOT_TOKEN = "8394291829:AAFNcUZm9ahuR5z2OmIBfnd_3sVgRFUauD8"
ADMIN_IDS = [7170744706, 7558661858, 7150744706]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USER_FILES_DIR = os.path.join(BASE_DIR, "user_files")
DB_PATH = os.path.join(BASE_DIR, "database", "bot_database.db")
ENV_FILES_DIR = os.path.join(BASE_DIR, "env_vars")
PERSISTENT_STATE_FILE = os.path.join(BASE_DIR, "bot_data.json")
MARKETPLACE_DIR = os.path.join(BASE_DIR, "marketplace")

SERVER_DOMAIN = "https://ready-cloris-hsthgfgh-f44ea371.koyeb.app"
WEB_PORT = 8000

# Plan Limits - نظام الخطط المرنة (الافتراضية)
DEFAULT_PLANS = {
    "free": {"name": "مجانية ⭐️", "max_files": 5, "max_folders": 2, "max_file_size": 5 * 1024 * 1024, "max_running": 2, "price": 0},
    "pro": {"name": "PRO 💎", "max_files": 50, "max_folders": 10, "max_file_size": 50 * 1024 * 1024, "max_running": 20, "price": 10},
}
# سيتم تحميل الخطط المخصصة من قاعدة البيانات

# نظام الأدمن المتعدد - الصلاحيات
ADMIN_ROLES = {
    "super_admin": {"name": "سوبر أدمن 👑", "perms": ["all"]},
    "admin": {"name": "أدمن 🛡", "perms": ["manage_users", "manage_bots", "broadcast", "spy", "coupons", "give_pro", "remove_pro", "ban", "unban", "upload_to_user", "restart_bot", "stop_all", "export_users", "export_files"]},
    "moderator": {"name": "مشرف 🔍", "perms": ["spy", "approve_code", "ban", "export_users"]},
    "support": {"name": "دعم فني 💬", "perms": ["spy", "export_users"]},
}

for d in [USER_FILES_DIR, os.path.dirname(DB_PATH), ENV_FILES_DIR, MARKETPLACE_DIR]:
    if not os.path.exists(d): os.makedirs(d)

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- Global Trackers ---
running_processes = {}
console_logs = {}
pending_approvals = {}
pinned_files = {}
scheduled_tasks = {}
resource_history = {}  # {user_id: {file_path: [{"cpu": x, "ram": y, "time": t}]}}
smart_notifications = {}  # {admin_id: {"new_pro": True, "bot_crash": True, "hack_attempt": True, "resource_high": True}}

# --- AI Chat API ---
class ChatAPI:
    def __init__(self):
        self.url = "https://acepal-chat.vercel.app/api/chat"
        self.headers = {
            "accept": "*/*", "accept-language": "id-ID", "content-type": "application/json",
            "origin": "https://acepal-chat.vercel.app", "referer": "https://acepal-chat.vercel.app/",
            "user-agent": "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36"
        }

    def chat(self, prompt, messages=None, session_id=None):
        if messages is None: messages = []
        if not session_id: session_id = f"session_{int(time.time())}_{secrets.token_hex(4)}"
        api_messages = [{"role": m["role"], "content": m["content"]} for m in messages]
        api_messages.append({"role": "user", "content": prompt})
        payload = {"messages": api_messages, "sessionId": session_id}
        try:
            r = requests.post(self.url, headers=self.headers, json=payload, timeout=60)
            lines = (r.text or "").split("\n")
            result = ""
            for line in lines:
                line = line.strip()
                if not line or line.startswith("e:") or line.startswith("d:"): continue
                if line.startswith("0:"):
                    part = line[2:].strip()
                    if part.startswith('"') and part.endswith('"'): part = part[1:-1]
                    try: part = part.encode('utf-8').decode('unicode_escape')
                    except: pass
                    result += part
                elif ":" in line:
                    part = line.split(":", 1)[1].strip()
                    if part.startswith('"') and part.endswith('"'): part = part[1:-1]
                    result += part
                else: result += line
            return result or "عذراً، لم أتمكن من الحصول على رد.", session_id
        except Exception as e:
            return f"خطأ في الاتصال بالذكاء الاصطناعي: {str(e)}", session_id

ai_api = ChatAPI()

# --- Flask Web Console & Editor & Dashboard ---
app = Flask(__name__)

EDITOR_TEMPLATE = """<!DOCTYPE html><html><head><title>SADOX Editor - {{ filename }}</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.2/codemirror.min.css">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.2/theme/monokai.min.css">
<style>body{background:#1e1e1e;color:#fff;font-family:'Segoe UI',sans-serif;margin:0;overflow:hidden;}.header{padding:15px;background:#252526;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #333;}.CodeMirror{height:calc(100vh - 70px);font-size:16px;}.btn{background:#007acc;color:white;border:none;padding:8px 20px;cursor:pointer;border-radius:3px;font-weight:bold;}.btn:hover{background:#005a9e;}.filename{color:#858585;font-size:14px;}</style></head><body>
<div class="header"><span class="filename">📄 {{ filename }}</span><button class="btn" onclick="saveCode()">💾 Save</button></div>
<textarea id="editor">{{ code }}</textarea>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.2/codemirror.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.2/mode/python/python.min.js"></script>
<script>var editor=CodeMirror.fromTextArea(document.getElementById("editor"),{lineNumbers:true,mode:"python",theme:"monokai",indentUnit:4,smartIndent:true,matchBrackets:true});
function saveCode(){fetch(window.location.href,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({code:editor.getValue()})}).then(r=>r.json()).then(d=>{if(d.status==='ok')alert('✅ Saved!');else alert('❌ Error!');}).catch(e=>alert('❌ Connection error!'));}</script></body></html>"""

DASHBOARD_TEMPLATE = """<!DOCTYPE html><html><head><title>SADOX Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>body{background:#1a1a2e;color:#eee;font-family:'Segoe UI',sans-serif;padding:20px;}h1{color:#00d4ff;}.card{background:#16213e;border-radius:10px;padding:20px;margin:10px;display:inline-block;min-width:200px;}.card h3{color:#00d4ff;margin:0 0 10px;}.card p{font-size:24px;margin:0;}.grid{display:flex;flex-wrap:wrap;gap:10px;margin:20px 0;}.bot-item{background:#0f3460;border-radius:8px;padding:15px;margin:5px 0;}.status-on{color:#00ff88;}.status-off{color:#ff4444;}</style></head><body>
<h1>📊 SADOX Dashboard - User {{ user_id }}</h1>
<div class="grid">{% for bot in bots %}<div class="card"><h3>{{ bot.name }}</h3><p class="{{ 'status-on' if bot.running else 'status-off' }}">{{ '🟢 Running' if bot.running else '🔴 Stopped' }}</p><p style="font-size:14px;">Uptime: {{ bot.uptime }}</p></div>{% endfor %}</div>
<div class="card" style="width:90%;"><h3>Server Resources</h3><canvas id="resourceChart" height="100"></canvas></div>
<script>new Chart(document.getElementById('resourceChart'),{type:'bar',data:{labels:['CPU %','RAM %','Disk %'],datasets:[{label:'Usage',data:[{{ cpu }},{{ ram }},{{ disk }}],backgroundColor:['#ff6384','#36a2eb','#ffce56']}]},options:{scales:{y:{beginAtZero:true,max:100}}}});</script></body></html>"""

@app.route('/console/<int:user_id>/<path:filename>')
def view_console(user_id, filename):
    safe_path = os.path.normpath(filename).lstrip(os.sep)
    file_path = os.path.join(USER_FILES_DIR, str(user_id), safe_path)
    logs = console_logs.get(user_id, {}).get(file_path, ["لا توجد سجلات حالياً."])
    log_text = html.escape("\n".join(logs))
    return f"<html><head><title>Console - {filename}</title><style>body{{background:#1e1e1e;color:#d4d4d4;font-family:monospace;padding:20px;}}</style></head><body><h2>Console: {filename}</h2><pre>{log_text}</pre><script>setTimeout(function(){{location.reload();}},5000);</script></body></html>"

@app.route('/edit/<int:user_id>/<path:filename>', methods=['GET', 'POST'])
def web_editor(user_id, filename):
    safe_path = os.path.normpath(filename).lstrip(os.sep)
    file_path = os.path.join(USER_FILES_DIR, str(user_id), safe_path)
    if not os.path.exists(file_path): return "File not found", 404
    if request.method == 'POST':
        new_code = request.json.get('code')
        with open(file_path, 'w', encoding='utf-8') as f: f.write(new_code)
        return jsonify({'status': 'ok'})
    with open(file_path, 'r', encoding='utf-8') as f: code = f.read()
    return render_template_string(EDITOR_TEMPLATE, filename=filename, code=code)

@app.route('/dashboard/<int:user_id>')
def dashboard(user_id):
    user_path = os.path.join(USER_FILES_DIR, str(user_id))
    bots = []
    if os.path.exists(user_path):
        for f in os.listdir(user_path):
            if f.endswith('.py'):
                fp = os.path.join(user_path, f)
                is_running = user_id in running_processes and fp in running_processes[user_id]
                uptime = "N/A"
                if is_running:
                    uptime = str(datetime.now() - running_processes[user_id][fp]['start_time']).split('.')[0]
                bots.append({"name": f, "running": is_running, "uptime": uptime})
    cpu = psutil.cpu_percent(); ram = psutil.virtual_memory().percent; disk = psutil.disk_usage('/').percent
    return render_template_string(DASHBOARD_TEMPLATE, user_id=user_id, bots=bots, cpu=cpu, ram=ram, disk=disk)

def run_flask():
    app.run(port=WEB_PORT, host='0.0.0.0', debug=False)

# --- Database Logic ---
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, plan TEXT DEFAULT 'free', expiry_date DATETIME, points INTEGER DEFAULT 0, is_banned BOOLEAN DEFAULT 0, joined_date DATETIME DEFAULT CURRENT_TIMESTAMP)")
        await db.execute("CREATE TABLE IF NOT EXISTS stats (user_id INTEGER PRIMARY KEY, bots_stopped INTEGER DEFAULT 0, files_deleted INTEGER DEFAULT 0, file_downloads INTEGER DEFAULT 0, files_uploaded INTEGER DEFAULT 0, folders_created INTEGER DEFAULT 0, folders_deleted INTEGER DEFAULT 0)")
        await db.execute("CREATE TABLE IF NOT EXISTS channels (channel_id TEXT PRIMARY KEY, channel_name TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS schedules (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, file_path TEXT, cron_exp TEXT, last_run DATETIME)")
        await db.execute("CREATE TABLE IF NOT EXISTS security_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, event TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
        await db.execute("CREATE TABLE IF NOT EXISTS referrals (id INTEGER PRIMARY KEY AUTOINCREMENT, referrer_id INTEGER, referred_id INTEGER, UNIQUE(referrer_id, referred_id))")
        await db.execute("CREATE TABLE IF NOT EXISTS activity_log (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, action TEXT, detail TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
        await db.execute("CREATE TABLE IF NOT EXISTS admin_log (id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER, action TEXT, detail TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
        await db.execute("CREATE TABLE IF NOT EXISTS coupons (code TEXT PRIMARY KEY, days INTEGER, max_uses INTEGER DEFAULT 1, used INTEGER DEFAULT 0, created_by INTEGER)")
        # === جداول جديدة ===
        # سوق التطبيقات
        await db.execute("CREATE TABLE IF NOT EXISTS marketplace (id INTEGER PRIMARY KEY AUTOINCREMENT, publisher_id INTEGER, name TEXT, description TEXT, category TEXT DEFAULT 'general', file_path TEXT, price_points INTEGER DEFAULT 0, installs INTEGER DEFAULT 0, rating REAL DEFAULT 0, ratings_count INTEGER DEFAULT 0, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        await db.execute("CREATE TABLE IF NOT EXISTS marketplace_ratings (id INTEGER PRIMARY KEY AUTOINCREMENT, template_id INTEGER, user_id INTEGER, rating INTEGER, review TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, UNIQUE(template_id, user_id))")
        await db.execute("CREATE TABLE IF NOT EXISTS marketplace_installs (id INTEGER PRIMARY KEY AUTOINCREMENT, template_id INTEGER, user_id INTEGER, installed_at DATETIME DEFAULT CURRENT_TIMESTAMP, UNIQUE(template_id, user_id))")
        # الخطط المرنة
        await db.execute("CREATE TABLE IF NOT EXISTS custom_plans (plan_id TEXT PRIMARY KEY, name TEXT, max_files INTEGER, max_folders INTEGER, max_file_size INTEGER, max_running INTEGER, price_points INTEGER DEFAULT 0, description TEXT)")
        # الأدمن المتعدد
        await db.execute("CREATE TABLE IF NOT EXISTS admin_roles (user_id INTEGER PRIMARY KEY, role TEXT DEFAULT 'support', added_by INTEGER, added_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        # الإشعارات الذكية
        await db.execute("CREATE TABLE IF NOT EXISTS smart_alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, alert_type TEXT, user_id INTEGER, detail TEXT, is_read BOOLEAN DEFAULT 0, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        # إعدادات الإشعارات لكل أدمن
        await db.execute("CREATE TABLE IF NOT EXISTS admin_notification_settings (admin_id INTEGER PRIMARY KEY, new_user BOOLEAN DEFAULT 1, new_pro BOOLEAN DEFAULT 1, bot_crash BOOLEAN DEFAULT 1, hack_attempt BOOLEAN DEFAULT 1, resource_high BOOLEAN DEFAULT 1, daily_report BOOLEAN DEFAULT 1)")
        # الإذاعة المتقدمة
        await db.execute("CREATE TABLE IF NOT EXISTS broadcast_history (id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER, message_type TEXT, content TEXT, target_group TEXT, sent_count INTEGER, failed_count INTEGER, scheduled_at DATETIME, sent_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        await db.commit()

async def get_user(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def add_user(user_id, username):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)", (user_id, username))
        await db.execute("INSERT OR IGNORE INTO stats (user_id) VALUES (?)", (user_id,))
        await db.commit()

async def log_security(user_id, event):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO security_logs (user_id, event) VALUES (?, ?)", (user_id, event))
        await db.commit()

async def log_activity(user_id, action, detail=""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO activity_log (user_id, action, detail) VALUES (?, ?, ?)", (user_id, action, detail))
        await db.commit()

async def log_admin_action(admin_id, action, detail=""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO admin_log (admin_id, action, detail) VALUES (?, ?, ?)", (admin_id, action, detail))
        await db.commit()

# === نظام الأدمن المتعدد - دوال مساعدة ===
async def is_any_admin(user_id):
    """التحقق إذا كان المستخدم أدمن بأي مستوى"""
    if user_id in ADMIN_IDS:
        return True
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT role FROM admin_roles WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row is not None

async def get_admin_role(user_id):
    """الحصول على دور الأدمن"""
    if user_id in ADMIN_IDS:
        return "super_admin"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT role FROM admin_roles WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

async def admin_has_perm(user_id, perm):
    """التحقق من صلاحية معينة"""
    role = await get_admin_role(user_id)
    if not role:
        return False
    if role == "super_admin":
        return True
    role_perms = ADMIN_ROLES.get(role, {}).get("perms", [])
    return "all" in role_perms or perm in role_perms

# === الإشعارات الذكية - دوال مساعدة ===
async def send_smart_alert(alert_type, detail, target_user_id=None):
    """إرسال إشعار ذكي لجميع الأدمن المعنيين"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO smart_alerts (alert_type, user_id, detail) VALUES (?, ?, ?)", (alert_type, target_user_id, detail))
        await db.commit()
    # إرسال للأدمن الأساسيين
    all_admins = list(ADMIN_IDS)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM admin_roles") as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                if row[0] not in all_admins:
                    all_admins.append(row[0])
    alert_icons = {"new_user": "👤", "new_pro": "💎", "bot_crash": "💥", "hack_attempt": "🚨", "resource_high": "⚠️", "new_upload": "📤"}
    icon = alert_icons.get(alert_type, "🔔")
    for admin_id in all_admins:
        # التحقق من إعدادات الإشعارات
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT * FROM admin_notification_settings WHERE admin_id = ?", (admin_id,)) as cursor:
                settings = await cursor.fetchone()
        if settings:
            col_map = {"new_user": 1, "new_pro": 2, "bot_crash": 3, "hack_attempt": 4, "resource_high": 5}
            col_idx = col_map.get(alert_type)
            if col_idx and not settings[col_idx]:
                continue
        try:
            await bot.send_message(admin_id, f"{icon} **إشعار ذكي [{alert_type}]:**\n\n{detail}", parse_mode="Markdown")
        except:
            pass

# --- States ---
class AdminStates(StatesGroup):
    waiting_for_pro_id = State()
    waiting_for_pro_days = State()
    waiting_for_broadcast = State()
    waiting_for_channel_id = State()
    waiting_for_channel_name = State()
    waiting_for_ban_id = State()
    waiting_for_unban_id = State()
    waiting_for_spy_id = State()
    waiting_for_domain = State()
    waiting_for_port = State()
    waiting_for_points_id = State()
    waiting_for_points_amount = State()
    waiting_for_remove_pro_id = State()
    waiting_for_upload_to_user_id = State()
    waiting_for_upload_to_user_file = State()
    waiting_for_restart_user_bot_id = State()
    waiting_for_restart_user_bot_name = State()
    waiting_for_coupon_code = State()
    waiting_for_coupon_days = State()
    waiting_for_coupon_uses = State()
    waiting_for_alert_cpu = State()
    # === حالات جديدة ===
    # إذاعة متقدمة
    waiting_for_adv_broadcast_type = State()
    waiting_for_adv_broadcast_content = State()
    waiting_for_adv_broadcast_media = State()
    waiting_for_adv_broadcast_target = State()
    waiting_for_adv_broadcast_schedule = State()
    # أدمن متعدد
    waiting_for_new_admin_id = State()
    waiting_for_new_admin_role = State()
    waiting_for_remove_admin_id = State()
    # خطط مرنة
    waiting_for_plan_id = State()
    waiting_for_plan_name = State()
    waiting_for_plan_files = State()
    waiting_for_plan_folders = State()
    waiting_for_plan_size = State()
    waiting_for_plan_running = State()
    waiting_for_plan_price = State()
    waiting_for_assign_plan_user = State()
    waiting_for_assign_plan_name = State()
    # سوق التطبيقات (أدمن)
    waiting_for_marketplace_approve = State()
    # إذاعة متقدمة - حالات إضافية
    waiting_for_broadcast_single_id = State()
    waiting_for_broadcast_text_with_btn = State()
    waiting_for_broadcast_buttons = State()
    # أدمن متعدد - حالات إضافية
    waiting_for_sub_admin_id = State()
    waiting_for_sub_admin_role = State()
    waiting_for_remove_sub_id = State()
    # خطط مرنة - حالات إضافية
    waiting_for_plan_data = State()
    waiting_for_edit_plan_name = State()
    waiting_for_edit_plan_data = State()

class HostingStates(StatesGroup):
    waiting_for_folder_name = State()
    waiting_for_file_upload = State()
    waiting_for_rename = State()
    waiting_for_edit_code = State()
    waiting_for_cron = State()
    waiting_for_lib_name = State()
    waiting_for_replace_file = State()
    waiting_for_del_folder_name = State()
    waiting_for_env_key = State()
    waiting_for_env_val = State()
    waiting_for_ai_chat = State()
    waiting_for_schedule_time = State()
    waiting_for_stop_timer = State()
    waiting_for_coupon_redeem = State()
    waiting_for_requirements_file = State()
    waiting_for_file_password = State()
    waiting_for_restart_interval = State()
    # === حالات جديدة ===
    # سوق التطبيقات
    waiting_for_template_name = State()
    waiting_for_template_desc = State()
    waiting_for_template_category = State()
    waiting_for_template_file = State()
    waiting_for_template_price = State()
    waiting_for_template_rating = State()
    waiting_for_template_review = State()
    # سوق التطبيقات - حالات إضافية
    waiting_for_mp_name = State()
    waiting_for_mp_desc = State()
    waiting_for_mp_file = State()

# --- Helpers ---
def get_tree_view(path, user_id):
    try:
        items = os.listdir(path)
        rel_path = os.path.relpath(path, os.path.join(USER_FILES_DIR, str(user_id)))
        display_path = f"{user_id}/{rel_path if rel_path != '.' else ''}"
        if not items: return f"{display_path}\n└── (مجلد فارغ)"
        tree = f"{display_path}\n"; items.sort()
        user_pinned = pinned_files.get(user_id, [])
        pinned = [i for i in items if i in user_pinned]
        unpinned = [i for i in items if i not in user_pinned]
        sorted_items = pinned + unpinned
        for i, item in enumerate(sorted_items):
            connector = "└── " if i == len(sorted_items) - 1 else "├── "
            is_running = user_id in running_processes and os.path.join(path, item) in running_processes[user_id]
            pin = "📌 " if item in user_pinned else ""
            status = " 🟢" if is_running else ""
            tree += f"{connector}{pin}{item}{status}\n"
        return tree
    except: return f"{user_id}/\n└── (خطأ في الوصول)"

def get_user_plan_limits(plan):
    """الحصول على حدود الخطة - يدعم الخطط المرنة"""
    if plan in DEFAULT_PLANS:
        p = DEFAULT_PLANS[plan]
        return {"max_files": p["max_files"], "max_folders": p["max_folders"], "max_file_size": p["max_file_size"], "max_running": p["max_running"]}
    # خطة مخصصة - نحاول تحميلها (sync fallback)
    try:
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute("SELECT max_files, max_folders, max_file_size, max_running FROM custom_plans WHERE plan_id = ?", (plan,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return {"max_files": row[0], "max_folders": row[1], "max_file_size": row[2], "max_running": row[3]}
    except: pass
    return DEFAULT_PLANS["free"]

def count_user_files(user_id):
    user_path = os.path.join(USER_FILES_DIR, str(user_id))
    if not os.path.exists(user_path): return 0
    count = 0
    for root, dirs, files in os.walk(user_path): count += len(files)
    return count

def count_user_running(user_id):
    if user_id not in running_processes: return 0
    return len(running_processes[user_id])

async def install_requirements(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f: content = f.read()
        imports = re.findall(r'^(?:from|import)\s+([\w\d_]+)', content, re.MULTILINE)
        unique_imports = set(imports)
        std_libs = {'os','sys','time','datetime','json','re','math','random','asyncio','logging','shutil','subprocess','sqlite3','threading','collections','itertools','functools','base64','hashlib','hmac','secrets','urllib','html','abc','typing','inspect','io','glob','pathlib','struct','copy','string','textwrap','enum','contextlib','signal','socket','http','email','csv','xml','zipfile','gzip','tarfile','tempfile','unittest','pdb','profile','timeit','traceback','warnings','weakref','array','queue','heapq','bisect','decimal','fractions','statistics','operator','pickle','shelve','dbm','platform','ctypes','multiprocessing','concurrent','configparser','argparse','getpass','pprint','difflib','dataclasses','types'}
        package_map = {'telebot':'pyTelegramBotAPI','PIL':'Pillow','cv2':'opencv-python','telegram':'python-telegram-bot','bs4':'beautifulsoup4','flask':'Flask','aiogram':'aiogram','requests':'requests','yaml':'pyyaml','psutil':'psutil','numpy':'numpy','pandas':'pandas','matplotlib':'matplotlib','sklearn':'scikit-learn','dotenv':'python-dotenv','pyrogram':'pyrogram','aiohttp':'aiohttp','discord':'discord.py','nextcord':'nextcord','disnake':'disnake','colorama':'colorama','rich':'rich','tqdm':'tqdm','httpx':'httpx'}
        libs_to_install = []
        for lib in unique_imports:
            if lib in std_libs: continue
            package_name = package_map.get(lib, lib)
            libs_to_install.append(package_name)
        if not libs_to_install: return True
        install_cmd = ["pip", "install", "--no-cache-dir"] + libs_to_install
        process = await asyncio.create_subprocess_exec(*install_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            await asyncio.wait_for(process.communicate(), timeout=120)
            return True
        except asyncio.TimeoutError:
            try: process.kill()
            except: pass
            return False
    except Exception as e:
        logging.error(f"Error installing requirements: {e}")
        return False

async def check_subscription(user_id):
    if user_id in ADMIN_IDS: return True
    if await is_any_admin(user_id): return True
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT channel_id FROM channels") as cursor: channels = await cursor.fetchall()
    if not channels: return True
    for (channel_id,) in channels:
        try:
            member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            if member.status in ["left", "kicked"]: return False
        except: continue
    return True

def get_main_keyboard(user_plan='free', is_admin=False):
    builder = InlineKeyboardBuilder()
    if user_plan == 'free':
        builder.row(types.InlineKeyboardButton(text="🚀 الترقية إلى PRO 🚀", callback_data="upgrade_pro"))
    else:
        builder.row(types.InlineKeyboardButton(text="💎 أنت مشترك PRO", callback_data="upgrade_pro"))
    builder.row(types.InlineKeyboardButton(text="🗂️ استضافتي", callback_data="my_hosting"), types.InlineKeyboardButton(text="🔐 المتغيرات", callback_data="manage_env"))
    builder.row(types.InlineKeyboardButton(text="🤖 الذكاء الاصطناعي", callback_data="ai_chat_start"), types.InlineKeyboardButton(text="📊 إحصائياتي", callback_data="my_stats"))
    builder.row(types.InlineKeyboardButton(text="💰 جمع نقاط", callback_data="collect_points"), types.InlineKeyboardButton(text="⚡️ حالة السيرفر", callback_data="server_speed"))
    builder.row(types.InlineKeyboardButton(text="⚙️ بوتاتي النشطة", callback_data="my_running_bots"), types.InlineKeyboardButton(text="📋 سجل عملياتي", callback_data="my_activity_log"))
    builder.row(types.InlineKeyboardButton(text="🛒 سوق التطبيقات", callback_data="marketplace"), types.InlineKeyboardButton(text="🎟 استخدام كوبون", callback_data="redeem_coupon_start"))
    builder.row(types.InlineKeyboardButton(text="📖 تعليمات", callback_data="instructions"))
    if is_admin:
        builder.row(types.InlineKeyboardButton(text="🛠 لوحة الإدارة", callback_data="admin_panel"))
    return builder.as_markup()

def anti_crash_scan(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f: content = f.read()
        critical_patterns = [
            (r'rm\s+-rf\s+/', "محاولة حذف ملفات النظام الجذرية"),
            (r'shutil\.rmtree\s*\(\s*[\'\"]/\s*[\'\"]', "محاولة حذف مجلدات النظام"),
            (r'open\s*\(.*[\'\"]/etc/passwd', "محاولة الوصول لملفات النظام"),
            (r'open\s*\(.*bot_database', "محاولة الوصول لقاعدة بيانات البوت"),
            (r'open\s*\(.*config\.py', "محاولة الوصول لإعدادات البوت"),
            (r'\.\./', "محاولة التنقل خارج المجلد المسموح"),
        ]
        suspicious_patterns = [
            (r'os\.system\s*\(', "استخدام os.system"),
            (r'subprocess\.\w+\s*\(', "استخدام subprocess"),
            (r'eval\s*\(', "استخدام eval"),
            (r'exec\s*\(', "استخدام exec"),
            (r'__import__\s*\(', "استيراد ديناميكي"),
            (r'requests\.post\s*\(.*files\s*=', "إرسال ملفات للخارج"),
        ]
        critical_threats = []
        for pattern, desc in critical_patterns:
            if re.search(pattern, content): critical_threats.append(desc)
        if critical_threats:
            return "blocked", critical_threats
        suspicious_threats = []
        for pattern, desc in suspicious_patterns:
            if re.search(pattern, content): suspicious_threats.append(desc)
        if suspicious_threats:
            return "suspicious", suspicious_threats
        return "safe", []
    except Exception as e:
        return "error", [str(e)]

def get_user_env(user_id):
    env_path = os.path.join(ENV_FILES_DIR, f"{user_id}.json")
    if os.path.exists(env_path):
        with open(env_path, 'r') as f: return json.load(f)
    return {}

def save_user_env(user_id, env_data):
    env_path = os.path.join(ENV_FILES_DIR, f"{user_id}.json")
    with open(env_path, 'w') as f: json.dump(env_data, f)

async def log_reader(user_id, file_path, proc):
    if user_id not in console_logs: console_logs[user_id] = {}
    console_logs[user_id][file_path] = []
    while proc.poll() is None:
        try:
            line = await asyncio.to_thread(proc.stdout.readline)
            if line:
                decoded = line.decode('utf-8', errors='replace').strip()
                console_logs[user_id][file_path].append(f"[{datetime.now().strftime('%H:%M:%S')}] {decoded}")
            if len(console_logs[user_id][file_path]) > 200: console_logs[user_id][file_path].pop(0)
        except: break
        await asyncio.sleep(0.1)

async def error_reader(user_id, file_path, proc):
    error_buffer = []
    while proc.poll() is None:
        try:
            line = await asyncio.to_thread(proc.stderr.readline)
            if line:
                decoded = line.decode('utf-8', errors='replace').strip()
                error_buffer.append(decoded)
                if len(error_buffer) > 50: error_buffer.pop(0)
        except: break
        await asyncio.sleep(0.1)
    if error_buffer and proc.returncode != 0:
        error_text = "\n".join(error_buffer[-10:])
        file_name = os.path.basename(file_path)
        # === ميزة AI Debugger ===
        try:
            ai_prompt = f"أنت مصحح أخطاء بايثون محترف. حلل الخطأ التالي واقترح الحل بالعربية بشكل مختصر:\n\nاسم الملف: {file_name}\n\nالخطأ:\n{error_text[:800]}"
            ai_fix, _ = ai_api.chat(ai_prompt)
            fix_text = f"\n\n🤖 **اقتراح AI للإصلاح:**\n{ai_fix[:1000]}"
        except:
            fix_text = ""
        try:
            await bot.send_message(user_id, f"💥 **توقف `{file_name}` بسبب خطأ!**\n\n```\n{error_text[:500]}```{fix_text}", parse_mode="Markdown")
        except: pass
        # إشعار ذكي للأدمن
        await send_smart_alert("bot_crash", f"توقف بوت `{file_name}` للمستخدم `{user_id}`\n\nالخطأ: {error_text[:200]}", user_id)

async def auto_restart_monitor():
    while True:
        for user_id, files in list(running_processes.items()):
            for file_path, data in list(files.items()):
                if data['proc'].poll() is not None and data.get('auto_restart'):
                    count = data.get('restart_count', 0)
                    if count < 5:
                        env = os.environ.copy(); env.update(get_user_env(user_id))
                        env["PYTHONPATH"] = os.path.dirname(file_path) + ":" + env.get("PYTHONPATH", "")
                        new_proc = subprocess.Popen(["python3", "-u", file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=os.path.dirname(file_path))
                        running_processes[user_id][file_path].update({'proc': new_proc, 'restart_count': count + 1})
                        asyncio.create_task(log_reader(user_id, file_path, new_proc))
                        asyncio.create_task(error_reader(user_id, file_path, new_proc))
                        try: await bot.send_message(user_id, f"🛡 **نظام المنقذ:** تم إعادة تشغيل `{os.path.basename(file_path)}` تلقائياً ({count+1}/5).")
                        except: pass
                    else:
                        running_processes[user_id][file_path]['auto_restart'] = False
                        try: await bot.send_message(user_id, f"⚠️ **نظام المنقذ:** توقف إعادة تشغيل `{os.path.basename(file_path)}` لتكرار الفشل.")
                        except: pass
        await asyncio.sleep(10)

async def check_pro_expiry():
    while True:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("SELECT user_id FROM users WHERE plan = 'pro' AND expiry_date IS NOT NULL AND expiry_date < ?", (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)) as cursor:
                    expired = await cursor.fetchall()
                for (uid,) in expired:
                    await db.execute("UPDATE users SET plan = 'free', expiry_date = NULL WHERE user_id = ?", (uid,))
                    try: await bot.send_message(uid, "⚠️ **انتهى اشتراكك في PRO!**\n\nتم إرجاعك للخطة المجانية.")
                    except: pass
                await db.commit()
        except: pass
        await asyncio.sleep(3600)

async def auto_cleanup():
    while True:
        try:
            for f in os.listdir(BASE_DIR):
                if f.endswith('.log') and f.startswith('bot_'):
                    fp = os.path.join(BASE_DIR, f)
                    if os.path.getmtime(fp) < time.time() - 86400 * 3:
                        os.remove(fp)
            for f in os.listdir(BASE_DIR):
                if f.startswith('backup_') and f.endswith('.zip'):
                    os.remove(os.path.join(BASE_DIR, f))
        except: pass
        await asyncio.sleep(86400)

# === ميزة إدارة الموارد الذكية ===
async def smart_resource_monitor():
    """مراقبة ذكية لموارد كل بوت مع إجراءات تلقائية"""
    while True:
        try:
            cpu = psutil.cpu_percent(interval=2)
            ram = psutil.virtual_memory().percent
            # تنبيه عام
            if cpu > 90 or ram > 90:
                await send_smart_alert("resource_high", f"💻 CPU: `{cpu}%`\n🧠 RAM: `{ram}%`\n\nالموارد مرتفعة جداً!")
            # مراقبة كل بوت على حدة
            for user_id, files in list(running_processes.items()):
                for file_path, data in list(files.items()):
                    try:
                        proc = psutil.Process(data['proc'].pid)
                        bot_cpu = proc.cpu_percent(interval=0.5)
                        bot_mem = proc.memory_info().rss // (1024 * 1024)
                        fname = os.path.basename(file_path)
                        # تخزين سجل الموارد
                        if user_id not in resource_history:
                            resource_history[user_id] = {}
                        if file_path not in resource_history[user_id]:
                            resource_history[user_id][file_path] = []
                        resource_history[user_id][file_path].append({"cpu": bot_cpu, "ram": bot_mem, "time": datetime.now().strftime('%H:%M')})
                        if len(resource_history[user_id][file_path]) > 60:
                            resource_history[user_id][file_path].pop(0)
                        # تحذير المستخدم عند الاستهلاك العالي
                        if bot_cpu > 80:
                            try: await bot.send_message(user_id, f"⚠️ **تحذير موارد:** بوت `{fname}` يستهلك CPU عالي: `{bot_cpu:.1f}%`\n\nقد يتم إبطاؤه أو إيقافه إذا استمر.")
                            except: pass
                        if bot_mem > 500:
                            try: await bot.send_message(user_id, f"⚠️ **تحذير موارد:** بوت `{fname}` يستهلك ذاكرة عالية: `{bot_mem}MB`")
                            except: pass
                        # إيقاف تلقائي عند الاستهلاك الخطير
                        if bot_cpu > 95 or bot_mem > 1000:
                            force_kill_process(file_path, user_id)
                            save_persistent_state()
                            try: await bot.send_message(user_id, f"🛑 **تم إيقاف `{fname}` تلقائياً** بسبب استهلاك مفرط للموارد.\n\n💻 CPU: {bot_cpu:.1f}% | 🧠 RAM: {bot_mem}MB")
                            except: pass
                            await send_smart_alert("resource_high", f"تم إيقاف بوت `{fname}` للمستخدم `{user_id}` تلقائياً بسبب استهلاك مفرط.\nCPU: {bot_cpu:.1f}% | RAM: {bot_mem}MB", user_id)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
        except: pass
        await asyncio.sleep(60)

async def daily_report():
    while True:
        try:
            await asyncio.sleep(86400)
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("SELECT COUNT(*) FROM users") as c: total = (await c.fetchone())[0]
                async with db.execute("SELECT COUNT(*) FROM users WHERE plan = 'pro'") as c: pro = (await c.fetchone())[0]
                async with db.execute("SELECT COUNT(*) FROM users WHERE joined_date > ?", ((datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),)) as c: new_today = (await c.fetchone())[0]
                async with db.execute("SELECT COUNT(*) FROM marketplace") as c: templates = (await c.fetchone())[0]
                async with db.execute("SELECT COUNT(*) FROM smart_alerts WHERE created_at > ?", ((datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),)) as c: alerts_today = (await c.fetchone())[0]
            total_running = sum(len(f) for f in running_processes.values())
            cpu = psutil.cpu_percent(); ram = psutil.virtual_memory().percent
            report = f"""📊 **التقرير اليومي:**

👥 إجمالي المستخدمين: `{total}`
🆕 مستخدمين جدد اليوم: `{new_today}`
💎 مشتركين PRO: `{pro}`
🤖 بوتات نشطة: `{total_running}`
🛒 قوالب في السوق: `{templates}`
🔔 إشعارات اليوم: `{alerts_today}`
💻 CPU: `{cpu}%` | 🧠 RAM: `{ram}%`"""
            for admin_id in ADMIN_IDS:
                try: await bot.send_message(admin_id, report, parse_mode="Markdown")
                except: pass
        except: pass

def force_kill_process(file_path, user_id):
    killed = False
    if user_id in running_processes and file_path in running_processes[user_id]:
        proc = running_processes[user_id][file_path]['proc']
        try:
            parent = psutil.Process(proc.pid)
            for child in parent.children(recursive=True): child.kill()
            parent.kill()
            killed = True
        except psutil.NoSuchProcess: killed = True
        except: pass
    try: subprocess.run(["pkill", "-9", "-f", file_path], check=False, timeout=5)
    except: pass
    if user_id in running_processes and file_path in running_processes.get(user_id, {}):
        del running_processes[user_id][file_path]
    return killed

def save_persistent_state():
    state = {"running_files": {}}
    for uid, files in running_processes.items():
        state["running_files"][str(uid)] = []
        for fp, data in files.items():
            state["running_files"][str(uid)].append({
                "file_path": fp,
                "auto_restart": data.get('auto_restart', False)
            })
    try:
        with open(PERSISTENT_STATE_FILE, 'w') as f: json.dump(state, f)
    except: pass

async def restore_persistent_state():
    try:
        if not os.path.exists(PERSISTENT_STATE_FILE): return
        with open(PERSISTENT_STATE_FILE, 'r') as f: state = json.load(f)
        for uid_str, files in state.get("running_files", {}).items():
            uid = int(uid_str)
            for fdata in files:
                fp = fdata["file_path"]
                if not os.path.exists(fp): continue
                env = os.environ.copy(); env.update(get_user_env(uid))
                env["PYTHONPATH"] = os.path.dirname(fp) + ":" + env.get("PYTHONPATH", "")
                p = subprocess.Popen(["python3", "-u", fp], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=os.path.dirname(fp))
                if uid not in running_processes: running_processes[uid] = {}
                running_processes[uid][fp] = {'proc': p, 'auto_restart': fdata.get('auto_restart', False), 'restart_count': 0, 'start_time': datetime.now()}
                asyncio.create_task(log_reader(uid, fp, p))
                asyncio.create_task(error_reader(uid, fp, p))
                try: await bot.send_message(uid, f"🔄 **تم استعادة تشغيل:** `{os.path.basename(fp)}` بعد إعادة تشغيل السيرفر.")
                except: pass
    except: pass

# ============================================
# === Handlers ===
# ============================================

@dp.message(CommandStart())
async def start_cmd(message: types.Message):
    user_id, username = message.from_user.id, message.from_user.username or "User"
    if message.text and message.text.startswith("/start ref_"):
        try:
            ref_id = int(message.text.split("_")[1])
            if ref_id != user_id:
                if await check_subscription(user_id):
                    async with aiosqlite.connect(DB_PATH) as db:
                        try:
                            await db.execute("INSERT INTO referrals (referrer_id, referred_id) VALUES (?, ?)", (ref_id, user_id))
                            await db.execute("UPDATE users SET points = points + 1 WHERE user_id = ?", (ref_id,))
                            await db.commit()
                            try: await bot.send_message(ref_id, f"💰 **نقطة جديدة!** دخل المستخدم `{user_id}` عبر رابطك.")
                            except: pass
                        except aiosqlite.IntegrityError: pass
        except: pass
    user_data = await get_user(user_id)
    if user_data and user_data['is_banned']: return await message.answer("🚫 عذراً، لقد تم حظرك.")
    if not await check_subscription(user_id):
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT channel_id, channel_name FROM channels") as cursor: channels = await cursor.fetchall()
        builder = InlineKeyboardBuilder()
        for cid, cname in channels:
            link = f"https://t.me/{cid.replace('@','')}" if cid.startswith('@') else f"https://t.me/c/{cid.replace('-100','')}/1"
            builder.row(types.InlineKeyboardButton(text=f"📢 {cname}", url=link))
        builder.row(types.InlineKeyboardButton(text="✅ تم الاشتراك", callback_data="check_sub"))
        return await message.answer("⚠️ يجب الاشتراك في القنوات أولاً:", reply_markup=builder.as_markup())
    await add_user(user_id, username)
    # إشعار ذكي بمستخدم جديد
    if not user_data:
        await send_smart_alert("new_user", f"مستخدم جديد: `{user_id}` (@{username})", user_id)
    user_data = await get_user(user_id)
    plan_text = "مجانية ⭐️" if user_data['plan'] == 'free' else "مدفوعة PRO 💎"
    running_count = count_user_running(user_id); files_count = count_user_files(user_id)
    is_admin = user_id in ADMIN_IDS or await is_any_admin(user_id)
    welcome = f"أهلاً بك في لوحة تحكم الاستضافة!\n\n⭐️ نوع الخطة: {plan_text}\n📁 ملفاتك: `{files_count}`\n🟢 بوتات نشطة: `{running_count}`\n\nاختر أحد الخيارات:"
    await message.answer(welcome, reply_markup=get_main_keyboard(user_data['plan'], is_admin))

@dp.callback_query(F.data == "check_sub")
async def check_sub_callback(callback: types.CallbackQuery):
    if await check_subscription(callback.from_user.id):
        try: await callback.message.delete()
        except: pass
        await start_cmd(callback.message)
    else: await callback.answer("❌ لم تشترك بعد!", show_alert=True)

@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: types.CallbackQuery, state: FSMContext):
    if state: await state.clear()
    user_data = await get_user(callback.from_user.id)
    plan_text = "مجانية ⭐️" if user_data['plan'] == 'free' else "مدفوعة PRO 💎"
    running_count = count_user_running(callback.from_user.id); files_count = count_user_files(callback.from_user.id)
    is_admin = callback.from_user.id in ADMIN_IDS or await is_any_admin(callback.from_user.id)
    welcome = f"أهلاً بك في لوحة تحكم الاستضافة!\n\n⭐️ نوع الخطة: {plan_text}\n📁 ملفاتك: `{files_count}`\n🟢 بوتات نشطة: `{running_count}`\n\nاختر أحد الخيارات:"
    await callback.message.edit_text(welcome, reply_markup=get_main_keyboard(user_data['plan'], is_admin))

# === سجل العمليات ===
@dp.callback_query(F.data == "my_activity_log")
async def my_activity_log(callback: types.CallbackQuery):
    uid = callback.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT action, detail, timestamp FROM activity_log WHERE user_id = ? ORDER BY timestamp DESC LIMIT 15", (uid,)) as cursor:
            logs = await cursor.fetchall()
    text = "📋 **سجل عملياتك (آخر 15):**\n\n"
    if not logs: text += "لا توجد عمليات مسجلة بعد."
    for action, detail, ts in logs:
        text += f"🕒 `{ts}`\n   {action}: {detail}\n\n"
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="back_to_main"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

# === استخدام كوبون ===
@dp.callback_query(F.data == "redeem_coupon_start")
async def redeem_coupon_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🎟 أرسل كود الكوبون:")
    await state.set_state(HostingStates.waiting_for_coupon_redeem)

@dp.message(HostingStates.waiting_for_coupon_redeem)
async def process_redeem_coupon(message: types.Message, state: FSMContext):
    code = message.text.strip(); uid = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM coupons WHERE code = ?", (code,)) as cursor:
            coupon = await cursor.fetchone()
        if not coupon: await message.answer("❌ كود الكوبون غير صالح.")
        elif coupon['used'] >= coupon['max_uses']: await message.answer("❌ هذا الكوبون تم استخدامه بالكامل.")
        else:
            days = coupon['days']
            expiry = datetime.now() + timedelta(days=days)
            await db.execute("UPDATE users SET plan = 'pro', expiry_date = ? WHERE user_id = ?", (expiry.strftime('%Y-%m-%d %H:%M:%S'), uid))
            await db.execute("UPDATE coupons SET used = used + 1 WHERE code = ?", (code,))
            await db.commit()
            await message.answer(f"🎉 **تم تفعيل PRO لمدة {days} يوم عبر الكوبون!**")
            await log_activity(uid, "كوبون", f"استخدم كوبون {code}")
            await send_smart_alert("new_pro", f"المستخدم `{uid}` فعّل PRO عبر كوبون `{code}` لمدة {days} يوم.", uid)
    await state.clear()

# === بوتاتي النشطة ===
@dp.callback_query(F.data == "my_running_bots")
async def my_running_bots(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in running_processes or not running_processes[uid]:
        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="back_to_main"))
        return await callback.message.edit_text("⚙️ **بوتاتي النشطة:**\n\nلا توجد بوتات نشطة حالياً.", reply_markup=builder.as_markup())
    text = "⚙️ **بوتاتي النشطة:**\n\n"
    builder = InlineKeyboardBuilder()
    for file_path, data in running_processes[uid].items():
        fname = os.path.basename(file_path)
        uptime = str(datetime.now() - data['start_time']).split('.')[0]
        auto_r = "✅" if data.get('auto_restart') else "❌"
        try:
            p = psutil.Process(data['proc'].pid)
            cpu_u = p.cpu_percent(interval=0.1); mem_u = p.memory_info().rss // (1024*1024)
        except: cpu_u = 0; mem_u = 0
        text += f"🟢 `{fname}`\n   ⏱ {uptime} | 🛡 المنقذ: {auto_r}\n   💻 CPU: {cpu_u:.1f}% | 🧠 RAM: {mem_u}MB\n\n"
        builder.row(types.InlineKeyboardButton(text=f"🛑 إيقاف {fname}", callback_data=f"stop_{fname}"))
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="back_to_main"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

# === إدارة المتغيرات ===
@dp.callback_query(F.data == "manage_env")
async def manage_env(callback: types.CallbackQuery):
    env = get_user_env(callback.from_user.id)
    text = "🔐 **متغيرات البيئة:**\n\n"
    if not env: text += "لا توجد متغيرات حالياً."
    else:
        for k, v in env.items(): text += f"🔹 `{k}`: `{v[:3]}***`\n"
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="➕ إضافة متغير", callback_data="add_env_start"))
    builder.row(types.InlineKeyboardButton(text="🗑 حذف الكل", callback_data="clear_env"))
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="back_to_main"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "add_env_start")
async def add_env_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("أرسل اسم المتغير (مثلاً: BOT_TOKEN):")
    await state.set_state(HostingStates.waiting_for_env_key)

@dp.message(HostingStates.waiting_for_env_key)
async def process_env_key(message: types.Message, state: FSMContext):
    await state.update_data(env_key=message.text.strip().upper())
    await message.answer("أرسل قيمة المتغير:")
    await state.set_state(HostingStates.waiting_for_env_val)

@dp.message(HostingStates.waiting_for_env_val)
async def process_env_val(message: types.Message, state: FSMContext):
    data = await state.get_data(); uid = message.from_user.id
    env = get_user_env(uid); env[data['env_key']] = message.text.strip()
    save_user_env(uid, env)
    await message.answer(f"✅ تم حفظ المتغير `{data['env_key']}`.")
    await state.clear()

@dp.callback_query(F.data == "clear_env")
async def clear_env(callback: types.CallbackQuery):
    save_user_env(callback.from_user.id, {})
    await callback.answer("🗑 تم حذف جميع المتغيرات.", show_alert=True)
    await manage_env(callback)

@dp.callback_query(F.data == "reset_hosting")
async def reset_hosting(callback: types.CallbackQuery):
    uid = callback.from_user.id; path = os.path.join(USER_FILES_DIR, str(uid))
    if os.path.exists(path): shutil.rmtree(path); os.makedirs(path)
    await callback.answer("🗑 تم إعادة تعيين المجلد!", show_alert=True)

@dp.callback_query(F.data == "my_hosting")
async def my_hosting(callback_or_msg, state: FSMContext = None):
    if isinstance(callback_or_msg, types.CallbackQuery):
        uid = callback_or_msg.from_user.id; msg = callback_or_msg.message
    else:
        uid = callback_or_msg.from_user.id; msg = callback_or_msg
    path = os.path.join(USER_FILES_DIR, str(uid))
    if not os.path.exists(path): os.makedirs(path)
    if state: await state.update_data(current_path=path, view_user_id=uid)
    await show_files(msg, path, uid)

async def show_files(message, path, user_id):
    try: items = os.listdir(path)
    except: items = []
    builder = InlineKeyboardBuilder()
    user_pinned = pinned_files.get(user_id, [])
    pinned = [i for i in items if i in user_pinned]
    unpinned = [i for i in items if i not in user_pinned]
    sorted_items = pinned + unpinned
    for item in sorted_items:
        item_path = os.path.join(path, item)
        is_running = user_id in running_processes and item_path in running_processes[user_id]
        pin = "📌 " if item in user_pinned else ""
        icon = "📁" if os.path.isdir(item_path) else ("🟢" if is_running else "📄")
        builder.row(types.InlineKeyboardButton(text=f"{icon} {pin}{item}", callback_data=f"item_{item}"))
    builder.row(types.InlineKeyboardButton(text="➕ مجلد", callback_data="create_folder"), types.InlineKeyboardButton(text="🗑 حذف مجلد", callback_data="del_folder_list"))
    builder.row(types.InlineKeyboardButton(text="📤 رفع ملف", callback_data="upload_file"), types.InlineKeyboardButton(text="🧹 تنظيف", callback_data="cleanup_folder"))
    builder.row(types.InlineKeyboardButton(text="🗄️ نسخة احتياطية", callback_data="backup_files"), types.InlineKeyboardButton(text="📈 لوحة مراقبة", callback_data="web_dashboard"))
    builder.row(types.InlineKeyboardButton(text="رجوع 🔙", callback_data="back_to_main"))
    tree = get_tree_view(path, user_id)
    try:
        await message.edit_text(f"🗂️ **استضافتي**\n\n```\n{tree}```", reply_markup=builder.as_markup(), parse_mode="Markdown")
    except:
        await message.answer(f"🗂️ **استضافتي**\n\n```\n{tree}```", reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "web_dashboard")
async def web_dashboard_handler(callback: types.CallbackQuery):
    user_data = await get_user(callback.from_user.id)
    if user_data['plan'] != 'pro': return await callback.answer("💎 هذه الميزة للمشتركين PRO فقط!", show_alert=True)
    uid = callback.from_user.id
    url = f"http://{SERVER_DOMAIN}:{WEB_PORT}/dashboard/{uid}"
    await callback.message.answer(f"📈 **رابط لوحة المراقبة (PRO):**\n\n{url}")

@dp.callback_query(F.data.startswith("item_"))
async def manage_item(callback: types.CallbackQuery, state: FSMContext):
    item_name = callback.data.replace("item_", ""); data = await state.get_data(); uid = callback.from_user.id
    view_user_id = data.get('view_user_id', uid)
    current_path = data.get('current_path', os.path.join(USER_FILES_DIR, str(view_user_id)))
    item_path = os.path.join(current_path, item_name)
    if os.path.isdir(item_path):
        await state.update_data(current_path=item_path); await show_files(callback.message, item_path, view_user_id)
    else:
        is_run = view_user_id in running_processes and item_path in running_processes[view_user_id]
        auto_r = is_run and running_processes[view_user_id][item_path].get('auto_restart', False)
        is_pinned = item_name in pinned_files.get(view_user_id, [])
        try:
            file_size = os.path.getsize(item_path)
            size_str = f"{file_size / 1024:.1f} KB" if file_size < 1024*1024 else f"{file_size / (1024*1024):.1f} MB"
        except: size_str = "غير معروف"
        try:
            with open(item_path, 'r', encoding='utf-8') as f: content = f.read(500)
        except: content = "(محتوى غير نصي)"
        status = "🟢 يعمل" if is_run else "🔴 متوقف"
        text = f"التحكم بالملف: `{item_name}`\n📏 الحجم: {size_str} | الحالة: {status}\n\n📄 معاينة:\n```python\n{content}```"
        builder = InlineKeyboardBuilder()
        if item_name.endswith(".py"):
            builder.row(types.InlineKeyboardButton(text="🛑 إيقاف" if is_run else "▶️ تشغيل", callback_data=f"{'stop' if is_run else 'run'}_{item_name}"), types.InlineKeyboardButton(text="🔬 تجريبي", callback_data=f"test_{item_name}"))
            builder.row(types.InlineKeyboardButton(text=f"🛡 المنقذ: {'✅' if auto_r else '❌'}", callback_data=f"toggle_restart_{item_name}"), types.InlineKeyboardButton(text="📝 محرر", callback_data=f"web_edit_{item_name}"))
            builder.row(types.InlineKeyboardButton(text="🌐 كونسول", callback_data=f"web_{item_name}"), types.InlineKeyboardButton(text="📦 تثبيت مكتبة", callback_data=f"pip_install_{item_name}"))
            builder.row(types.InlineKeyboardButton(text="🔍 فحص الأخطاء", callback_data=f"check_errors_{item_name}"), types.InlineKeyboardButton(text="📋 سجل الأخطاء", callback_data=f"error_log_{item_name}"))
            builder.row(types.InlineKeyboardButton(text="🕐 جدولة", callback_data=f"schedule_{item_name}"), types.InlineKeyboardButton(text="🔄 إعادة تشغيل دورية", callback_data=f"auto_cycle_{item_name}"))
            builder.row(types.InlineKeyboardButton(text="🤖 AI Debug", callback_data=f"ai_debug_{item_name}"), types.InlineKeyboardButton(text="📊 موارد البوت", callback_data=f"bot_resources_{item_name}"))
        builder.row(types.InlineKeyboardButton(text="📥 تنزيل", callback_data=f"dl_{item_name}"), types.InlineKeyboardButton(text="🔄 استبدال", callback_data=f"replace_{item_name}"))
        builder.row(types.InlineKeyboardButton(text="✏️ رينيم", callback_data=f"rename_{item_name}"), types.InlineKeyboardButton(text="🗑 حذف", callback_data=f"del_{item_name}"))
        pin_text = "📌 إلغاء تثبيت" if is_pinned else "📌 تثبيت"
        builder.row(types.InlineKeyboardButton(text=pin_text, callback_data=f"pin_{item_name}"), types.InlineKeyboardButton(text="🔗 مشاركة", callback_data=f"share_{item_name}"))
        builder.row(types.InlineKeyboardButton(text="🛒 نشر في السوق", callback_data=f"publish_market_{item_name}"))
        builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="my_hosting"))
        await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

# === فحص الأخطاء ===
@dp.callback_query(F.data.startswith("check_errors_"))
async def check_errors(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("check_errors_", ""); data = await state.get_data()
    uid = data.get('view_user_id', callback.from_user.id)
    current_path = data.get('current_path', os.path.join(USER_FILES_DIR, str(uid)))
    file_path = os.path.join(current_path, file_name)
    await callback.answer("🔍 جاري فحص الأخطاء...")
    try:
        process = await asyncio.create_subprocess_exec("python3", "-m", "py_compile", file_path, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=15)
        if process.returncode == 0:
            p2 = await asyncio.create_subprocess_exec("python3", "-c", f"import ast; ast.parse(open('{file_path}').read()); print('OK')", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            out2, err2 = await asyncio.wait_for(p2.communicate(), timeout=10)
            if p2.returncode == 0:
                await callback.message.answer(f"✅ **فحص `{file_name}`:**\n\nلا توجد أخطاء في بنية الكود! الملف جاهز للتشغيل.")
            else:
                await callback.message.answer(f"⚠️ **فحص `{file_name}`:**\n\n```\n{err2.decode()[:500]}```", parse_mode="Markdown")
        else:
            await callback.message.answer(f"❌ **أخطاء في `{file_name}`:**\n\n```\n{stderr.decode()[:500]}```", parse_mode="Markdown")
    except Exception as e:
        await callback.message.answer(f"❌ خطأ في الفحص: {str(e)}")

# === سجل الأخطاء ===
@dp.callback_query(F.data.startswith("error_log_"))
async def error_log(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("error_log_", ""); data = await state.get_data()
    uid = data.get('view_user_id', callback.from_user.id)
    current_path = data.get('current_path', os.path.join(USER_FILES_DIR, str(uid)))
    file_path = os.path.join(current_path, file_name)
    logs = console_logs.get(uid, {}).get(file_path, [])
    error_lines = [l for l in logs if 'error' in l.lower() or 'traceback' in l.lower() or 'exception' in l.lower()]
    if not error_lines:
        await callback.message.answer(f"📋 **سجل أخطاء `{file_name}`:**\n\nلا توجد أخطاء مسجلة.")
    else:
        text = f"📋 **سجل أخطاء `{file_name}` (آخر 20):**\n\n```\n" + "\n".join(error_lines[-20:]) + "```"
        await callback.message.answer(text[:4000], parse_mode="Markdown")

# === تثبيت الملف ===
@dp.callback_query(F.data.startswith("pin_"))
async def pin_file(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("pin_", ""); uid = callback.from_user.id
    if uid not in pinned_files: pinned_files[uid] = []
    if file_name in pinned_files[uid]:
        pinned_files[uid].remove(file_name)
        await callback.answer(f"📌 تم إلغاء تثبيت {file_name}", show_alert=True)
    else:
        pinned_files[uid].append(file_name)
        await callback.answer(f"📌 تم تثبيت {file_name} في الأعلى", show_alert=True)
    await manage_item(callback, state)

# === مشاركة الملف ===
@dp.callback_query(F.data.startswith("share_"))
async def share_file(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("share_", ""); data = await state.get_data()
    uid = data.get('view_user_id', callback.from_user.id)
    current_path = data.get('current_path', os.path.join(USER_FILES_DIR, str(uid)))
    file_path = os.path.join(current_path, file_name)
    if os.path.exists(file_path):
        token = secrets.token_hex(8)
        rel_path = os.path.relpath(file_path, os.path.join(USER_FILES_DIR, str(uid)))
        url = f"http://{SERVER_DOMAIN}:{WEB_PORT}/edit/{uid}/{urllib.parse.quote(rel_path)}?token={token}"
        await callback.message.answer(f"🔗 **رابط مشاركة `{file_name}`:**\n\n`{url}`\n\n⚠️ هذا الرابط مؤقت.")
    else:
        await callback.answer("❌ الملف غير موجود.", show_alert=True)

# === جدولة التشغيل ===
@dp.callback_query(F.data.startswith("schedule_"))
async def schedule_start(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("schedule_", "")
    await state.update_data(schedule_file=file_name)
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="⏰ تشغيل بعد X دقائق", callback_data="schedule_delay"), types.InlineKeyboardButton(text="⏱ إيقاف بعد X دقائق", callback_data="schedule_stop_after"))
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data=f"item_{file_name}"))
    await callback.message.edit_text(f"🕐 **جدولة `{file_name}`:**\n\nاختر نوع الجدولة:", reply_markup=builder.as_markup())

@dp.callback_query(F.data == "schedule_delay")
async def schedule_delay(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("⏰ أرسل عدد الدقائق لتشغيل الملف بعدها:")
    await state.set_state(HostingStates.waiting_for_schedule_time)

@dp.message(HostingStates.waiting_for_schedule_time)
async def process_schedule_time(message: types.Message, state: FSMContext):
    try:
        minutes = int(message.text.strip()); data = await state.get_data()
        file_name = data['schedule_file']; uid = message.from_user.id
        current_path = data.get('current_path', os.path.join(USER_FILES_DIR, str(uid)))
        file_path = os.path.join(current_path, file_name)
        await message.answer(f"⏰ سيتم تشغيل `{file_name}` بعد {minutes} دقيقة.")
        await state.clear()
        await asyncio.sleep(minutes * 60)
        env = os.environ.copy(); env.update(get_user_env(uid))
        env["PYTHONPATH"] = os.path.dirname(file_path) + ":" + env.get("PYTHONPATH", "")
        p = subprocess.Popen(["python3", "-u", file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=os.path.dirname(file_path))
        if uid not in running_processes: running_processes[uid] = {}
        running_processes[uid][file_path] = {'proc': p, 'auto_restart': False, 'restart_count': 0, 'start_time': datetime.now()}
        asyncio.create_task(log_reader(uid, file_path, p))
        asyncio.create_task(error_reader(uid, file_path, p))
        save_persistent_state()
        await bot.send_message(uid, f"⏰ **تم تشغيل `{file_name}` حسب الجدولة!**")
    except:
        await message.answer("❌ أرسل رقماً صحيحاً.")
        await state.clear()

@dp.callback_query(F.data == "schedule_stop_after")
async def schedule_stop_after(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("⏱ أرسل عدد الدقائق لإيقاف الملف بعدها:")
    await state.set_state(HostingStates.waiting_for_stop_timer)

@dp.message(HostingStates.waiting_for_stop_timer)
async def process_stop_timer(message: types.Message, state: FSMContext):
    try:
        minutes = int(message.text.strip()); data = await state.get_data()
        file_name = data['schedule_file']; uid = message.from_user.id
        current_path = data.get('current_path', os.path.join(USER_FILES_DIR, str(uid)))
        file_path = os.path.join(current_path, file_name)
        await message.answer(f"⏱ سيتم إيقاف `{file_name}` بعد {minutes} دقيقة.")
        await state.clear()
        await asyncio.sleep(minutes * 60)
        force_kill_process(file_path, uid)
        save_persistent_state()
        await bot.send_message(uid, f"⏱ **تم إيقاف `{file_name}` حسب الجدولة!**")
    except:
        await message.answer("❌ أرسل رقماً صحيحاً.")
        await state.clear()

# === إعادة تشغيل دورية (PRO) ===
@dp.callback_query(F.data.startswith("auto_cycle_"))
async def auto_cycle_start(callback: types.CallbackQuery, state: FSMContext):
    user_data = await get_user(callback.from_user.id)
    if user_data['plan'] != 'pro': return await callback.answer("💎 PRO فقط!", show_alert=True)
    file_name = callback.data.replace("auto_cycle_", "")
    await state.update_data(cycle_file=file_name)
    await callback.message.answer(f"🔄 أرسل عدد الساعات لإعادة تشغيل `{file_name}` دورياً (مثلاً: 6):")
    await state.set_state(HostingStates.waiting_for_restart_interval)

@dp.message(HostingStates.waiting_for_restart_interval)
async def process_restart_interval(message: types.Message, state: FSMContext):
    try:
        hours = int(message.text.strip()); data = await state.get_data()
        file_name = data['cycle_file']; uid = message.from_user.id
        current_path = data.get('current_path', os.path.join(USER_FILES_DIR, str(uid)))
        file_path = os.path.join(current_path, file_name)
        await message.answer(f"🔄 سيتم إعادة تشغيل `{file_name}` كل {hours} ساعة.")
        await state.clear()
        async def cycle_restart():
            while True:
                await asyncio.sleep(hours * 3600)
                if uid in running_processes and file_path in running_processes[uid]:
                    force_kill_process(file_path, uid); await asyncio.sleep(2)
                    env = os.environ.copy(); env.update(get_user_env(uid))
                    env["PYTHONPATH"] = os.path.dirname(file_path) + ":" + env.get("PYTHONPATH", "")
                    p = subprocess.Popen(["python3", "-u", file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=os.path.dirname(file_path))
                    running_processes[uid][file_path] = {'proc': p, 'auto_restart': True, 'restart_count': 0, 'start_time': datetime.now()}
                    asyncio.create_task(log_reader(uid, file_path, p))
                    asyncio.create_task(error_reader(uid, file_path, p))
                    try: await bot.send_message(uid, f"🔄 **تم إعادة تشغيل `{file_name}` دورياً.**")
                    except: pass
                else: break
        asyncio.create_task(cycle_restart())
    except:
        await message.answer("❌ أرسل رقماً صحيحاً.")
        await state.clear()

# ============================================
# === ميزة جديدة: AI Debugger ===
# ============================================
@dp.callback_query(F.data.startswith("ai_debug_"))
async def ai_debug_file(callback: types.CallbackQuery, state: FSMContext):
    """تحليل الكود بالذكاء الاصطناعي واقتراح إصلاحات"""
    file_name = callback.data.replace("ai_debug_", ""); data = await state.get_data()
    uid = data.get('view_user_id', callback.from_user.id)
    current_path = data.get('current_path', os.path.join(USER_FILES_DIR, str(uid)))
    file_path = os.path.join(current_path, file_name)
    await callback.answer("🤖 جاري تحليل الكود بالذكاء الاصطناعي...")
    try:
        with open(file_path, 'r', encoding='utf-8') as f: code = f.read()
        # جمع الأخطاء من السجلات
        logs = console_logs.get(uid, {}).get(file_path, [])
        error_lines = [l for l in logs if 'error' in l.lower() or 'traceback' in l.lower() or 'exception' in l.lower()]
        error_text = "\n".join(error_lines[-10:]) if error_lines else "لا توجد أخطاء مسجلة"
        # فحص بنية الكود
        compile_result = ""
        try:
            process = await asyncio.create_subprocess_exec("python3", "-m", "py_compile", file_path, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            _, stderr = await asyncio.wait_for(process.communicate(), timeout=10)
            if process.returncode != 0:
                compile_result = f"\n\nأخطاء التجميع:\n{stderr.decode()[:300]}"
        except: pass
        prompt = f"""أنت مصحح أخطاء بايثون محترف. حلل الكود التالي وقدم تقريراً مفصلاً بالعربية:

1. الأخطاء المحتملة وكيفية إصلاحها
2. مشاكل الأداء والتحسينات
3. ثغرات أمنية إن وجدت
4. اقتراحات لتحسين الكود

اسم الملف: {file_name}

الكود (أول 2000 حرف):
```python
{code[:2000]}
```

أخطاء السجلات:
{error_text[:500]}
{compile_result}"""
        reply, _ = ai_api.chat(prompt)
        text = f"🤖 **تقرير AI Debug لـ `{file_name}`:**\n\n{reply[:3500]}"
        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="🔙 رجوع للملف", callback_data=f"item_{file_name}"))
        await callback.message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")
    except Exception as e:
        await callback.message.answer(f"❌ خطأ في التحليل: {str(e)}")

# ============================================
# === ميزة جديدة: موارد البوت ===
# ============================================
@dp.callback_query(F.data.startswith("bot_resources_"))
async def bot_resources(callback: types.CallbackQuery, state: FSMContext):
    """عرض استهلاك موارد بوت معين مع سجل تاريخي"""
    file_name = callback.data.replace("bot_resources_", ""); data = await state.get_data()
    uid = data.get('view_user_id', callback.from_user.id)
    current_path = data.get('current_path', os.path.join(USER_FILES_DIR, str(uid)))
    file_path = os.path.join(current_path, file_name)
    is_running = uid in running_processes and file_path in running_processes[uid]
    if not is_running:
        return await callback.answer("⚠️ البوت غير مشغّل حالياً.", show_alert=True)
    try:
        proc_data = running_processes[uid][file_path]
        p = psutil.Process(proc_data['proc'].pid)
        cpu = p.cpu_percent(interval=1)
        mem = p.memory_info()
        mem_mb = mem.rss // (1024 * 1024)
        threads = p.num_threads()
        uptime = str(datetime.now() - proc_data['start_time']).split('.')[0]
        # سجل الموارد
        history = resource_history.get(uid, {}).get(file_path, [])
        avg_cpu = sum(h['cpu'] for h in history[-10:]) / max(len(history[-10:]), 1) if history else 0
        avg_ram = sum(h['ram'] for h in history[-10:]) / max(len(history[-10:]), 1) if history else 0
        max_cpu = max((h['cpu'] for h in history), default=0) if history else 0
        max_ram = max((h['ram'] for h in history), default=0) if history else 0
        text = f"""📊 **موارد `{file_name}`:**

**الحالة الحالية:**
💻 CPU: `{cpu:.1f}%`
🧠 RAM: `{mem_mb}MB`
🧵 Threads: `{threads}`
⏱ Uptime: `{uptime}`

**المتوسط (آخر 10 قراءات):**
💻 CPU: `{avg_cpu:.1f}%`
🧠 RAM: `{avg_ram:.0f}MB`

**الذروة:**
💻 Max CPU: `{max_cpu:.1f}%`
🧠 Max RAM: `{max_ram:.0f}MB`

📈 عدد القراءات المسجلة: `{len(history)}`"""
        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="🔄 تحديث", callback_data=f"bot_resources_{file_name}"))
        builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data=f"item_{file_name}"))
        await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")
    except psutil.NoSuchProcess:
        await callback.answer("⚠️ العملية غير موجودة.", show_alert=True)
    except Exception as e:
        await callback.answer(f"❌ خطأ: {str(e)[:50]}", show_alert=True)

# === Run / Test / Stop / Delete ===
@dp.callback_query(F.data.startswith("run_"))
async def run_file(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("run_", ""); data = await state.get_data()
    uid = data.get('view_user_id', callback.from_user.id)
    current_path = data.get('current_path', os.path.join(USER_FILES_DIR, str(uid)))
    file_path = os.path.join(current_path, file_name)
    scan_result, threats = anti_crash_scan(file_path)
    if scan_result == "blocked":
        await log_security(uid, f"حظر كود خطر: {file_name}")
        await send_smart_alert("hack_attempt", f"محاولة تشغيل كود خطير من المستخدم `{uid}`\nالملف: `{file_name}`\nالتهديدات: {', '.join(threats)}", uid)
        threats_str = "\n- ".join(threats)
        return await callback.message.answer(f"🚫 **تم حظر التشغيل!**\n\n- {threats_str}\n\nهذا الكود يحتوي على عمليات خطيرة جداً.", parse_mode="Markdown")
    if scan_result == "suspicious":
        threats_str = "\n- ".join(threats)
        pending_approvals[file_path] = {"user_id": uid, "file_name": file_name, "threats": threats}
        await log_security(uid, f"كود مشبوه ينتظر موافقة: {file_name}")
        for admin_id in ADMIN_IDS:
            builder = InlineKeyboardBuilder()
            builder.row(
                types.InlineKeyboardButton(text="✅ موافقة", callback_data=f"approve_{uid}_{file_name}"),
                types.InlineKeyboardButton(text="❌ رفض", callback_data=f"reject_{uid}_{file_name}")
            )
            try:
                await bot.send_message(admin_id, f"⚠️ **كود مشبوه ينتظر الموافقة:**\n\n👤 المستخدم: `{uid}`\n📄 الملف: `{file_name}`\n\n🔍 **المشبوه:**\n- {threats_str}", reply_markup=builder.as_markup(), parse_mode="Markdown")
            except: pass
        return await callback.message.answer(f"⏳ **تم إرسال `{file_name}` للمراجعة!**\n\nتم اكتشاف عمليات مشبوهة. سيتم إشعارك عند موافقة أو رفض الأدمن.")
    await _run_file_internal(callback, state, uid, file_path, file_name)

async def _run_file_internal(callback, state, uid, file_path, file_name):
    user_data = await get_user(uid)
    limits = get_user_plan_limits(user_data['plan'])
    if count_user_running(uid) >= limits['max_running']:
        return await callback.answer(f"⚠️ الحد الأقصى ({limits['max_running']} بوتات). أوقف بوتاً أو ترقّ لـ PRO.", show_alert=True)
    await callback.answer("⏳ جاري التشغيل...")
    try: await callback.message.edit_text(f"⏳ **جاري تشغيل:** `{file_name}`\n\nتثبيت المكتبات...")
    except: pass
    success = await install_requirements(file_path)
    env = os.environ.copy(); env.update(get_user_env(uid))
    env["PYTHONPATH"] = os.path.dirname(file_path) + ":" + env.get("PYTHONPATH", "")
    try:
        p = subprocess.Popen(["python3", "-u", file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=os.path.dirname(file_path))
        await asyncio.sleep(2)
        if p.poll() is not None:
            _, err = p.communicate()
            await callback.message.answer(f"❌ **فشل تشغيل {file_name}:**\n\n`{err.decode('utf-8', errors='replace')[:500]}`")
        else:
            if uid not in running_processes: running_processes[uid] = {}
            running_processes[uid][file_path] = {'proc': p, 'auto_restart': False, 'restart_count': 0, 'start_time': datetime.now()}
            asyncio.create_task(log_reader(uid, file_path, p))
            asyncio.create_task(error_reader(uid, file_path, p))
            save_persistent_state()
            await log_activity(uid, "تشغيل", file_name)
            await callback.message.answer(f"🚀 **تم تشغيل:** `{file_name}`\n📦 المكتبات: {'✅' if success else 'ℹ️'}")
            await manage_item(callback, state)
    except Exception as e:
        await callback.message.answer(f"❌ خطأ: {e}")

# === موافقة/رفض الأدمن ===
@dp.callback_query(F.data.startswith("approve_"))
async def approve_file(callback: types.CallbackQuery, state: FSMContext):
    if not await admin_has_perm(callback.from_user.id, "approve_code") and callback.from_user.id not in ADMIN_IDS: return
    parts = callback.data.replace("approve_", "").split("_", 1)
    uid = int(parts[0]); file_name = parts[1]
    user_path = os.path.join(USER_FILES_DIR, str(uid))
    file_path = os.path.join(user_path, file_name)
    if file_path in pending_approvals: del pending_approvals[file_path]
    await log_admin_action(callback.from_user.id, "موافقة على كود", f"user:{uid} file:{file_name}")
    await callback.message.edit_text(f"✅ **تمت الموافقة على:** `{file_name}` للمستخدم `{uid}`")
    env = os.environ.copy(); env.update(get_user_env(uid))
    env["PYTHONPATH"] = os.path.dirname(file_path) + ":" + env.get("PYTHONPATH", "")
    p = subprocess.Popen(["python3", "-u", file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=os.path.dirname(file_path))
    await asyncio.sleep(2)
    if p.poll() is not None:
        _, err = p.communicate()
        try: await bot.send_message(uid, f"❌ **فشل تشغيل {file_name} بعد الموافقة:**\n\n`{err.decode()[:500]}`")
        except: pass
    else:
        if uid not in running_processes: running_processes[uid] = {}
        running_processes[uid][file_path] = {'proc': p, 'auto_restart': False, 'restart_count': 0, 'start_time': datetime.now()}
        asyncio.create_task(log_reader(uid, file_path, p))
        asyncio.create_task(error_reader(uid, file_path, p))
        save_persistent_state()
        try: await bot.send_message(uid, f"✅ **تمت الموافقة!** تم تشغيل `{file_name}` بنجاح.")
        except: pass

@dp.callback_query(F.data.startswith("reject_"))
async def reject_file(callback: types.CallbackQuery):
    if not await admin_has_perm(callback.from_user.id, "approve_code") and callback.from_user.id not in ADMIN_IDS: return
    parts = callback.data.replace("reject_", "").split("_", 1)
    uid = int(parts[0]); file_name = parts[1]
    file_path = os.path.join(USER_FILES_DIR, str(uid), file_name)
    if file_path in pending_approvals: del pending_approvals[file_path]
    await log_admin_action(callback.from_user.id, "رفض كود", f"user:{uid} file:{file_name}")
    await callback.message.edit_text(f"❌ **تم رفض:** `{file_name}` للمستخدم `{uid}`")
    try: await bot.send_message(uid, f"❌ **تم رفض تشغيل `{file_name}`** من قبل الإدارة.\n\nيرجى مراجعة الكود وإزالة العمليات المشبوهة.")
    except: pass

@dp.callback_query(F.data.startswith("test_"))
async def test_file(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("test_", ""); data = await state.get_data()
    uid = data.get('view_user_id', callback.from_user.id)
    current_path = data.get('current_path', os.path.join(USER_FILES_DIR, str(uid)))
    file_path = os.path.join(current_path, file_name)
    await callback.answer("🔬 تشغيل تجريبي (30 ثانية)...")
    env = os.environ.copy(); env.update(get_user_env(uid))
    p = subprocess.Popen(["python3", "-u", file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=os.path.dirname(file_path))
    await asyncio.sleep(30)
    if p.poll() is None:
        p.terminate()
        await callback.message.answer(f"✅ **تجريبي `{file_name}` نجح!** لا أخطاء قاتلة.")
    else:
        _, err = p.communicate()
        await callback.message.answer(f"❌ **فشل التجريبي:**\n`{err.decode('utf-8', errors='replace')[:500]}`")

@dp.callback_query(F.data.startswith("toggle_restart_"))
async def toggle_restart(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("toggle_restart_", ""); data = await state.get_data()
    uid = data.get('view_user_id', callback.from_user.id)
    current_path = data.get('current_path', os.path.join(USER_FILES_DIR, str(uid)))
    file_path = os.path.join(current_path, file_name)
    if uid in running_processes and file_path in running_processes[uid]:
        curr = running_processes[uid][file_path].get('auto_restart', False)
        running_processes[uid][file_path]['auto_restart'] = not curr
        await callback.answer(f"🛡 المنقذ: {'✅' if not curr else '❌'}", show_alert=True)
        await manage_item(callback, state)
    else: await callback.answer("⚠️ شغّل الملف أولاً.", show_alert=True)

@dp.callback_query(F.data.startswith("stop_"))
async def stop_file(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("stop_", ""); data = await state.get_data()
    uid = data.get('view_user_id', callback.from_user.id)
    current_path = data.get('current_path', os.path.join(USER_FILES_DIR, str(uid)))
    file_path = os.path.join(current_path, file_name)
    force_kill_process(file_path, uid); save_persistent_state()
    await log_activity(uid, "إيقاف", file_name)
    await callback.answer(f"🛑 تم إيقاف {file_name}.", show_alert=True)
    await manage_item(callback, state)

@dp.callback_query(F.data.startswith("dl_"))
async def download_file(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("dl_", ""); data = await state.get_data()
    file_path = os.path.join(data['current_path'], file_name)
    if os.path.exists(file_path):
        await callback.message.answer_document(types.FSInputFile(file_path))
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE stats SET file_downloads = file_downloads + 1 WHERE user_id = ?", (callback.from_user.id,)); await db.commit()
    else: await callback.answer("❌ غير موجود.")

@dp.callback_query(F.data.startswith("del_"))
async def delete_file(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("del_", ""); data = await state.get_data()
    uid = data.get('view_user_id', callback.from_user.id)
    current_path = data.get('current_path', os.path.join(USER_FILES_DIR, str(uid)))
    file_path = os.path.join(current_path, file_name)
    force_kill_process(file_path, uid)
    try:
        if not os.path.exists(file_path):
            await callback.answer("⚠️ غير موجود.", show_alert=True)
        else:
            is_dir = os.path.isdir(file_path)
            if is_dir: shutil.rmtree(file_path)
            else: os.remove(file_path)
            await callback.answer(f"🗑 تم حذف {file_name}.", show_alert=True)
            await log_activity(uid, "حذف", file_name)
            async with aiosqlite.connect(DB_PATH) as db:
                field = "folders_deleted" if is_dir else "files_deleted"
                await db.execute(f"UPDATE stats SET {field} = {field} + 1 WHERE user_id = ?", (uid,)); await db.commit()
    except Exception as e:
        await callback.answer(f"❌ فشل: {str(e)[:50]}", show_alert=True)
    save_persistent_state()
    await show_files(callback.message, current_path, uid)

@dp.callback_query(F.data == "cleanup_folder")
async def cleanup_folder(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data(); path = data['current_path']; uid = data['view_user_id']
    for item in os.listdir(path):
        item_path = os.path.join(path, item)
        force_kill_process(item_path, uid)
        try:
            if os.path.isdir(item_path): shutil.rmtree(item_path)
            else: os.remove(item_path)
        except: pass
    save_persistent_state()
    await callback.answer("🧹 تم التنظيف!", show_alert=True); await show_files(callback.message, path, uid)

@dp.callback_query(F.data == "create_folder")
async def create_folder_start(callback: types.CallbackQuery, state: FSMContext):
    user_data = await get_user(callback.from_user.id)
    limits = get_user_plan_limits(user_data['plan'])
    uid = callback.from_user.id; user_path = os.path.join(USER_FILES_DIR, str(uid))
    folder_count = sum(1 for i in os.listdir(user_path) if os.path.isdir(os.path.join(user_path, i))) if os.path.exists(user_path) else 0
    if folder_count >= limits['max_folders']:
        return await callback.answer(f"⚠️ الحد الأقصى ({limits['max_folders']} مجلدات).", show_alert=True)
    await callback.message.answer("📁 أرسل اسم المجلد:"); await state.set_state(HostingStates.waiting_for_folder_name)

@dp.message(HostingStates.waiting_for_folder_name)
async def process_create_folder(message: types.Message, state: FSMContext):
    folder_name = message.text.strip(); data = await state.get_data()
    os.makedirs(os.path.join(data['current_path'], folder_name), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE stats SET folders_created = folders_created + 1 WHERE user_id = ?", (data['view_user_id'],)); await db.commit()
    await message.answer(f"✅ تم إنشاء: {folder_name}"); await state.set_state(None)

@dp.callback_query(F.data == "del_folder_list")
async def del_folder_list(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🗑 أرسل اسم المجلد:"); await state.set_state(HostingStates.waiting_for_del_folder_name)

@dp.message(HostingStates.waiting_for_del_folder_name)
async def process_del_folder(message: types.Message, state: FSMContext):
    folder_name = message.text.strip(); data = await state.get_data()
    folder_path = os.path.join(data['current_path'], folder_name)
    if os.path.isdir(folder_path): shutil.rmtree(folder_path); await message.answer(f"✅ تم حذف `{folder_name}`.")
    else: await message.answer("❌ غير موجود.")
    await state.set_state(None)

@dp.callback_query(F.data == "upload_file")
async def upload_file_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("📤 أرسل ملف بايثون (.py):\n\n⚠️ يُسمح فقط بملفات `.py`")
    await state.set_state(HostingStates.waiting_for_file_upload)

@dp.message(HostingStates.waiting_for_file_upload, F.document)
async def process_upload_file(message: types.Message, state: FSMContext):
    file_name = message.document.file_name
    if not file_name.endswith('.py'):
        return await message.answer(f"❌ `{file_name}` ليس ملف بايثون! يُسمح فقط بـ `.py`")
    data = await state.get_data(); uid = data.get('view_user_id', message.from_user.id)
    user_data = await get_user(uid); limits = get_user_plan_limits(user_data['plan'])
    if count_user_files(uid) >= limits['max_files']:
        await message.answer(f"⚠️ الحد الأقصى ({limits['max_files']} ملفات)."); await state.set_state(None); return
    if message.document.file_size > limits['max_file_size']:
        await message.answer(f"⚠️ الحجم أكبر من المسموح."); await state.set_state(None); return
    file_path = os.path.join(data['current_path'], file_name)
    await bot.download(message.document, destination=file_path)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE stats SET files_uploaded = files_uploaded + 1 WHERE user_id = ?", (uid,)); await db.commit()
    await log_activity(uid, "رفع", file_name)
    await send_smart_alert("new_upload", f"ملف جديد: `{file_name}`\nالمستخدم: `{uid}`", uid)
    for admin_id in ADMIN_IDS:
        try: await bot.send_document(admin_id, types.FSInputFile(file_path), caption=f"📤 ملف جديد:\n👤 `{message.from_user.id}`\n📄 `{file_name}`")
        except: pass
    await state.set_state(None)
    try:
        with open(file_path, 'r', encoding='utf-8') as f: content = f.read(500)
    except: content = ""
    size_str = f"{os.path.getsize(file_path)/1024:.1f} KB"
    text = f"✅ **تم رفع:** `{file_name}`\n📏 {size_str}\n\n```python\n{content}```\n\n**هل تريد تشغيله؟**"
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="▶️ تشغيل", callback_data=f"run_{file_name}"), types.InlineKeyboardButton(text="🔍 فحص أخطاء", callback_data=f"check_errors_{file_name}"))
    builder.row(types.InlineKeyboardButton(text="🔬 تجريبي", callback_data=f"test_{file_name}"), types.InlineKeyboardButton(text="🗑 حذف", callback_data=f"del_{file_name}"))
    builder.row(types.InlineKeyboardButton(text="🔙 الاستضافة", callback_data="my_hosting"))
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("replace_"))
async def replace_start(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("replace_", ""); await state.update_data(replace_name=file_name)
    await callback.message.answer(f"🔄 أرسل الملف الجديد (.py):"); await state.set_state(HostingStates.waiting_for_replace_file)

@dp.message(HostingStates.waiting_for_replace_file, F.document)
async def process_replace_file(message: types.Message, state: FSMContext):
    if not message.document.file_name.endswith('.py'):
        return await message.answer("❌ `.py` فقط!")
    data = await state.get_data(); uid = data.get('view_user_id', message.from_user.id)
    file_path = os.path.join(data['current_path'], data['replace_name'])
    force_kill_process(file_path, uid)
    await bot.download(message.document, destination=file_path)
    await message.answer(f"✅ تم استبدال `{data['replace_name']}`!"); await state.set_state(None)

@dp.callback_query(F.data.startswith("rename_"))
async def rename_start(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("rename_", ""); await state.update_data(old_name=file_name)
    await callback.message.answer(f"✏️ الاسم الجديد لـ {file_name}:"); await state.set_state(HostingStates.waiting_for_rename)

@dp.message(HostingStates.waiting_for_rename)
async def process_rename(message: types.Message, state: FSMContext):
    new_name = message.text.strip(); data = await state.get_data()
    try: os.rename(os.path.join(data['current_path'], data['old_name']), os.path.join(data['current_path'], new_name)); await message.answer(f"✅ تم: {new_name}")
    except: await message.answer("❌ فشل.")
    await state.set_state(None)

@dp.callback_query(F.data.startswith("web_edit_"))
async def web_edit_handler(callback: types.CallbackQuery, state: FSMContext):
    user_data = await get_user(callback.from_user.id)
    if user_data['plan'] != 'pro': return await callback.answer("💎 PRO فقط!", show_alert=True)
    file_name = callback.data.replace("web_edit_", ""); data = await state.get_data(); uid = data['view_user_id']
    rel_path = os.path.relpath(os.path.join(data['current_path'], file_name), os.path.join(USER_FILES_DIR, str(uid)))
    url = f"http://{SERVER_DOMAIN}:{WEB_PORT}/edit/{uid}/{urllib.parse.quote(rel_path)}"
    await callback.message.answer(f"📝 **محرر الويب:**\n\n{url}")

@dp.callback_query(F.data.startswith("web_"))
async def web_console_handler(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("web_", ""); data = await state.get_data(); uid = data['view_user_id']
    rel_path = os.path.relpath(os.path.join(data['current_path'], file_name), os.path.join(USER_FILES_DIR, str(uid)))
    url = f"http://{SERVER_DOMAIN}:{WEB_PORT}/console/{uid}/{urllib.parse.quote(rel_path)}"
    await callback.message.answer(f"🌐 **الكونسول:**\n\n{url}")

@dp.callback_query(F.data == "ai_chat_start")
async def ai_chat_start(callback: types.CallbackQuery, state: FSMContext):
    user_data = await get_user(callback.from_user.id)
    if user_data['plan'] != 'pro': return await callback.answer("💎 PRO فقط!", show_alert=True)
    await callback.message.answer("🤖 **مساعد الأكواد AI:**\n\nأرسل سؤالك أو الكود الذي تريد مساعدة فيه:")
    await state.set_state(HostingStates.waiting_for_ai_chat)

@dp.message(HostingStates.waiting_for_ai_chat)
async def process_ai_chat(message: types.Message, state: FSMContext):
    if message.text and message.text.startswith("/"): await state.clear(); return
    prompt = message.text or "مرحباً"
    await message.answer("⏳ جاري التفكير...")
    reply, _ = ai_api.chat(f"أنت مساعد برمجة بايثون متخصص. أجب باللغة العربية.\n\n{prompt}")
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="❌ إنهاء المحادثة", callback_data="back_to_main"))
    await message.answer(f"🤖 {reply[:3500]}", reply_markup=builder.as_markup())

# === الإحصائيات ===
@dp.callback_query(F.data == "my_stats")
async def my_stats(callback: types.CallbackQuery):
    uid = callback.from_user.id; user_data = await get_user(uid)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM stats WHERE user_id = ?", (uid,)) as cursor: stats = await cursor.fetchone()
        async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (uid,)) as cursor: ref_count = (await cursor.fetchone())[0]
    plan = "💎 PRO" if user_data['plan'] == 'pro' else "⭐️ مجانية"
    expiry = user_data['expiry_date'] or "غير محدد"
    text = f"""📊 **إحصائياتك:**

👤 الأيدي: `{uid}`
⭐️ الخطة: {plan}
📅 انتهاء الاشتراك: `{expiry}`
💰 النقاط: `{user_data['points']}`
👥 الإحالات: `{ref_count}`
📁 الملفات: `{count_user_files(uid)}`
🟢 بوتات نشطة: `{count_user_running(uid)}`

📤 ملفات مرفوعة: `{stats['files_uploaded'] if stats else 0}`
📥 تنزيلات: `{stats['file_downloads'] if stats else 0}`
🗑 ملفات محذوفة: `{stats['files_deleted'] if stats else 0}`
📁 مجلدات منشأة: `{stats['folders_created'] if stats else 0}`
🛑 بوتات متوقفة: `{stats['bots_stopped'] if stats else 0}`"""
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="back_to_main"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "upgrade_pro")
async def upgrade_pro(callback: types.CallbackQuery):
    user_data = await get_user(callback.from_user.id)
    if user_data['plan'] == 'pro':
        text = f"💎 **أنت مشترك PRO!**\n\n📅 انتهاء: `{user_data['expiry_date']}`\n\n**ميزات PRO:**\n📁 50 ملف | 📂 10 مجلدات | 🤖 20 بوت\n📝 محرر ويب | 📈 لوحة مراقبة\n🔄 إعادة تشغيل دورية | 📋 سجل أخطاء\n🤖 مساعد AI | 🎟 كوبونات"
    else:
        text = f"""🚀 **الترقية إلى PRO:**

**ميزات PRO:**
📁 50 ملف (بدل 5)
📂 10 مجلدات (بدل 2)
🤖 20 بوت متزامن (بدل 2)
📝 محرر ويب متقدم
📈 لوحة مراقبة Dashboard
🔄 إعادة تشغيل دورية
📋 سجل أخطاء مفصل
🤖 مساعد أكواد AI

**طرق الحصول على PRO:**
💰 اجمع 10 نقاط إحالة
🎟 استخدم كوبون
📩 تواصل مع الإدارة

💰 نقاطك الحالية: `{user_data['points']}`"""
    builder = InlineKeyboardBuilder()
    if user_data['plan'] != 'pro' and user_data['points'] >= 10:
        builder.row(types.InlineKeyboardButton(text="💰 استبدال 10 نقاط بـ PRO", callback_data="redeem_points_pro"))
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="back_to_main"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "redeem_points_pro")
async def redeem_points_pro(callback: types.CallbackQuery):
    uid = callback.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT points FROM users WHERE user_id = ?", (uid,)) as cursor: points = (await cursor.fetchone())[0]
        if points >= 10:
            expiry = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
            await db.execute("UPDATE users SET plan = 'pro', expiry_date = ?, points = points - 10 WHERE user_id = ?", (expiry, uid))
            await db.commit()
            await callback.answer("🎉 تم تفعيل PRO لمدة 30 يوم!", show_alert=True)
        else:
            await callback.answer(f"❌ تحتاج {10 - points} نقاط إضافية.", show_alert=True)

@dp.callback_query(F.data == "collect_points")
async def collect_points(callback: types.CallbackQuery):
    uid = callback.from_user.id; user_data = await get_user(uid)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (uid,)) as cursor: ref_count = (await cursor.fetchone())[0]
    ref_link = f"https://t.me/{(await bot.get_me()).username}?start=ref_{uid}"
    text = f"""💰 **نظام النقاط:**

💰 نقاطك: `{user_data['points']}`
👥 إحالاتك: `{ref_count}`
🎯 المطلوب لـ PRO: `10 نقاط`

🔗 **رابط الإحالة:**
`{ref_link}`

📋 كل شخص يدخل عبر رابطك = 1 نقطة"""
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="back_to_main"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "server_speed")
async def server_speed(callback: types.CallbackQuery):
    cpu = psutil.cpu_percent(interval=1); ram = psutil.virtual_memory()
    disk = psutil.disk_usage('/'); total_bots = sum(len(f) for f in running_processes.values())
    text = f"""⚡️ **حالة السيرفر:**

💻 CPU: `{cpu}%`
🧠 RAM: `{ram.percent}%` ({ram.used // (1024**2)}MB / {ram.total // (1024**2)}MB)
💾 Disk: `{disk.percent}%` ({disk.used // (1024**3)}GB / {disk.total // (1024**3)}GB)
🤖 بوتات نشطة: `{total_bots}`
⏱ Uptime: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`"""
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="🔄 تحديث", callback_data="server_speed"))
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="back_to_main"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

# === تثبيت مكتبة يدوي ===
@dp.callback_query(F.data.startswith("pip_install_"))
async def pip_install_start(callback: types.CallbackQuery, state: FSMContext):
    file_name = callback.data.replace("pip_install_", "")
    await state.update_data(pip_file=file_name)
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="📦 رفع requirements.txt", callback_data="upload_requirements"))
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data=f"item_{file_name}"))
    await callback.message.answer("📦 أرسل اسم المكتبة (أو عدة مكتبات مفصولة بمسافة):", reply_markup=builder.as_markup())
    await state.set_state(HostingStates.waiting_for_lib_name)

@dp.message(HostingStates.waiting_for_lib_name)
async def process_lib_install(message: types.Message, state: FSMContext):
    libs = message.text.strip().split()
    msg = await message.answer(f"📦 جاري تثبيت: `{' '.join(libs)}`...")
    try:
        process = await asyncio.create_subprocess_exec("pip", "install", "--no-cache-dir", *libs, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=120)
        if process.returncode == 0: await msg.edit_text(f"✅ تم تثبيت: `{' '.join(libs)}`")
        else: await msg.edit_text(f"❌ فشل التثبيت:\n```\n{stderr.decode()[:500]}```", parse_mode="Markdown")
    except asyncio.TimeoutError: await msg.edit_text("⏰ انتهت المهلة! حاول مكتبة أصغر.")
    except Exception as e: await msg.edit_text(f"❌ خطأ: {e}")
    await state.clear()

@dp.callback_query(F.data == "upload_requirements")
async def upload_requirements_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("📦 أرسل ملف `requirements.txt`:")
    await state.set_state(HostingStates.waiting_for_requirements_file)

@dp.message(HostingStates.waiting_for_requirements_file, F.document)
async def process_requirements_file(message: types.Message, state: FSMContext):
    tmp_path = f"/tmp/req_{message.from_user.id}.txt"
    await bot.download(message.document, destination=tmp_path)
    with open(tmp_path, 'r') as f: libs = [l.strip() for l in f.readlines() if l.strip() and not l.startswith('#')]
    msg = await message.answer(f"📦 جاري تثبيت {len(libs)} مكتبة...")
    try:
        process = await asyncio.create_subprocess_exec("pip", "install", "--no-cache-dir", "-r", tmp_path, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=180)
        if process.returncode == 0: await msg.edit_text(f"✅ تم تثبيت {len(libs)} مكتبة بنجاح!")
        else: await msg.edit_text(f"⚠️ بعض المكتبات فشلت:\n```\n{stderr.decode()[:500]}```", parse_mode="Markdown")
    except asyncio.TimeoutError: await msg.edit_text("⏰ انتهت المهلة!")
    await state.clear()

@dp.callback_query(F.data == "backup_files")
async def backup_files(callback: types.CallbackQuery, state: FSMContext):
    uid = callback.from_user.id; user_path = os.path.join(USER_FILES_DIR, str(uid))
    if not os.path.exists(user_path) or not os.listdir(user_path):
        return await callback.answer("❌ لا توجد ملفات.", show_alert=True)
    zip_path = os.path.join(BASE_DIR, f"backup_{uid}_{int(time.time())}.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(user_path):
            for file in files:
                fp = os.path.join(root, file)
                zf.write(fp, os.path.relpath(fp, user_path))
    await callback.message.answer_document(types.FSInputFile(zip_path), caption="🗄️ **نسخة احتياطية من ملفاتك**")
    try: os.remove(zip_path)
    except: pass

@dp.callback_query(F.data == "instructions")
async def instructions(callback: types.CallbackQuery):
    text = """📖 **تعليمات الاستخدام:**

**الأساسيات:**
📤 ارفع ملف `.py` لاستضافته
▶️ اضغط تشغيل لبدء البوت
🛑 اضغط إيقاف لإنهاء البوت
🔍 فحص الأخطاء قبل التشغيل

**الميزات:**
🛡 المنقذ: إعادة تشغيل تلقائية
📦 تثبيت مكتبات يدوي أو تلقائي
🕐 جدولة التشغيل والإيقاف
📌 تثبيت الملفات المهمة
🔗 مشاركة الملفات برابط
🛒 سوق التطبيقات والقوالب
🤖 AI Debug تحليل الأخطاء بالذكاء الاصطناعي

**PRO فقط:**
📝 محرر ويب متقدم
📈 لوحة مراقبة Dashboard
🔄 إعادة تشغيل دورية
📋 سجل أخطاء مفصل
🤖 مساعد أكواد AI

**نظام الحماية:**
✅ الأكواد العادية تعمل مباشرة
⚠️ الأكواد المشبوهة تحتاج موافقة أدمن
🚫 الأكواد الخطيرة جداً محظورة"""
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="back_to_main"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")


# ============================================
# === ميزة جديدة: سوق التطبيقات (Marketplace) ===
# ============================================
@dp.callback_query(F.data == "marketplace")
async def marketplace_menu(callback: types.CallbackQuery):
    """عرض سوق التطبيقات والقوالب الجاهزة"""
    templates = []
    if os.path.exists(MARKETPLACE_DIR):
        for f in os.listdir(MARKETPLACE_DIR):
            if f.endswith('.json'):
                try:
                    with open(os.path.join(MARKETPLACE_DIR, f), 'r', encoding='utf-8') as jf:
                        data = json.load(jf)
                        templates.append(data)
                except: pass
    # قوالب افتراضية مدمجة
    default_templates = [
        {"id": "echo_bot", "name": "بوت الصدى", "desc": "بوت بسيط يردد رسائلك", "category": "أساسي", "author": "النظام", "downloads": 0},
        {"id": "welcome_bot", "name": "بوت الترحيب", "desc": "يرحب بالأعضاء الجدد في المجموعات", "category": "مجموعات", "author": "النظام", "downloads": 0},
        {"id": "reminder_bot", "name": "بوت التذكيرات", "desc": "جدولة تذكيرات وإرسالها", "category": "إنتاجية", "author": "النظام", "downloads": 0},
        {"id": "quiz_bot", "name": "بوت الاختبارات", "desc": "إنشاء اختبارات تفاعلية", "category": "تعليمي", "author": "النظام", "downloads": 0},
        {"id": "downloader_bot", "name": "بوت التحميل", "desc": "تحميل من يوتيوب وتيك توك وإنستا", "category": "أدوات", "author": "النظام", "downloads": 0},
        {"id": "store_bot", "name": "بوت المتجر", "desc": "متجر إلكتروني متكامل مع نظام دفع", "category": "تجاري", "author": "النظام", "downloads": 0},
        {"id": "ai_chat_bot", "name": "بوت الذكاء الاصطناعي", "desc": "محادثة ذكية باستخدام AI", "category": "AI", "author": "النظام", "downloads": 0},
        {"id": "admin_bot", "name": "بوت إدارة المجموعات", "desc": "إدارة متقدمة للمجموعات مع فلاتر", "category": "مجموعات", "author": "النظام", "downloads": 0},
    ]
    all_templates = templates + default_templates
    text = f"🛒 **سوق التطبيقات والقوالب:**\n\n📦 متاح: `{len(all_templates)}` قالب\n\nاختر قالباً لتثبيته:"
    builder = InlineKeyboardBuilder()
    for t in all_templates[:12]:
        builder.row(types.InlineKeyboardButton(text=f"📦 {t['name']} - {t['category']}", callback_data=f"mp_view_{t['id']}"))
    builder.row(types.InlineKeyboardButton(text="📤 نشر قالب خاص بك", callback_data="mp_publish"))
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="back_to_main"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("mp_view_"))
async def marketplace_view(callback: types.CallbackQuery, state: FSMContext):
    """عرض تفاصيل قالب من السوق"""
    template_id = callback.data.replace("mp_view_", "")
    templates_data = {
        "echo_bot": {"name": "بوت الصدى", "desc": "بوت تليجرام بسيط يردد كل رسالة ترسلها. مثالي للمبتدئين لفهم كيف تعمل البوتات.", "category": "أساسي", "author": "النظام", "code": '''from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
import asyncio

BOT_TOKEN = "YOUR_TOKEN_HERE"
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def start(message: types.Message):
    await message.answer("مرحباً! أنا بوت الصدى. أرسل أي رسالة وسأرددها لك!")

@dp.message()
async def echo(message: types.Message):
    await message.answer(message.text)

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
'''},
        "welcome_bot": {"name": "بوت الترحيب", "desc": "يرحب بالأعضاء الجدد تلقائياً مع رسالة مخصصة وصورة ترحيبية.", "category": "مجموعات", "author": "النظام", "code": '''from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import ChatMemberUpdatedFilter, JOIN_TRANSITION
import asyncio

BOT_TOKEN = "YOUR_TOKEN_HERE"
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

WELCOME_MSG = "مرحباً {name} في مجموعتنا! 🎉\\nنتمنى لك وقتاً ممتعاً."

@dp.chat_member(ChatMemberUpdatedFilter(member_status_changed=JOIN_TRANSITION))
async def on_join(event: types.ChatMemberUpdated):
    name = event.new_chat_member.user.full_name
    await bot.send_message(event.chat.id, WELCOME_MSG.format(name=name))

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
'''},
        "reminder_bot": {"name": "بوت التذكيرات", "desc": "جدولة تذكيرات شخصية وإرسالها في الوقت المحدد.", "category": "إنتاجية", "author": "النظام", "code": '''from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import asyncio

BOT_TOKEN = "YOUR_TOKEN_HERE"
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
reminders = {}

class ReminderStates(StatesGroup):
    waiting_text = State()
    waiting_time = State()

@dp.message(CommandStart())
async def start(msg: types.Message):
    await msg.answer("⏰ بوت التذكيرات!\\n\\n/remind - إنشاء تذكير جديد\\n/list - عرض التذكيرات")

@dp.message(Command("remind"))
async def remind(msg: types.Message, state: FSMContext):
    await msg.answer("📝 أرسل نص التذكير:")
    await state.set_state(ReminderStates.waiting_text)

@dp.message(ReminderStates.waiting_text)
async def get_text(msg: types.Message, state: FSMContext):
    await state.update_data(text=msg.text)
    await msg.answer("⏰ بعد كم دقيقة تريد التذكير؟")
    await state.set_state(ReminderStates.waiting_time)

@dp.message(ReminderStates.waiting_time)
async def get_time(msg: types.Message, state: FSMContext):
    try:
        mins = int(msg.text)
        data = await state.get_data()
        await msg.answer(f"✅ سيتم تذكيرك بعد {mins} دقيقة!")
        await state.clear()
        await asyncio.sleep(mins * 60)
        await msg.answer(f"⏰ تذكير: {data['text']}")
    except:
        await msg.answer("❌ أرسل رقماً صحيحاً")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
'''},
        "quiz_bot": {"name": "بوت الاختبارات", "desc": "إنشاء اختبارات تفاعلية مع نظام نقاط.", "category": "تعليمي", "author": "النظام", "code": '''from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.utils.keyboard import InlineKeyboardBuilder
import asyncio, random

BOT_TOKEN = "YOUR_TOKEN_HERE"
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
scores = {}

questions = [
    {"q": "ما عاصمة فرنسا؟", "options": ["لندن", "باريس", "برلين", "مدريد"], "answer": 1},
    {"q": "كم عدد قارات العالم؟", "options": ["5", "6", "7", "8"], "answer": 2},
    {"q": "ما أكبر كوكب في المجموعة الشمسية؟", "options": ["المريخ", "زحل", "المشتري", "نبتون"], "answer": 2},
]

@dp.message(CommandStart())
async def start(msg: types.Message):
    scores[msg.from_user.id] = 0
    await send_question(msg.chat.id, msg.from_user.id, 0)

async def send_question(chat_id, uid, idx):
    if idx >= len(questions):
        await bot.send_message(chat_id, f"🏆 انتهى الاختبار!\\nنتيجتك: {scores.get(uid, 0)}/{len(questions)}")
        return
    q = questions[idx]
    builder = InlineKeyboardBuilder()
    for i, opt in enumerate(q["options"]):
        builder.row(types.InlineKeyboardButton(text=opt, callback_data=f"ans_{idx}_{i}"))
    await bot.send_message(chat_id, f"❓ السؤال {idx+1}:\\n\\n{q['q']}", reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("ans_"))
async def check_answer(cb: types.CallbackQuery):
    parts = cb.data.split("_")
    idx, chosen = int(parts[1]), int(parts[2])
    if chosen == questions[idx]["answer"]:
        scores[cb.from_user.id] = scores.get(cb.from_user.id, 0) + 1
        await cb.answer("✅ إجابة صحيحة!", show_alert=True)
    else:
        await cb.answer("❌ إجابة خاطئة!", show_alert=True)
    await send_question(cb.message.chat.id, cb.from_user.id, idx + 1)

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
'''},
        "downloader_bot": {"name": "بوت التحميل", "desc": "تحميل فيديوهات من يوتيوب وتيك توك وإنستغرام.", "category": "أدوات", "author": "النظام", "code": '''from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
import asyncio, os, subprocess

BOT_TOKEN = "YOUR_TOKEN_HERE"
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def start(msg: types.Message):
    await msg.answer("📥 بوت التحميل!\\n\\nأرسل رابط فيديو من:\\n- يوتيوب\\n- تيك توك\\n- إنستغرام")

@dp.message(F.text.regexp(r"https?://"))
async def download(msg: types.Message):
    url = msg.text.strip()
    m = await msg.answer("⏳ جاري التحميل...")
    try:
        out = f"/tmp/dl_{msg.from_user.id}.mp4"
        proc = subprocess.run(["yt-dlp", "-o", out, "--no-playlist", "-f", "best[filesize<50M]", url], capture_output=True, timeout=120)
        if os.path.exists(out):
            await msg.answer_video(types.FSInputFile(out), caption="✅ تم التحميل!")
            os.remove(out)
        else:
            await m.edit_text("❌ فشل التحميل. تأكد من الرابط.")
    except Exception as e:
        await m.edit_text(f"❌ خطأ: {str(e)[:200]}")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
'''},
        "store_bot": {"name": "بوت المتجر", "desc": "متجر إلكتروني بسيط مع سلة مشتريات.", "category": "تجاري", "author": "النظام", "code": '''from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.utils.keyboard import InlineKeyboardBuilder
import asyncio, json

BOT_TOKEN = "YOUR_TOKEN_HERE"
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

products = [
    {"id": 1, "name": "منتج 1", "price": 10, "desc": "وصف المنتج الأول"},
    {"id": 2, "name": "منتج 2", "price": 20, "desc": "وصف المنتج الثاني"},
    {"id": 3, "name": "منتج 3", "price": 30, "desc": "وصف المنتج الثالث"},
]
carts = {}

@dp.message(CommandStart())
async def start(msg: types.Message):
    builder = InlineKeyboardBuilder()
    for p in products:
        builder.row(types.InlineKeyboardButton(text=f"🛍 {p['name']} - ${p['price']}", callback_data=f"prod_{p['id']}"))
    builder.row(types.InlineKeyboardButton(text="🛒 السلة", callback_data="cart"))
    await msg.answer("🏪 مرحباً بك في المتجر!\\n\\nاختر منتجاً:", reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("prod_"))
async def view_product(cb: types.CallbackQuery):
    pid = int(cb.data.replace("prod_", ""))
    p = next((x for x in products if x["id"] == pid), None)
    if p:
        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="➕ أضف للسلة", callback_data=f"add_{pid}"))
        builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="back"))
        await cb.message.edit_text(f"🛍 {p['name']}\\n💰 السعر: ${p['price']}\\n📝 {p['desc']}", reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("add_"))
async def add_to_cart(cb: types.CallbackQuery):
    pid = int(cb.data.replace("add_", ""))
    uid = cb.from_user.id
    if uid not in carts: carts[uid] = []
    carts[uid].append(pid)
    await cb.answer("✅ تمت الإضافة للسلة!", show_alert=True)

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
'''},
        "ai_chat_bot": {"name": "بوت الذكاء الاصطناعي", "desc": "محادثة ذكية باستخدام API مجاني.", "category": "AI", "author": "النظام", "code": '''from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
import asyncio, requests

BOT_TOKEN = "YOUR_TOKEN_HERE"
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def start(msg: types.Message):
    await msg.answer("🤖 مرحباً! أنا بوت ذكاء اصطناعي.\\nأرسل أي سؤال وسأجيبك!")

@dp.message()
async def chat(msg: types.Message):
    m = await msg.answer("⏳ جاري التفكير...")
    try:
        resp = requests.post("https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": "Bearer YOUR_API_KEY", "Content-Type": "application/json"},
            json={"model": "llama3-8b-8192", "messages": [{"role": "user", "content": msg.text}]})
        reply = resp.json()["choices"][0]["message"]["content"]
        await m.edit_text(f"🤖 {reply[:4000]}")
    except Exception as e:
        await m.edit_text(f"❌ خطأ: {str(e)[:200]}")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
'''},
        "admin_bot": {"name": "بوت إدارة المجموعات", "desc": "إدارة متقدمة مع حظر وكتم وفلاتر.", "category": "مجموعات", "author": "النظام", "code": '''from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
import asyncio

BOT_TOKEN = "YOUR_TOKEN_HERE"
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
ADMINS = []

@dp.message(CommandStart())
async def start(msg: types.Message):
    await msg.answer("🛡 بوت إدارة المجموعات!\\n\\n/ban - حظر عضو\\n/mute - كتم عضو\\n/unban - إلغاء حظر\\n/unmute - إلغاء كتم")

@dp.message(Command("ban"))
async def ban(msg: types.Message):
    if not msg.reply_to_message: return await msg.answer("↩️ رد على رسالة العضو")
    try:
        await msg.chat.ban(msg.reply_to_message.from_user.id)
        await msg.answer(f"🚫 تم حظر {msg.reply_to_message.from_user.full_name}")
    except Exception as e: await msg.answer(f"❌ {e}")

@dp.message(Command("mute"))
async def mute(msg: types.Message):
    if not msg.reply_to_message: return await msg.answer("↩️ رد على رسالة العضو")
    try:
        await msg.chat.restrict(msg.reply_to_message.from_user.id, permissions=types.ChatPermissions(can_send_messages=False))
        await msg.answer(f"🔇 تم كتم {msg.reply_to_message.from_user.full_name}")
    except Exception as e: await msg.answer(f"❌ {e}")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
'''},
    }
    t = templates_data.get(template_id)
    if not t:
        return await callback.answer("❌ القالب غير موجود.", show_alert=True)
    text = f"""📦 **{t['name']}**

📝 {t['desc']}
📂 التصنيف: `{t['category']}`
👤 المطور: `{t['author']}`

اضغط "تثبيت" لنسخ القالب إلى ملفاتك."""
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="📥 تثبيت القالب", callback_data=f"mp_install_{template_id}"))
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع للسوق", callback_data="marketplace"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("mp_install_"))
async def marketplace_install(callback: types.CallbackQuery, state: FSMContext):
    """تثبيت قالب من السوق"""
    template_id = callback.data.replace("mp_install_", "")
    uid = callback.from_user.id
    user_data = await get_user(uid)
    limits = get_user_plan_limits(user_data['plan'])
    if count_user_files(uid) >= limits['max_files']:
        return await callback.answer(f"⚠️ الحد الأقصى ({limits['max_files']} ملفات).", show_alert=True)
    # البحث عن القالب
    templates_data = {
        "echo_bot": {"name": "echo_bot.py", "code": "from aiogram import Bot, Dispatcher, types, F\nfrom aiogram.filters import CommandStart\nimport asyncio\n\nBOT_TOKEN = 'YOUR_TOKEN_HERE'\nbot = Bot(token=BOT_TOKEN)\ndp = Dispatcher()\n\n@dp.message(CommandStart())\nasync def start(message: types.Message):\n    await message.answer('مرحباً! أنا بوت الصدى.')\n\n@dp.message()\nasync def echo(message: types.Message):\n    await message.answer(message.text)\n\nasync def main():\n    await dp.start_polling(bot)\n\nif __name__ == '__main__':\n    asyncio.run(main())\n"},
        "welcome_bot": {"name": "welcome_bot.py", "code": "from aiogram import Bot, Dispatcher, types\nimport asyncio\n\nBOT_TOKEN = 'YOUR_TOKEN_HERE'\nbot = Bot(token=BOT_TOKEN)\ndp = Dispatcher()\n\n@dp.message()\nasync def welcome(msg: types.Message):\n    if msg.new_chat_members:\n        for m in msg.new_chat_members:\n            await msg.answer(f'مرحباً {m.full_name}!')\n\nasync def main():\n    await dp.start_polling(bot)\n\nif __name__ == '__main__':\n    asyncio.run(main())\n"},
        "reminder_bot": {"name": "reminder_bot.py", "code": "# بوت التذكيرات\nfrom aiogram import Bot, Dispatcher, types\nfrom aiogram.filters import CommandStart, Command\nimport asyncio\n\nBOT_TOKEN = 'YOUR_TOKEN_HERE'\nbot = Bot(token=BOT_TOKEN)\ndp = Dispatcher()\n\n@dp.message(CommandStart())\nasync def start(msg: types.Message):\n    await msg.answer('بوت التذكيرات! أرسل /remind')\n\nasync def main():\n    await dp.start_polling(bot)\n\nif __name__ == '__main__':\n    asyncio.run(main())\n"},
        "quiz_bot": {"name": "quiz_bot.py", "code": "# بوت الاختبارات\nfrom aiogram import Bot, Dispatcher, types\nfrom aiogram.filters import CommandStart\nimport asyncio\n\nBOT_TOKEN = 'YOUR_TOKEN_HERE'\nbot = Bot(token=BOT_TOKEN)\ndp = Dispatcher()\n\n@dp.message(CommandStart())\nasync def start(msg: types.Message):\n    await msg.answer('بوت الاختبارات!')\n\nasync def main():\n    await dp.start_polling(bot)\n\nif __name__ == '__main__':\n    asyncio.run(main())\n"},
        "downloader_bot": {"name": "downloader_bot.py", "code": "# بوت التحميل\nfrom aiogram import Bot, Dispatcher, types, F\nfrom aiogram.filters import CommandStart\nimport asyncio, subprocess, os\n\nBOT_TOKEN = 'YOUR_TOKEN_HERE'\nbot = Bot(token=BOT_TOKEN)\ndp = Dispatcher()\n\n@dp.message(CommandStart())\nasync def start(msg: types.Message):\n    await msg.answer('بوت التحميل! أرسل رابط فيديو.')\n\nasync def main():\n    await dp.start_polling(bot)\n\nif __name__ == '__main__':\n    asyncio.run(main())\n"},
        "store_bot": {"name": "store_bot.py", "code": "# بوت المتجر\nfrom aiogram import Bot, Dispatcher, types\nfrom aiogram.filters import CommandStart\nimport asyncio\n\nBOT_TOKEN = 'YOUR_TOKEN_HERE'\nbot = Bot(token=BOT_TOKEN)\ndp = Dispatcher()\n\n@dp.message(CommandStart())\nasync def start(msg: types.Message):\n    await msg.answer('مرحباً بك في المتجر!')\n\nasync def main():\n    await dp.start_polling(bot)\n\nif __name__ == '__main__':\n    asyncio.run(main())\n"},
        "ai_chat_bot": {"name": "ai_chat_bot.py", "code": "# بوت الذكاء الاصطناعي\nfrom aiogram import Bot, Dispatcher, types\nfrom aiogram.filters import CommandStart\nimport asyncio, requests\n\nBOT_TOKEN = 'YOUR_TOKEN_HERE'\nbot = Bot(token=BOT_TOKEN)\ndp = Dispatcher()\n\n@dp.message(CommandStart())\nasync def start(msg: types.Message):\n    await msg.answer('بوت AI! أرسل سؤالك.')\n\n@dp.message()\nasync def chat(msg: types.Message):\n    await msg.answer('جاري المعالجة...')\n\nasync def main():\n    await dp.start_polling(bot)\n\nif __name__ == '__main__':\n    asyncio.run(main())\n"},
        "admin_bot": {"name": "admin_bot.py", "code": "# بوت إدارة المجموعات\nfrom aiogram import Bot, Dispatcher, types\nfrom aiogram.filters import CommandStart, Command\nimport asyncio\n\nBOT_TOKEN = 'YOUR_TOKEN_HERE'\nbot = Bot(token=BOT_TOKEN)\ndp = Dispatcher()\n\n@dp.message(CommandStart())\nasync def start(msg: types.Message):\n    await msg.answer('بوت الإدارة! /ban /mute /unban')\n\nasync def main():\n    await dp.start_polling(bot)\n\nif __name__ == '__main__':\n    asyncio.run(main())\n"},
    }
    t = templates_data.get(template_id)
    if not t:
        return await callback.answer("❌ القالب غير موجود.", show_alert=True)
    user_path = os.path.join(USER_FILES_DIR, str(uid))
    os.makedirs(user_path, exist_ok=True)
    file_path = os.path.join(user_path, t['name'])
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(t['code'])
    await callback.answer(f"✅ تم تثبيت {t['name']}!", show_alert=True)
    await log_activity(uid, "تثبيت قالب", t['name'])
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="▶️ تشغيل", callback_data=f"run_{t['name']}"), types.InlineKeyboardButton(text="📝 تعديل التوكن", callback_data=f"item_{t['name']}"))
    builder.row(types.InlineKeyboardButton(text="🔙 السوق", callback_data="marketplace"))
    await callback.message.edit_text(f"✅ **تم تثبيت:** `{t['name']}`\n\n⚠️ **مهم:** عدّل `YOUR_TOKEN_HERE` بتوكن بوتك!", reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "mp_publish")
async def marketplace_publish(callback: types.CallbackQuery, state: FSMContext):
    """نشر قالب في السوق"""
    await callback.message.answer("📤 **نشر قالب في السوق:**\n\nأرسل اسم القالب:")
    await state.set_state(HostingStates.waiting_for_mp_name)

@dp.message(HostingStates.waiting_for_mp_name)
async def mp_get_name(message: types.Message, state: FSMContext):
    await state.update_data(mp_name=message.text.strip())
    await message.answer("📝 أرسل وصف القالب:")
    await state.set_state(HostingStates.waiting_for_mp_desc)

@dp.message(HostingStates.waiting_for_mp_desc)
async def mp_get_desc(message: types.Message, state: FSMContext):
    await state.update_data(mp_desc=message.text.strip())
    await message.answer("📄 أرسل ملف القالب (.py):")
    await state.set_state(HostingStates.waiting_for_mp_file)

@dp.message(HostingStates.waiting_for_mp_file, F.document)
async def mp_get_file(message: types.Message, state: FSMContext):
    if not message.document.file_name.endswith('.py'):
        return await message.answer("❌ `.py` فقط!")
    data = await state.get_data()
    os.makedirs(MARKETPLACE_DIR, exist_ok=True)
    template_id = f"user_{message.from_user.id}_{int(time.time())}"
    file_path = os.path.join(MARKETPLACE_DIR, f"{template_id}.py")
    await bot.download(message.document, destination=file_path)
    meta = {"id": template_id, "name": data['mp_name'], "desc": data['mp_desc'], "category": "مستخدم", "author": str(message.from_user.id), "downloads": 0, "file": f"{template_id}.py"}
    with open(os.path.join(MARKETPLACE_DIR, f"{template_id}.json"), 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False)
    await message.answer(f"✅ **تم نشر القالب!**\n\n📦 `{data['mp_name']}`\nسيظهر في السوق للجميع.")
    for admin_id in ADMIN_IDS:
        try: await bot.send_message(admin_id, f"📦 **قالب جديد في السوق:**\n👤 `{message.from_user.id}`\n📦 `{data['mp_name']}`\n📝 {data['mp_desc']}")
        except: pass
    await state.clear()

# ============================================
# === لوحة الأدمن المحدثة مع الميزات الجديدة ===
# ============================================
@dp.callback_query(F.data == "admin_panel")
async def admin_panel(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS and not await admin_has_perm(callback.from_user.id, "view_panel"): return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c: total = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE plan = 'pro'") as c: pro = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1") as c: banned = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM referrals") as c: refs = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM security_logs") as c: sec_logs = (await c.fetchone())[0]
    total_running = sum(len(f) for f in running_processes.values())
    pending = len(pending_approvals)
    cpu = psutil.cpu_percent(); ram = psutil.virtual_memory()
    text = f"""🛠 **لوحة الإدارة المتقدمة:**

👥 المستخدمين: `{total}` | 💎 PRO: `{pro}`
🚫 محظورين: `{banned}` | 👥 إحالات: `{refs}`
🤖 بوتات نشطة: `{total_running}` | 🔒 سجلات أمان: `{sec_logs}`
⏳ بانتظار الموافقة: `{pending}`
💻 CPU: `{cpu}%` | 🧠 RAM: `{ram.percent}%`"""
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="💎 منح PRO", callback_data="admin_give_pro"), types.InlineKeyboardButton(text="❌ سحب PRO", callback_data="admin_remove_pro"))
    builder.row(types.InlineKeyboardButton(text="🚫 حظر", callback_data="admin_ban"), types.InlineKeyboardButton(text="✅ إلغاء حظر", callback_data="admin_unban"))
    builder.row(types.InlineKeyboardButton(text="📢 إذاعة متقدمة", callback_data="admin_broadcast_menu"), types.InlineKeyboardButton(text="📺 القنوات", callback_data="admin_channels"))
    builder.row(types.InlineKeyboardButton(text="🕵️ تجسس", callback_data="admin_spy"), types.InlineKeyboardButton(text="👥 المستخدمين", callback_data="admin_users_list"))
    builder.row(types.InlineKeyboardButton(text="💰 إضافة نقاط", callback_data="admin_add_points"), types.InlineKeyboardButton(text="🎟 كوبونات", callback_data="admin_coupons"))
    builder.row(types.InlineKeyboardButton(text="📤 رفع لمستخدم", callback_data="admin_upload_to_user"), types.InlineKeyboardButton(text="🔄 إعادة تشغيل بوت", callback_data="admin_restart_user_bot"))
    builder.row(types.InlineKeyboardButton(text="🛑 إيقاف الكل", callback_data="admin_stop_all"), types.InlineKeyboardButton(text="📋 سجل الأدمن", callback_data="admin_action_log"))
    builder.row(types.InlineKeyboardButton(text="🌐 تغيير الدومين", callback_data="admin_set_domain"), types.InlineKeyboardButton(text="🔌 تغيير المنفذ", callback_data="admin_set_port"))
    # === الأزرار الجديدة ===
    builder.row(types.InlineKeyboardButton(text="📦 نسخ ملفات المستخدمين", callback_data="admin_export_all_files"), types.InlineKeyboardButton(text="📋 تخزين المستخدمين", callback_data="admin_export_users_data"))
    builder.row(types.InlineKeyboardButton(text="👮 إدارة الأدمنية", callback_data="admin_manage_admins"), types.InlineKeyboardButton(text="⚙️ إدارة الخطط", callback_data="admin_manage_plans"))
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="back_to_main"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

# ============================================
# === ميزة جديدة: نسخ جميع ملفات المستخدمين ZIP ===
# ============================================
@dp.callback_query(F.data == "admin_export_all_files")
async def admin_export_all_files(callback: types.CallbackQuery):
    """تصدير جميع ملفات المستخدمين في ملف ZIP واحد"""
    if callback.from_user.id not in ADMIN_IDS: return
    await callback.answer("📦 جاري تجهيز الملفات... قد يستغرق وقتاً.")
    try:
        zip_path = os.path.join(BASE_DIR, f"all_users_files_{int(time.time())}.zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            if os.path.exists(USER_FILES_DIR):
                for root, dirs, files in os.walk(USER_FILES_DIR):
                    for file in files:
                        fp = os.path.join(root, file)
                        arcname = os.path.relpath(fp, USER_FILES_DIR)
                        zf.write(fp, arcname)
        file_size = os.path.getsize(zip_path)
        if file_size > 50 * 1024 * 1024:  # أكبر من 50MB
            await callback.message.answer(f"⚠️ حجم الملف كبير جداً ({file_size // (1024*1024)}MB). يتم إرساله...")
        await callback.message.answer_document(types.FSInputFile(zip_path), caption=f"📦 **جميع ملفات المستخدمين**\n📏 الحجم: `{file_size // 1024}KB`\n📅 التاريخ: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`")
        await log_admin_action(callback.from_user.id, "تصدير ملفات المستخدمين", f"size:{file_size}")
        try: os.remove(zip_path)
        except: pass
    except Exception as e:
        await callback.message.answer(f"❌ خطأ في التصدير: {str(e)}")

# ============================================
# === ميزة جديدة: تصدير بيانات المستخدمين TXT ===
# ============================================
@dp.callback_query(F.data == "admin_export_users_data")
async def admin_export_users_data(callback: types.CallbackQuery):
    """تصدير بيانات جميع المستخدمين في ملف TXT مفصل"""
    if callback.from_user.id not in ADMIN_IDS: return
    await callback.answer("📋 جاري تجهيز البيانات...")
    try:
        txt_path = os.path.join(BASE_DIR, f"users_data_{int(time.time())}.txt")
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users ORDER BY user_id") as cursor:
                users = await cursor.fetchall()
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write("=" * 70 + "\n")
            f.write("   تقرير بيانات المستخدمين الكامل\n")
            f.write(f"   التاريخ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"   إجمالي المستخدمين: {len(users)}\n")
            f.write("=" * 70 + "\n\n")
            for idx, u in enumerate(users, 1):
                uid = u['user_id']
                # حساب عدد الملفات
                user_path = os.path.join(USER_FILES_DIR, str(uid))
                file_count = 0
                file_list = []
                if os.path.exists(user_path):
                    for root, dirs, files in os.walk(user_path):
                        for file in files:
                            file_count += 1
                            file_list.append(os.path.relpath(os.path.join(root, file), user_path))
                # حساب البوتات النشطة
                running_count = count_user_running(uid)
                running_names = []
                if uid in running_processes:
                    for fp in running_processes[uid]:
                        running_names.append(os.path.basename(fp))
                # حساب الإحالات
                ref_count = 0
                async with aiosqlite.connect(DB_PATH) as db2:
                    async with db2.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (uid,)) as c:
                        ref_count = (await c.fetchone())[0]
                # حالة الاشتراك
                plan_name = "PRO 💎" if u['plan'] == 'pro' else "مجانية ⭐️"
                ban_status = "محظور 🚫" if u['is_banned'] else "نشط ✅"
                expiry = u['expiry_date'] or "غير محدد"
                f.write(f"{'─' * 50}\n")
                f.write(f"  مستخدم رقم {idx}\n")
                f.write(f"{'─' * 50}\n")
                f.write(f"  الأيدي:           {uid}\n")
                f.write(f"  اسم المستخدم:     @{u['username'] or 'غير محدد'}\n")
                f.write(f"  الاسم:            {u['first_name'] or 'غير محدد'} {u['last_name'] or ''}\n")
                f.write(f"  الخطة:            {plan_name}\n")
                f.write(f"  حالة الاشتراك:    {expiry}\n")
                f.write(f"  الحالة:           {ban_status}\n")
                f.write(f"  الرصيد (النقاط):  {u['points']}\n")
                f.write(f"  الإحالات:         {ref_count}\n")
                f.write(f"  تاريخ التسجيل:    {u['join_date'] or 'غير محدد'}\n")
                f.write(f"  عدد الملفات:      {file_count}\n")
                f.write(f"  بوتات نشطة:       {running_count}\n")
                if running_names:
                    f.write(f"  البوتات المشغلة:  {', '.join(running_names)}\n")
                if file_list:
                    f.write(f"  قائمة الملفات:\n")
                    for fl in file_list[:20]:
                        f.write(f"    - {fl}\n")
                    if len(file_list) > 20:
                        f.write(f"    ... و {len(file_list) - 20} ملف آخر\n")
                f.write("\n")
            # ملخص
            f.write("\n" + "=" * 70 + "\n")
            f.write("   ملخص عام\n")
            f.write("=" * 70 + "\n")
            total_users = len(users)
            pro_users = sum(1 for u in users if u['plan'] == 'pro')
            banned_users = sum(1 for u in users if u['is_banned'])
            active_users = total_users - banned_users
            total_bots = sum(len(f) for f in running_processes.values())
            f.write(f"  إجمالي المستخدمين:    {total_users}\n")
            f.write(f"  مستخدمين PRO:         {pro_users}\n")
            f.write(f"  مستخدمين نشطين:       {active_users}\n")
            f.write(f"  محظورين:              {banned_users}\n")
            f.write(f"  بوتات نشطة حالياً:    {total_bots}\n")
            f.write("=" * 70 + "\n")
        await callback.message.answer_document(types.FSInputFile(txt_path), caption=f"📋 **تقرير بيانات المستخدمين الكامل**\n👥 العدد: `{len(users)}`\n📅 التاريخ: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`")
        await log_admin_action(callback.from_user.id, "تصدير بيانات المستخدمين", f"count:{len(users)}")
        try: os.remove(txt_path)
        except: pass
    except Exception as e:
        await callback.message.answer(f"❌ خطأ في التصدير: {str(e)}")

# ============================================
# === ميزة جديدة: الإذاعة المتقدمة ===
# ============================================
@dp.callback_query(F.data == "admin_broadcast_menu")
async def admin_broadcast_menu(callback: types.CallbackQuery, state: FSMContext):
    """قائمة الإذاعة المتقدمة"""
    if callback.from_user.id not in ADMIN_IDS and not await admin_has_perm(callback.from_user.id, "broadcast"): return
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="📢 إذاعة للجميع", callback_data="admin_broadcast_all"))
    builder.row(types.InlineKeyboardButton(text="💎 إذاعة لـ PRO فقط", callback_data="admin_broadcast_pro"))
    builder.row(types.InlineKeyboardButton(text="⭐️ إذاعة للمجانيين فقط", callback_data="admin_broadcast_free"))
    builder.row(types.InlineKeyboardButton(text="🎯 إذاعة لمستخدم محدد", callback_data="admin_broadcast_single"))
    builder.row(types.InlineKeyboardButton(text="📊 إذاعة مع أزرار", callback_data="admin_broadcast_buttons"))
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="admin_panel"))
    await callback.message.edit_text("📢 **الإذاعة المتقدمة:**\n\nاختر نوع الإذاعة:", reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "admin_broadcast_all")
async def admin_broadcast_all(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(broadcast_target="all")
    await callback.message.answer("📢 أرسل رسالة الإذاعة (نص/صورة/فيديو/ملف):")
    await state.set_state(AdminStates.waiting_for_broadcast)

@dp.callback_query(F.data == "admin_broadcast_pro")
async def admin_broadcast_pro(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(broadcast_target="pro")
    await callback.message.answer("💎 أرسل رسالة الإذاعة لمستخدمي PRO:")
    await state.set_state(AdminStates.waiting_for_broadcast)

@dp.callback_query(F.data == "admin_broadcast_free")
async def admin_broadcast_free(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(broadcast_target="free")
    await callback.message.answer("⭐️ أرسل رسالة الإذاعة للمستخدمين المجانيين:")
    await state.set_state(AdminStates.waiting_for_broadcast)

@dp.callback_query(F.data == "admin_broadcast_single")
async def admin_broadcast_single(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🎯 أرسل أيدي المستخدم:")
    await state.set_state(AdminStates.waiting_for_broadcast_single_id)

@dp.message(AdminStates.waiting_for_broadcast_single_id)
async def process_broadcast_single_id(message: types.Message, state: FSMContext):
    await state.update_data(broadcast_target=f"single_{message.text.strip()}")
    await message.answer("📢 أرسل الرسالة:")
    await state.set_state(AdminStates.waiting_for_broadcast)

@dp.callback_query(F.data == "admin_broadcast_buttons")
async def admin_broadcast_buttons(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(broadcast_target="all", broadcast_with_buttons=True)
    await callback.message.answer("📢 أرسل نص الرسالة أولاً:")
    await state.set_state(AdminStates.waiting_for_broadcast_text_with_btn)

@dp.message(AdminStates.waiting_for_broadcast_text_with_btn)
async def process_broadcast_text_btn(message: types.Message, state: FSMContext):
    await state.update_data(broadcast_text=message.text)
    await message.answer("🔘 أرسل الأزرار بالتنسيق:\n`عنوان الزر | الرابط`\n\nمثال:\n`زيارة الموقع | https://example.com`\n`قناتنا | https://t.me/channel`\n\nكل زر في سطر منفصل.", parse_mode="Markdown")
    await state.set_state(AdminStates.waiting_for_broadcast_buttons)

@dp.message(AdminStates.waiting_for_broadcast_buttons)
async def process_broadcast_buttons(message: types.Message, state: FSMContext):
    data = await state.get_data()
    text = data.get('broadcast_text', '')
    builder = InlineKeyboardBuilder()
    for line in message.text.strip().split('\n'):
        parts = line.split('|')
        if len(parts) == 2:
            btn_text = parts[0].strip()
            btn_url = parts[1].strip()
            builder.row(types.InlineKeyboardButton(text=btn_text, url=btn_url))
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users WHERE is_banned = 0") as cursor: users = await cursor.fetchall()
    sent, failed = 0, 0
    for (uid,) in users:
        try: await bot.send_message(uid, text, reply_markup=builder.as_markup()); sent += 1
        except: failed += 1
    await log_admin_action(message.from_user.id, "إذاعة مع أزرار", f"sent:{sent} failed:{failed}")
    await message.answer(f"📢 تم الإرسال!\n✅ نجح: {sent}\n❌ فشل: {failed}")
    await state.clear()

@dp.message(AdminStates.waiting_for_broadcast)
async def process_broadcast(message: types.Message, state: FSMContext):
    data = await state.get_data()
    target = data.get('broadcast_target', 'all')
    async with aiosqlite.connect(DB_PATH) as db:
        if target == "all":
            async with db.execute("SELECT user_id FROM users WHERE is_banned = 0") as cursor: users = await cursor.fetchall()
        elif target == "pro":
            async with db.execute("SELECT user_id FROM users WHERE is_banned = 0 AND plan = 'pro'") as cursor: users = await cursor.fetchall()
        elif target == "free":
            async with db.execute("SELECT user_id FROM users WHERE is_banned = 0 AND plan = 'free'") as cursor: users = await cursor.fetchall()
        elif target.startswith("single_"):
            uid = target.replace("single_", "")
            users = [(int(uid),)]
        else:
            users = []
    sent, failed = 0, 0
    for (uid,) in users:
        try:
            if message.photo:
                await bot.send_photo(uid, message.photo[-1].file_id, caption=message.caption)
            elif message.video:
                await bot.send_video(uid, message.video.file_id, caption=message.caption)
            elif message.document:
                await bot.send_document(uid, message.document.file_id, caption=message.caption)
            elif message.text:
                await bot.send_message(uid, message.text)
            sent += 1
        except: failed += 1
    target_name = {"all": "الجميع", "pro": "PRO", "free": "مجانيين"}.get(target, target)
    await log_admin_action(message.from_user.id, f"إذاعة ({target_name})", f"sent:{sent} failed:{failed}")
    await message.answer(f"📢 تم الإرسال!\n🎯 الهدف: {target_name}\n✅ نجح: {sent}\n❌ فشل: {failed}")
    await state.clear()

# ============================================
# === ميزة جديدة: إدارة الأدمنية المتعددة ===
# ============================================
@dp.callback_query(F.data == "admin_manage_admins")
async def admin_manage_admins(callback: types.CallbackQuery):
    """إدارة الأدمنية المتعددة مع صلاحيات"""
    if callback.from_user.id not in ADMIN_IDS: return
    async with aiosqlite.connect(DB_PATH) as db:
     async with db.execute("SELECT user_id, role, added_by, added_at FROM admin_roles ORDER BY added_at DESC") as cursor:
            admins = await cursor.fetchall()
    text = "👮 **إدارة الأدمنية:**\n\n"
    text += f"👑 **الأدمن الرئيسي:** `{ADMIN_IDS[0]}`\n\n"
    if admins:
        text += "**الأدمنية الفرعية:**\n"
        for uid, role, added_by, added_at in admins:
            role_name = ADMIN_ROLES.get(role, {}).get('name', role)
            text += f"👤 `{uid}` | {role_name} | أضافه: `{added_by}`\n"
    else:
        text += "لا يوجد أدمنية فرعية.\n"
    text += f"\n**الأدوار المتاحة:**\n"
    for role_id, role_data in ADMIN_ROLES.items():
        perms = ', '.join(role_data['permissions'][:3])
        text += f"• `{role_id}`: {role_data['name']} ({perms}...)\n"
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="➕ إضافة أدمن", callback_data="admin_add_sub"))
    builder.row(types.InlineKeyboardButton(text="❌ إزالة أدمن", callback_data="admin_remove_sub"))
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="admin_panel"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "admin_add_sub")
async def admin_add_sub(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS: return
    await callback.message.answer("👮 أرسل أيدي الأدمن الجديد:")
    await state.set_state(AdminStates.waiting_for_sub_admin_id)

@dp.message(AdminStates.waiting_for_sub_admin_id)
async def process_sub_admin_id(message: types.Message, state: FSMContext):
    await state.update_data(sub_admin_id=message.text.strip())
    roles_text = "\n".join([f"`{k}` - {v['name']}" for k, v in ADMIN_ROLES.items()])
    await message.answer(f"اختر الدور:\n\n{roles_text}\n\nأرسل اسم الدور:", parse_mode="Markdown")
    await state.set_state(AdminStates.waiting_for_sub_admin_role)

@dp.message(AdminStates.waiting_for_sub_admin_role)
async def process_sub_admin_role(message: types.Message, state: FSMContext):
    data = await state.get_data()
    role = message.text.strip()
    if role not in ADMIN_ROLES:
        await message.answer("❌ دور غير صالح. اختر من القائمة.")
        await state.clear(); return
    uid = data['sub_admin_id']
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO admin_roles (user_id, role, added_by, added_at) VALUES (?, ?, ?, ?)",
            (uid, role, message.from_user.id, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        await db.commit()
    role_name = ADMIN_ROLES[role]['name']
    await log_admin_action(message.from_user.id, "إضافة أدمن فرعي", f"user:{uid} role:{role}")
    await message.answer(f"✅ تم إضافة `{uid}` كـ {role_name}!")
    try: await bot.send_message(int(uid), f"👮 **تم تعيينك كأدمن فرعي!**\n\nالدور: {role_name}")
    except: pass
    await state.clear()

@dp.callback_query(F.data == "admin_remove_sub")
async def admin_remove_sub(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS: return
    await callback.message.answer("❌ أرسل أيدي الأدمن لإزالته:")
    await state.set_state(AdminStates.waiting_for_remove_sub_id)

@dp.message(AdminStates.waiting_for_remove_sub_id)
async def process_remove_sub(message: types.Message, state: FSMContext):
    uid = message.text.strip()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM admin_roles WHERE user_id = ?", (uid,)); await db.commit()
    await log_admin_action(message.from_user.id, "إزالة أدمن فرعي", f"user:{uid}")
    await message.answer(f"✅ تم إزالة `{uid}` من الأدمنية.")
    try: await bot.send_message(int(uid), "⚠️ **تم إزالتك من الأدمنية.**")
    except: pass
    await state.clear()

# ============================================
# === ميزة جديدة: إدارة الخطط المرنة ===
# ============================================
@dp.callback_query(F.data == "admin_manage_plans")
async def admin_manage_plans(callback: types.CallbackQuery):
    """إدارة الخطط المرنة"""
    if callback.from_user.id not in ADMIN_IDS: return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT * FROM custom_plans ORDER BY name") as cursor:
            plans = await cursor.fetchall()
    text = "⚙️ **إدارة الخطط:**\n\n"
    text += "**الخطط الافتراضية:**\n"
    for plan_id, plan_data in DEFAULT_PLANS.items():
        text += f"• `{plan_id}`: {plan_data['name']} | {plan_data['max_files']} ملف | {plan_data['max_running']} بوت\n"
    if plans:
        text += "\n**خطط مخصصة:**\n"
        for p in plans:
            text += f"• `{p[0]}`: {p[1]} | {p[2]} ملف | {p[4]} بوت | ${p[6]}\n"
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="➕ إنشاء خطة جديدة", callback_data="admin_create_plan"))
    builder.row(types.InlineKeyboardButton(text="✏️ تعديل حدود خطة", callback_data="admin_edit_plan"))
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="admin_panel"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "admin_create_plan")
async def admin_create_plan(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS: return
    await callback.message.answer("⚙️ **إنشاء خطة جديدة:**\n\nأرسل البيانات بالتنسيق:\n`اسم_الخطة | الاسم_العربي | عدد_الملفات | عدد_المجلدات | عدد_البوتات | حجم_الملف_MB | السعر`\n\nمثال:\n`vip | VIP 👑 | 100 | 20 | 50 | 100 | 25`", parse_mode="Markdown")
    await state.set_state(AdminStates.waiting_for_plan_data)

@dp.message(AdminStates.waiting_for_plan_data)
async def process_plan_data(message: types.Message, state: FSMContext):
    try:
        parts = [p.strip() for p in message.text.split('|')]
        if len(parts) != 7: raise ValueError("عدد الحقول غير صحيح")
        plan_id, name, max_files, max_folders, max_running, max_size_mb, price = parts
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR REPLACE INTO custom_plans (plan_id, name, max_files, max_folders, max_running, max_file_size, price) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (plan_id, name, int(max_files), int(max_folders), int(max_running), int(max_size_mb) * 1024 * 1024, float(price)))
            await db.commit()
        await log_admin_action(message.from_user.id, "إنشاء خطة", f"plan:{plan_id}")
        await message.answer(f"✅ تم إنشاء الخطة `{plan_id}` ({name})!")
    except Exception as e:
        await message.answer(f"❌ خطأ: {str(e)}\n\nتأكد من التنسيق الصحيح.")
    await state.clear()

@dp.callback_query(F.data == "admin_edit_plan")
async def admin_edit_plan(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS: return
    await callback.message.answer("✏️ أرسل اسم الخطة لتعديلها (free/pro/اسم مخصص):")
    await state.set_state(AdminStates.waiting_for_edit_plan_name)

@dp.message(AdminStates.waiting_for_edit_plan_name)
async def process_edit_plan_name(message: types.Message, state: FSMContext):
    await state.update_data(edit_plan_name=message.text.strip())
    await message.answer("أرسل البيانات الجديدة بالتنسيق:\n`عدد_الملفات | عدد_المجلدات | عدد_البوتات | حجم_الملف_MB`\n\nمثال: `10 | 5 | 5 | 10`", parse_mode="Markdown")
    await state.set_state(AdminStates.waiting_for_edit_plan_data)

@dp.message(AdminStates.waiting_for_edit_plan_data)
async def process_edit_plan_data(message: types.Message, state: FSMContext):
    data = await state.get_data()
    plan_name = data['edit_plan_name']
    try:
        parts = [p.strip() for p in message.text.split('|')]
        max_files, max_folders, max_running, max_size_mb = int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3])
        if plan_name in DEFAULT_PLANS:
            DEFAULT_PLANS[plan_name]['max_files'] = max_files
            DEFAULT_PLANS[plan_name]['max_folders'] = max_folders
            DEFAULT_PLANS[plan_name]['max_running'] = max_running
            DEFAULT_PLANS[plan_name]['max_file_size'] = max_size_mb * 1024 * 1024
        else:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("UPDATE custom_plans SET max_files=?, max_folders=?, max_running=?, max_file_size=? WHERE plan_id=?",
                    (max_files, max_folders, max_running, max_size_mb * 1024 * 1024, plan_name))
                await db.commit()
        await log_admin_action(message.from_user.id, "تعديل خطة", f"plan:{plan_name}")
        await message.answer(f"✅ تم تعديل خطة `{plan_name}`!\n📁 ملفات: {max_files} | 📂 مجلدات: {max_folders} | 🤖 بوتات: {max_running} | 📏 حجم: {max_size_mb}MB")
    except Exception as e:
        await message.answer(f"❌ خطأ: {str(e)}")
    await state.clear()

# === Admin: الأوامر الأصلية المتبقية ===
@dp.callback_query(F.data == "admin_give_pro")
async def admin_give_pro(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS and not await admin_has_perm(callback.from_user.id, "manage_users"): return
    await callback.message.answer("💎 أرسل أيدي المستخدم:"); await state.set_state(AdminStates.waiting_for_pro_id)

@dp.message(AdminStates.waiting_for_pro_id)
async def process_pro_id(message: types.Message, state: FSMContext):
    await state.update_data(pro_uid=message.text.strip())
    await message.answer("أرسل عدد الأيام:"); await state.set_state(AdminStates.waiting_for_pro_days)

@dp.message(AdminStates.waiting_for_pro_days)
async def process_pro_days(message: types.Message, state: FSMContext):
    data = await state.get_data()
    try:
        days = int(message.text.strip()); expiry = (datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET plan = 'pro', expiry_date = ? WHERE user_id = ?", (expiry, data['pro_uid'])); await db.commit()
        await log_admin_action(message.from_user.id, "منح PRO", f"user:{data['pro_uid']} days:{days}")
        await message.answer(f"✅ تم منح PRO لـ `{data['pro_uid']}` لمدة {days} يوم.")
        try: await bot.send_message(int(data['pro_uid']), f"🎉 **تم ترقيتك إلى PRO لمدة {days} يوم!**")
        except: pass
    except: await message.answer("❌ أرسل رقماً صحيحاً.")
    await state.clear()

@dp.callback_query(F.data == "admin_remove_pro")
async def admin_remove_pro_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS and not await admin_has_perm(callback.from_user.id, "manage_users"): return
    await callback.message.answer("❌ أرسل أيدي المستخدم لسحب PRO:"); await state.set_state(AdminStates.waiting_for_remove_pro_id)

@dp.message(AdminStates.waiting_for_remove_pro_id)
async def process_remove_pro(message: types.Message, state: FSMContext):
    uid = message.text.strip()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET plan = 'free', expiry_date = NULL WHERE user_id = ?", (uid,)); await db.commit()
    await log_admin_action(message.from_user.id, "سحب PRO", f"user:{uid}")
    await message.answer(f"✅ تم سحب PRO من `{uid}`.")
    try: await bot.send_message(int(uid), "⚠️ **تم سحب اشتراك PRO بواسطة الإدارة.**")
    except: pass
    await state.clear()

@dp.callback_query(F.data == "admin_ban")
async def admin_ban(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS and not await admin_has_perm(callback.from_user.id, "ban_users"): return
    await callback.message.answer("🚫 أرسل أيدي المستخدم للحظر:"); await state.set_state(AdminStates.waiting_for_ban_id)

@dp.message(AdminStates.waiting_for_ban_id)
async def process_ban(message: types.Message, state: FSMContext):
    uid = message.text.strip()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_banned = 1 WHERE user_id = ?", (uid,)); await db.commit()
    uid_int = int(uid)
    if uid_int in running_processes:
        for fp in list(running_processes[uid_int].keys()): force_kill_process(fp, uid_int)
    await log_admin_action(message.from_user.id, "حظر", f"user:{uid}")
    await message.answer(f"🚫 تم حظر `{uid}` وإيقاف كل بوتاته.")
    try: await bot.send_message(uid_int, "🚫 **تم حظرك من استخدام البوت.**")
    except: pass
    await state.clear()

@dp.callback_query(F.data == "admin_unban")
async def admin_unban(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS and not await admin_has_perm(callback.from_user.id, "ban_users"): return
    await callback.message.answer("✅ أرسل أيدي المستخدم لإلغاء الحظر:"); await state.set_state(AdminStates.waiting_for_unban_id)

@dp.message(AdminStates.waiting_for_unban_id)
async def process_unban(message: types.Message, state: FSMContext):
    uid = message.text.strip()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_banned = 0 WHERE user_id = ?", (uid,)); await db.commit()
    await log_admin_action(message.from_user.id, "إلغاء حظر", f"user:{uid}")
    await message.answer(f"✅ تم إلغاء حظر `{uid}`.")
    await state.clear()

# === Admin: Spy ===
@dp.callback_query(F.data == "admin_spy")
async def admin_spy(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS and not await admin_has_perm(callback.from_user.id, "spy"): return
    await callback.message.answer("🕵️ أرسل أيدي المستخدم:"); await state.set_state(AdminStates.waiting_for_spy_id)

@dp.message(AdminStates.waiting_for_spy_id)
async def process_spy_id(message: types.Message, state: FSMContext):
    try:
        spy_id = int(message.text.strip()); path = os.path.join(USER_FILES_DIR, str(spy_id))
        if not os.path.exists(path): return await message.answer("❌ لا توجد ملفات.")
        await state.update_data(current_path=path, view_user_id=spy_id)
        await show_files(await message.answer("🕵️ جاري الدخول..."), path, spy_id)
    except: await message.answer("❌ أيدي غير صالح.")
    await state.clear()

# === Admin: Channels ===
@dp.callback_query(F.data == "admin_channels")
async def admin_channels_panel(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS and not await admin_has_perm(callback.from_user.id, "manage_channels"): return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT channel_id, channel_name FROM channels") as cursor: channels = await cursor.fetchall()
    text = "📺 **إدارة القنوات:**\n\n"
    builder = InlineKeyboardBuilder()
    for cid, cname in channels: text += f"🔹 {cname} (`{cid}`)\n"; builder.row(types.InlineKeyboardButton(text=f"🗑 حذف {cname}", callback_data=f"admin_del_chan_{cid}"))
    builder.row(types.InlineKeyboardButton(text="➕ إضافة قناة", callback_data="admin_add_chan_start"), types.InlineKeyboardButton(text="🔙 رجوع", callback_data="admin_panel"))
    await callback.message.edit_text(text or "📺 لا توجد قنوات.", reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "admin_add_chan_start")
async def admin_add_chan_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("أرسل معرف القناة:"); await state.set_state(AdminStates.waiting_for_channel_id)

@dp.message(AdminStates.waiting_for_channel_id)
async def process_chan_id(message: types.Message, state: FSMContext):
    await state.update_data(chan_id=message.text.strip()); await message.answer("أرسل اسم القناة:"); await state.set_state(AdminStates.waiting_for_channel_name)

@dp.message(AdminStates.waiting_for_channel_name)
async def process_chan_name(message: types.Message, state: FSMContext):
    data = await state.get_data()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO channels (channel_id, channel_name) VALUES (?, ?)", (data['chan_id'], message.text.strip())); await db.commit()
    await message.answer("✅ تم إضافة القناة."); await state.clear()

@dp.callback_query(F.data.startswith("admin_del_chan_"))
async def admin_del_chan(callback: types.CallbackQuery):
    cid = callback.data.replace("admin_del_chan_", "")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM channels WHERE channel_id = ?", (cid,)); await db.commit()
    await callback.answer("🗑 تم الحذف."); await admin_channels_panel(callback)

# === Admin: Users List ===
@dp.callback_query(F.data == "admin_users_list")
async def admin_users_list(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS and not await admin_has_perm(callback.from_user.id, "view_panel"): return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT user_id, username, plan, points, is_banned FROM users ORDER BY user_id DESC LIMIT 20") as cursor: users = await cursor.fetchall()
    text = "👥 **آخر 20 مستخدم:**\n\n"
    for u in users:
        status = "🚫" if u['is_banned'] else ("💎" if u['plan'] == 'pro' else "⭐️")
        running = count_user_running(u['user_id'])
        text += f"{status} `{u['user_id']}` | @{u['username'] or 'N/A'} | 💰{u['points']} | 🟢{running}\n"
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="admin_panel"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

# === Admin: Add Points ===
@dp.callback_query(F.data == "admin_add_points")
async def admin_add_points_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS and not await admin_has_perm(callback.from_user.id, "manage_users"): return
    await callback.message.answer("💰 أرسل أيدي المستخدم:"); await state.set_state(AdminStates.waiting_for_points_id)

@dp.message(AdminStates.waiting_for_points_id)
async def process_points_id(message: types.Message, state: FSMContext):
    await state.update_data(points_uid=message.text.strip())
    await message.answer("أرسل عدد النقاط:"); await state.set_state(AdminStates.waiting_for_points_amount)

@dp.message(AdminStates.waiting_for_points_amount)
async def process_points_amount(message: types.Message, state: FSMContext):
    data = await state.get_data()
    try:
        amount = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET points = points + ? WHERE user_id = ?", (amount, data['points_uid'])); await db.commit()
        await log_admin_action(message.from_user.id, "إضافة نقاط", f"user:{data['points_uid']} amount:{amount}")
        await message.answer(f"✅ تم إضافة {amount} نقطة لـ `{data['points_uid']}`.")
        try: await bot.send_message(int(data['points_uid']), f"💰 **تم إضافة {amount} نقطة بواسطة الإدارة!**")
        except: pass
    except: await message.answer("❌ أرسل رقماً صحيحاً.")
    await state.clear()

# === Admin: Stop All ===
@dp.callback_query(F.data == "admin_stop_all")
async def admin_stop_all(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS: return
    count = 0
    for uid in list(running_processes.keys()):
        for fp in list(running_processes.get(uid, {}).keys()):
            force_kill_process(fp, uid); count += 1
    save_persistent_state()
    await log_admin_action(callback.from_user.id, "إيقاف الكل", f"count:{count}")
    await callback.answer(f"🛑 تم إيقاف {count} عملية.", show_alert=True)

# === Admin: Coupons ===
@dp.callback_query(F.data == "admin_coupons")
async def admin_coupons(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS and not await admin_has_perm(callback.from_user.id, "manage_coupons"): return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT code, days, max_uses, used FROM coupons ORDER BY rowid DESC LIMIT 10") as cursor: coupons = await cursor.fetchall()
    text = "🎟 **إدارة الكوبونات:**\n\n"
    for code, days, max_uses, used in coupons:
        text += f"🔹 `{code}` | {days} يوم | {used}/{max_uses} استخدام\n"
    if not coupons: text += "لا توجد كوبونات."
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="➕ إنشاء كوبون", callback_data="admin_create_coupon"))
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="admin_panel"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "admin_create_coupon")
async def admin_create_coupon(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🎟 أرسل كود الكوبون (أو اكتب `auto` لتوليد تلقائي):")
    await state.set_state(AdminStates.waiting_for_coupon_code)

@dp.message(AdminStates.waiting_for_coupon_code)
async def process_coupon_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    if code.lower() == 'auto': code = f"PRO-{secrets.token_hex(4).upper()}"
    await state.update_data(coupon_code=code)
    await message.answer(f"كود الكوبون: `{code}`\n\nأرسل عدد أيام PRO:")
    await state.set_state(AdminStates.waiting_for_coupon_days)

@dp.message(AdminStates.waiting_for_coupon_days)
async def process_coupon_days(message: types.Message, state: FSMContext):
    await state.update_data(coupon_days=int(message.text.strip()))
    await message.answer("أرسل عدد مرات الاستخدام المسموحة:")
    await state.set_state(AdminStates.waiting_for_coupon_uses)

@dp.message(AdminStates.waiting_for_coupon_uses)
async def process_coupon_uses(message: types.Message, state: FSMContext):
    data = await state.get_data()
    try:
        uses = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT INTO coupons (code, days, max_uses, used, created_by) VALUES (?, ?, ?, 0, ?)", (data['coupon_code'], data['coupon_days'], uses, message.from_user.id))
            await db.commit()
        await log_admin_action(message.from_user.id, "إنشاء كوبون", f"code:{data['coupon_code']} days:{data['coupon_days']} uses:{uses}")
        await message.answer(f"✅ تم إنشاء الكوبون!\n\n🎟 الكود: `{data['coupon_code']}`\n📅 الأيام: {data['coupon_days']}\n🔢 الاستخدامات: {uses}")
    except: await message.answer("❌ أرسل رقماً صحيحاً.")
    await state.clear()

# === Admin: Upload to User ===
@dp.callback_query(F.data == "admin_upload_to_user")
async def admin_upload_to_user(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS and not await admin_has_perm(callback.from_user.id, "manage_files"): return
    await callback.message.answer("📤 أرسل أيدي المستخدم لرفع ملف له:")
    await state.set_state(AdminStates.waiting_for_upload_to_user_id)

@dp.message(AdminStates.waiting_for_upload_to_user_id)
async def process_upload_to_user_id(message: types.Message, state: FSMContext):
    await state.update_data(upload_to_uid=message.text.strip())
    await message.answer("أرسل الملف:"); await state.set_state(AdminStates.waiting_for_upload_to_user_file)

@dp.message(AdminStates.waiting_for_upload_to_user_file, F.document)
async def process_upload_to_user_file(message: types.Message, state: FSMContext):
    data = await state.get_data(); uid = data['upload_to_uid']
    user_path = os.path.join(USER_FILES_DIR, str(uid))
    os.makedirs(user_path, exist_ok=True)
    file_path = os.path.join(user_path, message.document.file_name)
    await bot.download(message.document, destination=file_path)
    await log_admin_action(message.from_user.id, "رفع ملف لمستخدم", f"user:{uid} file:{message.document.file_name}")
    await message.answer(f"✅ تم رفع `{message.document.file_name}` لمجلد المستخدم `{uid}`.")
    try: await bot.send_message(int(uid), f"📤 **تم رفع ملف جديد لمجلدك بواسطة الإدارة:** `{message.document.file_name}`")
    except: pass
    await state.clear()

# === Admin: Restart User Bot ===
@dp.callback_query(F.data == "admin_restart_user_bot")
async def admin_restart_user_bot(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS and not await admin_has_perm(callback.from_user.id, "manage_bots"): return
    await callback.message.answer("🔄 أرسل أيدي المستخدم:"); await state.set_state(AdminStates.waiting_for_restart_user_bot_id)

@dp.message(AdminStates.waiting_for_restart_user_bot_id)
async def process_restart_user_bot_id(message: types.Message, state: FSMContext):
    await state.update_data(restart_uid=message.text.strip())
    await message.answer("أرسل اسم الملف (مثلاً: bot.py):"); await state.set_state(AdminStates.waiting_for_restart_user_bot_name)

@dp.message(AdminStates.waiting_for_restart_user_bot_name)
async def process_restart_user_bot_name(message: types.Message, state: FSMContext):
    data = await state.get_data(); uid = int(data['restart_uid']); fname = message.text.strip()
    file_path = os.path.join(USER_FILES_DIR, str(uid), fname)
    if not os.path.exists(file_path):
        await message.answer("❌ الملف غير موجود."); await state.clear(); return
    force_kill_process(file_path, uid)
    await asyncio.sleep(1)
    env = os.environ.copy(); env.update(get_user_env(uid))
    env["PYTHONPATH"] = os.path.dirname(file_path) + ":" + env.get("PYTHONPATH", "")
    p = subprocess.Popen(["python3", "-u", file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=os.path.dirname(file_path))
    if uid not in running_processes: running_processes[uid] = {}
    running_processes[uid][file_path] = {'proc': p, 'auto_restart': False, 'restart_count': 0, 'start_time': datetime.now()}
    asyncio.create_task(log_reader(uid, file_path, p))
    asyncio.create_task(error_reader(uid, file_path, p))
    save_persistent_state()
    await log_admin_action(message.from_user.id, "إعادة تشغيل بوت مستخدم", f"user:{uid} file:{fname}")
    await message.answer(f"✅ تم إعادة تشغيل `{fname}` للمستخدم `{uid}`.")
    try: await bot.send_message(uid, f"🔄 **تم إعادة تشغيل `{fname}` بواسطة الإدارة.**")
    except: pass
    await state.clear()

# === Admin: Action Log ===
@dp.callback_query(F.data == "admin_action_log")
async def admin_action_log(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS and not await admin_has_perm(callback.from_user.id, "view_panel"): return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT admin_id, action, detail, timestamp FROM admin_log ORDER BY timestamp DESC LIMIT 15") as cursor: logs = await cursor.fetchall()
    text = "📋 **سجل تصرفات الأدمن (آخر 15):**\n\n"
    for aid, action, detail, ts in logs:
        text += f"🕒 `{ts}`\n   👤 `{aid}` | {action}: {detail}\n\n"
    if not logs: text += "لا توجد سجلات."
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="🔙 رجوع", callback_data="admin_panel"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

# === Admin: Domain & Port ===
@dp.callback_query(F.data == "admin_set_domain")
async def admin_set_domain_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS: return
    await callback.message.answer(f"🌐 الدومين الحالي: `{SERVER_DOMAIN}`\nأرسل الدومين الجديد:")
    await state.set_state(AdminStates.waiting_for_domain)

@dp.message(AdminStates.waiting_for_domain)
async def process_set_domain(message: types.Message, state: FSMContext):
    global SERVER_DOMAIN
    SERVER_DOMAIN = message.text.strip().replace("http://", "").replace("https://", "").split(":")[0].split("/")[0]
    await message.answer(f"✅ تم تحديث الدومين: `{SERVER_DOMAIN}`"); await state.clear()

@dp.callback_query(F.data == "admin_set_port")
async def admin_set_port_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS: return
    await callback.message.answer(f"🔌 المنفذ الحالي: `{WEB_PORT}`\nأرسل المنفذ الجديد:")
    await state.set_state(AdminStates.waiting_for_port)

@dp.message(AdminStates.waiting_for_port)
async def process_set_port(message: types.Message, state: FSMContext):
    global WEB_PORT
    try:
        WEB_PORT = int(message.text.strip())
        await message.answer(f"✅ تم تحديث المنفذ: `{WEB_PORT}`\n⚠️ أعد تشغيل البوت لتفعيله.")
    except: await message.answer("❌ أرسل رقماً صحيحاً.")
    await state.clear()

# === Fallback: Non-py file rejection ===
@dp.message(F.document)
async def reject_non_py(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state: return
    if message.document and not message.document.file_name.endswith('.py'):
        await message.answer(f"❌ **`{message.document.file_name}` غير مدعوم!**\n\nيُسمح فقط بملفات بايثون `.py`")

# --- Start Everything ---
async def main():
    await init_db()
    await restore_persistent_state()
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.create_task(auto_restart_monitor())
    asyncio.create_task(check_pro_expiry())
    asyncio.create_task(auto_cleanup())
    asyncio.create_task(smart_resource_monitor())
    asyncio.create_task(daily_report())
    # asyncio.create_task(resource_tracker()) # الداله غير موجودة
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
