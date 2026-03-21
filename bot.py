
import os
from flask import Flask, request, jsonify
from threading import Thread

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running on Koyeb!"

def run_web():
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)

Thread(target=run_web).start()

NOWPAYMENTS_API_KEY = os.getenv("NOWPAYMENTS_API_KEY", "PASTE_NOWPAYMENTS_API_KEY_HERE")
NOWPAYMENTS_IPN_SECRET = os.getenv("NOWPAYMENTS_IPN_SECRET", "PASTE_NOWPAYMENTS_IPN_SECRET_HERE")
NOWPAYMENTS_IPN_URL = os.getenv("NOWPAYMENTS_IPN_URL", "https://encouraging-robyn-fakhreddine-96f012f9.koyeb.app/nowpayments/ipn")
NOWPAYMENTS_BASE_URL = "https://api.nowpayments.io/v1"
TOPUP_AMOUNTS = [10, 25, 50, 100]
TOPUP_CURRENCIES = [("USDT (TRC20)", "usdttrc20"), ("LTC", "ltc"), ("BTC", "btc")]

IOSERTEST_TOKEN = os.getenv("IOSERTEST_TOKEN", "PASTE_YOUR_IOSERTEST_TOKEN_HERE")
IOSERTEST_CERT_ENDPOINT = os.getenv("IOSERTEST_CERT_ENDPOINT", "https://api.iosertest.com/api/PASTE_CERT_LOOKUP_ENDPOINT_HERE")
IOSERTEST_BALANCE_ENDPOINT = os.getenv("IOSERTEST_BALANCE_ENDPOINT", "https://api.iosertest.com/api/getassets")

import json
import os
import time
import hmac
import hashlib
import uuid
import sqlite3
import asyncio
import sys
import tempfile
from datetime import datetime, timedelta

import requests

import telegram
"""
  # Activity / statistics
  if data == "show_activity":
      DATA_STAT = load_data()
      uid = str(query.from_user.id)
      admins = set(DATA_STAT.get("admins", []))
      sellers_map = DATA_STAT.get("sellers", {})

      # Seller view: show seller balance and their sales
      if uid in sellers_map:
          seller = sellers_map.get(uid, {})
          balance = seller.get("balance", 0)
          sales = [s for s in DATA_STAT.get("sales_log", []) if s.get("seller_id") == uid]
          sold_count = len(sales)
          by_product = {}
          for s in sales:
              by_product[s.get("product")] = by_product.get(s.get("product"), 0) + 1
          msg = f"📊 Seller Activity ({seller.get('name','Unknown')} - {uid}):\n\nBalance: ${balance}\nKeys sold: {sold_count}\n\nSold by product:\n"
          if by_product:
              for p, c in by_product.items():
                  msg += f"- {p}: {c}\n"
          else:
              msg += "(no sales yet)\n"
          keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]
          await query.edit_message_text(msg + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
          return

      # Admin view: overall stats + per-seller breakdown
      if uid in admins:
          start_clicks = DATA_STAT.get("start_clicks", 0)
          users_count = len(DATA_STAT.get("users", []))
          used_keys = DATA_STAT.get("used_keys", []) or []
          total_keys_sold = len(used_keys)
          total_keys_available = sum(len(v) for v in DATA_STAT.get("keys", {}).values())
          sellers = DATA_STAT.get("sellers", {})
          sellers_count = len(sellers)
          sales_count = len(DATA_STAT.get("sales_log", []))
          sold_by_product = {}
          for s in DATA_STAT.get("sales_log", []):
              sold_by_product[s.get("product")] = sold_by_product.get(s.get("product"), 0) + 1
          msg = f"📊 Bot Activity Summary:\n\nStart clicks: {start_clicks}\nKnown users: {users_count}\nTotal sales entries: {sales_count}\nKeys sold: {total_keys_sold}\nKeys available: {total_keys_available}\nSellers: {sellers_count}\n\nSold by product:\n"
          for prod, cnt in sold_by_product.items():
              msg += f"- {prod}: {cnt}\n"
          msg += "\nPer-seller breakdown:\n"
          if sellers:
              for sid, info in sellers.items():
                  s_sales = [s for s in DATA_STAT.get("sales_log", []) if s.get("seller_id") == sid]
                  s_count = len(s_sales)
                  prod_counts = {}
                  for s in s_sales:
                      prod_counts[s.get("product")] = prod_counts.get(s.get("product"), 0) + 1
                  msg += f"- {info.get('name','?')} ({sid}) — Balance: ${info.get('balance',0)} — Keys sold: {s_count}\n"
                  if prod_counts:
                      for p, c in prod_counts.items():
                          msg += f"    • {p}: {c}\n"
          else:
              msg += "(no sellers configured)\n"

          keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]
          await query.edit_message_text(msg + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
          return

      # Customer view: personal activity
      purchases = [s for s in DATA_STAT.get("sales_log", []) if s.get("user") == uid]
      balance = DATA_STAT.get("balances", {}).get(uid, 0)
      by_product = {}
      for s in purchases:
          by_product[s.get("product")] = by_product.get(s.get("product"), 0) + 1
      msg = f"📊 Your Activity:\n\nBalance: ${balance}\nPurchases: {len(purchases)}\n\nBy product:\n"
      if by_product:
          for p, c in by_product.items():
              msg += f"- {p}: {c}\n"
      else:
          msg += "(no purchases yet)\n"
      keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]
      await query.edit_message_text(msg + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return
          uid = None

      DATA_START = load_data()
      admins = set(DATA_START.get("admins", []))
      approved = set(DATA_START.get("approved_users", []))
      if uid and uid in DATA_START.get("sellers", {}):
          approved.add(uid)
          DATA_START.setdefault("approved_users", [])
          if uid not in DATA_START["approved_users"]:
              DATA_START["approved_users"].append(uid)

      if uid and uid not in admins and uid not in approved:
          DATA_START.setdefault("pending_users", [])
          if uid not in DATA_START["pending_users"]:
              DATA_START["pending_users"].append(uid)
          save_data(DATA_START)
          try:
              for admin_id in admins:
                  await context.bot.send_message(
                      chat_id=int(admin_id),
                      text=f"طلب وصول جديد من المستخدم: {uid}",
                      reply_markup=InlineKeyboardMarkup([
                          [InlineKeyboardButton("✅ Accept", callback_data=f"admin_approve_user:{uid}")]
                      ])
                  )
          except Exception:
              pass
          pending_text = "⏳ Your access request is pending admin approval." if lang == "en" else "⏳ طلبك قيد المراجعة من الأدمن."
          if getattr(update, "message", None):
              await update.message.reply_text(pending_text + SIGNATURE)
          elif getattr(update, "callback_query", None):
              await update.callback_query.message.reply_text(pending_text + SIGNATURE)
          return

      try:
          if uid and uid in DATA_START.get("sellers", {}):
              lang = "en"
              context.user_data["lang"] = "en"
      except Exception:
          pass

      menu_items = [button_texts["buy"][lang], button_texts["balance"][lang], button_texts["activity"][lang]]
      if uid in admins:
          menu_items.append(button_texts["admin"][lang])
      reply_keyboard = build_rows(menu_items, 2)

      try:
          if uid and uid in DATA_START.get("sellers", {}):
              reply_keyboard.append([button_texts["seller_report"]["en"]])
          reply_keyboard.append([button_texts["add_balance"]["en"], button_texts["payment_history"]["en"]])
              for btn in DATA_START.get("seller_custom_buttons", []):
                  label = btn.get("label") if isinstance(btn, dict) else None
                  if label:
                      reply_keyboard.append([label])
      except Exception:
          pass

      reply_keyboard.append([button_texts["get_files"][lang]])
      reply_keyboard.append(["⬅️ Back"])

      text = "Welcome to the Sales Bot! Please choose an option:" if lang == "en" else "مرحباً بك في بوت المبيعات! اختر خياراً:"
      if getattr(update, "message", None):
          try:
              DATA_START["start_clicks"] = DATA_START.get("start_clicks", 0) + 1
              save_data(DATA_START)
          except Exception:
              pass
          await update.message.reply_text(text + SIGNATURE, reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
          try:
              uid_str = uid
              if uid_str and uid_str not in DATA_START.get("sellers", {}) and uid_str not in DATA_START.get("seller_promo_shown", []):
                  promo = "Become a seller — deposit just $30 one time to start selling. Join our channel for details:"
                  kb = InlineKeyboardMarkup([[InlineKeyboardButton("Join Channel", url=SELLER_CHANNEL_URL), InlineKeyboardButton("Dismiss", callback_data="dismiss_seller_promo")]])
                  await update.message.reply_text(promo + SIGNATURE, reply_markup=kb)
                  DATA_START.setdefault("seller_promo_shown", []).append(uid_str)
                  save_data(DATA_START)
          except Exception:
              pass
      elif getattr(update, "callback_query", None):
          try:
              DATA_START["start_clicks"] = DATA_START.get("start_clicks", 0) + 1
              save_data(DATA_START)
          except Exception:
              pass
          await update.callback_query.message.reply_text(text + SIGNATURE, reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
          try:
              uid_str = uid
              if uid_str and uid_str not in DATA_START.get("sellers", {}) and uid_str not in DATA_START.get("seller_promo_shown", []):
                  promo = "Become a seller — deposit just $30 one time to start selling. Join our channel for details:"
                  kb = InlineKeyboardMarkup([[InlineKeyboardButton("Join Channel", url=SELLER_CHANNEL_URL), InlineKeyboardButton("Dismiss", callback_data="dismiss_seller_promo")]])
                  await update.callback_query.message.reply_text(promo + SIGNATURE, reply_markup=kb)
                  DATA_START.setdefault("seller_promo_shown", []).append(uid_str)
                  save_data(DATA_START)
          except Exception:
              pass

def cert_key_matches_sale(sale: dict) -> bool:
    product = str(sale.get("product", "")).strip().lower()
    plan = str(sale.get("plan", "")).strip().lower()
    duration = str(sale.get("duration", "")).strip().lower()
    warranty = sale.get("warranty")
    if any(x in product for x in ["cert", "certificate"]):
        if "30" in product or "warranty" in product:
            return True
    if any(x in plan for x in ["30 day", "30d", "warranty"]):
        return True
    if duration in {"30", "30d", "30 day", "30 days"} and warranty is True:
        return True
    return False

def get_sale_owner_id(sale: dict) -> str:
    return str(sale.get("user") or sale.get("buyer_id") or sale.get("user_id") or sale.get("uid") or "")

def get_sale_key_value(sale: dict) -> str:
    return str(sale.get("key") or sale.get("license") or sale.get("serial") or sale.get("code") or "").strip()

def find_user_cert_sale(data: dict, uid: str, key_text: str):
    wanted = str(key_text or "").strip().lower()
    for sale in data.get("sales_log", []):
        if get_sale_owner_id(sale) != str(uid):
            continue
        if get_sale_key_value(sale).lower() != wanted:
            continue
        if cert_key_matches_sale(sale):
            return sale
    return None

def list_user_cert_sales(data: dict, uid: str):
    rows = []
    for sale in data.get("sales_log", []):
        if get_sale_owner_id(sale) == str(uid) and cert_key_matches_sale(sale):
            rows.append(sale)
    return rows

def format_cert_ts(ts) -> str:
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "Unknown"

def get_iosertest_balance():
    payload = {"token": IOSERTEST_TOKEN}
    r = requests.post(IOSERTEST_BALANCE_ENDPOINT, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def lookup_ios_certificate(udid: str):
    payload = {"token": IOSERTEST_TOKEN, "udid": udid}
    r = requests.post(IOSERTEST_CERT_ENDPOINT, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()

def save_b64_temp_file(b64_data: str, suffix: str):
    raw = base64.b64decode(b64_data)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(raw)
    tmp.close()
    return tmp.name

def build_cert_details_message(api_data: dict) -> str:
    state_text = "✅ Valid" if api_data.get("state") else "❌ Invalid"
    warranty_text = "✅ Yes" if api_data.get("warranty") else "❌ No"
    return (
        "🔐 Certificate Details\n\n"
        f"📱 Product: {api_data.get('product', 'Unknown')}\n"
        f"📄 State: {state_text}\n"
        f"🛡️ Warranty: {warranty_text}\n"
        f"📦 Plan: {api_data.get('plan', 'Unknown')}\n"
        f"⏳ Warranty Time: {format_cert_ts(api_data.get('warranty_time'))}\n"
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
  lang = context.user_data.get("lang")
  if not lang:
      lang = DEFAULT_LANG
      context.user_data["lang"] = lang
  button_texts = {
      "buy": {"en": "🛍️ BUY KEYS", "ar": "🛍️ شراء مفاتيح"},
      "balance": {"en": "💰 MY BALANCE", "ar": "💰 رصيدي"},
      "admin": {"en": "🔐 ADMIN PANEL", "ar": "🔐 لوحة الادمن"},
      "about": {"en": "ℹ️ ABOUT", "ar": "ℹ️ حول"},
      "lang": {"en": "🌐 Change Language", "ar": "🌐 تغيير اللغة"},
      "activity": {"en": "📊 Activity", "ar": "📊 النشاط"},
      "seller_report": {"en": "📈 Seller Report", "ar": "📈 تقرير البائع"},
      "add_balance": {"en": "💳 Add Balance", "ar": "💳 شحن الرصيد"},
      "payment_history": {"en": "📜 Payment History", "ar": "📜 سجل الشحن"},
      "get_files": {"en": "📁 Get Files", "ar": "📁 الحصول على الملفات"}
  }
  uid = None
  try:
      if getattr(update, "message", None):
          uid = str(update.message.from_user.id)
      elif getattr(update, "callback_query", None):
          uid = str(update.callback_query.from_user.id)
  except Exception:
      uid = None

  DATA_START = load_data()
  admins = set(DATA_START.get("admins", []))
  approved = set(DATA_START.get("approved_users", []))
  if uid and uid in DATA_START.get("sellers", {}):
      approved.add(uid)
      DATA_START.setdefault("approved_users", [])
      if uid not in DATA_START["approved_users"]:
          DATA_START["approved_users"].append(uid)

  if uid and uid not in admins and uid not in approved:
      DATA_START.setdefault("pending_users", [])
      if uid not in DATA_START["pending_users"]:
          DATA_START["pending_users"].append(uid)
      save_data(DATA_START)
      try:
          for admin_id in admins:
              await context.bot.send_message(
                  chat_id=int(admin_id),
                  text=f"طلب وصول جديد من المستخدم: {uid}",
                  reply_markup=InlineKeyboardMarkup([
                      [InlineKeyboardButton("✅ Accept", callback_data=f"admin_approve_user:{uid}")]
                  ])
              )
      except Exception:
          pass
      pending_text = "⏳ Your access request is pending admin approval." if lang == "en" else "⏳ طلبك قيد المراجعة من الأدمن."
      if getattr(update, "message", None):
          await update.message.reply_text(pending_text + SIGNATURE)
      elif getattr(update, "callback_query", None):
          await update.callback_query.message.reply_text(pending_text + SIGNATURE)
      return

  try:
      if uid and uid in DATA_START.get("sellers", {}):
          lang = "en"
          context.user_data["lang"] = "en"
  except Exception:
      pass

  sellers_map = DATA_START.get("sellers", {})
  is_seller = False
  try:
      if uid in sellers_map:
          is_seller = True
      elif uid and uid.isdigit() and int(uid) in sellers_map:
          is_seller = True
  except Exception:
      pass

  menu_items = [button_texts["buy"][lang], button_texts["balance"][lang], button_texts["activity"][lang]]
  if uid in admins:
      menu_items.append(button_texts["admin"][lang])
  reply_keyboard = build_rows(menu_items, 2)

  reply_keyboard.append([button_texts["seller_report"]["en"]])
  reply_keyboard.append([button_texts["add_balance"]["en"], button_texts["payment_history"]["en"]])
  try:
      if is_seller:
          for btn in DATA_START.get("seller_custom_buttons", []):
              label = btn.get("label") if isinstance(btn, dict) else None
              if label:
                  reply_keyboard.append([label])
  except Exception:
      pass

  reply_keyboard.append([button_texts["get_files"][lang]])
  reply_keyboard.append([button_texts["cert"][lang], button_texts["my_cert_keys"][lang]])
  reply_keyboard.append([button_texts["api_balance"][lang]])
  reply_keyboard.append(["⬅️ Back"])

  text = "Welcome to the Sales Bot! Please choose an option:" if lang == "en" else "مرحباً بك في بوت المبيعات! اختر خياراً:"
  if getattr(update, "message", None):
      try:
          DATA_START["start_clicks"] = DATA_START.get("start_clicks", 0) + 1
          save_data(DATA_START)
      except Exception:
          pass
      await update.message.reply_text(text + SIGNATURE, reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
      try:
          uid_str = uid
          if uid_str and uid_str not in DATA_START.get("sellers", {}) and uid_str not in DATA_START.get("seller_promo_shown", []):
              promo = "Become a seller — deposit just $30 one time to start selling. Join our channel for details:"
              kb = InlineKeyboardMarkup([[InlineKeyboardButton("Join Channel", url=SELLER_CHANNEL_URL), InlineKeyboardButton("Dismiss", callback_data="dismiss_seller_promo")]])
              await update.message.reply_text(promo + SIGNATURE, reply_markup=kb)
              DATA_START.setdefault("seller_promo_shown", []).append(uid_str)
              save_data(DATA_START)
      except Exception:
          pass
  elif getattr(update, "callback_query", None):
      try:
          DATA_START["start_clicks"] = DATA_START.get("start_clicks", 0) + 1
          save_data(DATA_START)
      except Exception:
          pass
      await update.callback_query.message.reply_text(text + SIGNATURE, reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
      try:
          uid_str = uid
          if uid_str and uid_str not in DATA_START.get("sellers", {}) and uid_str not in DATA_START.get("seller_promo_shown", []):
              promo = "Become a seller — deposit just $30 one time to start selling. Join our channel for details:"
              kb = InlineKeyboardMarkup([[InlineKeyboardButton("Join Channel", url=SELLER_CHANNEL_URL), InlineKeyboardButton("Dismiss", callback_data="dismiss_seller_promo")]])
              await update.callback_query.message.reply_text(promo + SIGNATURE, reply_markup=kb)
              DATA_START.setdefault("seller_promo_shown", []).append(uid_str)
              save_data(DATA_START)
      except Exception:
          pass


def build_customer_activity_message(data: dict, uid: str, username: str | None = None):
    balance = data.get("balances", {}).get(uid, 0)
  purchases = [s for s in data.get("sales_log", []) if s.get("user") == uid]
  total = len(purchases)
  by_item = {}
  total_spent = 0
  for s in purchases:
      prod = s.get("product")
      dur = s.get("duration")
      price = s.get("price")
      if prod:
          key = (prod, str(dur) if dur is not None else "?")
          by_item[key] = by_item.get(key, {"count": 0, "price": price})
          by_item[key]["count"] += 1
          by_item[key]["price"] = price
          try:
              total_spent += float(price)
          except Exception:
              pass
    handle = f"@{username}" if username else "(no username)"
    msg = f"📊 Your Activity:\nUser: {handle}\nID: {uid}\n\nBalance: ${balance}\nKeys bought: {total}\nTotal spent: ${total_spent}\n\nPurchased items:\n"
  if by_item:
      for (prod, dur), info in by_item.items():
          unit_price = info.get("price")
          line = f"- {prod} | {dur} days | x{info.get('count', 0)}"
          if unit_price is not None:
              line += f" | ${unit_price} each"
          msg += line + "\n"
  else:
      msg += "(no purchases yet)\n"
  return msg


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
  query = update.callback_query
  try:
      await query.answer()
  except telegram.error.BadRequest as e:
      if "Query is too old" in str(e) or "query id is invalid" in str(e):
          return
      print(f"CallbackQuery answer error: {e}")
      return
  except Exception as e:
      print(f"CallbackQuery answer unexpected error: {e}")
      return

  data = getattr(query, "data", None)
  DATA = load_data()
  PRICES = load_prices()
  uid = str(query.from_user.id)
  admins = set(DATA.get("admins", []))
  approved = set(DATA.get("approved_users", []))

  try:
      if uid in DATA.get("sellers", {}):
          context.user_data["lang"] = "en"
  except Exception:
      pass

  if uid not in admins and uid not in approved:
      await query.edit_message_text("⏳ Your access request is pending admin approval." + SIGNATURE)
      return
  if data and data.startswith("admin_") and uid not in admins:
      await query.edit_message_text("❌ Permission denied." + SIGNATURE)
      return

  # Back / main menu
  if data == "back_to_start":
      lang = context.user_data.get("lang", "en")
      menu_text = "Welcome to the Sales Bot! Please choose an option:" if lang == "en" else "مرحباً بك في بوت المبيعات! اختر خياراً:"
      menu_items = ["🛍️ BUY KEYS" if lang == "en" else "🛍️ شراء مفاتيح", "💰 MY BALANCE" if lang == "en" else "💰 رصيدي", "📊 Activity" if lang == "en" else "📊 النشاط"]
      if uid in admins:
          menu_items.append("🔐 ADMIN PANEL" if lang == "en" else "🔐 لوحة الادمن")
      reply_keyboard = build_rows(menu_items, 2)
      reply_keyboard.append(["📈 Seller Report"])
      reply_keyboard.append(["💳 Add Balance", "📜 Payment History"])
      try:
          if uid in DATA.get("sellers", {}):
              lang = "en"
              menu_text = "Welcome to the Sales Bot! Please choose an option:"
              for btn in DATA.get("seller_custom_buttons", []):
                  label = btn.get("label") if isinstance(btn, dict) else None
                  if label:
                      reply_keyboard.append([label])
      except Exception:
          pass
      reply_keyboard.append(["📁 Get Files" if lang == "en" else "📁 الحصول على الملفات"])
      reply_keyboard.append(["🔐 Get Certificate" if lang == "en" else "🔐 جلب الشهادة", "🗝️ My Cert Keys" if lang == "en" else "🗝️ مفاتيح الشهادة"])
      reply_keyboard.append(["💰 Check API Balance" if lang == "en" else "💰 فحص رصيد API"])
      reply_keyboard.append(["⬅️ Back"])
      lang_val = context.user_data.get("lang")
      context.user_data.clear()
      if lang_val:
          context.user_data["lang"] = lang_val
      await query.edit_message_text(menu_text + SIGNATURE)
      await query.message.reply_text(menu_text + SIGNATURE, reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
      return

  if data and data.startswith("topup_amount:"):
      if uid not in DATA.get("sellers", {}):
          await query.edit_message_text("❌ Seller only." + SIGNATURE)
          return
      amount = data.split(":", 1)[1]
      keyboard = [[InlineKeyboardButton(label, callback_data=f"topup_currency:{amount}:{code}")] for label, code in TOPUP_CURRENCIES]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")])
      await query.edit_message_text(f"Choose crypto for ${amount}:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("topup_currency:"):
      if uid not in DATA.get("sellers", {}):
          await query.edit_message_text("❌ Seller only." + SIGNATURE)
          return
      _, amount, currency = data.split(":", 2)
      try:
          topup = create_nowpayments_payment(uid, float(amount), currency)
      except Exception as e:
          await query.edit_message_text(f"❌ Failed to create payment: {e}" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]))
          return

      pay_amount = topup.get("pay_amount", "?")
      pay_address = topup.get("pay_address", "?")
      pay_currency = str(topup.get("pay_currency", currency)).upper()
      payment_id = topup.get("topup_id", "?")
      msg = (
          f"💳 Crypto top-up created\n\n"
          f"Amount to credit: ${amount}\n"
          f"Pay currency: {pay_currency}\n"
          f"Send: {pay_amount}\n"
          f"Address: `{pay_address}`\n"
          f"Payment ID: `{payment_id}`\n\n"
          f"After payment, balance will be added automatically when NOWPayments confirms it."
      )
      keyboard = [[InlineKeyboardButton("🔄 Check Payment", callback_data=f"topup_check:{payment_id}")], [InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]
      await query.edit_message_text(msg + SIGNATURE, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("topup_check:"):
      if uid not in DATA.get("sellers", {}):
          await query.edit_message_text("❌ Seller only." + SIGNATURE)
          return
      payment_id = data.split(":", 1)[1]
      if not requests:
          await query.edit_message_text("❌ requests package missing." + SIGNATURE)
          return
      try:
          response = requests.get(
              f"{NOWPAYMENTS_BASE_URL}/payment/{payment_id}",
              headers={"x-api-key": NOWPAYMENTS_API_KEY},
              timeout=30,
          )
          response.raise_for_status()
          api_data = response.json()
          seller_id, amount, result = apply_topup_from_ipn(api_data)
          data_after = load_data()
          new_balance = data_after.get("balances", {}).get(uid, 0)
          status = str(api_data.get("payment_status", result)).lower()
          if result in ("credited", "already_credited") or status == "finished":
              msg = f"✅ Payment confirmed.\nAmount credited: ${amount}\nNew balance: ${new_balance}"
          else:
              msg = f"⌛ Payment status: {status}\nBalance not added yet."
          await query.edit_message_text(msg + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]))
      except Exception as e:
          await query.edit_message_text(f"❌ Failed to check payment: {e}" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]))
      return

  if data == "admin_menu":
      if uid not in admins:
          await query.edit_message_text("❌ Permission denied." + SIGNATURE)
          return
      admin_keyboard = [
          [InlineKeyboardButton("🕹️ Controle", callback_data="admin_control")],
          [InlineKeyboardButton("💳 Add Balance", callback_data="admin_add_balance")],
          [InlineKeyboardButton("💸 Withdraw", callback_data="admin_withdraw")],
          [InlineKeyboardButton("🔑 Add Keys", callback_data="admin_add_keys")],
          [InlineKeyboardButton("💲 Edit Seller Prices", callback_data="admin_edit_prices")],
          [InlineKeyboardButton("🌐 Edit Global Prices", callback_data="admin_global_prices")],
          [InlineKeyboardButton("📥 Upload Product File", callback_data="admin_upload_file")],
          [InlineKeyboardButton("📤 Send File to User", callback_data="admin_send_file_to_user")],
          [InlineKeyboardButton("➕ Add Seller", callback_data="admin_add_seller_cb")],
          [InlineKeyboardButton("➖ Remove Seller", callback_data="admin_remove_seller_cb")],
          [InlineKeyboardButton("📋 List Sellers", callback_data="admin_list_sellers")],
          [InlineKeyboardButton("💰 Sellers Balance", callback_data="admin_sellers")],
          [InlineKeyboardButton("� Keys Count", callback_data="admin_available_keys")],
          [InlineKeyboardButton("📢 Broadcast Message", callback_data="admin_broadcast")],
          [InlineKeyboardButton("💾 Export Backup", callback_data="admin_export_backup")],
          [InlineKeyboardButton("✅ Pending Users", callback_data="admin_pending_users")],
          [InlineKeyboardButton("📝 آخر عمليات الشراء", callback_data="admin_last_sales")],
          [InlineKeyboardButton("📊 Activity", callback_data="show_activity")],
          [InlineKeyboardButton("📅 Sales Report", callback_data="admin_sales_report")],
          [InlineKeyboardButton("🧾 جميع أرصدة اللاعبين", callback_data="admin_all_balances")],
          [InlineKeyboardButton("🗝️ سحب المفاتيح", callback_data="admin_withdraw_keys")],
          [InlineKeyboardButton("🗂️ جميع المفاتيح والعوائد", callback_data="admin_keys_revenue")],
          [InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]
      ]
      await query.edit_message_text("قائمة الأدمن:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(admin_keyboard))
      return

  if data == "admin_control":
      try:
          await query.answer()
      except Exception:
          pass
      context.user_data.clear()
      keyboard = [
          [InlineKeyboardButton("📊 Sellers Activity", callback_data="admin_control_seller_activity")],
          [InlineKeyboardButton("🌐 Edit Global Prices", callback_data="admin_global_prices")],
          [InlineKeyboardButton("💰 Sellers Balance", callback_data="admin_sellers")],
          [InlineKeyboardButton("📝 Last Sales", callback_data="admin_last_sales")],
          [InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]
      ]
      try:
          await query.edit_message_text("Controle panel:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      except Exception:
          await context.bot.send_message(chat_id=query.from_user.id, text="Controle panel:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data == "admin_control_seller_activity":
      DATA_CTRL = load_data()
      sales_log = DATA_CTRL.get("sales_log", [])
      sellers = DATA_CTRL.get("sellers", {})
      if not sellers:
          await query.edit_message_text("No sellers configured." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_control")]]))
          return
      msg = "📊 Sellers activity summary:\n\n"
      for sid, info in sellers.items():
          seller_sales = [s for s in sales_log if str(s.get("seller_id")) == str(sid)]
          count = len(seller_sales)
          revenue = 0
          last_line = "No sales"
          if seller_sales:
              last_sale = seller_sales[-1]
              product = last_sale.get("product", "?")
              duration = last_sale.get("duration", "?")
              price = last_sale.get("price", 0)
              last_line = f"Last: {product} | {duration}d | ${price}"
              for s in seller_sales:
                  try:
                      revenue += float(s.get("price", 0))
                  except Exception:
                      pass
          msg += f"• {info.get('name','?')} ({sid})\n  Sales: {count} | Revenue: ${revenue} | Balance: ${info.get('balance',0)}\n  {last_line}\n\n"
      await query.edit_message_text(msg + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_control")]]))
      return

  if data == "admin_global_prices":
      try:
          await query.answer()
      except Exception:
          pass
      context.user_data.clear()
      PR_GLOBAL = load_prices()
      products = list(PR_GLOBAL.get("global", {}).keys())
      if not products:
          await query.edit_message_text("No products configured." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      keyboard = [[InlineKeyboardButton(prod, callback_data=f"admin_global_price_prod:{prod}")] for prod in products]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_control")])
      try:
          await query.edit_message_text("Select product to edit global price:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      except Exception:
          await context.bot.send_message(chat_id=query.from_user.id, text="Select product to edit global price:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_global_price_prod:"):
      _, prod = data.split(":", 1)
      PR_GLOBAL = load_prices()
      durations = list(PR_GLOBAL.get("global", {}).get(prod, {}).keys())
      if not durations:
          await query.edit_message_text("No durations configured for this product." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_global_prices")]]))
          return
      keyboard = [[InlineKeyboardButton(f"{d} days", callback_data=f"admin_global_price_dur:{prod}:{d}")] for d in durations]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_global_prices")])
      try:
          await query.edit_message_text(f"Select duration for {prod}:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      except Exception:
          await context.bot.send_message(chat_id=query.from_user.id, text=f"Select duration for {prod}:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_global_price_dur:"):
      _, prod, dur = data.split(":", 2)
      context.user_data["admin_action"] = "set_price"
      context.user_data["edit_price_product"] = prod
      context.user_data["edit_price_days"] = dur
      context.user_data["edit_price_seller"] = "global"
      try:
          await query.edit_message_text(
              f"Send new GLOBAL price for {prod} ({dur} days):" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_global_prices")]])
          )
      except Exception:
          await context.bot.send_message(
              chat_id=query.from_user.id,
              text=f"Send new GLOBAL price for {prod} ({dur} days):" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_global_prices")]])
          )
      return

  if data == "admin_export_backup":
      try:
          async def send_json_backup(path, filename, payload):
              if os.path.exists(path):
                  await context.bot.send_document(chat_id=query.from_user.id, document=InputFile(path), filename=filename)
                  return
              tmp_path = None
              try:
                  with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", suffix=".json") as tf:
                      json.dump(payload, tf, indent=2, ensure_ascii=False)
                      tmp_path = tf.name
                  await context.bot.send_document(chat_id=query.from_user.id, document=InputFile(tmp_path), filename=filename)
              finally:
                  if tmp_path and os.path.exists(tmp_path):
                      try:
                          os.remove(tmp_path)
                      except Exception:
                          pass

          await send_json_backup(DATA_FILE, "bot_data.json", load_data())
          await send_json_backup(PRICES_FILE, "prices.json", load_prices())
          await query.edit_message_text("✅ تم إرسال النسخة الاحتياطية." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      except Exception as e:
          await query.edit_message_text("❌ فشل إرسال النسخة الاحتياطية: " + str(e) + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  # Admin: List all sellers with pagination
  if data == "admin_list_sellers" or data.startswith("admin_list_sellers:"):
      DATA = load_data()
      sellers = DATA.get("sellers", {})
      if not sellers:
          await query.edit_message_text("لا يوجد بائعين مسجلين." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      
      # Pagination
      page = 0
      if ":" in data:
          page = int(data.split(":")[1])
      
      sellers_list = list(sellers.items())
      items_per_page = 10
      total_pages = (len(sellers_list) + items_per_page - 1) // items_per_page
      start_idx = page * items_per_page
      end_idx = min(start_idx + items_per_page, len(sellers_list))
      
      msg = f"📋 قائمة البائعين (Page {page+1}/{total_pages}):\n\n"
      for sid, info in sellers_list[start_idx:end_idx]:
          name = info.get("name", "Unknown")
          balance = info.get("balance", 0)
          sales_count = info.get("sales_count", 0)
          msg += f"• {name} (ID: {sid})\n  - Balance: ${balance}\n  - Sales: {sales_count}\n\n"
      
      # Navigation buttons
      keyboard = []
      nav_row = []
      if page > 0:
          nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"admin_list_sellers:{page-1}"))
      if page < total_pages - 1:
          nav_row.append(InlineKeyboardButton("➡️ Next", callback_data=f"admin_list_sellers:{page+1}"))
      if nav_row:
          keyboard.append(nav_row)
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      
      await query.edit_message_text(msg + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  # Admin: Show available keys
  if data == "admin_available_keys":
      DATA = load_data()
      keys_data = DATA.get("keys", {})
      used_keys = set(DATA.get("used_keys", []))
      if not keys_data:
          await query.edit_message_text("لا توجد أي مفاتيح متوفرة حالياً." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      msg = "🔑 المفاتيح المتوفرة:\n\n"
      total_available = 0
      for prod_dur, keys in keys_data.items():
          available = [k for k in keys if k not in used_keys]
          count = len(available)
          total_available += count
          msg += f"• {prod_dur}: {count} keys\n"
      msg += f"\n📊 Total available keys: {total_available}\n"
      if len(msg) > 4000:
          msg = msg[:4000] + "\n... [Message Truncated]"
      try:
          await query.edit_message_text(msg + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      except Exception as e:
          pass
      return

  if data == "admin_pending_users":
      pending = DATA.get("pending_users", [])
      if not pending:
          await query.edit_message_text("لا توجد طلبات معلقة." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      keyboard = []
      for pid in pending[:50]:
          keyboard.append([InlineKeyboardButton(f"✅ Accept {pid}", callback_data=f"admin_approve_user:{pid}")])
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text("طلبات الانتظار:", reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_approve_user:"):
      _, target = data.split(":", 1)
      DATA.setdefault("approved_users", [])
      DATA.setdefault("pending_users", [])
      if target not in DATA["approved_users"]:
          DATA["approved_users"].append(target)
      if target in DATA["pending_users"]:
          DATA["pending_users"].remove(target)
      save_data(DATA)
      try:
          await context.bot.send_message(chat_id=int(target), text="✅ Your access has been approved." + SIGNATURE)
      except Exception:
          pass
      await query.edit_message_text(f"✅ Approved {target}." + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return
  # Show all player balances
  if data == "admin_all_balances":
      DATA = load_data()
      balances = DATA.get("balances", {})
      users = DATA.get("users", [])
      msg = "🧾 جميع أرصدة اللاعبين:\n\n"
      for uid in users:
          bal = balances.get(uid, 0)
          msg += f"• {uid}: ${bal}\n"
      await query.edit_message_text(msg + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  # Buy menu
  if data == "buy_menu":
      lang = context.user_data.get("lang", "en")
      product_names = {
          "FREE": {"en": "🔮 FREE FIRE", "ar": "🔮 فري فاير"},
          "WIZARD": {"en": "✨ WIZARD", "ar": "✨ ويزارد"},
          "CLOUD": {"en": "☁️ CLOUD", "ar": "☁️ كلاود"},
          "CODM_IOS": {"en": "📱 CODM IOS", "ar": "📱 كودم IOS"},
          "TERMINAL_X_PC": {"en": "💻 TERMINAL X PC", "ar": "💻 تيرمنال X PC"},
          "HG_CHEATS_ROOT": {"en": "🛡️ HG CHEATS ROOT", "ar": "🛡️ HG شيتس روت"},
          "CERT_IPHONE_WARRANTY": {"en": "📱 iPhone Warranty (30d)", "ar": "📱 ضمان ايفون (شهر)"}
      }
      buttons = [product_names[p][lang] for p in product_names]
      reply_keyboard = build_rows(buttons, 2)
      reply_keyboard.append(["⬅️ Back"])
      # mark that user is in buy menu so reply-keyboard selections are processed
      context.user_data["buy_menu"] = True
      await query.edit_message_text("اختر المنتج:" + SIGNATURE)
      await query.message.reply_text("اختر المنتج:" + SIGNATURE, reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
      return

  if data == "show_balance":
      user_id = str(query.from_user.id)
      bal = DATA.get("balances", {}).get(user_id, 0)
      keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]
      await query.edit_message_text(f"Your balance: ${bal}" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  # Activity / statistics
  if data == "show_activity":
      DATA_STAT = load_data()
      uid = str(query.from_user.id)
      # only admins and sellers can view activity
      admins = set(DATA_STAT.get("admins", []))
      sellers_map = DATA_STAT.get("sellers", {})
      is_seller = uid in sellers_map or (uid.isdigit() and int(uid) in sellers_map)
      if uid not in admins and not is_seller:
          msg = build_customer_activity_message(DATA_STAT, uid, query.from_user.username)
          await query.edit_message_text(msg + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]))
          return

      # Seller view: show seller balance and their sales
      if is_seller:
          seller = sellers_map.get(uid, {}) if uid in sellers_map else sellers_map.get(int(uid), {})
          balance = seller.get("balance", 0)
          sales = [s for s in DATA_STAT.get("sales_log", []) if str(s.get("seller_id")) == str(uid)]
          sold_count = len(sales)
          by_product = {}
          for s in sales:
              by_product[s.get("product")] = by_product.get(s.get("product"), 0) + 1
          msg = f"📊 Seller Activity ({seller.get('name','Unknown')} - {uid}):\n\nBalance: ${balance}\nKeys sold: {sold_count}\n\nSold by product:\n"
          if by_product:
              for p, c in by_product.items():
                  msg += f"- {p}: {c}\n"
          else:
              msg += "(no sales yet)\n"
          keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]
          await query.edit_message_text(msg + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
          return

      # Admin view: overall stats + per-seller breakdown
      start_clicks = DATA_STAT.get("start_clicks", 0)
      users_count = len(DATA_STAT.get("users", []))
      used_keys = DATA_STAT.get("used_keys", []) or []
      total_keys_sold = len(used_keys)
      total_keys_available = sum(len(v) for v in DATA_STAT.get("keys", {}).values())
      sellers = DATA_STAT.get("sellers", {})
      sellers_count = len(sellers)
      sales_count = len(DATA_STAT.get("sales_log", []))
      sold_by_product = {}
      for s in DATA_STAT.get("sales_log", []):
          sold_by_product[s.get("product")] = sold_by_product.get(s.get("product"), 0) + 1
      msg = f"📊 Bot Activity Summary:\n\nStart clicks: {start_clicks}\nKnown users: {users_count}\nTotal sales entries: {sales_count}\nKeys sold: {total_keys_sold}\nKeys available: {total_keys_available}\nSellers: {sellers_count}\n\nSold by product:\n"
      for prod, cnt in sold_by_product.items():
          msg += f"- {prod}: {cnt}\n"
      msg += "\nPer-seller breakdown:\n"
      if sellers:
          for sid, info in sellers.items():
              s_sales = [s for s in DATA_STAT.get("sales_log", []) if s.get("seller_id") == sid]
              s_count = len(s_sales)
              prod_counts = {}
              for s in s_sales:
                  prod_counts[s.get("product")] = prod_counts.get(s.get("product"), 0) + 1
              msg += f"- {info.get('name','?')} ({sid}) — Balance: ${info.get('balance',0)} — Keys sold: {s_count}\n"
              if prod_counts:
                  for p, c in prod_counts.items():
                      msg += f"    • {p}: {c}\n"
      else:
          msg += "(no sellers configured)\n"

      keyboard = [
          [InlineKeyboardButton("📤 Send Activity to Me", callback_data="admin_send_activity")],
          [InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]
      ]
      await query.edit_message_text(msg + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data == "admin_send_activity":
      DATA_STAT = load_data()
      uid = str(query.from_user.id)
      admins = set(DATA_STAT.get("admins", []))
      if uid not in admins:
          await query.answer("❌ Permission denied.", show_alert=True)
          return
          
      start_clicks = DATA_STAT.get("start_clicks", 0)
      users_count = len(DATA_STAT.get("users", []))
      used_keys = DATA_STAT.get("used_keys", []) or []
      total_keys_sold = len(used_keys)
      total_keys_available = sum(len(v) for v in DATA_STAT.get("keys", {}).values())
      sellers = DATA_STAT.get("sellers", {})
      sellers_count = len(sellers)
      sales_count = len(DATA_STAT.get("sales_log", []))
      sold_by_product = {}
      for s in DATA_STAT.get("sales_log", []):
          sold_by_product[s.get("product")] = sold_by_product.get(s.get("product"), 0) + 1
      msg = f"📊 Bot Activity Summary:\n\nStart clicks: {start_clicks}\nKnown users: {users_count}\nTotal sales entries: {sales_count}\nKeys sold: {total_keys_sold}\nKeys available: {total_keys_available}\nSellers: {sellers_count}\n\nSold by product:\n"
      for prod, cnt in sold_by_product.items():
          msg += f"- {prod}: {cnt}\n"
      msg += "\nPer-seller breakdown:\n"
      if sellers:
          for sid, info in sellers.items():
              s_sales = [s for s in DATA_STAT.get("sales_log", []) if s.get("seller_id") == sid]
              s_count = len(s_sales)
              prod_counts = {}
              for s in s_sales:
                  prod_counts[s.get("product")] = prod_counts.get(s.get("product"), 0) + 1
              msg += f"- {info.get('name','?')} ({sid}) — Balance: ${info.get('balance',0)} — Keys sold: {s_count}\n"
              if prod_counts:
                  for p, c in prod_counts.items():
                      msg += f"    • {p}: {c}\n"
      else:
          msg += "(no sellers configured)\n"
          
      await context.bot.send_message(chat_id=int(uid), text=msg + SIGNATURE)
      await query.answer("✅ Activity sent to your DMs.", show_alert=True)
      return

  # Dismiss seller promo
  if data == "dismiss_seller_promo":
      try:
          await query.edit_message_text("Promo dismissed." + SIGNATURE)
      except Exception:
          pass
      return

  # Buy flow: show durations
  if data and data.startswith("buy:"):
      product = data.split(":", 1)[1]
      user_id = str(query.from_user.id)
      # Always show prices - they're either global or seller-specific
      prod_prices = PRICES.get("global", {}).get(product)
      if not prod_prices:
          await query.edit_message_text(f"⚠️ No pricing/config found for {product}. Ask the admin to set prices or add keys." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="buy_menu")]]))
          return
      keyboard = []
      for days in sorted(prod_prices.keys(), key=lambda x: int(x)):
          # Get user-specific or global price
          price = get_price_for_user(PRICES, product, days, user_id, DATA)
          btn_text = f"⏱️ {days}يوم - ${price}"
          keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"choose_qty:{product}:{days}")])
      keyboard.append([InlineKeyboardButton("⬅️ رجوع", callback_data="buy_menu")])
      await query.edit_message_text(f"اختر المدة للمنتج {product}:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  # Choose quantity
  if data and data.startswith("choose_qty:"):
      _, product, days = data.split(":", 2)
      qty_keyboard = [[InlineKeyboardButton(str(i), callback_data=f"pay:{product}:{days}:{i}")] for i in range(1, 11)]
      qty_keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"buy:{product}")])
      await query.edit_message_text(f"Select quantity for {product} ({days} days):" + SIGNATURE, reply_markup=InlineKeyboardMarkup(qty_keyboard))
      return

  # Pay / complete purchase (notify admins)
  if data and data.startswith("pay:"):
      parts = data.split(":")
      product = parts[1]
      days = parts[2]
      qty = int(parts[3]) if len(parts) > 3 else 1
      user_id = str(query.from_user.id)
      unit_price = get_price_for_user(PRICES, product, days, user_id, DATA)
      if unit_price is None:
          await query.edit_message_text(
              f"⚠️ Pricing not configured for {product} {days} days." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="buy_menu")]])
          )
          return
      price = unit_price * qty
      try:
          balance = float(DATA.get("balances", {}).get(user_id, 0))
      except Exception:
          balance = 0

      key_name = key_storage_name(product, days)
      keys_pool = DATA.get("keys", {}).get(key_name, [])
      used_keys = set(DATA.get("used_keys", []))
      available_keys = [k for k in keys_pool if k not in used_keys]

      if len(available_keys) < qty:
          # Debug help: find where keys might be
          msg = f"❌ Not enough keys available.\nAvailable: {len(available_keys)}\nExpected key bucket: {key_name}\n"
          
          # Suggest alternatives if keys exist elsewhere
          found_elsewhere = []
          for k_name, k_list in DATA.get("keys", {}).items():
              av = [k for k in k_list if k not in used_keys]
              if len(av) > 0 and (product in k_name or days in k_name):
                   found_elsewhere.append(f"{k_name} ({len(av)})")
          
          if found_elsewhere:
              msg += f"\n💡 Found keys in other buckets:\n" + "\n".join(found_elsewhere)
          
          msg += "\nAsk admin to add keys for this exact product/duration.\n" + SIGNATURE
          
          await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="buy_menu")]]))
          return

      if balance < price:
           await query.edit_message_text("❌ Insufficient balance!" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="buy_menu")]]))
           return

      # Process purchase
      keys = [available_keys.pop(0) for _ in range(qty)]
      DATA.setdefault("used_keys", [])
      DATA["used_keys"] = list(set(DATA.get("used_keys", [])).union(keys))
      
      DATA.setdefault("sold_keys", {}).setdefault(key_name, []).extend(keys)
      DATA["balances"][user_id] = balance - price
      
      seller_id = user_id if user_id in DATA.get("sellers", {}) else None
      seller_name = DATA.get("sellers", {}).get(seller_id, {}).get("name", "?") if seller_id else None
      
      for k in keys:
          sale_entry = {
              "user": user_id,
              "product": product,
              "duration": days,
              "price": unit_price,
              "buyer_balance": balance - price,
              "key": k,
              "timestamp": str(datetime.utcnow())
          }
          if seller_id:
              sale_entry["seller_id"] = seller_id
              sale_entry["seller_name"] = seller_name
          DATA.setdefault("sales_log", []).append(sale_entry)
          
      save_data(DATA)
      
      # Notify admins
      admins = DATA.get("admins", [])
      buyer_balance = balance - price
      for admin_id in admins:
          try:
              if seller_id:
                  text_msg = f"🔔 Purchase by Seller\nProduct: {product}\nDuration: {days}\nQty: {len(keys)}\nSeller: {seller_name} ({seller_id})\nBuyer Balance: ${buyer_balance}"
              else:
                  text_msg = f"🔔 Purchase\nProduct: {product}\nDuration: {days}\nQty: {len(keys)}\nBalance: ${buyer_balance}"
              await context.bot.send_message(chat_id=int(admin_id), text=text_msg)
          except Exception:
              pass
              
      # Send keys
      keys_str = "\n".join([f"`{k}`" for k in keys])
      await query.edit_message_text(
          f"✅ Purchase successful!\n\nYour keys:\n{keys_str}\n\n📁 Use Get Files to download product updates." + SIGNATURE,
          parse_mode="Markdown"
      )
      return

  if data == "vip_list_seller_options":
      DATA_VIP = load_data()
      opts = DATA_VIP.get("seller_custom_buttons", [])
      if not opts:
          await query.edit_message_text(
              "لا توجد خيارات مضافة." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="vip_menu")]])
          )
          return
      msg = "خيارات البائعين الحالية:\n\n"
      for o in opts:
          if isinstance(o, dict):
              msg += f"• {o.get('label','?')} → {o.get('response','')}\n"
      await query.edit_message_text(
          msg + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="vip_menu")]])
      )
      return

  if data == "vip_clear_seller_options":
      DATA_VIP = load_data()
      DATA_VIP["seller_custom_buttons"] = []
      save_data(DATA_VIP)
      await query.edit_message_text(
          "تم حذف جميع الخيارات." + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="vip_menu")]])
      )
      return

  # Admin: Download all keys as text file
  if data == "admin_download_keys":
      import tempfile
      DATA = load_data()
      keys_data = DATA.get("keys", {})
      used_keys = set(DATA.get("used_keys", []))
      lines = []
      for prod_dur, keys in keys_data.items():
          available = [k for k in keys if k not in used_keys]
          lines.append(f"{prod_dur}:")
          if available:
              for k in available:
                  lines.append(f"- {k}")
          else:
              lines.append("(لا يوجد مفاتيح متاحة)")
          lines.append("")
      content = "\n".join(lines)
      with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", suffix="_keys.txt") as tf:
          tf.write(content)
          temp_path = tf.name
      await context.bot.send_document(chat_id=query.from_user.id, document=InputFile(temp_path), filename="all_keys.txt", caption="جميع المفاتيح المتوفرة")
      import os
      try:
          os.remove(temp_path)
      except Exception:
          pass
      await query.edit_message_text("تم إرسال ملف جميع المفاتيح المتوفرة لك." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  # Admin: View all keys - show product selection first
  if data == "admin_view_keys":
      DATA = load_data()
      keys_data = DATA.get("keys", {})
      if not keys_data:
          await query.edit_message_text("لا توجد أي مفاتيح متوفرة حالياً." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      
      used_keys = set(DATA.get("used_keys", []))
      keyboard = []
      msg = "🔍 Select product to view keys:\n\n"
      
      # Show products with key counts
      for prod_dur, keys in sorted(keys_data.items()):
          available = [k for k in keys if k not in used_keys]
          count = len(available)
          keyboard.append([InlineKeyboardButton(f"{prod_dur} ({count} keys)", callback_data=f"view_keys_prod:{prod_dur}")])
      
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text(msg + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return
  
  # Admin: View keys for specific product with pagination
  if data.startswith("view_keys_prod:"):
      parts = data.split(":", 2)
      prod_dur = parts[1]
      page = int(parts[2]) if len(parts) > 2 else 0
      
      DATA = load_data()
      keys_data = DATA.get("keys", {})
      used_keys = set(DATA.get("used_keys", []))
      
      all_keys = keys_data.get(prod_dur, [])
      available = [k for k in all_keys if k not in used_keys]
      
      if not available:
          await query.edit_message_text(f"No available keys for {prod_dur}" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_view_keys")]]))
          return
      
      # Pagination
      keys_per_page = 20
      total_pages = (len(available) + keys_per_page - 1) // keys_per_page
      start_idx = page * keys_per_page
      end_idx = min(start_idx + keys_per_page, len(available))
      
      msg = f"🔑 {prod_dur} (Page {page+1}/{total_pages}):\n\n"
      for k in available[start_idx:end_idx]:
          msg += f"`{k}`\n"
      
      # Navigation buttons
      keyboard = []
      nav_row = []
      if page > 0:
          nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"view_keys_prod:{prod_dur}:{page-1}"))
      if page < total_pages - 1:
          nav_row.append(InlineKeyboardButton("➡️ Next", callback_data=f"view_keys_prod:{prod_dur}:{page+1}"))
      if nav_row:
          keyboard.append(nav_row)
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_view_keys")])
      
      await query.edit_message_text(msg + SIGNATURE, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
      return

  # Admin: show product list to edit per-seller prices
  if data == "admin_edit_prices":
      context.user_data.clear()
      context.user_data["admin_action"] = "edit_price_enter_seller"
      await query.edit_message_text("Enter Seller ID to set custom prices (or type 'global' for global prices):" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  # Handle product selection after seller ID is entered
  if data and data.startswith("edit_price_prod:"):
      _, prod = data.split(":", 1)
      context.user_data["edit_price_product"] = prod
      context.user_data["admin_action"] = "edit_price_choose_duration"
      PRICES = load_prices()
      durations = list(PRICES.get("global", {}).get(prod, {}).keys())
      if not durations:
          await query.edit_message_text(f"No durations configured for {prod}." + SIGNATURE, 
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      await query.edit_message_text(f"Product: {prod}\nEnter duration (available: {', '.join(durations)}):" + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data and data.startswith("edit_price:"):
      _, prod = data.split(":", 1)
      PRICES = load_prices()
      prod_prices = PRICES.get("global", {}).get(prod, {})
      if not prod_prices:
          await query.edit_message_text("No price durations configured for this product." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_edit_prices")]]))
          return
      keyboard = [[InlineKeyboardButton(f"{days} days", callback_data=f"edit_price_choose_days:{prod}:{days}")] for days in prod_prices.keys()]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_edit_prices")])
      await query.edit_message_text(f"Select duration for {prod}:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("edit_price_choose_days:"):
      _, prod, days = data.split(":", 2)
      sellers = DATA.get("sellers", {})
      if not sellers:
          await query.edit_message_text("No sellers configured." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_edit_prices")]]))
          return
      keyboard = [[InlineKeyboardButton("GLOBAL", callback_data=f"edit_price_choose_seller:{prod}:{days}:global")]]
      for sid, info in sellers.items():
          keyboard.append([InlineKeyboardButton(f"{info.get('name','?')} ({sid})", callback_data=f"edit_price_choose_seller:{prod}:{days}:{sid}")])
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_edit_prices")])
      await query.edit_message_text(f"Select seller for {prod} ({days} days):" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("edit_price_choose_seller:"):
      _, prod, days, seller_id = data.split(":", 3)
      context.user_data["admin_action"] = "set_price"
      context.user_data["edit_price_product"] = prod
      context.user_data["edit_price_days"] = days
      context.user_data["edit_price_seller"] = seller_id
      await query.edit_message_text(
          f"Send new price for {prod} ({days} days) for {'GLOBAL' if seller_id == 'global' else seller_id}:" + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_edit_prices")]])
      )
      return
  if data == "admin_create_key":
      PRICES = load_prices()
      keyboard = [[InlineKeyboardButton(prod, callback_data=f"admin_create_key_product:{prod}")] for prod in PRICES.get("global", {}).keys()]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text("Select product to create a single key for:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return
  if data and data.startswith("admin_create_key_product:"):
      _, prod = data.split(":", 1)
      PRICES = load_prices()
      durations = list(PRICES.get("global", {}).get(prod, {}).keys())
      if not durations:
          await query.edit_message_text(f"No durations configured for {prod}. Add durations in prices first." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_create_key")]]))
          return
      keyboard = [[InlineKeyboardButton(f"{d} days", callback_data=f"admin_create_key_duration:{prod}:{d}")] for d in durations]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text(f"Select duration for new key for {prod}:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_create_key_duration:"):
      _, prod, days = data.split(":", 2)
      context.user_data["admin_action"] = "create_key"
      context.user_data["create_key_product"] = prod
      context.user_data["create_key_duration"] = days
      await query.edit_message_text(f"Send the key string to create for product {prod} ({days} days)." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  # Admin: show sellers list with balances
  if data == "admin_sellers":
      DATA = load_data()
      sellers = DATA.get("sellers", {})
      if not sellers:
          await query.edit_message_text("No sellers configured." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      keyboard = []
      for sid, info in sellers.items():
          label = f"{info.get('name','?')} - {sid} - ${info.get('balance',0)}"
          if sid in DATA.get("stopped_sellers", []):
              label += " [STOPPED]"
          keyboard.append([InlineKeyboardButton(label, callback_data=f"admin_seller:{sid}")])
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text("Sellers:", reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_seller:"):
      _, sid = data.split(":", 1)
      DATA = load_data()
      s = DATA.get("sellers", {}).get(sid)
      if not s:
          await query.edit_message_text("Seller not found." + SIGNATURE)
          return
    stopped_sellers = [str(x) for x in DATA.get("stopped_sellers", [])]
    stopped = str(sid) in stopped_sellers
      keyboard = [
          [InlineKeyboardButton("Add Balance", callback_data=f"admin_add_seller_balance:{sid}")],
          [InlineKeyboardButton("Stop Seller" if not stopped else "Activate Seller", callback_data=f"admin_toggle_seller:{sid}")],
          [InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]
      ]
      status = "STOPPED" if stopped else "ACTIVE"
      await query.edit_message_text(f"Seller {s.get('name')} (ID: {sid})\nBalance: ${s.get('balance',0)}\nSales: {s.get('sales_count',0)}\nStatus: {status}" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return
  if data and data.startswith("admin_toggle_seller:"):
      _, sid = data.split(":", 1)
      DATA = load_data()
      stopped_sellers = [str(x) for x in DATA.get("stopped_sellers", [])]
      if str(sid) in stopped_sellers:
          # Activate seller
          DATA["stopped_sellers"] = [x for x in DATA.get("stopped_sellers", []) if str(x) != str(sid)]
          save_data(DATA)
          await query.edit_message_text(f"✅ Seller {sid} activated." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      else:
          # Stop seller
          DATA.setdefault("stopped_sellers", []).append(str(sid))
          save_data(DATA)
          await query.edit_message_text(f"⛔ Seller {sid} stopped." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      return

  if data and data.startswith("admin_add_seller_balance:"):
      _, sid = data.split(":", 1)
      context.user_data["admin_action"] = "add_seller_balance"
      context.user_data["target_seller"] = sid
      await query.edit_message_text(f"Send amount to add to seller {sid}:" + SIGNATURE)
      return

  # Admin broadcast
  if data == "admin_broadcast":
      context.user_data["admin_action"] = "broadcast"
      await query.edit_message_text("Send the broadcast message (text/photo/video/document/audio/voice/animation):" + SIGNATURE)
      return

  # Admin add file: choose product
  if data == "admin_add_balance":
      context.user_data.clear()
      context.user_data["admin_action"] = "add_balance"
      await query.edit_message_text("أدخل معرف المستخدم والمبلغ (مثال: 123456789 10)" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return
  if data == "admin_upload_file":
      PRICES = load_prices()
      keyboard = [[InlineKeyboardButton(prod, callback_data=f"admin_upload_file_product:{prod}")] for prod in PRICES.get("global", {}).keys()]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text("اختر المنتج لرفع الملف:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return
  if data and data.startswith("admin_upload_file_product:"):
      _, prod = data.split(":", 1)
      context.user_data.clear()
      context.user_data["admin_action"] = "upload_file"
      context.user_data["upload_product"] = prod
      await query.edit_message_text(f"أرسل الملف للمنتج {prod}." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return
  if data == "admin_send_file_to_user":
      context.user_data.clear()
      context.user_data["admin_action"] = "send_file_to_user"
      await query.edit_message_text("أدخل معرف المستخدم لإرسال الملف له (مثال: 123456789)" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return
  if data == "admin_withdraw":
      context.user_data.clear()
      context.user_data["admin_action"] = "withdraw"
      await query.edit_message_text("أدخل معرف المستخدم والمبلغ للسحب (مثال: 123456789 5)" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return
  if data == "admin_withdraw_keys":
      keys_data = DATA.get("keys", {})
      if not keys_data:
          await query.edit_message_text("لا توجد مفاتيح لسحبها." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      products = sorted({k.split("_")[0] for k in keys_data.keys() if "_" in k})
      if not products:
          products = sorted(list(PRICES.get("global", {}).keys()))
      keyboard = [[InlineKeyboardButton(prod, callback_data=f"admin_withdraw_keys_product:{prod}")] for prod in products]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text("اختر المنتج لسحب المفاتيح:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_withdraw_keys_product:"):
      _, prod = data.split(":", 1)
      durations = list(PRICES.get("global", {}).get(prod, {}).keys())
      if not durations:
          await query.edit_message_text("لا توجد مدد لهذا المنتج." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_withdraw_keys")]]))
          return
      keyboard = [[InlineKeyboardButton(f"{d} يوم", callback_data=f"admin_withdraw_keys_duration:{prod}:{d}")] for d in durations]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_withdraw_keys")])
      await query.edit_message_text(f"اختر المدة للمنتج {prod}:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_withdraw_keys_duration:"):
      _, prod, days = data.split(":", 2)
      key_name = key_storage_name(prod, days)
      keys_pool = [k for k in DATA.get("keys", {}).get(key_name, []) if k not in set(DATA.get("used_keys", []))]
      if not keys_pool:
          await query.edit_message_text(f"لا توجد مفاتيح متاحة للمنتج {prod} لمدة {days} يوم." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"admin_withdraw_keys_product:{prod}")]]))
          return
      max_qty = min(10, len(keys_pool))
      keyboard = [[InlineKeyboardButton(str(i), callback_data=f"admin_withdraw_keys_qty:{prod}:{days}:{i}")] for i in range(1, max_qty + 1)]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"admin_withdraw_keys_product:{prod}")])
      await query.edit_message_text(f"اختر الكمية لسحب مفاتيح {prod} ({days} يوم):" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_withdraw_keys_qty:"):
      _, prod, days, qty_str = data.split(":", 3)
      try:
          qty = int(qty_str)
      except Exception:
          qty = 1
      key_name = key_storage_name(prod, days)
      keys_pool = [k for k in DATA.get("keys", {}).get(key_name, []) if k not in set(DATA.get("used_keys", []))]
      if len(keys_pool) < qty:
          await query.edit_message_text(f"❌ لا يوجد مفاتيح كافية للسحب. المتوفر: {len(keys_pool)}" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"admin_withdraw_keys_duration:{prod}:{days}")]]))
          return
      withdrawn = keys_pool[:qty]
      DATA.setdefault("used_keys", [])
      DATA["used_keys"] = list(set(DATA.get("used_keys", [])).union(withdrawn))
      DATA.setdefault("sold_keys", {}).setdefault(key_name, []).extend(withdrawn)
      save_data(DATA)
      keys_str = "\n".join([f"`{k}`" for k in withdrawn])
      await query.edit_message_text(f"✅ تم سحب المفاتيح:\n{keys_str}" + SIGNATURE, parse_mode="Markdown",
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return
  if data == "admin_add_seller_cb":
      context.user_data.clear()
      context.user_data["admin_action"] = "add_seller"
      await query.edit_message_text("أدخل معرف البائع والاسم (مثال: 123456789 @sellername)" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return
  if data == "admin_remove_seller_cb":
      context.user_data.clear()
      context.user_data["admin_action"] = "remove_seller"
      await query.edit_message_text("أدخل معرف البائع للحذف (مثال: 123456789)" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return
  if data == "admin_manage_files":
      DATAF = load_data()
      files = DATAF.get("files", {})
      if not files:
          await query.edit_message_text("No files uploaded." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      keyboard = []
      for prod in files.keys():
          keyboard.append([InlineKeyboardButton(prod, callback_data=f"admin_send_file:{prod}"), InlineKeyboardButton("Delete", callback_data=f"admin_delete_file:{prod}")])
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text("Manage uploaded files:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_send_file:"):
      _, prod = data.split(":", 1)
      DATAF = load_data()
      file_ref = DATAF.get("files", {}).get(prod)
      if not file_ref:
          await query.edit_message_text("File not found." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_manage_files")]]))
          return
      try:
          # if local path exists, send local file; else use stored file_id
          if os.path.exists(file_ref):
              await context.bot.send_document(chat_id=query.from_user.id, document=InputFile(file_ref))
          else:
              candidate = file_ref
              if not os.path.exists(candidate) and os.path.exists(os.path.join(os.getcwd(), candidate)):
                  candidate = os.path.join(os.getcwd(), candidate)
              if os.path.exists(candidate):
                  await context.bot.send_document(chat_id=query.from_user.id, document=InputFile(candidate))
              else:
                  # fallback: try sending as file_id
                  await context.bot.send_document(chat_id=query.from_user.id, document=file_ref)
          await query.edit_message_text(f"File {prod} sent to you." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_manage_files")]]))
      except Exception as e:
          await query.edit_message_text("Failed to send file: " + str(e) + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_manage_files")]]))
      return

  if data and data.startswith("admin_delete_file:"):
      _, prod = data.split(":", 1)
      DATAF = load_data()
      files = DATAF.get("files", {})
      if prod not in files:
          await query.edit_message_text("File not found." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_manage_files")]]))
          return
      file_ref = files.get(prod)
      # attempt to remove local file if it exists and is within workspace
      try:
          if os.path.exists(file_ref):
              os.remove(file_ref)
      except Exception:
          pass
      # remove entries from data
      DATAF.get("files", {}).pop(prod, None)
      if "files_meta" in DATAF:
          DATAF.get("files_meta", {}).pop(prod, None)
      save_data(DATAF)
      await query.edit_message_text(f"Deleted file for product {prod}." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_manage_files")]]))
      return

  # Seller requesting file via callback (if using inline flow)
  if data and data.startswith("seller_get_file:"):
      _, prod = data.split(":", 1)
      DATA = load_data()
      uid = str(query.from_user.id)
      if uid not in DATA.get("sellers", {}):
          await query.edit_message_text("Only sellers can download product files." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]))
          return
      files = DATA.get("files", {})
      file_ref = files.get(prod)
      if not file_ref:
          await query.edit_message_text("No file uploaded for this product." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]))
          return
      try:
          # If stored path exists locally, send local file, else fallback to telegram file_id
          if os.path.exists(file_ref):
              await context.bot.send_document(chat_id=query.from_user.id, document=InputFile(file_ref))
          else:
              candidate = file_ref
              if not os.path.exists(candidate) and os.path.exists(os.path.join(os.getcwd(), candidate)):
                  candidate = os.path.join(os.getcwd(), candidate)
              if os.path.exists(candidate):
                  await context.bot.send_document(chat_id=query.from_user.id, document=InputFile(candidate))
              else:
                  await context.bot.send_document(chat_id=query.from_user.id, document=file_ref)
          await query.edit_message_text("File sent." + SIGNATURE)
      except Exception as e:
          await query.edit_message_text("Failed to send file: " + str(e) + SIGNATURE)
      return


# ========== MESSAGE HANDLER ==========
async def send_duration_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, selected_product: str, lang: str, lang_button_text: str):
  PRICES = load_prices()
  DATA = load_data()
  uid = str(update.message.from_user.id)
  durations = list(PRICES.get("global", {}).get(selected_product, {}).keys())
  if not durations:
      await update.message.reply_text(f"⚠️ لا توجد أسعار معرفة للمنتج {selected_product}. تواصل مع الأدمن." + SIGNATURE)
      return False
  duration_names = {
      "1": {"en": "1 day", "ar": "يوم", "fr": "1 jour", "es": "1 día", "de": "1 Tag", "tr": "1 gün"},
      "3": {"en": "3 days", "ar": "3 أيام", "fr": "3 jours", "es": "3 días", "de": "3 Tage", "tr": "3 gün"},
      "7": {"en": "7 days", "ar": "7 أيام", "fr": "7 jours", "es": "7 días", "de": "7 Tage", "tr": "7 gün"},
      "10": {"en": "10 days", "ar": "10 أيام", "fr": "10 jours", "es": "10 días", "de": "10 Tage", "tr": "10 gün"},
      "15": {"en": "15 days", "ar": "15 يوم", "fr": "15 jours", "es": "15 días", "de": "15 Tage", "tr": "15 gün"},
      "30": {"en": "30 days", "ar": "30 يوم", "fr": "30 jours", "es": "30 días", "de": "30 Tage", "tr": "30 gün"},
      "31": {"en": "1 month", "ar": "شهر", "fr": "1 mois", "es": "1 mes", "de": "1 Monat", "tr": "1 ay"},
      "365": {"en": "1 year", "ar": "سنة", "fr": "1 an", "es": "1 año", "de": "1 Jahr", "tr": "1 yıl"}
  }
  reply_keyboard = []
  for d in durations:
      if d in duration_names:
          price = get_price_for_user(PRICES, selected_product, d, uid, DATA)
          if price is not None:
              reply_keyboard.append([f"{duration_names[d][lang]} - ${price}"])
          else:
              reply_keyboard.append([duration_names[d][lang]])
  reply_keyboard.append(["⬅️ Back"])
  await update.message.reply_text(
      "اختر المدة:" if lang == "ar" else
      "Select duration:" if lang == "en" else
      "Sélectionnez la durée:" if lang == "fr" else
      "Seleccione la duración:" if lang == "es" else
      "Dauer auswählen:" if lang == "de" else
      "Süre seçin:",
      reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)
  )
  context.user_data["selected_product"] = selected_product
  context.user_data["choose_duration"] = True
  context.user_data["buy_menu"] = False
  return True

# ========== MESSAGE HANDLER ==========
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
  text = update.message.text.strip() if update.message.text else ""
  uid = str(update.message.from_user.id)
  if not context.user_data.get("lang"):
      context.user_data["lang"] = DEFAULT_LANG

  # record user as known
  try:
      DATA_LOCAL = load_data()
      if uid not in DATA_LOCAL.get("users", []):
          DATA_LOCAL.setdefault("users", []).append(uid)
          save_data(DATA_LOCAL)
  except Exception:
      pass

  # approval gate
  try:
      DATA_APPROVE = load_data()
      admins = set(DATA_APPROVE.get("admins", []))
      approved = set(DATA_APPROVE.get("approved_users", []))
      if uid not in admins and uid not in approved and text != ADMIN_CODE:
          DATA_APPROVE.setdefault("pending_users", [])
          if uid not in DATA_APPROVE["pending_users"]:
              DATA_APPROVE["pending_users"].append(uid)
              save_data(DATA_APPROVE)
          await update.message.reply_text("⏳ Your access request is pending admin approval." + SIGNATURE)
          return
  except Exception:
      pass

  # force English for sellers
  try:
      DATA_LANG = load_data()
      if uid in DATA_LANG.get("sellers", {}):
          context.user_data["lang"] = "en"
  except Exception:
      pass

  # Admin: Withdraw keys logic
  if context.user_data.get("admin_action") == "withdraw_keys":
      try:
          parts = text.split()
          product, duration, qty = parts[0], parts[1], int(parts[2])
          data = load_data()
          key_name = f"{product}_{duration}"
          keys_pool = data["keys"].get(key_name, [])
          used_keys = set(data.get("used_keys", []))
          available = [k for k in keys_pool if k not in used_keys]
          if len(available) < qty:
              await update.message.reply_text(f"❌ لا يوجد مفاتيح كافية للسحب. المتوفر: {len(available)}" + SIGNATURE,
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
              return
          withdrawn = available[:qty]
          data.setdefault("used_keys", [])
          data["used_keys"] = list(set(data.get("used_keys", [])).union(withdrawn))
          data.setdefault("sold_keys", {}).setdefault(key_name, []).extend(withdrawn)
          save_data(data)
          keys_str = "\n".join([f"`{k}`" for k in withdrawn])
          await update.message.reply_text(f"✅ تم سحب المفاتيح:\n{keys_str}" + SIGNATURE, parse_mode="Markdown",
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          context.user_data.pop("admin_action", None)
      except Exception:
          await update.message.reply_text("❌ صيغة خاطئة. مثال: FREE 7 2" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          context.user_data.pop("admin_action", None)
      return

  # Seller custom buttons
  try:
      DATA_BTN = load_data()
      if uid in DATA_BTN.get("sellers", {}):
          for o in DATA_BTN.get("seller_custom_buttons", []):
              if isinstance(o, dict) and text == o.get("label"):
                  await update.message.reply_text((o.get("response") or "") + SIGNATURE)
                  return
  except Exception:
      pass

  # handle uploaded document for admin file upload or direct send to user
  if update.message.document and context.user_data.get("admin_action") in ("upload_file", "send_file_to_user_upload"):
      if context.user_data.get("admin_action") == "send_file_to_user_upload":
          target_uid = context.user_data.get("target_user")
          if not target_uid:
              await update.message.reply_text("No target user set." + SIGNATURE)
          else:
              try:
                  await context.bot.send_document(chat_id=int(target_uid), document=update.message.document)
                  await update.message.reply_text("✅ File sent to user." + SIGNATURE)
              except Exception as e:
                  await update.message.reply_text("Failed to send file: " + str(e) + SIGNATURE)
          context.user_data.pop("admin_action", None)
          context.user_data.pop("target_user", None)
          return

      prod = context.user_data.get("upload_product")
      if not prod:
          await update.message.reply_text("No product selected for file upload." + SIGNATURE)
      else:
          DATA2 = load_data()
          try:
              files_dir = os.path.join(os.getcwd(), "files")
              os.makedirs(files_dir, exist_ok=True)
              doc = update.message.document
              file_id = doc.file_id
              original_name = getattr(doc, "file_name", None) or f"{prod}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.dat"
              safe_name = original_name.replace("/", "_").replace("\\", "_")
              local_path = os.path.join(files_dir, safe_name)
              tg_file = await context.bot.get_file(file_id)
              await tg_file.download_to_drive(local_path)
              rel_path = os.path.relpath(local_path, os.getcwd())
              DATA2.setdefault("files", {})[prod] = rel_path
              DATA2.setdefault("files_meta", {})[prod] = {"tg_file_id": file_id, "local": rel_path}
              save_data(DATA2)
              await update.message.reply_text(f"File saved for product {prod} to {rel_path}." + SIGNATURE)
          except Exception as e:
              await update.message.reply_text("Failed to save file: " + str(e) + SIGNATURE)
      context.user_data.pop("admin_action", None)
      context.user_data.pop("upload_product", None)
      return

  # VIP: add seller custom option
  if context.user_data.get("admin_action") == "vip_add_seller_option":
      DATA_VIP = load_data()
      raw = text
      if "|" in raw:
          label, response = [p.strip() for p in raw.split("|", 1)]
      else:
          label, response = raw.strip(), raw.strip()
      if not label:
          await update.message.reply_text("❌ الصيغة غير صحيحة." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="vip_menu")]]))
          context.user_data.pop("admin_action", None)
          return
      DATA_VIP.setdefault("seller_custom_buttons", [])
      DATA_VIP["seller_custom_buttons"].append({"label": label, "response": response})
      save_data(DATA_VIP)
      await update.message.reply_text("✅ تم إضافة الخيار بنجاح." + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="vip_menu")]]))
      context.user_data.pop("admin_action", None)
      return

  lang = context.user_data.get("lang", "en")
  button_texts = {
      "buy": {"en": "🛍️ BUY KEYS", "ar": "🛍️ شراء مفاتيح"},
      "balance": {"en": "💰 MY BALANCE", "ar": "💰 رصيدي"},
      "admin": {"en": "🔐 ADMIN PANEL", "ar": "🔐 لوحة الادمن"},
      "activity": {"en": "📊 Activity", "ar": "📊 النشاط"},
      "seller_report": {"en": "📈 Seller Report", "ar": "📈 تقرير البائع"},
      "add_balance": {"en": "💳 Add Balance", "ar": "💳 شحن الرصيد"},
      "payment_history": {"en": "📜 Payment History", "ar": "📜 سجل الشحن"},
      "get_files": {"en": "📁 Get Files", "ar": "📁 الحصول على الملفات"}
  }

  if text == "⬅️ Back":
      context.user_data.clear()
      await start(update, context)
      return

  if context.user_data.get("choose_qty_inline"):
      product = context.user_data.get("inline_qty_product")
      days = context.user_data.get("inline_qty_days")
      try:
          qty = int(text.strip())
          if qty <= 0:
              raise ValueError()
      except Exception:
          await update.message.reply_text("❌ Invalid quantity. Send a positive number." + SIGNATURE)
          return
      context.user_data.pop("choose_qty_inline", None)
      context.user_data.pop("inline_qty_product", None)
      context.user_data.pop("inline_qty_days", None)
      await callback_handler(
          type("obj", (), {
              "callback_query": type("obj", (), {
                  "data": f"pay:{product}:{days}:{qty}",
                  "from_user": update.message.from_user,
                  "message": update.message,
                  "answer": (lambda *args, **kwargs: None),
                  "edit_message_text": update.message.reply_text
              })
          }),
          context
      )
      return


  if text == button_texts["seller_report"]["en"]:
      data = load_data()
      if uid not in data.get("sellers", {}):
          await update.message.reply_text("❌ Seller only." + SIGNATURE)
          return
      await update.message.reply_text(build_seller_activity_message(data, uid) + SIGNATURE)
      return

  if text == button_texts["add_balance"]["en"]:
      data = load_data()
      if uid not in data.get("sellers", {}):
          await update.message.reply_text("❌ Seller only." + SIGNATURE)
          return
      keyboard = [[InlineKeyboardButton(f"${amount}", callback_data=f"topup_amount:{amount}") for amount in TOPUP_AMOUNTS[:2]],
                  [InlineKeyboardButton(f"${amount}", callback_data=f"topup_amount:{amount}") for amount in TOPUP_AMOUNTS[2:]],
                  [InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]
      await update.message.reply_text("Choose top-up amount:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if text == button_texts["payment_history"]["en"]:
      data = load_data()
      if uid not in data.get("sellers", {}):
          await update.message.reply_text("❌ Seller only." + SIGNATURE)
          return
      await update.message.reply_text(build_topup_history_message(data, uid) + SIGNATURE)
      return

  if text == button_texts["balance"][lang]:
      data = load_data()
      bal = data.get("balances", {}).get(uid, 0)
      await update.message.reply_text((f"Your balance: ${bal}" if lang == "en" else f"رصيدك: ${bal}") + SIGNATURE)
      return

  if text == button_texts["admin"][lang]:
      await update.message.reply_text("Enter admin code:" + SIGNATURE)
      context.user_data["awaiting_admin_code"] = True
      return

  if context.user_data.get("awaiting_admin_code"):
      if text == ADMIN_CODE:
          context.user_data.pop("awaiting_admin_code", None)
          try:
              DATA_AD = load_data()
              DATA_AD.setdefault("admins", [])
              if uid not in DATA_AD["admins"]:
                  DATA_AD["admins"].append(uid)
                  save_data(DATA_AD)
          except Exception:
              pass
          await update.message.reply_text("👍 Admin panel access granted." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Main Menu", callback_data="admin_menu")]]))
      else:
          await update.message.reply_text("❌ Wrong code!" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]))
      return

  if text == button_texts["get_files"][lang]:
      DATAF = load_data()
      files = DATAF.get("files", {})
      if uid in DATAF.get("sellers", {}):
          available = list(files.keys())
      else:
          purchases = [s.get("product") for s in DATAF.get("sales_log", []) if s.get("user") == uid]
          available = sorted(set(purchases))
      if not available:
          await update.message.reply_text("No files available for you." + SIGNATURE)
          return
      reply_keyboard = [[p] for p in available]
      reply_keyboard.append(["⬅️ Back"])
      await update.message.reply_text("Select product to download file:" + SIGNATURE,
          reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
      context.user_data["getting_file"] = True
      return

  if context.user_data.get("getting_file"):
      DATAF = load_data()
      sel = None
      for p in DATAF.get("files", {}).keys():
          if text == p or p.lower() in text.lower() or p in text:
              sel = p
              break
      if sel:
          file_ref = DATAF.get("files", {}).get(sel)
          if file_ref:
              try:
                  allowed = False
                  if uid in DATAF.get("sellers", {}):
                      allowed = True
                  else:
                      for s in DATAF.get("sales_log", []):
                          if s.get("user") == uid and s.get("product") == sel:
                              allowed = True
                              break
                  if not allowed:
                      await update.message.reply_text("You don't have access to this file." + SIGNATURE)
                      context.user_data.pop("getting_file", None)
                      return
                  if os.path.exists(file_ref):
                      await update.message.reply_document(document=InputFile(file_ref))
                  else:
                      candidate = file_ref
                      if not os.path.exists(candidate) and os.path.exists(os.path.join(os.getcwd(), candidate)):
                          candidate = os.path.join(os.getcwd(), candidate)
                      if os.path.exists(candidate):
                          await update.message.reply_document(document=InputFile(candidate))
                      else:
                          await update.message.reply_document(document=file_ref)
                  context.user_data.pop("getting_file", None)
                  return
              except Exception as e:
                  await update.message.reply_text("Failed to send file: " + str(e) + SIGNATURE)
                  context.user_data.pop("getting_file", None)
                  return

  if text == button_texts["buy"][lang]:
      PRICES = load_prices()
      product_names = {
          "FREE": {"en": "🔮 FREE FIRE", "ar": "🔮 فري فاير"},
          "WIZARD": {"en": "✨ WIZARD", "ar": "✨ ويزارد"},
          "CERT_IPHONE_WARRANTY": {"en": "📱 iPhone Warranty (30d)", "ar": "📱 ضمان ايفون (شهر)"}
      }
      products = list(product_names.keys())
      buttons = [product_names[p][lang] for p in products]
      reply_keyboard = build_rows(buttons, 2)
      reply_keyboard.append(["⬅️ Back"])
      await update.message.reply_text("Select a product:" + SIGNATURE,
          reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
      context.user_data["buy_menu"] = True
      return

  if context.user_data.get("buy_menu"):
      PRICES = load_prices()
      product_names = {
          "FREE": {"en": "🔮 FREE FIRE", "ar": "🔮 فري فاير"},
          "WIZARD": {"en": "WIZARD", "ar": "ويزارد"},
          "CERT_IPHONE_WARRANTY": {"en": "📱 iPhone Warranty (30d)", "ar": "📱 ضمان ايفون (شهر)"}
      }
      selected_product = None
      lower_text = text.lower()
      for key, names in product_names.items():
          display = names[lang]
          if text == display or display in text or key.lower() in lower_text or key in text:
              selected_product = key
              break
      if selected_product:
          if selected_product == "FREE":
              reply_keyboard = [["📱 iOS"], ["🤖 Android"], ["⬅️ Back"]]
              await update.message.reply_text("Select OS:" + SIGNATURE,
                  reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
              context.user_data["buy_menu"] = False
              context.user_data["choose_free_os"] = True
              return
          await send_duration_menu(update, context, selected_product, lang, "")
          return

  if context.user_data.get("choose_free_os"):
      if "ios" in text.lower():
          reply_keyboard = [["FLUORIT"], ["MIGUL PRO"], ["⬅️ Back"]]
          await update.message.reply_text("Select option:" + SIGNATURE,
              reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
          context.user_data["choose_free_os"] = False
          context.user_data["choose_free_variant"] = True
          context.user_data["free_os"] = "ios"
          return
      if "android" in text.lower():
          reply_keyboard = [["DRIP"], ["DRIP CLIENT ROOT DEVICE"], ["HG CHEAT"], ["PATO TEAM"], ["⬅️ Back"]]
          await update.message.reply_text("Select option:" + SIGNATURE,
              reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
          context.user_data["choose_free_os"] = False
          context.user_data["choose_free_variant"] = True
          context.user_data["free_os"] = "android"
          return

  if context.user_data.get("choose_free_variant"):
      free_os = context.user_data.get("free_os")
      if free_os == "ios":
          if text.upper() == "FLUORIT":
              context.user_data.pop("choose_free_variant", None)
              context.user_data.pop("free_os", None)
              await send_duration_menu(update, context, "FF_IOS_FLUORIT", lang, "")
              return
          if text.upper() in ("MIGUL PRO", "MUGIL PRO"):
              context.user_data.pop("choose_free_variant", None)
              context.user_data.pop("free_os", None)
              await send_duration_menu(update, context, "FF_IOS_MUGIL_PRO", lang, "")
              return
      if free_os == "android":
          if text.upper() == "DRIP":
              context.user_data.pop("choose_free_variant", None)
              context.user_data.pop("free_os", None)
              await send_duration_menu(update, context, "DRIP", lang, "")
              return
          if text.upper() in ("DRIP CLIENT ROOT DEVICE", "DRIP_CLIENT_ROOT_DEVICE"):
              context.user_data.pop("choose_free_variant", None)
              context.user_data.pop("free_os", None)
              await send_duration_menu(update, context, "DRIP_CLIENT_ROOT_DEVICE", lang, "")
              return
          if text.upper() in ("HG CHEAT", "HG CHEATS", "HG_CHEAT"):
              context.user_data.pop("choose_free_variant", None)
              context.user_data.pop("free_os", None)
              await send_duration_menu(update, context, "HG_CHEAT_ANDROID", lang, "")
              return
          if text.upper() in ("PATO TEAM", "PATO_TEAM"):
              context.user_data.pop("choose_free_variant", None)
              context.user_data.pop("free_os", None)
              await send_duration_menu(update, context, "PATO_TEAM", lang, "")
              return

  if context.user_data.get("choose_duration"):
      selected_product = context.user_data.get("selected_product")
      PRICES = load_prices()
      durations = list(PRICES.get("global", {}).get(selected_product, {}).keys())
      duration_names = {
          "1": {"en": "1 day", "ar": "يوم"},
          "3": {"en": "3 days", "ar": "3 أيام"},
          "7": {"en": "7 days", "ar": "7 أيام"},
          "10": {"en": "10 days", "ar": "10 أيام"},
          "15": {"en": "15 days", "ar": "15 يوم"},
          "30": {"en": "30 days", "ar": "30 يوم"},
          "31": {"en": "1 month", "ar": "شهر"},
          "365": {"en": "1 year", "ar": "سنة"}
      }
      selected_duration = None
      for d in durations:
          if d in duration_names and duration_names[d][lang] in text:
              selected_duration = d
              break
      if selected_duration:
          # Expanded quantity options for all products including Fluorit
          qty_buttons = [str(i) for i in range(1, 11)] + ["20", "50", "100", "200"]
          reply_keyboard = build_rows(qty_buttons, 4)
          reply_keyboard.append(["⬅️ Back"])
          await update.message.reply_text("Select quantity:" + SIGNATURE,
              reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
          context.user_data["selected_duration"] = selected_duration
          context.user_data["choose_duration"] = False
          context.user_data["choose_qty"] = True
          return

  if context.user_data.get("choose_qty"):
      try:
          qty = int(text)
          if qty <= 0:
              raise ValueError()
      except Exception:
          await update.message.reply_text("Invalid quantity!" + SIGNATURE)
          return
      selected_product = context.user_data.get("selected_product")
      selected_duration = context.user_data.get("selected_duration")
      
      # Determine bonus keys
      bonus_keys = 0
      if selected_product == "FF_IOS_FLUORIT" and selected_duration == "31":
          if qty >= 20:
              bonus_keys = 4
          elif qty >= 15:
              bonus_keys = 3
          elif qty >= 10:
              bonus_keys = 2
          elif qty >= 5:
              bonus_keys = 1
      
      total_keys_needed = qty + bonus_keys

      PRICES = load_prices()
      DATA = load_data()
      unit_price = PRICES["global"][selected_product][selected_duration] # Assuming standard price lookup
      price = unit_price * qty
      
      try:
          balance = float(DATA["balances"].get(uid, 0))
      except Exception:
          balance = 0
          
      key_name = f"{selected_product}_{selected_duration}" # Matches logic in this block
      keys_pool = DATA["keys"].get(key_name, [])
      used_keys = set(DATA.get("used_keys", []))
      keys_pool = [k for k in keys_pool if k not in used_keys]
      
      if balance < price:
          await update.message.reply_text("❌ Insufficient balance!" + SIGNATURE)
          return
      if len(keys_pool) < total_keys_needed:
          await update.message.reply_text(
              f"❌ Not enough keys available.\nAvailable: {len(keys_pool)}\nRequired including bonus: {total_keys_needed}\nExpected key bucket: {key_name}\nAsk admin to add keys for this exact product/duration." + SIGNATURE
          )
          return
          
      keys = [keys_pool.pop(0) for _ in range(qty)] # The paid keys
      bonus_keys_list = [keys_pool.pop(0) for _ in range(bonus_keys)] # The free keys
      all_keys = keys + bonus_keys_list
      
      DATA["used_keys"] = list(set(DATA.get("used_keys", [])).union(all_keys))
      DATA["keys"][key_name] = keys_pool
      DATA.setdefault("sold_keys", {}).setdefault(key_name, []).extend(all_keys)
      DATA["balances"][uid] = balance - price
      seller_id = uid if uid in DATA.get("sellers", {}) else None
      seller_name = DATA.get("sellers", {}).get(seller_id, {}).get("name", "?") if seller_id else None
      
      # Log paid keys sales
      for k in keys:
          sale_entry = {
              "user": uid,
              "product": selected_product,
              "duration": selected_duration,
              "price": PRICES["global"][selected_product][selected_duration],
              "buyer_balance": balance - price,
              "key": k,
          }
          if seller_id:
              sale_entry["seller_id"] = seller_id
              sale_entry["seller_name"] = seller_name
          DATA["sales_log"].append(sale_entry)

      # Log bonus keys sales (price 0)
      for k in bonus_keys_list:
          sale_entry = {
              "user": uid,
              "product": selected_product,
              "duration": selected_duration,
              "price": 0,
              "buyer_balance": balance - price,
              "key": k,
              "note": "Bonus Key"
          }
          if seller_id:
              sale_entry["seller_id"] = seller_id
              sale_entry["seller_name"] = seller_name
          DATA.setdefault("sales_log", []).append(sale_entry)
          
      admins = DATA.get("admins", [])
      buyer_balance = balance - price
      buyer_username = update.message.from_user.username
      buyer_handle = f"@{buyer_username}" if buyer_username else "(no username)"
      keys_str_admin = "\n".join(all_keys)
      
      for admin_id in admins:
          try:
              if seller_id:
                  text_msg = (
                      f"🔔 تم شراء مفاتيح بواسطة بائع\n"
                      f"المنتج: {selected_product}\n"
                      f"المدة: {selected_duration} يوم\n"
                      f"الكمية المدفوعة: {qty} (Bonus: {bonus_keys})\n"
                      f"المشتري: {buyer_handle} ({uid})\n"
                      f"المفاتيح:\n{keys_str_admin}\n"
                      f"البائع: {seller_name} ({seller_id})\n"
                      f"رصيد المشتري بعد الشراء: ${buyer_balance}"
                  )
              else:
                  text_msg = (
                      f"🔔 تم شراء مفاتيح\n"
                      f"المنتج: {selected_product}\n"
                      f"المدة: {selected_duration} يوم\n"
                      f"الكمية المدفوعة: {qty} (Bonus: {bonus_keys})\n"
                      f"المشتري: {buyer_handle} ({uid})\n"
                      f"المفاتيح:\n{keys_str_admin}\n"
                      f"رصيد المشتري بعد الشراء: ${buyer_balance}"
                  )
              await context.bot.send_message(chat_id=int(admin_id), text=text_msg)
          except Exception:
              pass
      save_data(DATA)
      keys_str = "\n".join([f"`{k}`" for k in all_keys])

      balance_bonus_msg = ""
      if selected_product in ("FF_IOS_FLUORIT", "DRIP") and selected_duration == "31":
          bonus_amount = 0.25 * qty
          DATA.setdefault("balances", {})[uid] = DATA.get("balances", {}).get(uid, 0) + bonus_amount
          save_data(DATA)
          balance_bonus_msg = f"\n\n🎁 Balance Bonus: ${bonus_amount} added to your balance!"

      bonus_keys_msg = ""
      if bonus_keys > 0:
          bonus_keys_msg = f"\n\n🎉 You got {bonus_keys} FREE key(s)!"

      await update.message.reply_text(
          f"✅ Purchase successful!\n\nYour keys:\n{keys_str}{balance_bonus_msg}{bonus_keys_msg}\n\n📁 Use Get Files to download product updates." + SIGNATURE,
          parse_mode="Markdown"
      )
      context.user_data.pop("selected_product", None)
      context.user_data.pop("selected_duration", None)
      context.user_data.pop("choose_qty", None)
      return

  action = context.user_data.get("admin_action")
  if action == "send_file_to_user":
      target_uid = text.strip()
      if not target_uid.isdigit():
          await update.message.reply_text("❌ معرف المستخدم غير صحيح." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      context.user_data["admin_action"] = "send_file_to_user_upload"
      context.user_data["target_user"] = target_uid
      await update.message.reply_text("أرسل الملف الآن (أي نوع ملف)." + SIGNATURE)
      return
  if action == "broadcast":
      data = load_data()
      users = data.get("users", [])
      sent = 0
      failed = 0
      caption = update.message.caption or ""
      caption = (caption + SIGNATURE) if caption else SIGNATURE

      async def _send_to(user_id: str):
          nonlocal sent, failed
          try:
              if update.message.photo:
                  await context.bot.send_photo(chat_id=int(user_id), photo=update.message.photo[-1].file_id, caption=caption)
              elif update.message.video:
                  await context.bot.send_video(chat_id=int(user_id), video=update.message.video.file_id, caption=caption)
                  await context.bot.send_document(chat_id=int(user_id), document=update.message.document.file_id, caption=caption)
              elif update.message.audio:
                  await context.bot.send_audio(chat_id=int(user_id), audio=update.message.audio.file_id, caption=caption)
              elif update.message.voice:
                  await context.bot.send_voice(chat_id=int(user_id), voice=update.message.voice.file_id, caption=caption)
              elif update.message.animation:
                  await context.bot.send_animation(chat_id=int(user_id), animation=update.message.animation.file_id, caption=caption)
              else:
                  await context.bot.send_message(chat_id=int(user_id), text=text + SIGNATURE)
              sent += 1
          except Exception:
              failed += 1

      for u in users:
          await _send_to(u)

      await update.message.reply_text(f"Broadcast complete. Sent: {sent}, Failed: {failed}" + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      context.user_data.pop("admin_action", None)
      return
  if action == "add_balance":
      try:
          parts = text.split()
          target_uid, amount = parts[0], float(parts[1])
          data = load_data()
          data["balances"][target_uid] = data["balances"].get(target_uid, 0) + amount
          save_data(data)
          await update.message.reply_text(f"✅ Added ${amount} to user {target_uid}. Balance: ${data['balances'][target_uid]}" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          context.user_data.pop("admin_action", None)
      except Exception:
          await update.message.reply_text("❌ Wrong format: user_id amount" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return
  if action == "withdraw":
      try:
          parts = text.split()
          target_uid, amount = parts[0], float(parts[1])
          data = load_data()
          if data["balances"].get(target_uid, 0) < amount:
              await update.message.reply_text(f"❌ Insufficient balance! Available: ${data['balances'].get(target_uid, 0)}" + SIGNATURE,
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          else:
              data["balances"][target_uid] -= amount
              save_data(data)
              await update.message.reply_text(f"✅ Withdrawn ${amount} from {target_uid}. Balance: ${data['balances'][target_uid]}" + SIGNATURE,
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
              context.user_data.pop("admin_action", None)
      except Exception:
          await update.message.reply_text("❌ Wrong format: user_id amount" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return
  if action == "add_keys":
      product = context.user_data.get("add_keys_product")
      duration = context.user_data.get("add_keys_duration")
      if not product or not duration:
          await update.message.reply_text("❌ يجب اختيار المنتج والمدة أولاً من لوحة الأدمن." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_add_keys")]]))
          return
      keys = [k.strip() for k in text.split("\n") if k.strip()]
      if not keys:
          await update.message.reply_text("❌ لم يتم إدخال أي مفاتيح." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"admin_add_keys_duration:{product}:{duration}")]]))
          return
      data = load_data()
      key_name = key_storage_name(product, duration)
      data.setdefault("keys", {}).setdefault(key_name, []).extend(keys)
      save_data(data)
      await update.message.reply_text(f"✅ تم إضافة {len(keys)} مفتاح لـ {product} ({duration}يوم)." + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data=f"admin_add_keys_product:{product}")]]))
      context.user_data.clear()
      return
  if action == "add_seller_balance":
      try:
          sid = context.user_data.get("target_seller")
          amount = float(text)
          data = load_data()
          if sid not in data.get("sellers", {}):
              await update.message.reply_text("❌ Seller not found!" + SIGNATURE,
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
          else:
              data["sellers"][sid]["balance"] = data["sellers"][sid].get("balance", 0) + amount
              save_data(data)
              await update.message.reply_text(f"✅ Added ${amount} to seller {sid}. New balance: ${data['sellers'][sid]['balance']}" + SIGNATURE,
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      except Exception:
          await update.message.reply_text("❌ Wrong format: amount" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      context.user_data.pop("admin_action", None)
      context.user_data.pop("target_seller", None)
      return
  if action == "create_key":
      product = context.user_data.get("create_key_product")
      duration = context.user_data.get("create_key_duration")
      key = text.strip()
      data = load_data()
      key_name = key_storage_name(product, duration)
      data.setdefault("keys", {}).setdefault(key_name, []).append(key)
      save_data(data)
      await update.message.reply_text(
          f"✅ تم إنشاء الكيز لـ {product} ({duration}يوم)!\n\nها هو الكيز:\n`{key}`" + SIGNATURE,
          parse_mode="Markdown",
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="admin_create_key_product:"+product)]])
      )
      context.user_data.pop("admin_action", None)
      context.user_data.pop("create_key_product", None)
      context.user_data.pop("create_key_duration", None)
      return
  if action == "set_price":
      product = context.user_data.get("edit_price_product")
      duration = context.user_data.get("edit_price_days")
      seller_id = context.user_data.get("edit_price_seller")
      try:
          price_val = float(text)
          prices = load_prices()
          if seller_id == "global":
              prices.setdefault("global", {}).setdefault(product, {})[duration] = price_val
          else:
              prices.setdefault("sellers", {}).setdefault(seller_id, {}).setdefault(product, {})[duration] = price_val
          save_prices(prices)
          await update.message.reply_text(
              f"✅ تم تعديل السعر: {product} ({duration}يوم) - السعر الجديد لـ {'GLOBAL' if seller_id == 'global' else seller_id}: ${price_val}" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data=f"edit_price:{product}")]])
          )
      except Exception:
          await update.message.reply_text("❌ أدخل رقم صحيح للسعر." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data=f"edit_price:{product}")]]))
      context.user_data.pop("admin_action", None)
      context.user_data.pop("edit_price_product", None)
      context.user_data.pop("edit_price_days", None)
      return
  if action == "add_seller":
      try:
          parts = text.split(maxsplit=1)
          sid, name = parts[0], parts[1] if len(parts) > 1 else "Unknown"
          data = load_data()
          data.setdefault("sellers", {})[sid] = {"name": name, "balance": 0, "sales_count": 0}
          save_data(data)
          await update.message.reply_text(f"✅ Seller '{name}' (ID: {sid}) added." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
          context.user_data.pop("admin_action", None)
      except Exception:
          await update.message.reply_text("❌ Wrong format: seller_id name" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      return
  if action == "remove_seller":
      data = load_data()
      sid = text.strip()
      if sid in data.get("sellers", {}):
          name = data["sellers"][sid]["name"]
          del data["sellers"][sid]
          save_data(data)
          await update.message.reply_text(f"✅ Seller '{name}' (ID: {sid}) removed." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
          context.user_data.pop("admin_action", None)
      else:
          await update.message.reply_text("❌ Seller not found!" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      return

  # Activity (reply keyboard)
  if text == button_texts["activity"][lang]:
      DATA_STAT = load_data()
      uid = str(update.message.from_user.id)
      admins = set(DATA_STAT.get("admins", []))
      sellers_map = DATA_STAT.get("sellers", {})
      is_seller = uid in sellers_map or (uid.isdigit() and int(uid) in sellers_map)
      if uid not in admins and not is_seller:
          msg = build_customer_activity_message(DATA_STAT, uid)
          await update.message.reply_text(msg + SIGNATURE)
          return

      # Seller view
      if is_seller:
          seller = sellers_map.get(uid, {}) if uid in sellers_map else sellers_map.get(int(uid), {})
          balance = seller.get("balance", 0)
          sales = [s for s in DATA_STAT.get("sales_log", []) if str(s.get("seller_id")) == str(uid)]
          sold_count = len(sales)
          by_product = {}
          for s in sales:
              by_product[s.get("product")] = by_product.get(s.get("product"), 0) + 1
          msg = f"📊 Seller Activity ({seller.get('name','Unknown')} - {uid}):\n\nBalance: ${balance}\nKeys sold: {sold_count}\n\nSold by product:\n"
          if by_product:
              for p, c in by_product.items():
                  msg += f"- {p}: {c}\n"
          else:
              msg += "(no sales yet)\n"
          await update.message.reply_text(msg + SIGNATURE)
          return

      # Admin view
      start_clicks = DATA_STAT.get("start_clicks", 0)
      users_count = len(DATA_STAT.get("users", []))
      used_keys = DATA_STAT.get("used_keys", []) or []
      total_keys_sold = len(used_keys)
      total_keys_available = sum(len(v) for v in DATA_STAT.get("keys", {}).values())
      sellers = DATA_STAT.get("sellers", {})
      sellers_count = len(sellers)
      sales_count = len(DATA_STAT.get("sales_log", []))
      sold_by_product = {}
      for s in DATA_STAT.get("sales_log", []):
          sold_by_product[s.get("product")] = sold_by_product.get(s.get("product"), 0) + 1
      msg = f"📊 Bot Activity Summary:\n\nStart clicks: {start_clicks}\nKnown users: {users_count}\nTotal sales entries: {sales_count}\nKeys sold: {total_keys_sold}\nKeys available: {total_keys_available}\nSellers: {sellers_count}\n\nSold by product:\n"
      for prod, cnt in sold_by_product.items():
          msg += f"- {prod}: {cnt}\n"
      msg += "\nPer-seller breakdown:\n"
      if sellers:
          for sid, info in sellers.items():
              s_sales = [s for s in DATA_STAT.get("sales_log", []) if s.get("seller_id") == sid]
              s_count = len(s_sales)
              prod_counts = {}
              for s in s_sales:
                  prod_counts[s.get("product")] = prod_counts.get(s.get("product"), 0) + 1
              msg += f"- {info.get('name','?')} ({sid}) — Balance: ${info.get('balance',0)} — Keys sold: {s_count}\n"
              if prod_counts:
                  for p, c in prod_counts.items():
                      msg += f"    • {p}: {c}\n"
      else:
          msg += "(no sellers configured)\n"
      await update.message.reply_text(msg + SIGNATURE)
      return

  # Main menu: BUY
  if text == button_texts["buy"][lang]:
      PRICES = load_prices()
      product_names = {
          "FREE": {"en": "🔮 FREE FIRE", "ar": "🔮 فري فاير", "fr": "🔮 FREE FIRE", "es": "🔮 FREE FIRE", "de": "🔮 FREE FIRE", "tr": "🔮 FREE FIRE"},
          "WIZARD": {"en": "✨ WIZARD", "ar": "✨ ويزارد", "fr": "✨ WIZARD", "es": "✨ WIZARD", "de": "✨ WIZARD", "tr": "✨ WIZARD"},
          "CLOUD": {"en": "☁️ CLOUD", "ar": "☁️ كلاود", "fr": "☁️ CLOUD", "es": "☁️ CLOUD", "de": "☁️ CLOUD", "tr": "☁️ CLOUD"},
          "CODM_IOS": {"en": "📱 CODM IOS", "ar": "📱 كودم IOS", "fr": "📱 CODM IOS", "es": "📱 CODM IOS", "de": "📱 CODM IOS", "tr": "📱 CODM IOS"},
          "TERMINAL_X_PC": {"en": "💻 TERMINAL X PC", "ar": "💻 تيرمنال X PC", "fr": "💻 TERMINAL X PC", "es": "💻 TERMINAL X PC", "de": "💻 TERMINAL X PC", "tr": "💻 TERMINAL X PC"},
          "HG_CHEATS_ROOT": {"en": "🛡️ HG CHEATS ROOT", "ar": "🛡️ HG شيتس روت", "fr": "🛡️ HG CHEATS ROOT", "es": "🛡️ HG CHEATS ROOT", "de": "🛡️ HG CHEATS ROOT", "tr": "🛡️ HG CHEATS ROOT"},
          "CERT_IPHONE_WARRANTY": {
              "en": "📱 iPhone Warranty (30d)",
              "ar": "📱 ضمان ايفون (شهر)",
              "fr": "📱 iPhone Warranty (30d)",
              "es": "📱 iPhone Warranty (30d)",
              "de": "📱 iPhone Warranty (30d)",
              "tr": "📱 iPhone Warranty (30d)"
          }
      }
      products = list(product_names.keys())
      buttons = [product_names[p][lang] for p in products]
      reply_keyboard = build_rows(buttons, 2)
      # if user is a seller, add Get Files button
      DATA_CHECK = load_data()
      uid = str(update.message.from_user.id)
      if uid in DATA_CHECK.get("sellers", {}):
          reply_keyboard.append([button_texts["get_files"][lang]])
      reply_keyboard.append(["⬅️ Back"])
      await update.message.reply_text(
          "Select a product:" if lang == "en" else
          "اختر المنتج:" if lang == "ar" else
          "Sélectionnez un produit:" if lang == "fr" else
          "Seleccione un producto:" if lang == "es" else
          "Produkt auswählen:" if lang == "de" else
          "Bir ürün seçin:",
          reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)
      )
      context.user_data["buy_menu"] = True
      return

  # Main menu: BALANCE
  if text == button_texts["balance"][lang]:
      user_id = str(update.message.from_user.id)
      data = load_data()
      bal = data["balances"].get(user_id, 0)
      msg = {
          "en": f"Your balance: ${bal}",
          "ar": f"رصيدك: ${bal}",
          "fr": f"Votre solde: ${bal}",
          "es": f"Su saldo: ${bal}",
          "de": f"Ihr Guthaben: ${bal}",
          "tr": f"Bakiyeniz: ${bal}"
      }[lang]
      await update.message.reply_text(msg + SIGNATURE)
      return

  # Main menu: ADMIN (await code)
  if text == button_texts["admin"][lang]:
      msg = {
          "en": "Enter admin code:",
          "ar": "ادخل رمز الادمن:",
          "fr": "Entrez le code admin:",
          "es": "Ingrese el código de administrador:",
          "de": "Admin-Code eingeben:",
          "tr": "Yönetici kodunu girin:"
      }[lang]
      for k in ("buy_menu", "choose_duration", "choose_qty", "getting_file", "upload_product", "admin_action"):
          context.user_data.pop(k, None)
      await update.message.reply_text(msg + SIGNATURE)
      context.user_data["awaiting_admin_code"] = True
      return


  if context.user_data.get("awaiting_admin_code"):
      if text == ADMIN_CODE:
          context.user_data.pop("awaiting_admin_code", None)
          # persist this user as an admin
          try:
              DATA_AD = load_data()
              uid = str(update.message.from_user.id)
              DATA_AD.setdefault("admins", [])
              if uid not in DATA_AD["admins"]:
                  DATA_AD["admins"].append(uid)
                  save_data(DATA_AD)
          except Exception:
              pass
          context.user_data["is_admin"] = True
          await update.message.reply_text("👍 Admin panel access granted." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([
              [InlineKeyboardButton("Main Menu", callback_data="admin_menu")]]))
      else:
          await update.message.reply_text("❌ Wrong code!" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([
                  [InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]
              ]))
      return

  action = context.user_data.get("admin_action")
  # Handle broadcast, seller balance add from admin callback and other admin actions
  if action == "send_file_to_user":
      target_uid = text.strip()
      if not target_uid.isdigit():
          await update.message.reply_text("❌ معرف المستخدم غير صحيح." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      context.user_data["admin_action"] = "send_file_to_user_upload"
      context.user_data["target_user"] = target_uid
      await update.message.reply_text("أرسل الملف الآن (أي نوع ملف)." + SIGNATURE)
      return
  if action == "broadcast":
      text_to_send = text
      data = load_data()
      users = data.get("users", [])
      sent = 0
      failed = 0
      for u in users:
          try:
              await context.bot.send_message(chat_id=int(u), text=text_to_send + SIGNATURE)
              sent += 1
          except Exception:
              failed += 1
      await update.message.reply_text(f"Broadcast complete. Sent: {sent}, Failed: {failed}" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      context.user_data.pop("admin_action", None)
      return
  if action == "add_balance":
      try:
          parts = text.split()
          uid, amount = parts[0], float(parts[1])
          data = load_data()
          data["balances"][uid] = data["balances"].get(uid, 0) + amount
          save_data(data)
          await update.message.reply_text(f"✅ Added ${amount} to user {uid}. Balance: ${data['balances'][uid]}" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          context.user_data.pop("admin_action", None)
      except:
          await update.message.reply_text("❌ Wrong format: user_id amount" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return
  if action == "withdraw":
      try:
          parts = text.split()
          uid, amount = parts[0], float(parts[1])
          data = load_data()
          if data["balances"].get(uid, 0) < amount:
              await update.message.reply_text(f"❌ Insufficient balance! Available: ${data['balances'].get(uid, 0)}" + SIGNATURE,
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          else:
              data["balances"][uid] -= amount
              save_data(data)
              await update.message.reply_text(f"✅ Withdrawn ${amount} from {uid}. Balance: ${data['balances'][uid]}" + SIGNATURE,
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
              context.user_data.pop("admin_action", None)
      except:
          await update.message.reply_text("❌ Wrong format: user_id amount" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return
  if action == "add_keys":
      product = context.user_data.get("add_keys_product")
      duration = context.user_data.get("add_keys_duration")
      if not product or not duration:
          await update.message.reply_text("❌ يجب اختيار المنتج والمدة أولاً من لوحة الأدمن." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_add_keys")]]))
          return
      keys = [k.strip() for k in text.split("\n") if k.strip()]
      if not keys:
          await update.message.reply_text("❌ لم يتم إدخال أي مفاتيح. أرسل المفاتيح (كل مفتاح في سطر)." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"admin_add_keys_duration:{product}:{duration}")]]))
          return
      data = load_data()
      key_name = f"{product}_{duration}"
      if key_name not in data["keys"]:
          data["keys"][key_name] = []
      data["keys"][key_name].extend(keys)
      save_data(data)
      await update.message.reply_text(f"✅ تم إضافة {len(keys)} مفتاح لـ {product} ({duration}يوم)." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data=f"admin_add_keys_product:{product}")]]))
      context.user_data.clear()
      return
  if action == "add_seller_balance":
      try:
          sid = context.user_data.get("target_seller")
          amount = float(text)
          data = load_data()
          if sid not in data.get("sellers", {}):
              await update.message.reply_text("❌ Seller not found!" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
          else:
              data["sellers"][sid]["balance"] = data["sellers"][sid].get("balance", 0) + amount
              save_data(data)
              await update.message.reply_text(f"✅ Added ${amount} to seller {sid}. New balance: ${data['sellers'][sid]['balance']}" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      except Exception:
          await update.message.reply_text("❌ Wrong format: amount" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      context.user_data.pop("admin_action", None)
      context.user_data.pop("target_seller", None)
      return
  if action == "create_key":
      product = context.user_data.get("create_key_product")
      duration = context.user_data.get("create_key_duration")
      key = text.strip()
      data = load_data()
      key_name = key_storage_name(product, duration)
      if key_name not in data["keys"]:
          data["keys"][key_name] = []
      data["keys"][key_name].append(key)
      save_data(data)
      await update.message.reply_text(
          f"✅ تم إنشاء الكيز لـ {product} ({duration}يوم)!\n\nها هو الكيز:\n`{key}`" + SIGNATURE, parse_mode="Markdown",
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="admin_create_key_product:"+product)]]))
      context.user_data.pop("admin_action", None)
      context.user_data.pop("create_key_product", None)
      context.user_data.pop("create_key_duration", None)
      return
  if action == "set_price":
      product = context.user_data.get("edit_price_product")
      duration = context.user_data.get("edit_price_days")
      seller_id = context.user_data.get("edit_price_seller")
      try:
          price_val = float(text)
          prices = load_prices()
          if seller_id == "global":
              prices.setdefault("global", {}).setdefault(product, {})[duration] = price_val
          else:
              if seller_id not in prices["sellers"]:
                  prices["sellers"][seller_id] = {}
              if product not in prices["sellers"][seller_id]:
                  prices["sellers"][seller_id][product] = {}
              prices["sellers"][seller_id][product][duration] = price_val
          save_prices(prices)
          await update.message.reply_text(
              f"✅ تم تعديل السعر: {product} ({duration}يوم) - السعر الجديد لـ {'GLOBAL' if seller_id == 'global' else seller_id}: ${price_val}" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="admin_menu")]]))
      except Exception as e:
          await update.message.reply_text("❌ أدخل رقم صحيح للسعر." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="admin_menu")]]))
      context.user_data.clear()
      return
  
  if action == "edit_price_enter_seller":
      seller_id = text.strip().lower()
      data = load_data()
      # Validate seller ID if not global
      if seller_id != "global" and seller_id not in data.get("sellers", {}):
          await update.message.reply_text(f"❌ Seller ID '{seller_id}' not found. Please enter a valid seller ID or 'global'." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      # Store seller ID and show products
      context.user_data["edit_price_seller"] = seller_id
      context.user_data["admin_action"] = "edit_price_choose_product"
      PRICES = load_prices()
      keyboard = [[InlineKeyboardButton(prod, callback_data=f"edit_price_prod:{prod}")] for prod in PRICES.get("global", {}).keys()]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      seller_name = data.get("sellers", {}).get(seller_id, {}).get("name", seller_id) if seller_id != "global" else "GLOBAL"
      await update.message.reply_text(f"Select product to edit prices for {seller_name}:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if action == "edit_price_choose_duration":
      duration = text.strip()
      product = context.user_data.get("edit_price_product")
      seller_id = context.user_data.get("edit_price_seller")
      # Validate duration
      PRICES = load_prices()
      valid_durations = list(PRICES.get("global", {}).get(product, {}).keys())
      if duration not in valid_durations:
          await update.message.reply_text(f"❌ Invalid duration. Valid options: {', '.join(valid_durations)}" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      context.user_data["edit_price_days"] = duration
      context.user_data["admin_action"] = "set_price"
      current_price = get_price_for_user(PRICES, product, duration, seller_id, load_data()) if seller_id != "global" else PRICES.get("global", {}).get(product, {}).get(duration, "N/A")
      seller_name = load_data().get("sellers", {}).get(seller_id, {}).get("name", seller_id) if seller_id != "global" else "GLOBAL"
      await update.message.reply_text(
          f"Product: {product}\nDuration: {duration} days\nSeller: {seller_name}\nCurrent price: ${current_price}\n\nEnter new price:" + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if action == "add_seller":
      try:
          parts = text.split(maxsplit=1)
          sid, name = parts[0], parts[1] if len(parts) > 1 else "Unknown"
          data = load_data()
          data["sellers"][sid] = {"name": name, "balance": 0, "sales_count": 0}
          save_data(data)
          await update.message.reply_text(f"✅ Seller '{name}' (ID: {sid}) added." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
          context.user_data.pop("admin_action", None)
      except:
          await update.message.reply_text("❌ Wrong format: seller_id name" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      return
  if action == "remove_seller":
      data = load_data()
      sid = text.strip()
      if sid in data["sellers"]:
          name = data["sellers"][sid]["name"]
          del data["sellers"][sid]
          save_data(data)
          await update.message.reply_text(f"✅ Seller '{name}' (ID: {sid}) removed." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
          context.user_data.pop("admin_action", None)
      else:
          await update.message.reply_text("❌ Seller not found!" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      return
  if action == "seller_balance":
      try:
          parts = text.split()
          sid, amount = parts[0], float(parts[1])
          data = load_data()
          if sid not in data["sellers"]:
              await update.message.reply_text(f"❌ Seller {sid} not found!" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
          else:
              data["sellers"][sid]["balance"] = data["sellers"][sid].get("balance", 0) + amount
              save_data(data)
              name = data["sellers"][sid]["name"]
              await update.message.reply_text(f"✅ Added ${amount} to seller {name}. Balance: ${data['sellers'][sid]['balance']}" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
              context.user_data.pop("admin_action", None)
      except:
          await update.message.reply_text("❌ Wrong format: seller_id amount" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      return

# ========== MAIN ==========
def main():
    if not TOKEN or TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("Error: Set your TELEGRAM_BOT_TOKEN in the code!")
        return
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    print("Bot running...")
    try:
        # drop_pending_updates can help in some cases where old updates block flow
        app.run_polling(drop_pending_updates=True)
    except telegram.error.Conflict as e:
        print("Failed to start bot: another getUpdates poller is active for this token.")
        print("telegram.error.Conflict:", e)
        print("Make sure no other bot instance is running (other machines, containers, or processes).")
        print("On Windows you can list Python processes in PowerShell: Get-CimInstance Win32_Process | Where-Object { $_.Name -like 'python*' } | Select-Object ProcessId, CommandLine")
        return
    except Exception as e:
        print("Failed to start bot:", e)
        return

if __name__ == "__main__":
    main()
"""

from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, InputFile
import telegram
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
try:
  import requests
except Exception:
  requests = None
try:
  import psycopg as pg
except Exception:
  try:
      import psycopg2 as pg
  except Exception:
      pg = None

TOKEN = os.getenv("BOT_TOKEN", "8428090354:AAF-skw72MrdGssNTCjQLGSLQQt3utgPzP0")
ADMIN_CODE = "123123NNK"
SIGNATURE = "\n\n© @FAKHERDDIN5"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)
DATA_FILE = os.path.join(DATA_DIR, "bot_data.json")
DB_FILE = os.path.join(DATA_DIR, "bot_data.sqlite3")
PRICES_FILE = os.path.join(BASE_DIR, "prices.json")
SELLER_CHANNEL_URL = "https://t.me/stonexff"
DEFAULT_LANG = "en"
PG_DATABASE_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("POSTGRES_URL")
    or os.getenv("POSTGRES_DSN")
    or "postgres://koyeb-adm:npg_n1az7LoGXBZY@ep-jolly-fog-agc79icr.c-2.eu-central-1.pg.koyeb.app/koyebdb"
)
FIRSTONE_BOT_TOKEN = os.getenv("FIRSTONE_BOT_TOKEN", "")


def _get_db_connection():
  conn = sqlite3.connect(DB_FILE)
  conn.execute("PRAGMA journal_mode=WAL;")
  conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
  return conn


def _get_pg_connection():
  if not PG_DATABASE_URL or not pg:
      return None
  conn = pg.connect(PG_DATABASE_URL)
  with conn.cursor() as cur:
      cur.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
  conn.commit()
  return conn


def _load_data_from_sqlite():
  try:
      conn = _get_db_connection()
      cur = conn.execute("SELECT value FROM kv WHERE key='data'")
      row = cur.fetchone()
      conn.close()
      if row and row[0]:
          return json.loads(row[0])
  except Exception:
      return None
  return None


def _load_data_from_postgres():
  try:
      conn = _get_pg_connection()
      if not conn:
          return None
      with conn.cursor() as cur:
          cur.execute("SELECT value FROM kv WHERE key='data'")
          row = cur.fetchone()
      conn.close()
      if row and row[0]:
          return json.loads(row[0])
  except Exception:
      return None
  return None


def _save_data_to_sqlite(data):
  try:
      payload = json.dumps(data, ensure_ascii=False)
      conn = _get_db_connection()
      conn.execute("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", ("data", payload))
      conn.commit()
      conn.close()
      return True
  except Exception:
      return False


def _save_data_to_postgres(data):
  try:
      conn = _get_pg_connection()
      if not conn:
          return False
      payload = json.dumps(data, ensure_ascii=False)
      with conn.cursor() as cur:
          cur.execute("INSERT INTO kv (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", ("data", payload))
      conn.commit()
      conn.close()
      return True
  except Exception:
      return False


def _load_data_from_storage():
  if PG_DATABASE_URL and pg:
      data = _load_data_from_postgres()
      if isinstance(data, dict):
          return data
  return _load_data_from_sqlite()


def _save_data_to_storage(data):
  if PG_DATABASE_URL and pg:
      return _save_data_to_postgres(data)
  return _save_data_to_sqlite(data)


def load_data():
  db_data = _load_data_from_storage()
  if isinstance(db_data, dict):
      return db_data
  try:
      with open(DATA_FILE, "r", encoding="utf-8") as f:
          data = json.load(f)
          _save_data_to_storage(data)
          return data
  except json.JSONDecodeError:
      try:
          corrupt_path = DATA_FILE + ".corrupt." + datetime.utcnow().strftime("%Y%m%d%H%M%S")
          os.replace(DATA_FILE, corrupt_path)
      except Exception:
          pass
  except Exception:
      pass
  default_data = {
      "balances": {},
      "sellers": {},
      "keys": {},
      "users": [],
      "files": {},
      "sales_log": [],
      "start_clicks": 0,
      "used_keys": [],
      "sold_keys": {},
      "seller_custom_buttons": [],
      "approved_users": [],
      "pending_users": [],
      "pending_payments": {},
      "admins": ["7210704553"],
      "stopped_sellers": [],
  }
  _save_data_to_storage(default_data)
  return default_data


def save_data(data):
  _save_data_to_storage(data)
  tmp_path = DATA_FILE + ".tmp"
  with open(tmp_path, "w", encoding="utf-8") as f:
      json.dump(data, f, indent=2, ensure_ascii=False)
  os.replace(tmp_path, DATA_FILE)


def load_prices():
  defaults = {
      "global": {
          "FREE": {"1": 3, "7": 7, "31": 13},
          "WIZARD": {"1": 3, "7": 7, "31": 13},
          "DRIP": {"1": 1, "7": 2, "15": 4, "31": 5},
          "FF_IOS_FLUORIT": {"1": 2, "7": 8, "31": 13},
          "FF_IOS_MUGIL_PRO": {"31": 10.5},
          "HG_CHEAT_ANDROID": {"1": 2, "10": 3, "30": 6},
          "DRIP_CLIENT_ROOT_DEVICE": {"1": 1, "7": 3, "30": 6},
          "PATO_TEAM": {"3": 1, "7": 2, "15": 2.5, "30": 5},
          "CERT_IPHONE_WARRANTY": {"30": 3}
      },
      "sellers": {}
  }
  try:
      with open(PRICES_FILE, "r", encoding="utf-8") as f:
          prices = json.load(f)
  except json.JSONDecodeError:
      try:
          corrupt_path = PRICES_FILE + ".corrupt." + datetime.utcnow().strftime("%Y%m%d%H%M%S")
          os.replace(PRICES_FILE, corrupt_path)
      except Exception:
          pass
      save_prices(defaults)
      return defaults
  except Exception:
      save_prices(defaults)
      return defaults

  if "global" not in prices:
      prices["global"] = {}
  if "sellers" not in prices:
      prices["sellers"] = {}

  changed = False
  for prod, durs in defaults["global"].items():
      if prod not in prices["global"]:
          prices["global"][prod] = durs
          changed = True
      else:
          for dur, val in durs.items():
              if str(dur) not in prices["global"][prod]:
                  prices["global"][prod][str(dur)] = val
                  changed = True

  desired_month_prices = {
      "FF_IOS_FLUORIT": {"1": 2, "7": 8, "31": 13},
      "FF_IOS_MUGIL_PRO": {"31": 10.5},
      "HG_CHEAT_ANDROID": {"1": 2, "10": 3, "30": 6},
      "DRIP": {"31": 5},
      "DRIP_CLIENT_ROOT_DEVICE": {"1": 1, "7": 3, "30": 6},
      "PATO_TEAM": {"3": 1, "7": 2, "15": 2.5, "30": 5}
  }
  for prod, durs in desired_month_prices.items():
      prices.setdefault("global", {}).setdefault(prod, {})
      for dur, val in durs.items():
          if str(prices["global"][prod].get(str(dur))) != str(val):
              prices["global"][prod][str(dur)] = val
              changed = True

  if changed:
      save_prices(prices)
  return prices


def save_prices(prices):
  tmp_path = PRICES_FILE + ".tmp"
  with open(tmp_path, "w", encoding="utf-8") as f:
      json.dump(prices, f, indent=2, ensure_ascii=False)
  os.replace(tmp_path, PRICES_FILE)


def get_price_for_user(prices, product, duration, user_id, data):
  """Get price for a user, checking seller-specific prices first"""
  # Check if user is a seller and has custom pricing
  if user_id in data.get("sellers", {}):
      seller_prices = prices.get("sellers", {}).get(user_id, {})
      if product in seller_prices and str(duration) in seller_prices[product]:
          return seller_prices[product][str(duration)]
  # Fallback to global pricing
  return prices.get("global", {}).get(product, {}).get(str(duration))


def key_storage_name(prod, dur):
  return f"{prod}_{dur}"


def build_rows(items, row_size=2):
  rows = []
  for i in range(0, len(items), row_size):
      rows.append(items[i:i + row_size])
  return rows


def build_seller_options_keyboard(data):
  rows = []
  for btn in data.get("seller_custom_buttons", []):
      label = btn.get("label") if isinstance(btn, dict) else None
      if label:
          rows.append([label])
  return rows


def _parse_sale_datetime(value):
  if not value:
      return None
  try:
      return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
  except Exception:
      pass
  for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
      try:
          return datetime.strptime(str(value), fmt)
      except Exception:
          pass
  return None


def _format_sale_line(sale: dict):
  product = sale.get("product", "?")
  duration = sale.get("duration", "?")
  price = sale.get("price", 0)
  purchased = sale.get("timestamp", "?")
  expires = sale.get("expires", "?")
  buyer = sale.get("user", "?")
  note = sale.get("note")
  line = f"- {product} | {duration}d | ${price} | buyer: {buyer}\n  buy: {purchased}\n  expire: {expires}"
  if note:
      line += f"\n  note: {note}"
  return line


def build_seller_activity_message(data: dict, uid: str):
  sellers_map = data.get("sellers", {})
  seller = sellers_map.get(uid, {}) if uid in sellers_map else sellers_map.get(int(uid), {})
  balance = seller.get("balance", 0)
  sales = [s for s in data.get("sales_log", []) if str(s.get("seller_id")) == str(uid)]
  now_dt = datetime.utcnow()
  week_ago = now_dt - timedelta(days=7)
  month_start = now_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

  week_revenue = 0.0
  month_revenue = 0.0
  total_revenue = 0.0
  by_product = {}
  detailed_lines = []

  sales_sorted = sorted(sales, key=lambda s: _parse_sale_datetime(s.get("timestamp")) or datetime.min, reverse=True)
  for sale in sales_sorted:
      product = sale.get("product", "?")
      by_product[product] = by_product.get(product, 0) + 1
      try:
          price = float(sale.get("price", 0) or 0)
      except Exception:
          price = 0.0
      total_revenue += price
      sale_dt = _parse_sale_datetime(sale.get("timestamp"))
      if sale_dt:
          if sale_dt >= week_ago:
              week_revenue += price
          if sale_dt >= month_start:
              month_revenue += price
      detailed_lines.append(_format_sale_line(sale))

  msg = (
      f"📊 Seller Activity ({seller.get('name','Unknown')} - {uid}):\n\n"
      f"Balance: ${balance}\n"
      f"Keys sold: {len(sales)}\n"
      f"Total return: ${round(total_revenue, 2)}\n"
      f"Return this week: ${round(week_revenue, 2)}\n"
      f"Return this month: ${round(month_revenue, 2)}\n\n"
      f"Sold by product:\n"
  )

  if by_product:
      for p, c in by_product.items():
          msg += f"- {p}: {c}\n"
  else:
      msg += "(no sales yet)\n"

  msg += "\nRecent sales details:\n"
  if detailed_lines:
      for line in detailed_lines[:10]:
          msg += line + "\n"
      if len(detailed_lines) > 10:
          msg += f"\n... and {len(detailed_lines) - 10} more sales\n"
  else:
      msg += "(no sales yet)\n"

  return msg


def build_customer_activity_message(data: dict, uid: str):
  balance = data.get("balances", {}).get(uid, 0)
  purchases = [s for s in data.get("sales_log", []) if s.get("user") == uid]
  total = len(purchases)
  by_product = {}
  for s in purchases:
      prod = s.get("product")
      if prod:
          by_product[prod] = by_product.get(prod, 0) + 1
  msg = f"📊 Your Activity:\n\nBalance: ${balance}\nPurchases: {total}\n\nPurchases by product:\n"
  if by_product:
      for p, c in by_product.items():
          msg += f"- {p}: {c}\n"
  else:
      msg += "(no purchases yet)\n"
  return msg


def _utc_iso_now():
  return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(value, default=0.0):
  try:
      return float(value)
  except Exception:
      return default


def telegram_notify(chat_id, text):
  if not TOKEN or TOKEN.startswith("PASTE_") or not requests:
      return
  try:
      requests.post(
          f"https://api.telegram.org/bot{TOKEN}/sendMessage",
          json={"chat_id": int(chat_id), "text": text},
          timeout=20,
      )
  except Exception:
      pass


def create_nowpayments_payment(seller_id: str, amount_usd: float, pay_currency: str):
  if not requests:
      raise RuntimeError("requests package is not available")
  if not NOWPAYMENTS_API_KEY or NOWPAYMENTS_API_KEY.startswith("PASTE_"):
      raise RuntimeError("NOWPAYMENTS_API_KEY is missing")
  order_id = f"topup_{seller_id}_{uuid.uuid4().hex[:10]}"
  payload = {
      "price_amount": float(amount_usd),
      "price_currency": "usd",
      "pay_currency": str(pay_currency).lower(),
      "order_id": order_id,
      "order_description": f"Seller wallet topup for {seller_id}",
      "ipn_callback_url": NOWPAYMENTS_IPN_URL,
  }
  response = requests.post(
      f"{NOWPAYMENTS_BASE_URL}/payment",
      json=payload,
      headers={"x-api-key": NOWPAYMENTS_API_KEY, "Content-Type": "application/json"},
      timeout=30,
  )
  response.raise_for_status()
  data = response.json()

  stored = {
      "topup_id": str(data.get("payment_id") or ""),
      "seller_id": str(seller_id),
      "amount_usd": float(amount_usd),
      "pay_currency": str(pay_currency).lower(),
      "pay_amount": data.get("pay_amount"),
      "pay_address": data.get("pay_address"),
      "order_id": order_id,
      "payment_status": data.get("payment_status", "waiting"),
      "created_at": _utc_iso_now(),
      "credited": False,
      "network": data.get("network"),
      "price_currency": "usd",
      "raw": data,
  }
  data_store = load_data()
  data_store.setdefault("topups", []).append(stored)
  save_data(data_store)
  return stored


def verify_nowpayments_signature(raw_body: bytes, received_sig: str) -> bool:
  if not NOWPAYMENTS_IPN_SECRET or NOWPAYMENTS_IPN_SECRET.startswith("PASTE_"):
      return False
  try:
      expected_sig = hmac.new(
          NOWPAYMENTS_IPN_SECRET.encode("utf-8"),
          raw_body,
          hashlib.sha512
      ).hexdigest()
      return hmac.compare_digest(expected_sig, received_sig or "")
  except Exception:
      return False


def apply_topup_from_ipn(ipn_data: dict):
  payment_id = str(ipn_data.get("payment_id") or "")
  order_id = str(ipn_data.get("order_id") or "")
  payment_status = str(ipn_data.get("payment_status") or "").lower()

  data_store = load_data()
  data_store.setdefault("topups", [])
  data_store.setdefault("processed_payments", [])
  data_store.setdefault("balances", {})

  matched = None
  for topup in data_store["topups"]:
      if str(topup.get("topup_id") or "") == payment_id or str(topup.get("order_id") or "") == order_id:
          matched = topup
          break

  if not matched:
      return None, None, "not_found"

  matched["payment_status"] = payment_status
  matched["updated_at"] = _utc_iso_now()
  matched["ipn_last"] = ipn_data

  if payment_status == "finished":
      if payment_id and payment_id in data_store["processed_payments"]:
          save_data(data_store)
          return matched.get("seller_id"), matched.get("amount_usd"), "already_credited"

      if not matched.get("credited"):
          seller_id = str(matched.get("seller_id"))
          amount = _safe_float(matched.get("amount_usd"), 0.0)
          data_store["balances"][seller_id] = round(_safe_float(data_store["balances"].get(seller_id), 0.0) + amount, 2)
          try:
              if seller_id in data_store.get("sellers", {}):
                  data_store["sellers"][seller_id]["balance"] = data_store["balances"][seller_id]
          except Exception:
              pass
          matched["credited"] = True
          matched["credited_at"] = _utc_iso_now()
          if payment_id:
              data_store["processed_payments"].append(payment_id)
          save_data(data_store)
          return seller_id, amount, "credited"

  save_data(data_store)
  return matched.get("seller_id"), matched.get("amount_usd"), payment_status or "updated"


def build_topup_history_message(data: dict, uid: str):
  items = [t for t in data.get("topups", []) if str(t.get("seller_id")) == str(uid)]
  items = sorted(items, key=lambda x: str(x.get("created_at", "")), reverse=True)
  lines = [f"💳 Top-up History ({uid})", ""]
  if not items:
      lines.append("No top-ups yet.")
      return "\n".join(lines)
  for item in items[:10]:
      lines.append(
          f"- ${item.get('amount_usd')} | {str(item.get('pay_currency', '')).upper()} | {item.get('payment_status', 'waiting')}\n"
          f"  created: {item.get('created_at', '?')}\n"
          f"  payment id: {item.get('topup_id', '?')}"
      )
  if len(items) > 10:
      lines.append(f"\n... and {len(items) - 10} more")
  return "\n".join(lines)


@app.post("/nowpayments/ipn")
def nowpayments_ipn():
  raw_body = request.get_data()
  signature = request.headers.get("x-nowpayments-sig", "")
  if not verify_nowpayments_signature(raw_body, signature):
      return jsonify({"ok": False, "error": "bad signature"}), 403

  payload = request.get_json(silent=True) or {}
  seller_id, amount, result = apply_topup_from_ipn(payload)
  if result == "credited" and seller_id:
      data_store = load_data()
      new_balance = data_store.get("balances", {}).get(str(seller_id), 0)
      telegram_notify(seller_id, f"✅ Crypto top-up confirmed.\nAmount: ${amount}\nNew balance: ${new_balance}")
  return jsonify({"ok": True, "result": result})


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
  lang = context.user_data.get("lang")
  if not lang:
      lang = DEFAULT_LANG
      context.user_data["lang"] = lang
  button_texts = {
      "buy": {"en": "🛍️ BUY KEYS", "ar": "🛍️ شراء مفاتيح"},
      "balance": {"en": "💰 MY BALANCE", "ar": "💰 رصيدي"},
      "admin": {"en": "🔐 ADMIN PANEL", "ar": "🔐 لوحة الادمن"},
      "about": {"en": "ℹ️ ABOUT", "ar": "ℹ️ حول"},
      "lang": {"en": "🌐 Change Language", "ar": "🌐 تغيير اللغة"},
      "activity": {"en": "📊 Activity", "ar": "📊 النشاط"},
      "seller_report": {"en": "📈 Seller Report", "ar": "📈 تقرير البائع"},
      "get_files": {"en": "📁 Get Files", "ar": "📁 الحصول على الملفات"},
      "cert": {"en": "🔐 Get Certificate", "ar": "🔐 جلب الشهادة"},
      "my_cert_keys": {"en": "🗝️ My Cert Keys", "ar": "🗝️ مفاتيح الشهادة"},
      "api_balance": {"en": "💰 Check API Balance", "ar": "💰 فحص رصيد API"}
  }
  uid = None
  try:
      if getattr(update, "message", None):
          uid = str(update.message.from_user.id)
      elif getattr(update, "callback_query", None):
          uid = str(update.callback_query.from_user.id)
  except Exception:
      uid = None

  DATA_START = load_data()
  admins = set(DATA_START.get("admins", []))
  approved = set(DATA_START.get("approved_users", []))
  if uid and uid in DATA_START.get("sellers", {}):
      approved.add(uid)
      DATA_START.setdefault("approved_users", [])
      if uid not in DATA_START["approved_users"]:
          DATA_START["approved_users"].append(uid)

  if uid and uid not in admins and uid not in approved:
      DATA_START.setdefault("pending_users", [])
      if uid not in DATA_START["pending_users"]:
          DATA_START["pending_users"].append(uid)
      save_data(DATA_START)
      try:
          for admin_id in admins:
              await context.bot.send_message(
                  chat_id=int(admin_id),
                  text=f"طلب وصول جديد من المستخدم: {uid}",
                  reply_markup=InlineKeyboardMarkup([
                      [InlineKeyboardButton("✅ Accept", callback_data=f"admin_approve_user:{uid}")]
                  ])
              )
      except Exception:
          pass
      pending_text = "⏳ Your access request is pending admin approval." if lang == "en" else "⏳ طلبك قيد المراجعة من الأدمن."
      if getattr(update, "message", None):
          await update.message.reply_text(pending_text + SIGNATURE)
      elif getattr(update, "callback_query", None):
          await update.callback_query.message.reply_text(pending_text + SIGNATURE)
      return

  try:
      if uid and uid in DATA_START.get("sellers", {}):
          lang = "en"
          context.user_data["lang"] = "en"
  except Exception:
      pass

  sellers_map = DATA_START.get("sellers", {})
  is_seller = False
  try:
      if uid in sellers_map:
          is_seller = True
      elif uid and uid.isdigit() and int(uid) in sellers_map:
          is_seller = True
  except Exception:
      pass

  menu_items = [button_texts["buy"][lang], button_texts["balance"][lang], button_texts["activity"][lang]]
  if uid in admins:
      menu_items.append(button_texts["admin"][lang])
  reply_keyboard = build_rows(menu_items, 2)

  try:
      if is_seller:
          reply_keyboard.append([button_texts["seller_report"]["en"]])
          reply_keyboard.append([button_texts["add_balance"]["en"], button_texts["payment_history"]["en"]])
          for btn in DATA_START.get("seller_custom_buttons", []):
              label = btn.get("label") if isinstance(btn, dict) else None
              if label:
                  reply_keyboard.append([label])
  except Exception:
      pass

  reply_keyboard.append([button_texts["get_files"][lang]])
  reply_keyboard.append(["⬅️ Back"])

  text = "Welcome to the Sales Bot! Please choose an option:" if lang == "en" else "مرحباً بك في بوت المبيعات! اختر خياراً:"
  if getattr(update, "message", None):
      try:
          DATA_START["start_clicks"] = DATA_START.get("start_clicks", 0) + 1
          save_data(DATA_START)
      except Exception:
          pass
      await update.message.reply_text(text + SIGNATURE, reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
      try:
          uid_str = uid
          if uid_str and uid_str not in DATA_START.get("sellers", {}) and uid_str not in DATA_START.get("seller_promo_shown", []):
              promo = "Become a seller — deposit just $30 one time to start selling. Join our channel for details:"
              kb = InlineKeyboardMarkup([[InlineKeyboardButton("Join Channel", url=SELLER_CHANNEL_URL), InlineKeyboardButton("Dismiss", callback_data="dismiss_seller_promo")]])
              await update.message.reply_text(promo + SIGNATURE, reply_markup=kb)
              DATA_START.setdefault("seller_promo_shown", []).append(uid_str)
              save_data(DATA_START)
      except Exception:
          pass
  elif getattr(update, "callback_query", None):
      try:
          DATA_START["start_clicks"] = DATA_START.get("start_clicks", 0) + 1
          save_data(DATA_START)
      except Exception:
          pass
      await update.callback_query.message.reply_text(text + SIGNATURE, reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
      try:
          uid_str = uid
          if uid_str and uid_str not in DATA_START.get("sellers", {}) and uid_str not in DATA_START.get("seller_promo_shown", []):
              promo = "Become a seller — deposit just $30 one time to start selling. Join our channel for details:"
              kb = InlineKeyboardMarkup([[InlineKeyboardButton("Join Channel", url=SELLER_CHANNEL_URL), InlineKeyboardButton("Dismiss", callback_data="dismiss_seller_promo")]])
              await update.callback_query.message.reply_text(promo + SIGNATURE, reply_markup=kb)
              DATA_START.setdefault("seller_promo_shown", []).append(uid_str)
              save_data(DATA_START)
      except Exception:
          pass


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
  query = update.callback_query
  try:
      await query.answer()
  except telegram.error.BadRequest as e:
      if "Query is too old" in str(e) or "query id is invalid" in str(e):
          return
      print(f"CallbackQuery answer error: {e}")
      return
  except Exception as e:
      print(f"CallbackQuery answer unexpected error: {e}")
      return

  data = getattr(query, "data", None)
  DATA = load_data()
  PRICES = load_prices()
  uid = str(query.from_user.id)
  admins = set(DATA.get("admins", []))
  approved = set(DATA.get("approved_users", []))

  try:
      if uid in DATA.get("sellers", {}):
          context.user_data["lang"] = "en"
  except Exception:
      pass

  if uid not in admins and uid not in approved:
      await query.edit_message_text("⏳ Your access request is pending admin approval." + SIGNATURE)
      return
  if data and data.startswith("admin_") and uid not in admins:
      await query.edit_message_text("❌ Permission denied." + SIGNATURE)
      return

  if data == "back_to_start":
      lang = context.user_data.get("lang", "en")
      menu_text = "Welcome to the Sales Bot! Please choose an option:" if lang == "en" else "مرحباً بك في بوت المبيعات! اختر خياراً:"
      menu_items = ["🛍️ BUY KEYS" if lang == "en" else "🛍️ شراء مفاتيح", "💰 MY BALANCE" if lang == "en" else "💰 رصيدي", "📊 Activity" if lang == "en" else "📊 النشاط"]
      if uid in admins:
          menu_items.append("🔐 ADMIN PANEL" if lang == "en" else "🔐 لوحة الادمن")
      reply_keyboard = build_rows(menu_items, 2)
      try:
          if uid in DATA.get("sellers", {}):
              lang = "en"
              menu_text = "Welcome to the Sales Bot! Please choose an option:"
              reply_keyboard.append(["📈 Seller Report"])
              reply_keyboard.append(["💳 Add Balance", "📜 Payment History"])
              for btn in DATA.get("seller_custom_buttons", []):
                  label = btn.get("label") if isinstance(btn, dict) else None
                  if label:
                      reply_keyboard.append([label])
      except Exception:
          pass
      reply_keyboard.append(["📁 Get Files" if lang == "en" else "📁 الحصول على الملفات"])
      reply_keyboard.append(["⬅️ Back"])
      lang_val = context.user_data.get("lang")
      context.user_data.clear()
      if lang_val:
          context.user_data["lang"] = lang_val
      await query.edit_message_text(menu_text + SIGNATURE)
      await query.message.reply_text(menu_text + SIGNATURE, reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
      return

  if data == "admin_menu":
      if uid not in admins:
          await query.edit_message_text("❌ Permission denied." + SIGNATURE)
          return
      admin_keyboard = [
          [InlineKeyboardButton("🕹️ Controle", callback_data="admin_control")],
          [InlineKeyboardButton("💳 Add Balance", callback_data="admin_add_balance")],
          [InlineKeyboardButton("💸 Withdraw", callback_data="admin_withdraw")],
          [InlineKeyboardButton("🔑 Add Keys", callback_data="admin_add_keys")],
          [InlineKeyboardButton("💲 Edit Seller Prices", callback_data="admin_edit_prices")],
          [InlineKeyboardButton("🌐 Edit Global Prices", callback_data="admin_global_prices")],
          [InlineKeyboardButton("📥 Upload Product File", callback_data="admin_upload_file")],
          [InlineKeyboardButton("📤 Send File to User", callback_data="admin_send_file_to_user")],
          [InlineKeyboardButton("➕ Add Seller", callback_data="admin_add_seller_cb")],
          [InlineKeyboardButton("➖ Remove Seller", callback_data="admin_remove_seller_cb")],
          [InlineKeyboardButton("📋 List Sellers", callback_data="admin_list_sellers")],
          [InlineKeyboardButton("👤 Manage Sellers", callback_data="admin_sellers")],
          [InlineKeyboardButton("🔑 Available Keys", callback_data="admin_available_keys")],
          [InlineKeyboardButton("📢 Broadcast Message", callback_data="admin_broadcast")],
          [InlineKeyboardButton("💾 Export Backup", callback_data="admin_export_backup")],
          [InlineKeyboardButton("✅ Pending Users", callback_data="admin_pending_users")],
          [InlineKeyboardButton("📝 آخر عمليات الشراء", callback_data="admin_last_sales")],
          [InlineKeyboardButton("📊 Activity", callback_data="show_activity")],
          [InlineKeyboardButton("🧾 جميع أرصدة اللاعبين", callback_data="admin_all_balances")],
          [InlineKeyboardButton("🗝️ سحب المفاتيح", callback_data="admin_withdraw_keys")],
          [InlineKeyboardButton("🗂️ جميع المفاتيح والعوائد", callback_data="admin_keys_revenue")],
          [InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]
      ]
      await query.edit_message_text("قائمة الأدمن:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(admin_keyboard))
      return

  if data == "admin_export_backup":
      try:
          await context.bot.send_document(chat_id=query.from_user.id, document=InputFile(DATA_FILE), filename="bot_data.json")
          await context.bot.send_document(chat_id=query.from_user.id, document=InputFile(PRICES_FILE), filename="prices.json")
          await query.edit_message_text("✅ تم إرسال النسخة الاحتياطية." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      except Exception as e:
          await query.edit_message_text("❌ فشل إرسال النسخة الاحتياطية: " + str(e) + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_pending_users":
      pending = DATA.get("pending_users", [])
      if not pending:
          await query.edit_message_text("لا توجد طلبات معلقة." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      keyboard = []
      for pid in pending[:50]:
          keyboard.append([InlineKeyboardButton(f"✅ Accept {pid}", callback_data=f"admin_approve_user:{pid}")])
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text("طلبات الانتظار:", reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_approve_user:"):
      _, target = data.split(":", 1)
      DATA.setdefault("approved_users", [])
      DATA.setdefault("pending_users", [])
      if target not in DATA["approved_users"]:
          DATA["approved_users"].append(target)
      if target in DATA["pending_users"]:
          DATA["pending_users"].remove(target)
      save_data(DATA)
      try:
          await context.bot.send_message(chat_id=int(target), text="✅ Your access has been approved." + SIGNATURE)
      except Exception:
          pass
      await query.edit_message_text(f"✅ Approved {target}." + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_all_balances":
      balances = DATA.get("balances", {})
      users = DATA.get("users", [])
      msg = "🧾 جميع أرصدة اللاعبين:\n\n"
      for uid_item in users:
          bal = balances.get(uid_item, 0)
          msg += f"• {uid_item}: ${bal}\n"
      await query.edit_message_text(msg + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "buy_menu":
      lang = context.user_data.get("lang", "en")
      product_names = {
          "FREE": {"en": "🔮 FREE FIRE", "ar": "🔮 فري فاير"},
          "WIZARD": {"en": "✨ WIZARD", "ar": "✨ ويزارد"},
          "CLOUD": {"en": "☁️ CLOUD", "ar": "☁️ كلاود"},
          "CODM_IOS": {"en": "📱 CODM IOS", "ar": "📱 كودم IOS"},
          "TERMINAL_X_PC": {"en": "💻 TERMINAL X PC", "ar": "💻 تيرمنال X PC"},
          "HG_CHEATS_ROOT": {"en": "🛡️ HG CHEATS ROOT", "ar": "🛡️ HG شيتس روت"},
          "CERT_IPHONE_WARRANTY": {"en": "📱 iPhone Warranty (30d)", "ar": "📱 ضمان ايفون (شهر)"}
      }
      buttons = [product_names[p][lang] for p in product_names]
      reply_keyboard = build_rows(buttons, 2)
      reply_keyboard.append(["⬅️ Back"])
      context.user_data["buy_menu"] = True
      await query.edit_message_text("اختر المنتج:" + SIGNATURE)
      await query.message.reply_text("اختر المنتج:" + SIGNATURE, reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
      return

  if data == "show_balance":
      user_id = str(query.from_user.id)
      bal = DATA.get("balances", {}).get(user_id, 0)
      keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]
      await query.edit_message_text(f"Your balance: ${bal}" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data == "show_activity":
      DATA_STAT = load_data()
      uid = str(query.from_user.id)
      admins = set(DATA_STAT.get("admins", []))
      sellers_map = DATA_STAT.get("sellers", {})
      is_seller = uid in sellers_map or (uid.isdigit() and int(uid) in sellers_map)
      if uid not in admins and not is_seller:
          msg = build_customer_activity_message(DATA_STAT, uid)
          await query.edit_message_text(msg + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]))
          return

      if is_seller:
          msg = build_seller_activity_message(DATA_STAT, uid)
          keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]
          await query.edit_message_text(msg + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
          return

      start_clicks = DATA_STAT.get("start_clicks", 0)
      users_count = len(DATA_STAT.get("users", []))
      used_keys = DATA_STAT.get("used_keys", []) or []
      total_keys_sold = len(used_keys)
      total_keys_available = sum(len(v) for v in DATA_STAT.get("keys", {}).values())
      sellers = DATA_STAT.get("sellers", {})
      sellers_count = len(sellers)
      sales_count = len(DATA_STAT.get("sales_log", []))
      sold_by_product = {}
      for s in DATA_STAT.get("sales_log", []):
          sold_by_product[s.get("product")] = sold_by_product.get(s.get("product"), 0) + 1
      msg = f"📊 Bot Activity Summary:\n\nStart clicks: {start_clicks}\nKnown users: {users_count}\nTotal sales entries: {sales_count}\nKeys sold: {total_keys_sold}\nKeys available: {total_keys_available}\nSellers: {sellers_count}\n\nSold by product:\n"
      for prod, cnt in sold_by_product.items():
          msg += f"- {prod}: {cnt}\n"
      msg += "\nPer-seller breakdown:\n"
      if sellers:
          for sid, info in sellers.items():
              s_sales = [s for s in DATA_STAT.get("sales_log", []) if s.get("seller_id") == sid]
              s_count = len(s_sales)
              prod_counts = {}
              for s in s_sales:
                  prod_counts[s.get("product")] = prod_counts.get(s.get("product"), 0) + 1
              msg += f"- {info.get('name','?')} ({sid}) — Balance: ${info.get('balance',0)} — Keys sold: {s_count}\n"
              if prod_counts:
                  for p, c in prod_counts.items():
                      msg += f"    • {p}: {c}\n"
      else:
          msg += "(no sellers configured)\n"
      keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]
      await query.edit_message_text(msg + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data == "dismiss_seller_promo":
      try:
          await query.edit_message_text("Promo dismissed." + SIGNATURE)
      except Exception:
          pass
      return

  if data and data.startswith("buy:"):
      product = data.split(":", 1)[1]
      user_id = str(query.from_user.id)
      show_prices = user_id in DATA.get("balances", {})
      prod_prices = PRICES.get("global", {}).get(product)
      if not prod_prices:
          await query.edit_message_text(f"⚠️ No pricing/config found for {product}. Ask the admin to set prices or add keys." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="buy_menu")]]))
          return
      keyboard = []
      for days in sorted(prod_prices.keys(), key=lambda x: int(x)):
          # Get user-specific or global price
          price = get_price_for_user(PRICES, product, days, user_id, DATA)
          btn_text = f"⏱️ {days}يوم - ${price}" if show_prices else f"⏱️ {days}يوم"
          keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"choose_qty:{product}:{days}")])
      keyboard.append([InlineKeyboardButton("⬅️ رجوع", callback_data="buy_menu")])
      if show_prices:
          await query.edit_message_text(f"اختر المدة للمنتج {product}:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      else:
          await query.edit_message_text(f"اختر المدة للمنتج {product}:\n(سيتم عرض الأسعار بعد إضافتك من قبل الأدمن)" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("choose_qty:"):
      _, product, days = data.split(":", 2)
      qty_keyboard = [[InlineKeyboardButton(str(i), callback_data=f"pay:{product}:{days}:{i}")] for i in range(1, 11)]
      qty_keyboard += [[InlineKeyboardButton(str(i), callback_data=f"pay:{product}:{days}:{i}")] for i in [20, 50, 100, 200, 500]]
      qty_keyboard.append([InlineKeyboardButton("✍️ Custom Quantity", callback_data=f"choose_qty_custom:{product}:{days}")])
      qty_keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"buy:{product}")])
      await query.edit_message_text(f"Select quantity for {product} ({days} days):" + SIGNATURE, reply_markup=InlineKeyboardMarkup(qty_keyboard))
      return

  if data and data.startswith("choose_qty_custom:"):
      _, product, days = data.split(":", 2)
      context.user_data["selected_product"] = product
      context.user_data["selected_duration"] = days
      context.user_data["choose_duration"] = False
      context.user_data["choose_qty"] = True
      await query.edit_message_text(
          f"Send quantity for {product} ({days} days). Any positive number is allowed:" + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"buy:{product}")]])
      )
      return

  if data and data.startswith("pay:"):
      parts = data.split(":")
      product = parts[1]
      days = parts[2]
      qty = int(parts[3]) if len(parts) > 3 else 1
      if qty <= 0:
          await query.edit_message_text(
              "❌ Invalid quantity." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"buy:{product}")]])
          )
          return
      unit_price = PRICES.get("global", {}).get(product, {}).get(days)
      if unit_price is None:
          await query.edit_message_text(
              f"⚠️ Pricing not configured for {product} {days} days." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="buy_menu")]])
          )
          return

      bonus_keys = 0
      if product == "FF_IOS_FLUORIT" and days == "31":
          if qty >= 16:
              bonus_keys = 4
          elif qty >= 12:
              bonus_keys = 3
          elif qty >= 8:
              bonus_keys = 2
          elif qty >= 3:
              bonus_keys = 1

      total_keys_needed = qty + bonus_keys
      price = unit_price * qty
      user_id = str(query.from_user.id)
      try:
          balance = float(DATA.get("balances", {}).get(user_id, 0))
      except Exception:
          balance = 0
      key_name = key_storage_name(product, days)
      keys_pool = [k for k in DATA.get("keys", {}).get(key_name, []) if k not in set(DATA.get("used_keys", []))]
      if balance < price:
          await query.edit_message_text(
              f"❌ Insufficient balance!\nPrice: ${price}\nYour balance: ${balance}" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="buy_menu")]])
          )
          return
      if len(keys_pool) < total_keys_needed:
          await query.edit_message_text(
              f"❌ Not enough keys available for {product} - {days} days.\nAvailable: {len(keys_pool)}\nRequired including bonus: {total_keys_needed}\nExpected key bucket: {key_name}\nAsk admin to add keys for this exact product/duration." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="buy_menu")]])
          )
          return
      keys = [keys_pool.pop(0) for _ in range(qty)]
      bonus_keys_list = [keys_pool.pop(0) for _ in range(bonus_keys)]
      all_keys = keys + bonus_keys_list
      DATA.setdefault("used_keys", [])
      DATA["used_keys"] = list(set(DATA.get("used_keys", [])).union(all_keys))
      DATA.setdefault("keys", {})[key_name] = keys_pool
      DATA.setdefault("sold_keys", {}).setdefault(key_name, []).extend(all_keys)
      DATA.setdefault("balances", {})[user_id] = DATA.get("balances", {}).get(user_id, 0) - price
      
      bonus_msg = ""
      if product in ("FF_IOS_FLUORIT", "DRIP") and days == "31":
          bonus = 0.25 * qty
          DATA["balances"][user_id] = DATA["balances"].get(user_id, 0) + bonus
          bonus_msg = f"\n\n🎁 Bonus! You received ${bonus} added to your balance for buying a 1-month key."
          
      seller_id = str(query.from_user.id) if str(query.from_user.id) in DATA.get("sellers", {}) else None
      seller_name = DATA.get("sellers", {}).get(seller_id, {}).get("name", "?") if seller_id else None
      # Track monthly sales for price reduction
      now_dt = datetime.utcnow()
      month_key = now_dt.strftime('%Y-%m')
      if seller_id and days in ["30", "31"]:
          DATA.setdefault("monthly_sales", {})
          DATA["monthly_sales"].setdefault(seller_id, {})
          DATA["monthly_sales"][seller_id].setdefault(month_key, 0)
          DATA["monthly_sales"][seller_id][month_key] += len(keys)
      for k in keys:
          try:
              expires_at = (now_dt + timedelta(days=int(days))).isoformat(sep=" ")
          except Exception:
              expires_at = "Unknown"
          sale_entry = {"user": user_id, "product": product, "duration": days, "price": unit_price, "key": k, "timestamp": now_dt.isoformat(sep=" "), "expires": expires_at}
          if seller_id:
              sale_entry["seller_id"] = seller_id
              sale_entry["seller_name"] = seller_name
          DATA.setdefault("sales_log", []).append(sale_entry)
      for k in bonus_keys_list:
          try:
              bonus_expires_at = (now_dt + timedelta(days=int(days))).isoformat(sep=" ")
          except Exception:
              bonus_expires_at = "Unknown"
          sale_entry = {"user": user_id, "product": product, "duration": days, "price": 0, "key": k, "note": "Bonus Key", "timestamp": now_dt.isoformat(sep=" "), "expires": bonus_expires_at}
          if seller_id:
              sale_entry["seller_id"] = seller_id
              sale_entry["seller_name"] = seller_name
          DATA.setdefault("sales_log", []).append(sale_entry)
      admins = DATA.get("admins", [])
      seller_balance = DATA.get("balances", {}).get(seller_id, 0) if seller_id else None
      buyer_username = query.from_user.username
      buyer_handle = f"@{buyer_username}" if buyer_username else "(no username)"
      keys_str_admin = "\n".join(all_keys)
      for admin_id in admins:
          try:
              if seller_id:
                  msg_text = (
                      f"🔔 تم بيع مفاتيح!\n"
                      f"المنتج: {product}\n"
                      f"المدة: {days} يوم\n"
                      f"الكمية المدفوعة: {qty} (Bonus: {bonus_keys})\n"
                      f"المشتري: {buyer_handle} ({user_id})\n"
                      f"المفاتيح:\n{keys_str_admin}\n"
                      f"البائع: {seller_name} ({seller_id})\n"
                      f"الرصيد الحالي للبائع: ${seller_balance}"
                  )
              else:
                  msg_text = (
                      f"🔔 تم شراء مفاتيح\n"
                      f"المنتج: {product}\n"
                      f"المدة: {days} يوم\n"
                      f"الكمية المدفوعة: {qty} (Bonus: {bonus_keys})\n"
                      f"المشتري: {buyer_handle} ({user_id})\n"
                      f"المفاتيح:\n{keys_str_admin}"
                  )
              await context.bot.send_message(chat_id=int(admin_id), text=msg_text)
          except Exception:
              pass
      save_data(DATA)
      # Build detailed key info for user
      detailed_keys = []
      for k in all_keys:
          entry = next((s for s in DATA.get("sales_log", []) if s.get("key") == k), {})
          duration = entry.get("duration", "?")
          expires = entry.get("expires", "?")
          purchased = entry.get("timestamp", "?")
          key_name = k
          line = f"`{key_name}` | مدة: {duration} يوم | تم الشراء: {purchased}"
          if expires != "?":
              line += f" | ينتهي: {expires}"
          # Certifica bot link
          if product.lower().startswith("cert"):
              line += f" | استخدم المفتاح في: @Next_ibot"
          detailed_keys.append(line)
      keys_str = "\n".join(detailed_keys)
      bonus_keys_msg = f"\n\n🎉 Bonus keys: {bonus_keys}" if bonus_keys > 0 else ""
      await query.edit_message_text(
          f"✅ تم الشراء بنجاح!\n\nمفاتيحك:\n{keys_str}{bonus_keys_msg}\n\n📁 استخدم Get Files لتحميل تحديثات المنتج." + bonus_msg + SIGNATURE,
          parse_mode="Markdown",
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="buy_menu")]])
      )
      return

  if data == "admin_add_keys":
      keyboard = [[InlineKeyboardButton(prod, callback_data=f"admin_add_keys_product:{prod}")] for prod in PRICES.get("global", {}).keys()]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text("اختر المنتج لإضافة المفاتيح:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_add_keys_product:"):
      _, prod = data.split(":", 1)
      durations = list(PRICES.get("global", {}).get(prod, {}).keys())
      if not durations:
          await query.edit_message_text(
              f"لا يوجد مدد معرفة لهذا المنتج {prod}. أضف مدد أولاً من الأسعار." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_add_keys")]])
          )
          return
      keyboard = [[InlineKeyboardButton(f"{d} يوم", callback_data=f"admin_add_keys_duration:{prod}:{d}")] for d in durations]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_add_keys")])
      await query.edit_message_text(
          f"اختر المدة لإضافة المفاتيح لـ {prod}:" + SIGNATURE,
          reply_markup=InlineKeyboardMarkup(keyboard)
      )
      return

  if data and data.startswith("admin_add_keys_duration:"):
      _, prod, days = data.split(":", 2)
      context.user_data.clear()
      context.user_data["admin_action"] = "add_keys"
      context.user_data["add_keys_product"] = prod
      context.user_data["add_keys_duration"] = days
      await query.edit_message_text(
          f"أرسل المفاتيح (كل مفتاح في سطر) ليتم إضافتها للمنتج {prod} لمدة {days} يوم." + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_add_keys")]])
      )
      return

  if data == "vip_menu":
      vip_keyboard = [
          [InlineKeyboardButton("➕ إضافة خيار للبائعين", callback_data="vip_add_seller_option")],
          [InlineKeyboardButton("📋 عرض خيارات البائعين", callback_data="vip_list_seller_options")],
          [InlineKeyboardButton("🗑️ حذف كل الخيارات", callback_data="vip_clear_seller_options")],
          [InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]
      ]
      await query.edit_message_text("⭐ لوحة VIP:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(vip_keyboard))
      return

  if data == "vip_add_seller_option":
      context.user_data.clear()
      context.user_data["admin_action"] = "vip_add_seller_option"
      await query.edit_message_text(
          "أرسل النص بهذا الشكل:\nالعنوان | الرد\nمثال: أسعار جديدة | تواصل مع الأدمن" + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="vip_menu")]])
      )
      return

  if data == "vip_list_seller_options":
      opts = DATA.get("seller_custom_buttons", [])
      if not opts:
          await query.edit_message_text(
              "لا توجد خيارات مضافة." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="vip_menu")]])
          )
          return
      msg = "خيارات البائعين الحالية:\n\n"
      for o in opts:
          if isinstance(o, dict):
              msg += f"• {o.get('label','?')} → {o.get('response','')}\n"
      await query.edit_message_text(
          msg + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="vip_menu")]])
      )
      return

  if data == "vip_clear_seller_options":
      DATA["seller_custom_buttons"] = []
      save_data(DATA)
      await query.edit_message_text(
          "تم حذف جميع الخيارات." + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="vip_menu")]])
      )
      return

  if data == "admin_download_keys":
      import tempfile
      keys_data = DATA.get("keys", {})
      used_keys = set(DATA.get("used_keys", []))
      lines = []
      for prod_dur, keys in keys_data.items():
          available = [k for k in keys if k not in used_keys]
          lines.append(f"{prod_dur}:")
          if available:
              for k in available:
                  lines.append(f"- {k}")
          else:
              lines.append("(لا يوجد مفاتيح متاحة)")
          lines.append("")
      content = "\n".join(lines)
      with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", suffix="_keys.txt") as tf:
          tf.write(content)
          temp_path = tf.name
      await context.bot.send_document(chat_id=query.from_user.id, document=InputFile(temp_path), filename="all_keys.txt", caption="جميع المفاتيح المتوفرة")
      try:
          os.remove(temp_path)
      except Exception:
          pass
      await query.edit_message_text("تم إرسال ملف جميع المفاتيح المتوفرة لك." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_view_keys":
      keys_data = DATA.get("keys", {})
      used_keys = set(DATA.get("used_keys", []))
      if not keys_data:
          await query.edit_message_text("لا توجد أي مفاتيح متوفرة حالياً." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      msg = "🔍 جميع المفاتيح المتوفرة:\n\n"
      for prod_dur, keys in keys_data.items():
          available = [k for k in keys if k not in used_keys]
          msg += f"{prod_dur}:\n"
          if available:
              for k in available:
                  msg += f"- `{k}`\n"
          else:
              msg += "(لا يوجد مفاتيح متاحة)\n"
          msg += "\n"
      await query.edit_message_text(msg + SIGNATURE, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_available_keys":
      keys_data = DATA.get("keys", {})
      used_keys = set(DATA.get("used_keys", []))
      if not keys_data:
          await query.edit_message_text("لا توجد أي مفاتيح متوفرة حالياً." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      msg = "🔑 Available keys count:\n\n"
      total_available = 0
      for prod_dur, keys in keys_data.items():
          available = [k for k in keys if k not in used_keys]
          count = len(available)
          total_available += count
          msg += f"• {prod_dur}: {count} keys \n"
          
      msg += f"\nTotal available: {total_available}"
      if len(msg) > 4000:
          msg = msg[:4000] + "\n... [Message Truncated]"

      # Removed parse_mode="Markdown" to prevent markdown parsing errors with unescaped characters like underscores
      try:
          await query.edit_message_text(msg + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      except Exception as e:
          await query.edit_message_text(f"Error displaying keys: {e}" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_list_sellers":
      sellers = DATA.get("sellers", {})
      if not sellers:
          await query.edit_message_text("No sellers configured." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      try:
          msg = "📋 Sellers list:\n\n"
          sales_log = DATA.get("sales_log", [])
          for sid, info in sellers.items():
              if not isinstance(info, dict):
                  continue
              seller_balance = info.get("balance", 0)
              wallet_balance = DATA.get("balances", {}).get(str(sid), 0)
              if wallet_balance != seller_balance:
                  msg += f"- {info.get('name','?')} ({sid}) — Balance: ${seller_balance} | Wallet: ${wallet_balance}\n"
              else:
                  msg += f"- {info.get('name','?')} ({sid}) — Balance: ${seller_balance}\n"
              seller_sales = [s for s in sales_log if isinstance(s, dict) and str(s.get("seller_id")) == str(sid)]
              if not seller_sales:
                  msg += "Keys: (none)\n\n"
                  continue
              msg += "Keys by product:\n"
              by_product = {}
              for s in seller_sales:
                  product = s.get("product", "?")
                  duration = s.get("duration", "?")
                  key = s.get("key")
                  duration_str = str(duration)
                  if duration_str in {"30", "31"}:
                      dur_label = "month"
                  elif duration_str == "7":
                      dur_label = "week"
                  elif duration_str == "1":
                      dur_label = "day"
                  else:
                      dur_label = f"{duration_str} days"
                  label = f"{product} {dur_label}"
                  by_product.setdefault(label, [])
                  if key:
                      by_product[label].append(key)
              for label, keys in by_product.items():
                  if not keys:
                      msg += f"- {label}: (no keys stored)\n"
                  else:
                      msg += f"- {label}: {len(keys)} keys sold\n"
              msg += "\n"
              
          if len(msg) > 4000:
              msg = msg[:4000] + "\n... [Message Truncated]"

          # Removed parse_mode="Markdown" to avoid parsing errors with unescaped characters in labels/names
          await query.edit_message_text(
              text=msg + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]])
          )
      except Exception as e:
          await query.edit_message_text(
              text=f"Error displaying sellers: {e}\nTry clearing some logs." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]])
          )
      return

  if data == "admin_last_sales":
      sales = DATA.get("sales_log", [])[-20:]
      if not sales:
          await query.edit_message_text("No sales yet." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      msg = "📝 Last purchases:\n\n"
      for s in sales:
          buyer = s.get("user")
          product = s.get("product")
          duration = s.get("duration")
          price = s.get("price")
          msg += f"- {buyer} | {product} | {duration} days | ${price}\n"
      await query.edit_message_text(msg + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_keys_revenue":
      sales = DATA.get("sales_log", [])
      if not sales:
          await query.edit_message_text("No sales yet." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      total_revenue = 0
      by_product = {}
      for s in sales:
          product = s.get("product")
          price = s.get("price")
          by_product.setdefault(product, {"count": 0, "revenue": 0})
          by_product[product]["count"] += 1
          try:
              by_product[product]["revenue"] += float(price)
              total_revenue += float(price)
          except Exception:
              pass
      msg = "🗂️ Keys & revenue:\n\n"
      for product, info in by_product.items():
          msg += f"- {product}: {info['count']} keys — ${info['revenue']}\n"
      msg += f"\nTotal revenue: ${total_revenue}"
      await query.edit_message_text(msg + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_sales_report":
      sales = DATA.get("sales_log", [])
      if not sales:
          await query.edit_message_text("لا توجد مبيعات." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      
      now = datetime.utcnow()
      week_ago = now - timedelta(days=7)
      month_ago = now - timedelta(days=30)
      
      stats = {"week": {"count": 0, "rev": 0}, "month": {"count": 0, "rev": 0}, "all": {"count": 0, "rev": 0}}
      seller_stats = {}
      
      for s in sales:
          ts_str = s.get("timestamp")
          s_time = None
          try:
              if ts_str:
                  s_time = datetime.fromisoformat(ts_str)
          except Exception:
              pass
              
          price = 0
          try: price = float(s.get("price", 0))
          except Exception: pass
          
          stats["all"]["count"] += 1
          stats["all"]["rev"] += price
          
          if s_time:
              if s_time >= week_ago:
                  stats["week"]["count"] += 1
                  stats["week"]["rev"] += price
              if s_time >= month_ago:
                  stats["month"]["count"] += 1
                  stats["month"]["rev"] += price
          else:
              # If no timestamp, count it in all but not week/month strictly (or assume it's old)
              pass
              
          sid = s.get("seller_id", "Direct")
          seller_name = s.get("seller_name", "Bot")
          if sid not in seller_stats:
              seller_stats[sid] = {"name": seller_name, "count": 0, "rev": 0}
          seller_stats[sid]["count"] += 1
          seller_stats[sid]["rev"] += price
          
      msg = "📊 Sales Report:\n\n"
      msg += f"🗓️ Last 7 Days:\nKeys sold: {stats['week']['count']} | Rev: ${stats['week']['rev']}\n\n"
      msg += f"📅 Last 30 Days:\nKeys sold: {stats['month']['count']} | Rev: ${stats['month']['rev']}\n\n"
      msg += f"♾️ All Time:\nKeys sold: {stats['all']['count']} | Rev: ${stats['all']['rev']}\n\n"
      msg += "👥 Sellers Information:\n"
      for sid, info in seller_stats.items():
          # Calculate how many keys this seller sold
          msg += f"- {info['name']} ({sid}): {info['count']} keys, ${info['rev']}\n"
          
      await query.edit_message_text(msg + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_edit_prices":
      keyboard = [[InlineKeyboardButton(prod, callback_data=f"edit_price:{prod}")] for prod in PRICES.get("global", {}).keys()]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text("Select product to edit prices for:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("edit_price:"):
      _, prod = data.split(":", 1)
      prod_prices = PRICES.get("global", {}).get(prod, {})
      if not prod_prices:
          await query.edit_message_text("No price durations configured for this product." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_edit_prices")]]))
          return
      keyboard = [[InlineKeyboardButton(f"{days} days", callback_data=f"edit_price_choose_days:{prod}:{days}")] for days in prod_prices.keys()]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_edit_prices")])
      await query.edit_message_text(f"Select duration for {prod}:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("edit_price_choose_days:"):
      _, prod, days = data.split(":", 2)
      sellers = DATA.get("sellers", {})
      if not sellers:
          await query.edit_message_text("No sellers configured." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_edit_prices")]]))
          return
      keyboard = [[InlineKeyboardButton("GLOBAL", callback_data=f"edit_price_choose_seller:{prod}:{days}:global")]]
      for sid, info in sellers.items():
          keyboard.append([InlineKeyboardButton(f"{info.get('name','?')} ({sid})", callback_data=f"edit_price_choose_seller:{prod}:{days}:{sid}")])
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_edit_prices")])
      await query.edit_message_text(f"Select seller for {prod} ({days} days):" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("edit_price_choose_seller:"):
      _, prod, days, seller_id = data.split(":", 3)
      context.user_data["admin_action"] = "set_price"
      context.user_data["edit_price_product"] = prod
      context.user_data["edit_price_days"] = days
      context.user_data["edit_price_seller"] = seller_id
      await query.edit_message_text(
          f"Send new price for {prod} ({days} days) for {'GLOBAL' if seller_id == 'global' else seller_id}:" + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_edit_prices")]])
      )
      return

  if data == "admin_create_key":
      keyboard = [[InlineKeyboardButton(prod, callback_data=f"admin_create_key_product:{prod}")] for prod in PRICES.get("global", {}).keys()]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text("Select product to create a single key for:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_create_key_product:"):
      _, prod = data.split(":", 1)
      durations = list(PRICES.get("global", {}).get(prod, {}).keys())
      if not durations:
          await query.edit_message_text(f"No durations configured for {prod}. Add durations in prices first." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_create_key")]]))
          return
      keyboard = [[InlineKeyboardButton(f"{d} days", callback_data=f"admin_create_key_duration:{prod}:{d}")] for d in durations]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text(f"Select duration for new key for {prod}:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_create_key_duration:"):
      _, prod, days = data.split(":", 2)
      context.user_data["admin_action"] = "create_key"
      context.user_data["create_key_product"] = prod
      context.user_data["create_key_duration"] = days
      await query.edit_message_text(f"Send the key string to create for product {prod} ({days} days)." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_sellers":
      sellers = DATA.get("sellers", {})
      if not sellers:
          await query.edit_message_text("No sellers configured." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      keyboard = []
      for sid, info in sellers.items():
          seller_balance = info.get("balance", 0)
          wallet_balance = DATA.get("balances", {}).get(str(sid), 0)
          if wallet_balance != seller_balance:
              label = f"{info.get('name','?')} - {sid} - ${seller_balance} | Wallet ${wallet_balance}"
          else:
              label = f"{info.get('name','?')} - {sid} - ${seller_balance}"
          keyboard.append([InlineKeyboardButton(label, callback_data=f"admin_seller:{sid}")])
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text("Sellers:", reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_seller:"):
      _, sid = data.split(":", 1)
      s = DATA.get("sellers", {}).get(sid)
      if not s:
          await query.edit_message_text("Seller not found." + SIGNATURE)
          return
      seller_balance = s.get("balance", 0)
      wallet_balance = DATA.get("balances", {}).get(str(sid), 0)
      stopped_sellers = DATA.get("stopped_sellers", [])
      is_stopped = str(sid) in stopped_sellers
      keyboard = [[InlineKeyboardButton("Add Balance", callback_data=f"admin_add_seller_balance:{sid}")]]
      if is_stopped:
          keyboard.append([InlineKeyboardButton("Resume Seller", callback_data=f"admin_resume_seller:{sid}")])
      else:
          keyboard.append([InlineKeyboardButton("Stop Seller", callback_data=f"admin_stop_seller:{sid}")])
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")])
      await query.edit_message_text(
          f"Seller {s.get('name')} (ID: {sid})\nBalance: ${seller_balance}\nWallet: ${wallet_balance}\nSales: {s.get('sales_count',0)}" + SIGNATURE,
          reply_markup=InlineKeyboardMarkup(keyboard)
      )
      return

  if data and data.startswith("admin_stop_seller:"):
      _, sid = data.split(":", 1)
      stopped_sellers = DATA.get("stopped_sellers", [])
      if str(sid) not in stopped_sellers:
          stopped_sellers.append(str(sid))
          DATA["stopped_sellers"] = stopped_sellers
          save_data(DATA)
      await query.edit_message_text(f"✅ Seller {sid} has been stopped." + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"admin_seller:{sid}")]]))
      return

  if data and data.startswith("admin_resume_seller:"):
      _, sid = data.split(":", 1)
      stopped_sellers = DATA.get("stopped_sellers", [])
      if str(sid) in stopped_sellers:
          stopped_sellers.remove(str(sid))
          DATA["stopped_sellers"] = stopped_sellers
          save_data(DATA)
      await query.edit_message_text(f"✅ Seller {sid} has been resumed." + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"admin_seller:{sid}")]]))
      return
  if data and data.startswith("admin_add_seller_balance:"):
      _, sid = data.split(":", 1)
      context.user_data["admin_action"] = "add_seller_balance"
      context.user_data["target_seller"] = sid
      await query.edit_message_text(f"Send amount to add to seller {sid}:" + SIGNATURE)
      return

  if data == "admin_broadcast":
      context.user_data["admin_action"] = "broadcast"
      await query.edit_message_text("Send the broadcast message to deliver to all known users:" + SIGNATURE)
      return

  if data == "admin_add_balance":
      context.user_data.clear()
      context.user_data["admin_action"] = "add_balance"
      await query.edit_message_text("أدخل معرف المستخدم والمبلغ (مثال: 123456789 10)" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_upload_file":
      keyboard = [[InlineKeyboardButton(prod, callback_data=f"admin_upload_file_product:{prod}")] for prod in PRICES.get("global", {}).keys()]
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text("اختر المنتج لرفع الملف:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_upload_file_product:"):
      _, prod = data.split(":", 1)
      context.user_data.clear()
      context.user_data["admin_action"] = "upload_file"
      context.user_data["upload_product"] = prod
      await query.edit_message_text(f"أرسل الملف للمنتج {prod}." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_send_file_to_user":
      context.user_data.clear()
      context.user_data["admin_action"] = "send_file_to_user"
      await query.edit_message_text("أدخل معرف المستخدم لإرسال الملف له (مثال: 123456789)" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_withdraw":
      context.user_data.clear()
      context.user_data["admin_action"] = "withdraw"
      await query.edit_message_text("أدخل معرف المستخدم والمبلغ للسحب (مثال: 123456789 5)" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_withdraw_keys":
      context.user_data.clear()
      context.user_data["admin_action"] = "withdraw_keys"
      await query.edit_message_text("أدخل المنتج والمدة والكمية (مثال: FREE 7 2)" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_add_seller_cb":
      context.user_data.clear()
      context.user_data["admin_action"] = "add_seller"
      await query.edit_message_text("أدخل معرف البائع والاسم (مثال: 123456789 @sellername)" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_remove_seller_cb":
      context.user_data.clear()
      context.user_data["admin_action"] = "remove_seller"
      await query.edit_message_text("أدخل معرف البائع للحذف (مثال: 123456789)" + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return

  if data == "admin_manage_files":
      files = DATA.get("files", {})
      if not files:
          await query.edit_message_text("No files uploaded." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      keyboard = []
      for prod in files.keys():
          keyboard.append([InlineKeyboardButton(prod, callback_data=f"admin_send_file:{prod}"), InlineKeyboardButton("Delete", callback_data=f"admin_delete_file:{prod}")])
      keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
      await query.edit_message_text("Manage uploaded files:" + SIGNATURE, reply_markup=InlineKeyboardMarkup(keyboard))
      return

  if data and data.startswith("admin_send_file:"):
      _, prod = data.split(":", 1)
      file_ref = DATA.get("files", {}).get(prod)
      if not file_ref:
          await query.edit_message_text("File not found." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_manage_files")]]))
          return
      try:
          if os.path.exists(file_ref):
              await context.bot.send_document(chat_id=query.from_user.id, document=InputFile(file_ref))
          else:
              candidate = file_ref
              if not os.path.exists(candidate) and os.path.exists(os.path.join(os.getcwd(), candidate)):
                  candidate = os.path.join(os.getcwd(), candidate)
              if os.path.exists(candidate):
                  await context.bot.send_document(chat_id=query.from_user.id, document=InputFile(candidate))
              else:
                  await context.bot.send_document(chat_id=query.from_user.id, document=file_ref)
          await query.edit_message_text(f"File {prod} sent to you." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_manage_files")]]))
      except Exception as e:
          await query.edit_message_text("Failed to send file: " + str(e) + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_manage_files")]]))
      return

  if data and data.startswith("admin_delete_file:"):
      _, prod = data.split(":", 1)
      files = DATA.get("files", {})
      if prod not in files:
          await query.edit_message_text("File not found." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_manage_files")]]))
          return
      file_ref = files.get(prod)
      try:
          if os.path.exists(file_ref):
              os.remove(file_ref)
      except Exception:
          pass
      DATA.get("files", {}).pop(prod, None)
      if "files_meta" in DATA:
          DATA.get("files_meta", {}).pop(prod, None)
      save_data(DATA)
      await query.edit_message_text(f"Deleted file for product {prod}." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_manage_files")]]))
      return

  if data and data.startswith("seller_get_file:"):
      _, prod = data.split(":", 1)
      if uid not in DATA.get("sellers", {}):
          await query.edit_message_text("Only sellers can download product files." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]))
          return
      file_ref = DATA.get("files", {}).get(prod)
      if not file_ref:
          await query.edit_message_text("No file uploaded for this product." + SIGNATURE, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]))
          return
      try:
          if os.path.exists(file_ref):
              await context.bot.send_document(chat_id=query.from_user.id, document=InputFile(file_ref))
          else:
              candidate = file_ref
              if not os.path.exists(candidate) and os.path.exists(os.path.join(os.getcwd(), candidate)):
                  candidate = os.path.join(os.getcwd(), candidate)
              if os.path.exists(candidate):
                  await context.bot.send_document(chat_id=query.from_user.id, document=InputFile(candidate))
              else:
                  await context.bot.send_document(chat_id=query.from_user.id, document=file_ref)
          await query.edit_message_text("File sent." + SIGNATURE)
      except Exception as e:
          await query.edit_message_text("Failed to send file: " + str(e) + SIGNATURE)
      return


async def send_duration_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, selected_product: str, lang: str, lang_button_text: str):
  PRICES = load_prices()
  durations = list(PRICES.get("global", {}).get(selected_product, {}).keys())
  if not durations:
      await update.message.reply_text(f"⚠️ لا توجد أسعار معرفة للمنتج {selected_product}. تواصل مع الأدمن." + SIGNATURE)
      return False
  duration_names = {
      "1": {"en": "1 day", "ar": "يوم", "fr": "1 jour", "es": "1 día", "de": "1 Tag", "tr": "1 gün"},
      "3": {"en": "3 days", "ar": "3 أيام", "fr": "3 jours", "es": "3 días", "de": "3 Tage", "tr": "3 gün"},
      "7": {"en": "7 days", "ar": "7 أيام", "fr": "7 jours", "es": "7 días", "de": "7 Tage", "tr": "7 gün"},
      "10": {"en": "10 days", "ar": "10 أيام", "fr": "10 jours", "es": "10 días", "de": "10 Tage", "tr": "10 gün"},
      "15": {"en": "15 days", "ar": "15 يوم", "fr": "15 jours", "es": "15 días", "de": "15 Tage", "tr": "15 gün"},
      "30": {"en": "30 days", "ar": "30 يوم", "fr": "30 jours", "es": "30 días", "de": "30 Tage", "tr": "30 gün"},
      "31": {"en": "1 month", "ar": "شهر", "fr": "1 mois", "es": "1 mes", "de": "1 Monat", "tr": "1 ay"},
      "365": {"en": "1 year", "ar": "سنة", "fr": "1 an", "es": "1 año", "de": "1 Jahr", "tr": "1 yıl"}
  }
  reply_keyboard = []
  for d in durations:
      if d in duration_names:
          price = PRICES.get("global", {}).get(selected_product, {}).get(d)
          if price is not None:
              reply_keyboard.append([f"{duration_names[d][lang]} - ${price}"])
          else:
              reply_keyboard.append([duration_names[d][lang]])
  reply_keyboard.append(["⬅️ Back"])
  await update.message.reply_text(
      "اختر المدة:" if lang == "ar" else
      "Select duration:" if lang == "en" else
      "Sélectionnez la durée:" if lang == "fr" else
      "Seleccione la duración:" if lang == "es" else
      "Dauer auswählen:" if lang == "de" else
      "Süre seçin:",
      reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)
  )
  context.user_data["selected_product"] = selected_product
  context.user_data["choose_duration"] = True
  context.user_data["buy_menu"] = False
  return True


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
  text = update.message.text.strip() if update.message.text else ""
  uid = str(update.message.from_user.id)
  if not context.user_data.get("lang"):
      context.user_data["lang"] = DEFAULT_LANG

  try:
      DATA_LOCAL = load_data()
      if uid not in DATA_LOCAL.get("users", []):
          DATA_LOCAL.setdefault("users", []).append(uid)
          save_data(DATA_LOCAL)
  except Exception:
      pass

  try:
      DATA_APPROVE = load_data()
      admins = set(DATA_APPROVE.get("admins", []))
      approved = set(DATA_APPROVE.get("approved_users", []))
      if uid not in admins and uid not in approved and text != ADMIN_CODE:
          DATA_APPROVE.setdefault("pending_users", [])
          if uid not in DATA_APPROVE["pending_users"]:
              DATA_APPROVE["pending_users"].append(uid)
              save_data(DATA_APPROVE)
          await update.message.reply_text("⏳ Your access request is pending admin approval." + SIGNATURE)
          return
  except Exception:
      pass

  try:
      DATA_LANG = load_data()
      if uid in DATA_LANG.get("sellers", {}):
          # Block stopped sellers
          if uid in DATA_LANG.get("stopped_sellers", []):
              await update.message.reply_text("⛔ You are blocked as a seller. Contact admin to restore access." + SIGNATURE)
              return
          context.user_data["lang"] = "en"
  except Exception:
      pass

  if context.user_data.get("admin_action") == "withdraw_keys":
      try:
          parts = text.split()
          product, duration, qty = parts[0], parts[1], int(parts[2])
          data = load_data()
          key_name = f"{product}_{duration}"
          keys_pool = data.get("keys", {}).get(key_name, [])
          used_keys = set(data.get("used_keys", []))
          available = [k for k in keys_pool if k not in used_keys]
          if len(available) < qty:
              await update.message.reply_text(f"❌ لا يوجد مفاتيح كافية للسحب. المتوفر: {len(available)}" + SIGNATURE,
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
              return
          withdrawn = available[:qty]
          data.setdefault("used_keys", [])
          data["used_keys"] = list(set(data.get("used_keys", [])).union(withdrawn))
          data.setdefault("sold_keys", {}).setdefault(key_name, []).extend(withdrawn)
          save_data(data)
          keys_str = "\n".join([f"`{k}`" for k in withdrawn])
          await update.message.reply_text(f"✅ تم سحب المفاتيح:\n{keys_str}" + SIGNATURE, parse_mode="Markdown",
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          context.user_data.pop("admin_action", None)
      except Exception:
          await update.message.reply_text("❌ صيغة خاطئة. مثال: FREE 7 2" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          context.user_data.pop("admin_action", None)
      return

  try:
      DATA_BTN = load_data()
      if uid in DATA_BTN.get("sellers", {}):
          for o in DATA_BTN.get("seller_custom_buttons", []):
              if isinstance(o, dict) and text == o.get("label"):
                  await update.message.reply_text((o.get("response") or "") + SIGNATURE)
                  return
  except Exception:
      pass

  if update.message.document and context.user_data.get("admin_action") in ("upload_file", "send_file_to_user_upload"):
      if context.user_data.get("admin_action") == "send_file_to_user_upload":
          target_uid = context.user_data.get("target_user")
          if not target_uid:
              await update.message.reply_text("No target user set." + SIGNATURE)
          else:
              try:
                  await context.bot.send_document(chat_id=int(target_uid), document=update.message.document)
                  await update.message.reply_text("✅ File sent to user." + SIGNATURE)
              except Exception as e:
                  await update.message.reply_text("Failed to send file: " + str(e) + SIGNATURE)
          context.user_data.pop("admin_action", None)
          context.user_data.pop("target_user", None)
          return

      prod = context.user_data.get("upload_product")
      if not prod:
          await update.message.reply_text("No product selected for file upload." + SIGNATURE)
      else:
          DATA2 = load_data()
          try:
              files_dir = os.path.join(os.getcwd(), "files")
              os.makedirs(files_dir, exist_ok=True)
              doc = update.message.document
              file_id = doc.file_id
              original_name = getattr(doc, "file_name", None) or f"{prod}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.dat"
              safe_name = original_name.replace("/", "_").replace("\\", "_")
              local_path = os.path.join(files_dir, safe_name)
              tg_file = await context.bot.get_file(file_id)
              await tg_file.download_to_drive(local_path)
              rel_path = os.path.relpath(local_path, os.getcwd())
              DATA2.setdefault("files", {})[prod] = rel_path
              DATA2.setdefault("files_meta", {})[prod] = {"tg_file_id": file_id, "local": rel_path}
              save_data(DATA2)
              await update.message.reply_text(f"File saved for product {prod} to {rel_path}." + SIGNATURE)
          except Exception as e:
              await update.message.reply_text("Failed to save file: " + str(e) + SIGNATURE)
      context.user_data.pop("admin_action", None)
      context.user_data.pop("upload_product", None)
      return

  if context.user_data.get("admin_action") == "vip_add_seller_option":
      DATA_VIP = load_data()
      raw = text
      if "|" in raw:
          label, response = [p.strip() for p in raw.split("|", 1)]
      else:
          label, response = raw.strip(), raw.strip()
      if not label:
          await update.message.reply_text("❌ الصيغة غير صحيحة." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="vip_menu")]]))
          context.user_data.pop("admin_action", None)
          return
      DATA_VIP.setdefault("seller_custom_buttons", [])
      DATA_VIP["seller_custom_buttons"].append({"label": label, "response": response})
      save_data(DATA_VIP)
      await update.message.reply_text("✅ تم إضافة الخيار بنجاح." + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="vip_menu")]]))
      context.user_data.pop("admin_action", None)
      return

  lang = context.user_data.get("lang", "en")
  button_texts = {
      "buy": {"en": "🛍️ BUY KEYS", "ar": "🛍️ شراء مفاتيح"},
      "balance": {"en": "💰 MY BALANCE", "ar": "💰 رصيدي"},
      "admin": {"en": "🔐 ADMIN PANEL", "ar": "🔐 لوحة الادمن"},
      "activity": {"en": "📊 Activity", "ar": "📊 النشاط"},
      "seller_report": {"en": "📈 Seller Report", "ar": "📈 تقرير البائع"},
      "get_files": {"en": "📁 Get Files", "ar": "📁 الحصول على الملفات"},
      "cert": {"en": "🔐 Get Certificate", "ar": "🔐 جلب الشهادة"},
      "my_cert_keys": {"en": "🗝️ My Cert Keys", "ar": "🗝️ مفاتيح الشهادة"},
      "api_balance": {"en": "💰 Check API Balance", "ar": "💰 فحص رصيد API"}
  }

  if text == "⬅️ Back":
      context.user_data.clear()
      await start(update, context)
      return

  if text == button_texts["balance"][lang]:
      data = load_data()
      bal = data.get("balances", {}).get(uid, 0)
      await update.message.reply_text((f"Your balance: ${bal}" if lang == "en" else f"رصيدك: ${bal}") + SIGNATURE)
      return

  if text == button_texts["admin"][lang]:
      await update.message.reply_text("Enter admin code:" + SIGNATURE)
      context.user_data["awaiting_admin_code"] = True
      return

  if context.user_data.get("awaiting_admin_code"):
      if text == ADMIN_CODE:
          context.user_data.pop("awaiting_admin_code", None)
          try:
              DATA_AD = load_data()
              DATA_AD.setdefault("admins", [])
              if uid not in DATA_AD["admins"]:
                  DATA_AD["admins"].append(uid)
                  save_data(DATA_AD)
          except Exception:
              pass
          await update.message.reply_text("👍 Admin panel access granted." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Main Menu", callback_data="admin_menu")]]))
      else:
          await update.message.reply_text("❌ Wrong code!" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]))
      return

  if text == button_texts["cert"][lang]:
      context.user_data["waiting_for_cert_key"] = True
      context.user_data.pop("waiting_for_udid", None)
      await update.message.reply_text(("Send your certificate key for the 30-day warranty plan." if lang == "en" else "أرسل مفتاح الشهادة الخاصة بخطة ضمان 30 يوم.") + SIGNATURE)
      return

  if text == button_texts["my_cert_keys"][lang]:
      DATA_CERT = load_data()
      rows = list_user_cert_sales(DATA_CERT, uid)
      if not rows:
          await update.message.reply_text(("No certificate keys found for your account." if lang == "en" else "لم يتم العثور على مفاتيح شهادة لحسابك.") + SIGNATURE)
      else:
          msg = ("🗝️ Your certificate keys:\n\n" if lang == "en" else "🗝️ مفاتيح الشهادة الخاصة بك:\n\n")
          for i, sale in enumerate(rows[:20], 1):
              msg += f"{i}. {get_sale_key_value(sale)}\n"
          await update.message.reply_text(msg + SIGNATURE)
      return

  if text == button_texts["api_balance"][lang]:
      try:
          resp = get_iosertest_balance()
          if resp.get("code") == 1:
              bdata = resp.get("data", {}) or {}
              if "deviceNum" in bdata:
                  msg = f"📦 Remaining devices: {bdata.get('deviceNum')}" if lang == "en" else f"📦 عدد الأجهزة المتبقية: {bdata.get('deviceNum')}"
              elif "balance" in bdata:
                  msg = f"💰 Current API balance: {bdata.get('balance')}" if lang == "en" else f"💰 رصيد API الحالي: {bdata.get('balance')}"
              else:
                  msg = "⚠️ Balance response received, but no known field was found." if lang == "en" else "⚠️ تم استلام الرد ولكن بدون حقل رصيد معروف."
          else:
              msg = f"❌ API request failed: {resp.get('msg', 'Unknown error')}"
      except Exception as e:
          msg = f"❌ Error: {e}"
      await update.message.reply_text(msg + SIGNATURE)
      return

  if text == button_texts["get_files"][lang]:
      DATAF = load_data()
      files = DATAF.get("files", {})
      if uid in DATAF.get("sellers", {}):
          available = list(files.keys())
      else:
          purchases = [s.get("product") for s in DATAF.get("sales_log", []) if s.get("user") == uid]
          available = sorted(set(purchases))
      if not available:
          await update.message.reply_text("No files available for you." + SIGNATURE)
          return
      reply_keyboard = [[p] for p in available]
      reply_keyboard.append(["⬅️ Back"])
      await update.message.reply_text("Select product to download file:" + SIGNATURE,
          reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
      context.user_data["getting_file"] = True
      return

  if context.user_data.get("getting_file"):
      DATAF = load_data()
      sel = None
      for p in DATAF.get("files", {}).keys():
          if text == p or p.lower() in text.lower() or p in text:
              sel = p
              break
      if sel:
          file_ref = DATAF.get("files", {}).get(sel)
          if file_ref:
              try:
                  allowed = False
                  if uid in DATAF.get("sellers", {}):
                      allowed = True
                  else:
                      for s in DATAF.get("sales_log", []):
                          if s.get("user") == uid and s.get("product") == sel:
                              allowed = True
                              break
                  if not allowed:
                      await update.message.reply_text("You don't have access to this file." + SIGNATURE)
                      context.user_data.pop("getting_file", None)
                      return
                  if os.path.exists(file_ref):
                      await update.message.reply_document(document=InputFile(file_ref))
                  else:
                      candidate = file_ref
                      if not os.path.exists(candidate) and os.path.exists(os.path.join(os.getcwd(), candidate)):
                          candidate = os.path.join(os.getcwd(), candidate)
                      if os.path.exists(candidate):
                          await update.message.reply_document(document=InputFile(candidate))
                      else:
                          await update.message.reply_document(document=file_ref)
                  context.user_data.pop("getting_file", None)
                  return
              except Exception as e:
                  await update.message.reply_text("Failed to send file: " + str(e) + SIGNATURE)
                  context.user_data.pop("getting_file", None)
                  return

  if text == button_texts["buy"][lang]:
      PRICES = load_prices()
      product_names = {
          "FREE": {"en": "🔮 FREE FIRE", "ar": "🔮 فري فاير"},
          "WIZARD": {"en": "✨ WIZARD", "ar": "✨ ويزارد"},
          "CERT_IPHONE_WARRANTY": {"en": "📱 iPhone Warranty (30d)", "ar": "📱 ضمان ايفون (شهر)"},
      }
      products = list(product_names.keys())
      buttons = [product_names[p][lang] for p in products]
      reply_keyboard = build_rows(buttons, 2)
      reply_keyboard.append(["⬅️ Back"])
      await update.message.reply_text("Select a product:" + SIGNATURE,
          reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
      context.user_data["buy_menu"] = True
      return

  if context.user_data.get("buy_menu"):
      product_names = {
          "FREE": {"en": "🔮 FREE FIRE", "ar": "🔮 فري فاير"},
          "WIZARD": {"en": "WIZARD", "ar": "ويزارد"},
          "CERT_IPHONE_WARRANTY": {"en": "📱 iPhone Warranty (30d)", "ar": "📱 ضمان ايفون (شهر)"},
      }
      selected_product = None
      lower_text = text.lower()
      for key, names in product_names.items():
          display = names[lang]
          if text == display or display in text or key.lower() in lower_text or key in text:
              selected_product = key
              break
      if selected_product:
          if selected_product == "FREE":
              reply_keyboard = [["📱 iOS"], ["🤖 Android"], ["⬅️ Back"]]
              await update.message.reply_text("Select OS:" + SIGNATURE,
                  reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
              context.user_data["buy_menu"] = False
              context.user_data["choose_free_os"] = True
              return
          await send_duration_menu(update, context, selected_product, lang, "")
          return

  if context.user_data.get("choose_free_os"):
      if "ios" in text.lower():
          reply_keyboard = [["FLUORIT"], ["MIGUL PRO"], ["⬅️ Back"]]
          await update.message.reply_text("Select option:" + SIGNATURE,
              reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
          context.user_data["choose_free_os"] = False
          context.user_data["choose_free_variant"] = True
          context.user_data["free_os"] = "ios"
          return
      if "android" in text.lower():
          reply_keyboard = [["DRIP"], ["DRIP CLIENT ROOT DEVICE"], ["HG CHEAT"], ["PATO TEAM"], ["⬅️ Back"]]
          await update.message.reply_text("Select option:" + SIGNATURE,
              reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
          context.user_data["choose_free_os"] = False
          context.user_data["choose_free_variant"] = True
          context.user_data["free_os"] = "android"
          return

  if context.user_data.get("choose_free_variant"):
      free_os = context.user_data.get("free_os")
      if free_os == "ios":
          if text.upper() == "FLUORIT":
              context.user_data.pop("choose_free_variant", None)
              context.user_data.pop("free_os", None)
              await send_duration_menu(update, context, "FF_IOS_FLUORIT", lang, "")
              return
          if text.upper() in ("MIGUL PRO", "MUGIL PRO"):
              context.user_data.pop("choose_free_variant", None)
              context.user_data.pop("free_os", None)
              await send_duration_menu(update, context, "FF_IOS_MUGIL_PRO", lang, "")
              return
      if free_os == "android":
          if text.upper() == "DRIP":
              context.user_data.pop("choose_free_variant", None)
              context.user_data.pop("free_os", None)
              await send_duration_menu(update, context, "DRIP", lang, "")
              return
          if text.upper() in ("DRIP CLIENT ROOT DEVICE", "DRIP_CLIENT_ROOT_DEVICE"):
              context.user_data.pop("choose_free_variant", None)
              context.user_data.pop("free_os", None)
              await send_duration_menu(update, context, "DRIP_CLIENT_ROOT_DEVICE", lang, "")
              return
          if text.upper() in ("HG CHEAT", "HG CHEATS", "HG_CHEAT"):
              context.user_data.pop("choose_free_variant", None)
              context.user_data.pop("free_os", None)
              await send_duration_menu(update, context, "HG_CHEAT_ANDROID", lang, "")
              return
          if text.upper() in ("PATO TEAM", "PATO_TEAM"):
              context.user_data.pop("choose_free_variant", None)
              context.user_data.pop("free_os", None)
              await send_duration_menu(update, context, "PATO_TEAM", lang, "")
              return

  if context.user_data.get("choose_duration"):
      selected_product = context.user_data.get("selected_product")
      PRICES = load_prices()
      durations = list(PRICES.get("global", {}).get(selected_product, {}).keys())
      duration_names = {
          "1": {"en": "1 day", "ar": "يوم"},
          "3": {"en": "3 days", "ar": "3 أيام"},
          "7": {"en": "7 days", "ar": "7 أيام"},
          "10": {"en": "10 days", "ar": "10 أيام"},
          "15": {"en": "15 days", "ar": "15 يوم"},
          "30": {"en": "30 days", "ar": "30 يوم"},
          "31": {"en": "1 month", "ar": "شهر"},
          "365": {"en": "1 year", "ar": "سنة"}
      }
      selected_duration = None
      for d in durations:
          if d in duration_names and duration_names[d][lang] in text:
              selected_duration = d
              break
      if selected_duration:
          # Expanded quantity options
          qty_buttons = [str(i) for i in range(1, 11)] + ["20", "50", "100", "200"]
          reply_keyboard = build_rows(qty_buttons, 4)
          reply_keyboard.append(["⬅️ Back"])
          await update.message.reply_text("Select quantity:" + SIGNATURE,
              reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))
          context.user_data["selected_duration"] = selected_duration
          context.user_data["choose_duration"] = False
          context.user_data["choose_qty"] = True
          return

  if context.user_data.get("choose_qty"):
      try:
          qty = int(text)
          if not (qty > 0):
              raise ValueError()
      except Exception:
          await update.message.reply_text("Invalid quantity!" + SIGNATURE)
          return
      selected_product = context.user_data.get("selected_product")
      selected_duration = context.user_data.get("selected_duration")
      
      # Determine bonus keys
      bonus_keys = 0
      if selected_product == "FF_IOS_FLUORIT" and selected_duration == "31":
          if qty >= 16:
              bonus_keys = 4
          elif qty >= 12:
              bonus_keys = 3
          elif qty >= 8:
              bonus_keys = 2
          elif qty >= 3:
              bonus_keys = 1
      
      total_keys_needed = qty + bonus_keys
      
      PRICES = load_prices()
      DATA = load_data()
      unit_price = get_price_for_user(PRICES, selected_product, selected_duration, uid, DATA)
      price = unit_price * qty # User pays for qty only
      
      try:
          balance = float(DATA.get("balances", {}).get(uid, 0))
      except Exception:
          balance = 0
          
      key_name = key_storage_name(selected_product, selected_duration)
      keys_pool = DATA.get("keys", {}).get(key_name, [])
      used_keys = set(DATA.get("used_keys", []))
      keys_pool = [k for k in keys_pool if k not in used_keys]
      
      if balance < price:
          await update.message.reply_text("❌ Insufficient balance!" + SIGNATURE)
          return
      if len(keys_pool) < total_keys_needed:
          await update.message.reply_text(
              f"❌ Not enough keys available.\nAvailable: {len(keys_pool)}\nRequired including bonus: {total_keys_needed}\nExpected key bucket: {key_name}\nAsk admin to add keys for this exact product/duration." + SIGNATURE
          )
          return
          
      keys = [keys_pool.pop(0) for _ in range(qty)] # The paid keys
      bonus_keys_list = [keys_pool.pop(0) for _ in range(bonus_keys)] # The free keys
      all_keys = keys + bonus_keys_list
      
      DATA.setdefault("used_keys", [])
      DATA["used_keys"] = list(set(DATA.get("used_keys", [])).union(all_keys))
      DATA.setdefault("keys", {})[key_name] = keys_pool
      DATA.setdefault("sold_keys", {}).setdefault(key_name, []).extend(all_keys)
      DATA.setdefault("balances", {})[uid] = balance - price
      seller_id = uid if uid in DATA.get("sellers", {}) else None
      seller_name = DATA.get("sellers", {}).get(seller_id, {}).get("name", "?") if seller_id else None
      
      # Log paid keys sales
      for k in keys:
          sale_entry = {
              "user": uid,
              "product": selected_product,
              "duration": selected_duration,
              "price": unit_price,
              "buyer_balance": balance - price,
              "key": k,
              "timestamp": str(datetime.utcnow())
          }
          if seller_id:
              sale_entry["seller_id"] = seller_id
              sale_entry["seller_name"] = seller_name
          DATA.setdefault("sales_log", []).append(sale_entry)
          
      # Log bonus keys sales (price 0)
      for k in bonus_keys_list:
          sale_entry = {
              "user": uid,
              "product": selected_product,
              "duration": selected_duration,
              "price": 0,
              "buyer_balance": balance - price,
              "key": k,
              "note": "Bonus Key",
              "timestamp": str(datetime.utcnow())
          }
          if seller_id:
              sale_entry["seller_id"] = seller_id
              sale_entry["seller_name"] = seller_name
          DATA.setdefault("sales_log", []).append(sale_entry)
          
      admins_list = DATA.get("admins", [])
      buyer_balance = balance - price
      buyer_username = update.message.from_user.username
      buyer_handle = f"@{buyer_username}" if buyer_username else "(no username)"
      keys_str_admin = "\n".join(all_keys)
      
      # Notify Admin logic (using all_keys)...
      for admin_id in admins_list:
          try:
              if seller_id:
                  text_msg = (
                      f"🔔 تم شراء مفاتيح بواسطة بائع\n"
                      f"المنتج: {selected_product}\n"
                      f"المدة: {selected_duration} يوم\n"
                      f"الكمية المدفوعة: {qty} (Bonus: {bonus_keys})\n"
                      f"المشتري: {buyer_handle} ({uid})\n"
                      f"المفاتيح:\n{keys_str_admin}\n"
                      f"البائع: {seller_name} ({seller_id})\n"
                      f"رصيد المشتري بعد الشراء: ${buyer_balance}"
                  )
              else:
                  text_msg = (
                      f"🔔 تم شراء مفاتيح\n"
                      f"المنتج: {selected_product}\n"
                      f"المدة: {selected_duration} يوم\n"
                      f"الكمية المدفوعة: {qty} (Bonus: {bonus_keys})\n"
                      f"المشتري: {buyer_handle} ({uid})\n"
                      f"المفاتيح:\n{keys_str_admin}\n"
                      f"رصيد المشتري بعد الشراء: ${buyer_balance}"
                  )
              await context.bot.send_message(chat_id=int(admin_id), text=text_msg)
          except Exception:
              pass
      save_data(DATA)
      keys_str = "\n".join([f"`{k}`" for k in all_keys])

      balance_bonus_msg = ""
      if selected_product in ("FF_IOS_FLUORIT", "DRIP") and selected_duration == "31":
          bonus_amount = 0.25 * qty
          DATA.setdefault("balances", {})[uid] = DATA.get("balances", {}).get(uid, 0) + bonus_amount
          save_data(DATA)
          balance_bonus_msg = f"\n\n🎁 Balance Bonus: ${bonus_amount} added to your balance!"

      bonus_keys_msg = ""
      if bonus_keys > 0:
          bonus_keys_msg = f"\n\n🎉 You got {bonus_keys} FREE key(s)!"

      await update.message.reply_text(
          f"✅ Purchase successful!\n\nYour keys:\n{keys_str}{balance_bonus_msg}{bonus_keys_msg}\n\n📁 Use Get Files to download product updates." + SIGNATURE,
          parse_mode="Markdown"
      )
      context.user_data.pop("selected_product", None)
      context.user_data.pop("selected_duration", None)
      context.user_data.pop("choose_qty", None)
      return

  action = context.user_data.get("admin_action")
  if action == "send_file_to_user":
      target_uid = text.strip()
      if not target_uid.isdigit():
          await update.message.reply_text("❌ معرف المستخدم غير صحيح." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          return
      context.user_data["admin_action"] = "send_file_to_user_upload"
      context.user_data["target_user"] = target_uid
      await update.message.reply_text("أرسل الملف الآن (أي نوع ملف)." + SIGNATURE)
      return
  if action == "broadcast":
      text_to_send = text
      data = load_data()
      users = data.get("users", [])
      sent = 0
      failed = 0
      for u in users:
          try:
              await context.bot.send_message(chat_id=int(u), text=text_to_send + SIGNATURE)
              sent += 1
          except Exception:
              failed += 1
      await update.message.reply_text(f"Broadcast complete. Sent: {sent}, Failed: {failed}" + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      context.user_data.pop("admin_action", None)
      return
  if action == "add_balance":
      try:
          parts = text.split()
          target_uid, amount = parts[0], float(parts[1])
          data = load_data()
          data.setdefault("balances", {})[target_uid] = data.get("balances", {}).get(target_uid, 0) + amount
          save_data(data)
          await update.message.reply_text(f"✅ Added ${amount} to user {target_uid}. Balance: ${data['balances'][target_uid]}" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          context.user_data.pop("admin_action", None)
      except Exception:
          await update.message.reply_text("❌ Wrong format: user_id amount" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return
  if action == "withdraw":
      try:
          parts = text.split()
          target_uid, amount = parts[0], float(parts[1])
          data = load_data()
          if data.get("balances", {}).get(target_uid, 0) < amount:
              await update.message.reply_text(f"❌ Insufficient balance! Available: ${data.get('balances', {}).get(target_uid, 0)}" + SIGNATURE,
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
          else:
              data["balances"][target_uid] -= amount
              save_data(data)
              await update.message.reply_text(f"✅ Withdrawn ${amount} from {target_uid}. Balance: ${data['balances'][target_uid]}" + SIGNATURE,
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
              context.user_data.pop("admin_action", None)
      except Exception:
          await update.message.reply_text("❌ Wrong format: user_id amount" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
      return
  if action == "add_keys":
      product = context.user_data.get("add_keys_product")
      duration = context.user_data.get("add_keys_duration")
      if not product or not duration:
          await update.message.reply_text("❌ يجب اختيار المنتج والمدة أولاً من لوحة الأدمن." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_add_keys")]]))
          return
      keys = [k.strip() for k in text.split("\n") if k.strip()]
      if not keys:
          await update.message.reply_text("❌ لم يتم إدخال أي مفاتيح." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"admin_add_keys_duration:{product}:{duration}")]]))
          return
      data = load_data()
      key_name = f"{product}_{duration}"
      data.setdefault("keys", {}).setdefault(key_name, []).extend(keys)
      save_data(data)
      await update.message.reply_text(f"✅ تم إضافة {len(keys)} مفتاح لـ {product} ({duration}يوم)." + SIGNATURE,
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data=f"admin_add_keys_product:{product}")]]))
      context.user_data.clear()
      return
  if action == "add_seller_balance":
      try:
          sid = context.user_data.get("target_seller")
          amount = float(text)
          data = load_data()
          if sid not in data.get("sellers", {}):
              await update.message.reply_text("❌ Seller not found!" + SIGNATURE,
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
          else:
              data["sellers"][sid]["balance"] = data["sellers"][sid].get("balance", 0) + amount
              save_data(data)
              await update.message.reply_text(f"✅ Added ${amount} to seller {sid}. New balance: ${data['sellers'][sid]['balance']}" + SIGNATURE,
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      except Exception:
          await update.message.reply_text("❌ Wrong format: amount" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      context.user_data.pop("admin_action", None)
      context.user_data.pop("target_seller", None)
      return
  if action == "create_key":
      product = context.user_data.get("create_key_product")
      duration = context.user_data.get("create_key_duration")
      key = text.strip()
      data = load_data()
      key_name = key_storage_name(product, duration)
      data.setdefault("keys", {}).setdefault(key_name, []).append(key)
      save_data(data)
      await update.message.reply_text(
          f"✅ تم إنشاء الكيز لـ {product} ({duration}يوم)!\n\nها هو الكيز:\n`{key}`" + SIGNATURE,
          parse_mode="Markdown",
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="admin_create_key_product:"+product)]])
      )
      context.user_data.pop("admin_action", None)
      context.user_data.pop("create_key_product", None)
      context.user_data.pop("create_key_duration", None)
      return
  if action == "set_price":
      product = context.user_data.get("edit_price_product")
      duration = context.user_data.get("edit_price_days")
      seller_id = context.user_data.get("edit_price_seller")
      try:
          price_val = float(text)
          prices = load_prices()
          if seller_id == "global":
              prices.setdefault("global", {}).setdefault(product, {})[duration] = price_val
          else:
              prices.setdefault("sellers", {}).setdefault(seller_id, {}).setdefault(product, {})[duration] = price_val
          save_prices(prices)
          await update.message.reply_text(
              f"✅ تم تعديل السعر: {product} ({duration}يوم) - السعر الجديد لـ {'GLOBAL' if seller_id == 'global' else seller_id}: ${price_val}" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data=f"edit_price:{product}")]])
          )
      except Exception:
          await update.message.reply_text("❌ أدخل رقم صحيح للسعر." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data=f"edit_price:{product}")]]))
      context.user_data.pop("admin_action", None)
      context.user_data.pop("edit_price_product", None)
      context.user_data.pop("edit_price_days", None)
      return
  if action == "add_seller":
      try:
          parts = text.split(maxsplit=1)
          sid, name = parts[0], parts[1] if len(parts) > 1 else "Unknown"
          data = load_data()
          data.setdefault("sellers", {})[sid] = {"name": name, "balance": 0, "sales_count": 0}
          save_data(data)
          await update.message.reply_text(f"✅ Seller '{name}' (ID: {sid}) added." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
          context.user_data.pop("admin_action", None)
      except Exception:
          await update.message.reply_text("❌ Wrong format: seller_id name" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      return
  if action == "remove_seller":
      data = load_data()
      sid = text.strip()
      if sid in data.get("sellers", {}):
          name = data["sellers"][sid]["name"]
          del data["sellers"][sid]
          save_data(data)
          await update.message.reply_text(f"✅ Seller '{name}' (ID: {sid}) removed." + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
          context.user_data.pop("admin_action", None)
      else:
          await update.message.reply_text("❌ Seller not found!" + SIGNATURE,
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_sellers")]]))
      return

  if text == button_texts["activity"][lang] or text == button_texts["seller_report"]["en"]:
      DATA_STAT = load_data()
      uid = str(update.message.from_user.id)
      admins = set(DATA_STAT.get("admins", []))
      sellers_map = DATA_STAT.get("sellers", {})
      is_seller = uid in sellers_map or (uid.isdigit() and int(uid) in sellers_map)
      if is_seller:
          await update.message.reply_text(build_seller_activity_message(DATA_STAT, uid) + SIGNATURE)
          return
      if uid in admins:
          start_clicks = DATA_STAT.get("start_clicks", 0)
          users_count = len(DATA_STAT.get("users", []))
          used_keys = DATA_STAT.get("used_keys", []) or []
          total_keys_sold = len(used_keys)
          total_keys_available = sum(len(v) for v in DATA_STAT.get("keys", {}).values())
          sellers = DATA_STAT.get("sellers", {})
          sellers_count = len(sellers)
          sales_count = len(DATA_STAT.get("sales_log", []))
          sold_by_product = {}
          for s in DATA_STAT.get("sales_log", []):
              sold_by_product[s.get("product")] = sold_by_product.get(s.get("product"), 0) + 1
          msg = f"📊 Bot Activity Summary:\n\nStart clicks: {start_clicks}\nKnown users: {users_count}\nTotal sales entries: {sales_count}\nKeys sold: {total_keys_sold}\nKeys available: {total_keys_available}\nSellers: {sellers_count}\n\nSold by product:\n"
          for prod, cnt in sold_by_product.items():
              msg += f"- {prod}: {cnt}\n"
          msg += "\nPer-seller breakdown:\n"
          if sellers:
              for sid, info in sellers.items():
                  s_sales = [s for s in DATA_STAT.get("sales_log", []) if s.get("seller_id") == sid]
                  s_count = len(s_sales)
                  prod_counts = {}
                  for s in s_sales:
                      prod_counts[s.get("product")] = prod_counts.get(s.get("product"), 0) + 1
                  msg += f"- {info.get('name','?')} ({sid}) — Balance: ${info.get('balance',0)} — Keys sold: {s_count}\n"
                  if prod_counts:
                      for p, c in prod_counts.items():
                          msg += f"    • {p}: {c}\n"
          else:
              msg += "(no sellers configured)\n"
          await update.message.reply_text(msg + SIGNATURE)
          return
      msg = build_customer_activity_message(DATA_STAT, uid, update.message.from_user.username)
      await update.message.reply_text(msg + SIGNATURE)
      return

  if text == button_texts["buy"][lang]:
      PRICES = load_prices()
      product_names = {
          "FREE": {"en": "🔮 FREE FIRE", "ar": "🔮 فري فاير", "fr": "🔮 FREE FIRE", "es": "🔮 FREE FIRE", "de": "🔮 FREE FIRE", "tr": "🔮 FREE FIRE"},
          "WIZARD": {"en": "✨ WIZARD", "ar": "✨ ويزارد", "fr": "✨ WIZARD", "es": "✨ WIZARD", "de": "✨ WIZARD", "tr": "✨ WIZARD"},
          "CLOUD": {"en": "☁️ CLOUD", "ar": "☁️ كلاود", "fr": "☁️ CLOUD", "es": "☁️ CLOUD", "de": "☁️ CLOUD", "tr": "☁️ CLOUD"},
          "CODM_IOS": {"en": "📱 CODM IOS", "ar": "📱 كودم IOS", "fr": "📱 CODM IOS", "es": "📱 CODM IOS", "de": "📱 CODM IOS", "tr": "📱 CODM IOS"},
          "TERMINAL_X_PC": {"en": "💻 TERMINAL X PC", "ar": "💻 تيرمنال X PC", "fr": "💻 TERMINAL X PC", "es": "💻 TERMINAL X PC", "de": "💻 TERMINAL X PC", "tr": "💻 TERMINAL X PC"},
          "HG_CHEATS_ROOT": {"en": "🛡️ HG CHEATS ROOT", "ar": "🛡️ HG شيتس روت", "fr": "🛡️ HG CHEATS ROOT", "es": "🛡️ HG CHEATS ROOT", "de": "🛡️ HG CHEATS ROOT", "tr": "🛡️ HG CHEATS ROOT"},
          "CERT_IPHONE_WARRANTY": {
              "en": "📱 iPhone Warranty (30d)",
              "ar": "📱 ضمان ايفون (شهر)",
              "fr": "📱 iPhone Warranty (30d)",
              "es": "📱 iPhone Warranty (30d)",
              "de": "📱 iPhone Warranty (30d)",
              "tr": "📱 iPhone Warranty (30d)"
          }
      }
      products = list(product_names.keys())
      buttons = [product_names[p][lang] for p in products]
      reply_keyboard = build_rows(buttons, 2)
      DATA_CHECK = load_data()
      if uid in DATA_CHECK.get("sellers", {}):
          reply_keyboard.append([button_texts["get_files"][lang]])
      reply_keyboard.append(["⬅️ Back"])
      await update.message.reply_text(
          "Select a product:" if lang == "en" else
          "اختر المنتج:" if lang == "ar" else
          "Sélectionnez un produit:" if lang == "fr" else
          "Seleccione un producto:" if lang == "es" else
          "Produkt auswählen:" if lang == "de" else
          "Bir ürün seçin:",
          reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)
      )
      context.user_data["buy_menu"] = True
      return

  if context.user_data.get("waiting_for_cert_key"):
      DATA_CERT = load_data()
      sale = find_user_cert_sale(DATA_CERT, uid, text)
      if not sale:
          await update.message.reply_text(("❌ Invalid key, or this key is not a 30-day warranty certificate for your account." if lang == "en" else "❌ المفتاح غير صالح أو لا يخص شهادة 30 يوم لهذا الحساب.") + SIGNATURE)
          return
      context.user_data["waiting_for_cert_key"] = False
      context.user_data["waiting_for_udid"] = True
      context.user_data["cert_sale"] = sale
      await update.message.reply_text(("✅ Key accepted. Now send your UDID." if lang == "en" else "✅ تم قبول المفتاح. الآن أرسل UDID الخاص بك.") + SIGNATURE)
      return

  if context.user_data.get("waiting_for_udid"):
      try:
          resp = lookup_ios_certificate(text)
          if resp.get("code") != 1:
              await update.message.reply_text((f"❌ API request failed: {resp.get('msg', 'Unknown error')}") + SIGNATURE)
              return
          api_data = resp.get("data", {}) or {}
          await update.message.reply_text(build_cert_details_message(api_data) + SIGNATURE)
          mobileprovision2 = api_data.get("mobileprovision2", "") or ""
          p12_data = api_data.get("p12", "") or ""
          if not mobileprovision2 and not p12_data:
              warn = "⚠️ Record found, but certificate files are empty. Call Add Device API again to regenerate certificates." if lang == "en" else "⚠️ تم العثور على السجل لكن ملفات الشهادة فارغة. أعد طلب Add Device API لتوليد الشهادات من جديد."
              await update.message.reply_text(warn + SIGNATURE)
              return
          if mobileprovision2:
              path = save_b64_temp_file(mobileprovision2, ".mobileprovision")
              with open(path, "rb") as f:
                  await update.message.reply_document(document=InputFile(f, filename="certificate.mobileprovision"))
          if p12_data:
              path = save_b64_temp_file(p12_data, ".p12")
              with open(path, "rb") as f:
                  await update.message.reply_document(document=InputFile(f, filename="certificate.p12"))
      except Exception as e:
          await update.message.reply_text((f"❌ Error: {e}") + SIGNATURE)
      finally:
          context.user_data["waiting_for_udid"] = False
          context.user_data.pop("cert_sale", None)
      return

  if text == button_texts["balance"][lang]:
      user_id = str(update.message.from_user.id)
      data = load_data()
      bal = data.get("balances", {}).get(user_id, 0)
      msg = {
          "en": f"Your balance: ${bal}",
          "ar": f"رصيدك: ${bal}",
          "fr": f"Votre solde: ${bal}",
          "es": f"Su saldo: ${bal}",
          "de": f"Ihr Guthaben: ${bal}",
          "tr": f"Bakiyeniz: ${bal}"
      }[lang]
      await update.message.reply_text(msg + SIGNATURE)
      return


def main():
    if not TOKEN or TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("Error: Set your TELEGRAM_BOT_TOKEN in the code!")
        return
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler((filters.TEXT | filters.Document.ALL | filters.PHOTO | filters.VIDEO | filters.AUDIO | filters.VOICE | filters.ANIMATION) & ~filters.COMMAND, message_handler))
    print("Bot running...")
    try:
        app.run_polling(drop_pending_updates=True)
    except telegram.error.Conflict as e:
        print("Failed to start bot: another getUpdates poller is active for this token.")
        print("telegram.error.Conflict:", e)
        return
    except Exception as e:
        print("Failed to start bot:", e)
        return


if __name__ == "__main__":
  main()
