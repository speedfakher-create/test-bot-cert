import os
import json
import base64
import tempfile
from datetime import datetime

import requests
from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

TOKEN = os.getenv("BOT_TOKEN", "8428090354:AAF-skw72MrdGssNTCjQLGSLQQt3utgPzP0")
IOSERTEST_TOKEN = os.getenv("IOSERTEST_TOKEN", "PYdLuEu5POfM3PkiLMrVRUzN3JNxPYjx")
IOSERTEST_CERT_ENDPOINT = os.getenv(
    "IOSERTEST_CERT_ENDPOINT",
    "https://api.iosertest.com/api/PASTE_CERT_LOOKUP_ENDPOINT_HERE",
)
IOSERTEST_BALANCE_ENDPOINT = os.getenv(
    "IOSERTEST_BALANCE_ENDPOINT",
    "https://api.iosertest.com/api/getassets",
)
DATA_FILE = os.getenv("DATA_FILE", "data.json")

BTN_GET_CERT = "🔐 Get Certificate"
BTN_MY_KEYS = "🗝️ My Cert Keys"
BTN_API_BAL = "💰 Check API Balance"
BTN_START = "🏠 Start"


def load_data():
    if not os.path.exists(DATA_FILE):
        return {"sales_log": []}
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"sales_log": []}


def keyboard():
    return ReplyKeyboardMarkup(
        [
            [BTN_GET_CERT],
            [BTN_MY_KEYS, BTN_API_BAL],
            [BTN_START],
        ],
        resize_keyboard=True,
    )


def normalize(value):
    return str(value or "").strip().lower()


def get_sale_key(sale: dict) -> str:
    return str(
        sale.get("key")
        or sale.get("license")
        or sale.get("serial")
        or sale.get("code")
        or ""
    ).strip()


def is_certificate_sale(sale: dict) -> bool:
    product = normalize(sale.get("product"))
    duration = str(sale.get("duration") or "")
    plan = normalize(sale.get("plan"))
    warranty = sale.get("warranty")

    if "cert" in product or "certificate" in product:
        return True
    if "30" in duration and ("warranty" in plan or warranty is True):
        return True
    if "30 day" in plan and "warranty" in plan:
        return True
    return False


def list_user_keys(user_id: str):
    data = load_data()
    results = []
    for sale in data.get("sales_log", []):
        buyer = str(sale.get("buyer_id") or sale.get("user_id") or sale.get("uid") or "")
        if buyer != str(user_id):
            continue
        if is_certificate_sale(sale):
            results.append(sale)
    return results


def find_user_key(user_id: str, key_text: str):
    wanted = normalize(key_text)
    for sale in list_user_keys(user_id):
        if normalize(get_sale_key(sale)) == wanted:
            return sale
    return None


def format_ts(ts):
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "Unknown"


def build_cert_message(api_data: dict):
    state_text = "✅ Valid" if api_data.get("state") else "❌ Invalid"
    warranty_text = "✅ Yes" if api_data.get("warranty") else "❌ No"
    return (
        "🔐 Certificate Details\n\n"
        f"📱 Product: {api_data.get('product', 'Unknown')}\n"
        f"📄 State: {state_text}\n"
        f"🛡️ Warranty: {warranty_text}\n"
        f"📦 Plan: {api_data.get('plan', 'Unknown')}\n"
        f"⏳ Warranty Time: {format_ts(api_data.get('warranty_time'))}\n"
    )


def save_base64_file(b64_data: str, suffix: str):
    raw = base64.b64decode(b64_data)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(raw)
    tmp.close()
    return tmp.name


def call_cert_api(udid: str):
    payload = {
        "token": IOSERTEST_TOKEN,
        "udid": udid,
    }
    r = requests.post(IOSERTEST_CERT_ENDPOINT, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()


def call_balance_api():
    payload = {"token": IOSERTEST_TOKEN}
    r = requests.post(IOSERTEST_BALANCE_ENDPOINT, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    text = (
        "Welcome to the Certificate Bot.\n\n"
        "Available options:\n"
        "🔐 Get Certificate\n"
        "🗝️ My Cert Keys\n"
        "💰 Check API Balance"
    )
    await update.message.reply_text(text, reply_markup=keyboard())


async def show_my_keys(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    rows = list_user_keys(uid)
    if not rows:
        await update.message.reply_text(
            "No certificate keys found for your account.",
            reply_markup=keyboard()
        )
        return

    text = "🗝️ Your certificate keys:\n\n"
    for idx, sale in enumerate(rows[:30], 1):
        text += f"{idx}. {get_sale_key(sale)}\n"
    await update.message.reply_text(text, reply_markup=keyboard())


async def check_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        resp = call_balance_api()
        if resp.get("code") == 1:
            data = resp.get("data", {}) or {}
            if "deviceNum" in data:
                msg = f"📦 Remaining devices: {data.get('deviceNum')}"
            elif "balance" in data:
                msg = f"💰 Current balance: {data.get('balance')}"
            else:
                msg = "⚠️ Balance response received, but no known field was found."
        else:
            msg = f"❌ Request failed: {resp.get('msg', 'Unknown error')}"
    except Exception as e:
        msg = f"❌ Error while checking API balance:\n{e}"
    await update.message.reply_text(msg, reply_markup=keyboard())


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    uid = str(update.effective_user.id)

    if text == BTN_START:
        await start(update, context)
        return

    if text == BTN_GET_CERT:
        context.user_data.clear()
        context.user_data["waiting_key"] = True
        await update.message.reply_text(
            "🔐 Send your certificate key.",
            reply_markup=keyboard(),
        )
        return

    if text == BTN_MY_KEYS:
        await show_my_keys(update, context)
        return

    if text == BTN_API_BAL:
        await check_balance(update, context)
        return

    if context.user_data.get("waiting_key"):
        sale = find_user_key(uid, text)
        if not sale:
            await update.message.reply_text(
                "❌ Invalid key, or this key is not a certificate key for your account.",
                reply_markup=keyboard(),
            )
            return

        context.user_data["waiting_key"] = False
        context.user_data["waiting_udid"] = True
        context.user_data["verified_sale"] = sale
        await update.message.reply_text(
            "✅ Key accepted.\nNow send your UDID.",
            reply_markup=keyboard(),
        )
        return

    if context.user_data.get("waiting_udid"):
        try:
            resp = call_cert_api(text)
            if resp.get("code") != 1:
                await update.message.reply_text(
                    f"❌ API request failed: {resp.get('msg', 'Unknown error')}",
                    reply_markup=keyboard(),
                )
                return

            api_data = resp.get("data", {}) or {}
            await update.message.reply_text(build_cert_message(api_data), reply_markup=keyboard())

            mobileprovision2 = api_data.get("mobileprovision2", "") or ""
            p12_data = api_data.get("p12", "") or ""

            if not mobileprovision2 and not p12_data:
                await update.message.reply_text(
                    "⚠️ Record found, but certificate files are empty.\n"
                    "You may need to call Add Device API again to regenerate certificates.",
                    reply_markup=keyboard(),
                )
                return

            if mobileprovision2:
                path = save_base64_file(mobileprovision2, ".mobileprovision")
                with open(path, "rb") as f:
                    await update.message.reply_document(
                        document=f,
                        filename="certificate.mobileprovision"
                    )

            if p12_data:
                path = save_base64_file(p12_data, ".p12")
                with open(path, "rb") as f:
                    await update.message.reply_document(
                        document=f,
                        filename="certificate.p12"
                    )

        except Exception as e:
            await update.message.reply_text(f"❌ Error:\n{e}", reply_markup=keyboard())
        finally:
            context.user_data.clear()
        return

    await update.message.reply_text("Choose an option from the menu.", reply_markup=keyboard())


def main():
    if "PASTE_YOUR_BOT_TOKEN_HERE" in TOKEN or not TOKEN:
        raise RuntimeError("BOT_TOKEN is missing.")
    if "PASTE_YOUR_IOSERTEST_TOKEN_HERE" in IOSERTEST_TOKEN or not IOSERTEST_TOKEN:
        raise RuntimeError("IOSERTEST_TOKEN is missing.")

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT, text_router))
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
