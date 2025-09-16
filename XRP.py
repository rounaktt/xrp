import requests
import time
import sys
from datetime import datetime
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# ---------- CONFIG ----------
BOT_TOKEN = "8443720145:AAGgPmFSkg8Pgs5toFNRUDo9PhxjEVircYg"
BASE_URL = "https://faucetearner.org/api.php"
DEFAULT_INTERVAL = 60

# ---------- GLOBAL STORAGE ----------
user_data = {}

# ---------- COLORS (for console only) ----------
RESET = "\033[0m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"


def log(msg, color=RESET):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"{color}[{ts}] {msg}{RESET}")
    sys.stdout.flush()


# ---------- LOGIN FUNCTION ----------
def create_session(email, password):
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Linux; Android 15) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/139.0 Mobile Safari/537.36",
        "Origin": "https://faucetearner.org",
        "X-Requested-With": "com.jsk.browse",
        "Referer": "https://faucetearner.org/login.php",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    })

    login_payload = {"email": email, "password": password}
    r1 = session.post(f"{BASE_URL}?act=login", json=login_payload)
    try:
        resp = r1.json()
    except Exception:
        return None, "❌ Login response invalid"
    if resp.get("code") != 0:
        return None, "❌ Login failed"
    return session, "✅ Login successful!"


# ---------- CLAIM FUNCTION ----------
def claim_once(session):
    try:
        session.headers.update({"Referer": "https://faucetearner.org/faucet.php"})
        r2 = session.post(f"{BASE_URL}?act=get_faucet", json={})
        faucet_info = r2.json()
        token = faucet_info.get("token")

        claim_payload = {"token": token} if token else {}
        r3 = session.post(f"{BASE_URL}?act=faucet", json=claim_payload)

        wait_time = DEFAULT_INTERVAL
        try:
            resp = r3.json()
            wait_time = int(resp.get("wait", DEFAULT_INTERVAL))
        except:
            if "wait" in r3.text.lower():
                for part in r3.text.split():
                    if part.isdigit():
                        wait_time = int(part)
                        break
        return wait_time, r3.text
    except Exception as e:
        return DEFAULT_INTERVAL, f"⚠️ Error: {e}"


# ---------- TELEGRAM BOT HANDLERS ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Hello! Send your Gmail like this:\n/gmail=yourmail@gmail.com")


async def set_gmail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gmail = update.message.text.replace("/gmail=", "").strip()
    user_id = update.message.from_user.id
    if user_id not in user_data:
        user_data[user_id] = {}
    user_data[user_id]["gmail"] = gmail
    await update.message.reply_text("📩 Gmail saved! Now send password:\n/pass=yourpassword")


async def set_pass(update: Update, context: ContextTypes.DEFAULT_TYPE):
    password = update.message.text.replace("/pass=", "").strip()
    user_id = update.message.from_user.id
    if user_id not in user_data or "gmail" not in user_data[user_id]:
        await update.message.reply_text("⚠️ Please set Gmail first with /gmail=")
        return

    email = user_data[user_id]["gmail"]
    session, msg = create_session(email, password)
    if not session:
        await update.message.reply_text(msg)
        return

    user_data[user_id]["session"] = session
    await update.message.reply_text(f"{msg}\n🚀 Auto-claim starting...")

    while True:
        wait_time, response = claim_once(session)
        await update.message.reply_text(f"💠 Claim Response:\n{response}")
        await update.message.reply_text(f"⏳ Waiting {wait_time} sec before next claim...")
        time.sleep(wait_time)


# ---------- MAIN ----------
def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Regex(r"^/gmail=.*"), set_gmail))
    app.add_handler(MessageHandler(filters.Regex(r"^/pass=.*"), set_pass))

    log("🤖 Bot started...", GREEN)
    app.run_polling()


if __name__ == "__main__":
    main()
