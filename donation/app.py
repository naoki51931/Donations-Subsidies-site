import io
import os
import re
import smtplib
from functools import wraps
from pathlib import Path
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from urllib.parse import urlencode
from uuid import uuid4

import pymysql
import stripe
from dotenv import load_dotenv
from flask import (
    Flask,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    send_from_directory,
    session,
)
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfgen import canvas

load_dotenv()
JST = timezone(timedelta(hours=9), name="JST")

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", uuid4().hex)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
app.json.ensure_ascii = False
default_receipt_dir = f"/tmp/donation_receipts_{os.geteuid()}"
RECEIPT_DIR = Path(os.getenv("RECEIPT_DIR", default_receipt_dir))
RECEIPT_DIR.mkdir(parents=True, exist_ok=True)
BASE_DIR = Path(__file__).resolve().parent
PUBLIC_WEB_DIR = BASE_DIR.parent
SEAL_IMAGE_PATH = Path(os.getenv("SEAL_IMAGE_PATH", str(BASE_DIR / "assets/seals/issuer_seal.png")))
SIGNATURE_IMAGE_PATH = Path(
    os.getenv("SIGNATURE_IMAGE_PATH", str(BASE_DIR / "assets/seals/issuer_signature.png"))
)


# ===== メール設定（環境変数） =====
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "").replace(" ", "")
FROM_MAIL = os.getenv("FROM_MAIL", SMTP_USER)
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "kifukin_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "donation")
CREDIT_CARD_INPUT_URL = os.getenv("CREDIT_CARD_INPUT_URL", "").strip()
PUBLIC_DONATION_PREFIX = os.getenv("PUBLIC_DONATION_PREFIX", "/donation").rstrip("/")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin").strip() or "admin"
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
USER_USERNAME = os.getenv("USER_USERNAME", "user").strip() or "user"
USER_PASSWORD = os.getenv("USER_PASSWORD", "")
ALLOWED_PAYMENT_METHODS = {"現金", "振込", "クレジットカード"}
ALLOWED_DONATION_PLANS = {"one_time", "monthly"}
HANDLER_OPTIONS_RAW = os.getenv("DONATION_HANDLER_OPTIONS", "未設定,admin,user")
HANDLER_PLACEHOLDER = "未設定"
NOINDEX_PATH_PREFIXES = (
    "/admin",
    "/account",
    "/db-check",
    "/download/",
    "/submit",
    "/api/",
    "/donation/admin",
    "/donation/account",
    "/donation/download/",
    "/donation/submit",
    "/donation/api/",
    "/payment/credit-card",
    "/payment/success",
    "/payment/cancel",
    "/donation/payment/credit-card",
    "/donation/payment/success",
    "/donation/payment/cancel",
)


def normalize_stripe_mode(value: str) -> str:
    mode = (value or "").strip().lower()
    if mode in {"live", "production", "prod"}:
        return "live"
    return "test"


STRIPE_MODE = normalize_stripe_mode(os.getenv("STRIPE_MODE", "test"))
LEGACY_STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "").strip()
LEGACY_STRIPE_PUBLISHABLE_KEY = os.getenv("STRIPE_PUBLISHABLE_KEY", "").strip()
LEGACY_STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()
STRIPE_TEST_SECRET_KEY = os.getenv("STRIPE_TEST_SECRET_KEY", "").strip()
STRIPE_TEST_PUBLISHABLE_KEY = os.getenv("STRIPE_TEST_PUBLISHABLE_KEY", "").strip()
STRIPE_TEST_WEBHOOK_SECRET = os.getenv("STRIPE_TEST_WEBHOOK_SECRET", "").strip()
STRIPE_LIVE_SECRET_KEY = os.getenv("STRIPE_LIVE_SECRET_KEY", "").strip()
STRIPE_LIVE_PUBLISHABLE_KEY = os.getenv("STRIPE_LIVE_PUBLISHABLE_KEY", "").strip()
STRIPE_LIVE_WEBHOOK_SECRET = os.getenv("STRIPE_LIVE_WEBHOOK_SECRET", "").strip()
if STRIPE_MODE == "live":
    STRIPE_SECRET_KEY = STRIPE_LIVE_SECRET_KEY or LEGACY_STRIPE_SECRET_KEY
    STRIPE_PUBLISHABLE_KEY = STRIPE_LIVE_PUBLISHABLE_KEY or LEGACY_STRIPE_PUBLISHABLE_KEY
    STRIPE_WEBHOOK_SECRET = STRIPE_LIVE_WEBHOOK_SECRET or LEGACY_STRIPE_WEBHOOK_SECRET
else:
    STRIPE_SECRET_KEY = STRIPE_TEST_SECRET_KEY or LEGACY_STRIPE_SECRET_KEY
    STRIPE_PUBLISHABLE_KEY = STRIPE_TEST_PUBLISHABLE_KEY or LEGACY_STRIPE_PUBLISHABLE_KEY
    STRIPE_WEBHOOK_SECRET = STRIPE_TEST_WEBHOOK_SECRET or LEGACY_STRIPE_WEBHOOK_SECRET
STRIPE_CURRENCY = (os.getenv("STRIPE_CURRENCY", "jpy").strip() or "jpy").lower()
STRIPE_SUCCESS_URL = os.getenv("STRIPE_SUCCESS_URL", "").strip()
STRIPE_CANCEL_URL = os.getenv("STRIPE_CANCEL_URL", "").strip()
DONATION_MIN_AMOUNT = int(os.getenv("DONATION_MIN_AMOUNT", "1000"))
DONATION_MAX_AMOUNT = int(os.getenv("DONATION_MAX_AMOUNT", "1000000"))

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY


def parse_multiline_env(value: str) -> str:
    normalized = (
        (value or "")
        .replace("\r\n", "\n")
        .replace("\\r\\n", "\n")
        .replace("\\n", "\n")
        .replace("¥n", "\n")
        .strip()
    )
    # Fallback for values like "支店n普通 1234n口座名義..." (missing backslash).
    if "\n" not in normalized and "n" in normalized:
        parts = [part.strip() for part in normalized.split("n")]
        if len(parts) >= 2 and all(parts):
            normalized = "\n".join(parts)
    return normalized


BANK_TRANSFER_INFO = parse_multiline_env(os.getenv("BANK_TRANSFER_INFO", ""))


def parse_csv_env(value: str) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def get_seed_handlers() -> list[str]:
    seen: set[str] = set()
    options: list[str] = []
    for item in parse_csv_env(HANDLER_OPTIONS_RAW):
        if item == HANDLER_PLACEHOLDER:
            continue
        if item in seen:
            continue
        seen.add(item)
        options.append(item)
    return options


def get_handler_options_fallback() -> list[str]:
    options = [HANDLER_PLACEHOLDER]
    options.extend(get_seed_handlers())
    return options


def get_dashboard_users() -> dict[str, str]:
    users: dict[str, str] = {}
    if ADMIN_PASSWORD:
        users[ADMIN_USERNAME] = ADMIN_PASSWORD
    if USER_PASSWORD:
        users[USER_USERNAME] = USER_PASSWORD
    return users


def require_dashboard_login(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not session.get("dashboard_user"):
            return redirect(public_admin_path("/login"))
        return view_func(*args, **kwargs)

    return wrapped


def require_donor_login(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not session.get("donor_email"):
            return redirect(f"{PUBLIC_DONATION_PREFIX}/account/login")
        return view_func(*args, **kwargs)

    return wrapped


def public_admin_path(path: str = "") -> str:
    base = PUBLIC_DONATION_PREFIX or ""
    if path and not path.startswith("/"):
        path = f"/{path}"
    return f"{base}/admin{path}"


@app.after_request
def apply_robots_header(response):
    path = request.path or ""
    if any(path == prefix or path.startswith(prefix) for prefix in NOINDEX_PATH_PREFIXES):
        response.headers["X-Robots-Tag"] = "noindex, nofollow, noarchive"
    return response


@app.route("/", methods=["GET"])
@app.route("/index.html", methods=["GET"])
def top_page():
    return send_from_directory(str(PUBLIC_WEB_DIR), "index.html")


@app.route("/style.css", methods=["GET"])
@app.route("/main.js", methods=["GET"])
@app.route("/jquery.min.js", methods=["GET"])
@app.route("/favicon.ico", methods=["GET"])
@app.route("/robots.txt", methods=["GET"])
@app.route("/sitemap.xml", methods=["GET"])
def top_assets_root():
    return send_from_directory(str(PUBLIC_WEB_DIR), request.path.lstrip("/"))


@app.route("/images/<path:filename>", methods=["GET"])
def top_assets_images(filename: str):
    return send_from_directory(str(PUBLIC_WEB_DIR / "images"), filename)


@app.route("/meishi/<path:filename>", methods=["GET"])
def top_assets_meishi(filename: str):
    return send_from_directory(str(PUBLIC_WEB_DIR / "meishi"), filename)


@app.route("/meishi", methods=["GET"])
@app.route("/meishi/", methods=["GET"])
def top_assets_meishi_index():
    return send_from_directory(str(PUBLIC_WEB_DIR / "meishi"), "index.html")


@app.route("/chirashi/<path:filename>", methods=["GET"])
def top_assets_chirashi(filename: str):
    return send_from_directory(str(PUBLIC_WEB_DIR / "chirashi"), filename)


@app.route("/chirashi", methods=["GET"])
@app.route("/chirashi/", methods=["GET"])
def top_assets_chirashi_index():
    return send_from_directory(str(PUBLIC_WEB_DIR / "chirashi"), "index.html")


@app.route("/donation", methods=["GET"])
@app.route("/donation/", methods=["GET"])
def form_page():
    return send_from_directory(str(BASE_DIR), "index.html")


def build_receipt_pdf(
    name: str,
    address: str,
    amount: str,
    payment_method: str,
    donated_at: datetime,
    certificate_no: str,
) -> bytes:
    """Create receipt PDF bytes (Japanese compatible)."""
    pdfmetrics.registerFont(UnicodeCIDFont("HeiseiKakuGo-W5"))

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    c.setFont("HeiseiKakuGo-W5", 12)

    text = c.beginText(50, 800)
    text.setFont("HeiseiKakuGo-W5", 12)
    text.textLine("寄付受領書")
    text.textLine("")
    text.textLine(f"証明書番号：{certificate_no}")
    text.textLine("")
    text.textLine(f"{name} 様")
    text.textLine(f"住所：{address}")
    text.textLine("")
    text.textLine(f"寄附金額：{amount} 円")
    text.textLine(f"支払方法：{payment_method}")
    # アプリ内ではJST運用。naiveな日時はJSTとして扱ってPDFへ表示する。
    if donated_at.tzinfo is None:
        donated_at_jst = donated_at.replace(tzinfo=JST)
    else:
        donated_at_jst = donated_at.astimezone(JST)
    text.textLine(f"日付：{donated_at_jst.strftime('%Y年%m月%d日 %H:%M:%S JST')}")
    text.textLine("")
    text.textLine("受け入れ団体：NPO法人ほっこり サポートホーム／ほっこりくろちゃん")
    text.textLine("所在地：〒612-8403 京都市伏見区深草ヲカヤ町23-6 サポートホーム")

    c.drawText(text)
    draw_issuer_assets(c)
    c.showPage()
    c.save()

    return buffer.getvalue()


def draw_issuer_assets(c: canvas.Canvas) -> None:
    c.setFont("HeiseiKakuGo-W5", 10)
    y_label = 140
    has_seal = SEAL_IMAGE_PATH.exists()
    has_signature = SIGNATURE_IMAGE_PATH.exists()

    if has_seal:
        c.drawString(60, y_label, "略印")
    if has_signature:
        c.drawString(250, y_label, "代表者署名")

    try:
        if has_seal:
            c.drawImage(
                str(SEAL_IMAGE_PATH),
                x=60,
                y=55,
                width=130,
                height=75,
                preserveAspectRatio=True,
                mask="auto",
            )
    except Exception:
        pass

    try:
        if has_signature:
            c.drawImage(
                str(SIGNATURE_IMAGE_PATH),
                x=250,
                y=55,
                width=220,
                height=75,
                preserveAspectRatio=True,
                mask="auto",
            )
    except Exception:
        pass


def normalize_payment_method(payment_method: str) -> str:
    value = payment_method.strip()
    if value in {"銀行振込", "振込", "振り込み"}:
        return "bank_transfer"
    if value in {"クレジットカード"}:
        return "credit_card"
    return "cash"


def normalize_donation_plan(plan: str) -> str:
    value = (plan or "").strip().lower()
    if value == "monthly":
        return "monthly"
    return "one_time"


def parse_amount_yen(raw_amount: str) -> int:
    digits_only = re.sub(r"[^\d]", "", (raw_amount or "").strip())
    if not digits_only:
        raise ValueError("寄付金額が不正です。")
    amount = int(digits_only)
    if amount < DONATION_MIN_AMOUNT or amount > DONATION_MAX_AMOUNT:
        raise ValueError(f"寄付金額は {DONATION_MIN_AMOUNT} 〜 {DONATION_MAX_AMOUNT} 円で指定してください。")
    return amount


def validate_account_password(raw_password: str) -> None:
    value = raw_password or ""
    if len(value) < 8:
        raise ValueError("ログイン用パスワードは8文字以上で入力してください。")
    has_upper = any(ch.isalpha() and ch.isupper() for ch in value)
    has_lower = any(ch.isalpha() and ch.islower() for ch in value)
    has_digit = any(ch.isdigit() for ch in value)
    has_symbol = any(not ch.isalnum() for ch in value)
    if not (has_upper and has_lower and has_digit and has_symbol):
        raise ValueError("パスワードは大文字・小文字・数字・記号をすべて含めてください。")


def build_public_url(path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{request.url_root.rstrip('/')}{PUBLIC_DONATION_PREFIX}{path}"


def build_credit_card_input_url(certificate_no: str, receipt_id: int, donation_plan: str = "one_time") -> str:
    base_url = CREDIT_CARD_INPUT_URL or f"{request.url_root.rstrip('/')}/donation/payment/credit-card"
    query = urlencode(
        {"certificate_no": certificate_no, "receipt_id": str(receipt_id), "donation_plan": donation_plan}
    )
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{query}"


def send_receipt_email(
    name: str,
    email: str,
    pdf_bytes: bytes,
    payment_method: str,
    credit_card_input_url: str,
) -> None:
    if not SMTP_USER or not SMTP_PASS or not FROM_MAIL:
        raise RuntimeError("SMTP設定が未完了です。SMTP_USER / SMTP_PASS / FROM_MAIL を設定してください。")

    payment_kind = normalize_payment_method(payment_method)
    body_lines = [
        f"{name} 様",
        "",
        "この度はご寄附ありがとうございます。",
        "受領書をPDFにてお送りいたします。",
        "",
    ]
    if payment_kind == "bank_transfer":
        body_lines.extend(
            [
                "【お振込先情報】",
                BANK_TRANSFER_INFO or "振込先情報が未設定です。運営までお問い合わせください。",
                "",
            ]
        )
    elif payment_kind == "credit_card":
        body_lines.extend(
            [
                "【クレジットカード情報入力】",
                "以下のページからカード情報をご入力ください。",
                credit_card_input_url,
                "",
            ]
        )
    body_lines.extend(["NPO法人ほっこり"])

    msg = EmailMessage()
    msg["Subject"] = "【NPO法人ほっこり】寄付受領書"
    msg["From"] = FROM_MAIL
    msg["To"] = email
    msg.set_content("\n".join(body_lines))
    msg.add_attachment(
        pdf_bytes,
        maintype="application",
        subtype="pdf",
        filename="寄付受領書.pdf",
    )

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=20) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)


def get_db_connection():
    if not DB_HOST or not DB_USER or not DB_NAME:
        raise RuntimeError("DB設定が未完了です。DB_HOST / DB_USER / DB_NAME を設定してください。")

    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        init_command="SET time_zone = '+09:00'",
    )


def ensure_receipts_table(conn) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS donation_receipts (
        id BIGINT NOT NULL AUTO_INCREMENT,
        certificate_no VARCHAR(32) NOT NULL,
        donor_name VARCHAR(255) NOT NULL,
        is_name_public TINYINT(1) NOT NULL DEFAULT 0,
        assigned_handler VARCHAR(64) NOT NULL DEFAULT '',
        donor_postal_code VARCHAR(16) NOT NULL,
        donor_address VARCHAR(255) NOT NULL,
        donor_email VARCHAR(255) NOT NULL,
        amount_yen VARCHAR(64) NOT NULL,
        payment_method VARCHAR(64) NOT NULL,
        donated_at DATETIME NOT NULL,
        download_token VARCHAR(64) DEFAULT NULL,
        status VARCHAR(32) NOT NULL DEFAULT 'created',
        is_checked TINYINT(1) NOT NULL DEFAULT 0,
        checked_at DATETIME DEFAULT NULL,
        checked_by VARCHAR(64) DEFAULT NULL,
        is_deleted TINYINT(1) NOT NULL DEFAULT 0,
        deleted_at DATETIME DEFAULT NULL,
        deleted_by VARCHAR(64) DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uk_certificate_no (certificate_no),
        UNIQUE KEY uk_download_token (download_token)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='donor_postal_code'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN donor_postal_code VARCHAR(16) NOT NULL DEFAULT ''")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='is_name_public'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN is_name_public TINYINT(1) NOT NULL DEFAULT 0")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='assigned_handler'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN assigned_handler VARCHAR(64) NOT NULL DEFAULT ''")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='donor_address'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN donor_address VARCHAR(255) NOT NULL DEFAULT ''")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='is_checked'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN is_checked TINYINT(1) NOT NULL DEFAULT 0")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='checked_at'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN checked_at DATETIME DEFAULT NULL")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='checked_by'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN checked_by VARCHAR(64) DEFAULT NULL")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='is_deleted'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN is_deleted TINYINT(1) NOT NULL DEFAULT 0")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='deleted_at'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN deleted_at DATETIME DEFAULT NULL")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='deleted_by'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN deleted_by VARCHAR(64) DEFAULT NULL")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='stripe_checkout_session_id'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN stripe_checkout_session_id VARCHAR(255) DEFAULT NULL")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='stripe_payment_intent_id'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN stripe_payment_intent_id VARCHAR(255) DEFAULT NULL")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='stripe_last_event_id'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN stripe_last_event_id VARCHAR(255) DEFAULT NULL")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='paid_at'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN paid_at DATETIME DEFAULT NULL")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='donation_plan'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute(
                "ALTER TABLE donation_receipts ADD COLUMN donation_plan VARCHAR(16) NOT NULL DEFAULT 'one_time'"
            )

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='stripe_subscription_id'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN stripe_subscription_id VARCHAR(255) DEFAULT NULL")

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='donation_receipts' AND COLUMN_NAME='stripe_customer_id'
            """,
            (DB_NAME,),
        )
        if cur.fetchone()["cnt"] == 0:
            cur.execute("ALTER TABLE donation_receipts ADD COLUMN stripe_customer_id VARCHAR(255) DEFAULT NULL")
    conn.commit()


def ensure_handlers_table(conn) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS donation_handlers (
        id BIGINT NOT NULL AUTO_INCREMENT,
        handler_name VARCHAR(64) NOT NULL,
        sort_order INT NOT NULL DEFAULT 0,
        is_active TINYINT(1) NOT NULL DEFAULT 1,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uk_handler_name (handler_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute("SELECT COUNT(*) AS cnt FROM donation_handlers WHERE is_active=1")
        count = int(cur.fetchone()["cnt"])
        if count == 0:
            seed_handlers = get_seed_handlers()
            if seed_handlers:
                cur.executemany(
                    """
                    INSERT INTO donation_handlers (handler_name, sort_order, is_active)
                    VALUES (%s, %s, 1)
                    ON DUPLICATE KEY UPDATE is_active=1
                    """,
                    [(name, idx + 1) for idx, name in enumerate(seed_handlers)],
                )
    conn.commit()


def get_handler_records(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, handler_name, sort_order, is_active, created_at, updated_at
            FROM donation_handlers
            WHERE is_active=1
            ORDER BY sort_order ASC, id ASC
            """
        )
        return cur.fetchall()


def get_handler_options(conn) -> list[str]:
    options = [HANDLER_PLACEHOLDER]
    records = get_handler_records(conn)
    options.extend([row["handler_name"] for row in records])
    return options


def ensure_donor_accounts_table(conn) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS donor_accounts (
        id BIGINT NOT NULL AUTO_INCREMENT,
        email VARCHAR(255) NOT NULL,
        donor_name VARCHAR(255) NOT NULL DEFAULT '',
        password_hash VARCHAR(255) NOT NULL,
        is_active TINYINT(1) NOT NULL DEFAULT 1,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        last_login_at DATETIME DEFAULT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uk_email (email)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def upsert_donor_account(conn, email: str, donor_name: str, raw_password: str) -> None:
    password_hash = generate_password_hash(raw_password)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO donor_accounts (email, donor_name, password_hash, is_active)
            VALUES (%s, %s, %s, 1)
            ON DUPLICATE KEY UPDATE
                donor_name=VALUES(donor_name),
                password_hash=VALUES(password_hash),
                is_active=1
            """,
            (email, donor_name, password_hash),
        )
    conn.commit()


def authenticate_donor(conn, email: str, raw_password: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, email, donor_name, password_hash, is_active
            FROM donor_accounts
            WHERE email=%s
            LIMIT 1
            """,
            (email,),
        )
        row = cur.fetchone()
        if not row:
            return None
        if not row.get("is_active"):
            return None
        if not check_password_hash(row["password_hash"], raw_password):
            return None
        cur.execute("UPDATE donor_accounts SET last_login_at=NOW() WHERE id=%s", (row["id"],))
    conn.commit()
    return row


def get_receipts_by_email(conn, email: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                certificate_no,
                donor_name,
                amount_yen,
                donation_plan,
                payment_method,
                status,
                donated_at,
                created_at,
                download_token,
                stripe_subscription_id
            FROM donation_receipts
            WHERE donor_email=%s AND is_deleted=0
            ORDER BY id DESC
            LIMIT 100
            """,
            (email,),
        )
        return cur.fetchall()


def sync_monthly_statuses_for_donor(conn, rows: list[dict]) -> list[dict]:
    if not STRIPE_SECRET_KEY:
        return rows
    for row in rows:
        if row.get("donation_plan") != "monthly":
            continue
        subscription_id = (row.get("stripe_subscription_id") or "").strip()
        if not subscription_id:
            continue
        try:
            subscription = stripe.Subscription.retrieve(subscription_id)
            stripe_status = (subscription.get("status") or "").strip()
            cancel_at_period_end = bool(subscription.get("cancel_at_period_end"))
            if stripe_status == "canceled" and row.get("status") != "subscription_canceled":
                update_receipt_payment_status(
                    conn,
                    receipt_id=int(row["id"]),
                    status="subscription_canceled",
                    subscription_id=subscription_id,
                )
                row["status"] = "subscription_canceled"
            elif cancel_at_period_end and row.get("status") not in {"subscription_cancel_scheduled", "subscription_canceled"}:
                update_receipt_payment_status(
                    conn,
                    receipt_id=int(row["id"]),
                    status="subscription_cancel_scheduled",
                    subscription_id=subscription_id,
                )
                row["status"] = "subscription_cancel_scheduled"
        except Exception:
            # Keep UI available even if Stripe synchronization fails.
            continue
    return rows


def create_receipt_record(
    conn,
    name: str,
    is_name_public: int,
    postal_code: str,
    address: str,
    email: str,
    amount: str,
    payment_method: str,
    donation_plan: str,
    donated_at: datetime,
) -> tuple[int, str]:
    with conn.cursor() as cur:
        # certificate_no is VARCHAR(32), so keep temporary value within 32 chars.
        temp_certificate_no = f"TEMP-{uuid4().hex[:27]}"
        cur.execute(
            """
            INSERT INTO donation_receipts (
                certificate_no, donor_name, is_name_public, donor_postal_code, donor_address, donor_email, amount_yen,
                payment_method, donation_plan, donated_at, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'created')
            """,
            (
                temp_certificate_no,
                name,
                is_name_public,
                postal_code,
                address,
                email,
                amount,
                payment_method,
                donation_plan,
                donated_at,
            ),
        )
        receipt_id = cur.lastrowid
        certificate_no = f"RCPT-{donated_at.year}-{receipt_id:06d}"
        cur.execute(
            "UPDATE donation_receipts SET certificate_no=%s WHERE id=%s",
            (certificate_no, receipt_id),
        )
    conn.commit()
    return receipt_id, certificate_no


def update_receipt_status(conn, receipt_id: int, status: str, token: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE donation_receipts
            SET status=%s, download_token=COALESCE(%s, download_token)
            WHERE id=%s
            """,
            (status, token, receipt_id),
        )
    conn.commit()


def get_receipt_by_id(conn, receipt_id: int) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                certificate_no,
                donor_name,
                donor_email,
                amount_yen,
                donation_plan,
                payment_method,
                download_token,
                status,
                stripe_checkout_session_id,
                stripe_payment_intent_id,
                stripe_subscription_id,
                stripe_customer_id,
                stripe_last_event_id
            FROM donation_receipts
            WHERE id=%s AND is_deleted=0
            LIMIT 1
            """,
            (receipt_id,),
        )
        return cur.fetchone()


def get_receipt_by_certificate_no(conn, certificate_no: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                certificate_no,
                donor_name,
                donor_email,
                amount_yen,
                donation_plan,
                payment_method,
                download_token,
                status,
                stripe_checkout_session_id,
                stripe_payment_intent_id,
                stripe_subscription_id,
                stripe_customer_id,
                stripe_last_event_id
            FROM donation_receipts
            WHERE certificate_no=%s AND is_deleted=0
            LIMIT 1
            """,
            (certificate_no,),
        )
        return cur.fetchone()


def get_receipt_by_stripe_payment_intent(conn, payment_intent_id: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                certificate_no,
                donor_name,
                donor_email,
                amount_yen,
                donation_plan,
                payment_method,
                download_token,
                status,
                stripe_checkout_session_id,
                stripe_payment_intent_id,
                stripe_subscription_id,
                stripe_customer_id,
                stripe_last_event_id
            FROM donation_receipts
            WHERE stripe_payment_intent_id=%s
            LIMIT 1
            """,
            (payment_intent_id,),
        )
        return cur.fetchone()


def get_receipt_by_stripe_subscription(conn, subscription_id: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                certificate_no,
                donor_name,
                donor_email,
                amount_yen,
                donation_plan,
                payment_method,
                download_token,
                status,
                stripe_checkout_session_id,
                stripe_payment_intent_id,
                stripe_subscription_id,
                stripe_customer_id,
                stripe_last_event_id
            FROM donation_receipts
            WHERE stripe_subscription_id=%s
            LIMIT 1
            """,
            (subscription_id,),
        )
        return cur.fetchone()


def update_receipt_payment_status(
    conn,
    receipt_id: int,
    status: str,
    checkout_session_id: str | None = None,
    payment_intent_id: str | None = None,
    subscription_id: str | None = None,
    customer_id: str | None = None,
    stripe_event_id: str | None = None,
    amount_yen: int | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE donation_receipts
            SET
                status=%s,
                stripe_checkout_session_id=COALESCE(%s, stripe_checkout_session_id),
                stripe_payment_intent_id=COALESCE(%s, stripe_payment_intent_id),
                stripe_subscription_id=COALESCE(%s, stripe_subscription_id),
                stripe_customer_id=COALESCE(%s, stripe_customer_id),
                stripe_last_event_id=COALESCE(%s, stripe_last_event_id),
                amount_yen=COALESCE(%s, amount_yen),
                paid_at=CASE WHEN %s='paid' THEN COALESCE(paid_at, NOW()) ELSE paid_at END
            WHERE id=%s
            """,
            (
                status,
                checkout_session_id,
                payment_intent_id,
                subscription_id,
                customer_id,
                stripe_event_id,
                str(amount_yen) if amount_yen is not None else None,
                status,
                receipt_id,
            ),
        )
    conn.commit()


def save_receipt(pdf_bytes: bytes) -> str:
    # Keep files for a day and clean older ones opportunistically.
    now_ts = datetime.now(JST).timestamp()
    for path in RECEIPT_DIR.glob("*.pdf"):
        try:
            if now_ts - path.stat().st_mtime > 24 * 60 * 60:
                path.unlink(missing_ok=True)
        except OSError:
            continue

    token = uuid4().hex
    (RECEIPT_DIR / f"{token}.pdf").write_bytes(pdf_bytes)
    return token


@app.route("/download/<token>", methods=["GET"])
@app.route("/donation/download/<token>", methods=["GET"])
def download_receipt(token: str):
    receipt_path = RECEIPT_DIR / f"{token}.pdf"
    if not receipt_path.exists():
        abort(404, description="受領書PDFが見つかりません。再度寄付フォームからお試しください。")

    timestamp = datetime.now(JST).strftime("%Y%m%d_%H%M%S")
    return send_file(
        receipt_path,
        as_attachment=True,
        download_name=f"寄付受領書_{timestamp}.pdf",
        mimetype="application/pdf",
    )


@app.route("/admin/login", methods=["GET", "POST"])
@app.route("/donation/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        users = get_dashboard_users()
        if users.get(username) == password:
            session["dashboard_user"] = username
            return redirect(public_admin_path())
        return render_template("admin_login.html", error="ユーザー名またはパスワードが違います。"), 401

    if session.get("dashboard_user"):
        return redirect(public_admin_path())
    return render_template("admin_login.html")


@app.route("/admin/logout", methods=["POST"])
@app.route("/donation/admin/logout", methods=["POST"])
def admin_logout():
    session.pop("dashboard_user", None)
    return redirect(public_admin_path("/login"))


@app.route("/admin", methods=["GET"])
@app.route("/donation/admin", methods=["GET"])
@require_dashboard_login
def admin_dashboard():
    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        ensure_handlers_table(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total FROM donation_receipts WHERE is_deleted=0")
            total = cur.fetchone()["total"]
            cur.execute(
                """
                SELECT
                    id,
                    certificate_no,
                    donor_name,
                    is_name_public,
                    assigned_handler,
                    donor_postal_code,
                    donor_address,
                    donor_email,
                    amount_yen,
                    donation_plan,
                    payment_method,
                    status,
                    is_checked,
                    checked_at,
                    checked_by,
                    is_deleted,
                    deleted_at,
                    deleted_by,
                    donated_at,
                    created_at
                FROM donation_receipts
                WHERE is_deleted=0
                ORDER BY id DESC
                LIMIT 100
                """
            )
            rows = cur.fetchall()
        handler_options = get_handler_options(conn)
        handlers = get_handler_records(conn)
    except Exception as exc:
        return render_template(
            "admin_dashboard.html",
            total=0,
            rows=[],
            current_user=session.get("dashboard_user", ""),
            handler_options=get_handler_options_fallback(),
            handlers=[],
            db_error=str(exc),
        )
    finally:
        if conn:
            conn.close()

    return render_template(
        "admin_dashboard.html",
        total=total,
        rows=rows,
        current_user=session.get("dashboard_user", ""),
        handler_options=handler_options,
        handlers=handlers,
        db_error=None,
    )


@app.route("/admin/confirm/<int:receipt_id>", methods=["POST"])
@app.route("/donation/admin/confirm/<int:receipt_id>", methods=["POST"])
@require_dashboard_login
def admin_confirm(receipt_id: int):
    checked = "1" in request.form.getlist("checked")
    current_user = session.get("dashboard_user", "")
    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        with conn.cursor() as cur:
            if checked:
                cur.execute(
                    """
                    SELECT
                        id,
                        certificate_no,
                        donor_name,
                        donor_address,
                        donor_email,
                        amount_yen,
                        payment_method,
                        donation_plan,
                        donated_at,
                        download_token,
                        status,
                        is_checked
                    FROM donation_receipts
                    WHERE id=%s AND is_deleted=0
                    LIMIT 1
                    """,
                    (receipt_id,),
                )
                row = cur.fetchone()
                if not row:
                    abort(404, description="対象データが見つかりません。")
                if row.get("is_checked"):
                    return redirect(public_admin_path())

                donated_at = row.get("donated_at") or datetime.now(JST).replace(tzinfo=None)
                pdf_bytes = build_receipt_pdf(
                    name=row["donor_name"],
                    address=row["donor_address"],
                    amount=row["amount_yen"],
                    payment_method=row["payment_method"],
                    donated_at=donated_at,
                    certificate_no=row["certificate_no"],
                )
                token = row.get("download_token") or save_receipt(pdf_bytes)
                credit_card_input_url = build_credit_card_input_url(
                    row["certificate_no"],
                    int(row["id"]),
                    donation_plan=normalize_donation_plan(row.get("donation_plan", "one_time")),
                )
                try:
                    send_receipt_email(
                        name=row["donor_name"],
                        email=row["donor_email"],
                        pdf_bytes=pdf_bytes,
                        payment_method=row["payment_method"],
                        credit_card_input_url=credit_card_input_url,
                    )
                except Exception as exc:
                    cur.execute(
                        """
                        UPDATE donation_receipts
                        SET status='mail_failed'
                        WHERE id=%s AND is_deleted=0
                        """,
                        (receipt_id,),
                    )
                    conn.commit()
                    return jsonify({"ok": False, "error": str(exc)}), 502

                payment_kind = normalize_payment_method(row["payment_method"])
                next_status = row.get("status") if payment_kind == "credit_card" else "issued"
                cur.execute(
                    """
                    UPDATE donation_receipts
                    SET
                        is_checked=1,
                        checked_at=NOW(),
                        checked_by=%s,
                        status=%s,
                        download_token=COALESCE(%s, download_token)
                    WHERE id=%s AND is_deleted=0
                    """,
                    (current_user, next_status or "created", token, receipt_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE donation_receipts
                    SET is_checked=0, checked_at=NULL, checked_by=NULL
                    WHERE id=%s AND is_deleted=0
                    """,
                    (receipt_id,),
                )
        conn.commit()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn:
            conn.close()

    return redirect(public_admin_path())


@app.route("/admin/handler/<int:receipt_id>", methods=["POST"])
@app.route("/donation/admin/handler/<int:receipt_id>", methods=["POST"])
@require_dashboard_login
def admin_update_handler(receipt_id: int):
    assigned_handler = request.form.get("assigned_handler", "").strip()
    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        ensure_handlers_table(conn)
        valid_options = set(get_handler_options(conn))
        if assigned_handler not in valid_options:
            return jsonify({"ok": False, "error": "担当者の値が不正です。"}), 400
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE donation_receipts
                SET assigned_handler=%s
                WHERE id=%s AND is_deleted=0
                """,
                (assigned_handler, receipt_id),
            )
        conn.commit()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn:
            conn.close()

    return redirect(public_admin_path())


@app.route("/admin/handlers/add", methods=["POST"])
@app.route("/donation/admin/handlers/add", methods=["POST"])
@require_dashboard_login
def admin_add_handler():
    handler_name = request.form.get("handler_name", "").strip()
    if not handler_name or handler_name == HANDLER_PLACEHOLDER:
        return jsonify({"ok": False, "error": "担当者名を入力してください。"}), 400

    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        ensure_handlers_table(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(sort_order), 0) + 1 AS next_order FROM donation_handlers")
            next_order = int(cur.fetchone()["next_order"])
            cur.execute(
                """
                INSERT INTO donation_handlers (handler_name, sort_order, is_active)
                VALUES (%s, %s, 1)
                ON DUPLICATE KEY UPDATE is_active=1
                """,
                (handler_name, next_order),
            )
        conn.commit()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn:
            conn.close()

    return redirect(public_admin_path())


@app.route("/admin/handlers/update/<int:handler_id>", methods=["POST"])
@app.route("/donation/admin/handlers/update/<int:handler_id>", methods=["POST"])
@require_dashboard_login
def admin_update_handler_master(handler_id: int):
    new_name = request.form.get("handler_name", "").strip()
    if not new_name or new_name == HANDLER_PLACEHOLDER:
        return jsonify({"ok": False, "error": "担当者名が不正です。"}), 400

    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        ensure_handlers_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT handler_name FROM donation_handlers WHERE id=%s AND is_active=1 LIMIT 1",
                (handler_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"ok": False, "error": "担当者が見つかりません。"}), 404
            old_name = row["handler_name"]
            cur.execute(
                "UPDATE donation_handlers SET handler_name=%s WHERE id=%s",
                (new_name, handler_id),
            )
            cur.execute(
                "UPDATE donation_receipts SET assigned_handler=%s WHERE assigned_handler=%s",
                (new_name, old_name),
            )
        conn.commit()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn:
            conn.close()

    return redirect(public_admin_path())


@app.route("/admin/handlers/delete/<int:handler_id>", methods=["POST"])
@app.route("/donation/admin/handlers/delete/<int:handler_id>", methods=["POST"])
@require_dashboard_login
def admin_delete_handler_master(handler_id: int):
    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        ensure_handlers_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT handler_name FROM donation_handlers WHERE id=%s AND is_active=1 LIMIT 1",
                (handler_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"ok": False, "error": "担当者が見つかりません。"}), 404
            handler_name = row["handler_name"]
            cur.execute(
                "UPDATE donation_receipts SET assigned_handler=%s WHERE assigned_handler=%s",
                (HANDLER_PLACEHOLDER, handler_name),
            )
            cur.execute("DELETE FROM donation_handlers WHERE id=%s", (handler_id,))
        conn.commit()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn:
            conn.close()

    return redirect(public_admin_path())


@app.route("/admin/delete/<int:receipt_id>", methods=["POST"])
@app.route("/donation/admin/delete/<int:receipt_id>", methods=["POST"])
@require_dashboard_login
def admin_delete(receipt_id: int):
    current_user = session.get("dashboard_user", "")
    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE donation_receipts
                SET is_deleted=1, deleted_at=NOW(), deleted_by=%s
                WHERE id=%s
                """,
                (current_user, receipt_id),
            )
        conn.commit()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn:
            conn.close()

    return redirect(public_admin_path())


@app.route("/admin/edit/<int:receipt_id>", methods=["GET", "POST"])
@app.route("/donation/admin/edit/<int:receipt_id>", methods=["GET", "POST"])
@require_dashboard_login
def admin_edit(receipt_id: int):
    def parse_dt(value: str) -> datetime:
        normalized = (value or "").strip().replace(" ", "T")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError("日時形式が不正です。") from exc

    conn = None
    handler_options = get_handler_options_fallback()
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        ensure_handlers_table(conn)
        handler_options = get_handler_options(conn)
        with conn.cursor() as cur:
            if request.method == "POST":
                donor_name = request.form.get("donor_name", "").strip() or "匿名"
                is_name_public = 1 if request.form.get("is_name_public", "0").strip() == "1" else 0
                assigned_handler = request.form.get("assigned_handler", "").strip()
                donor_postal_code = request.form.get("donor_postal_code", "").strip()
                donor_address = request.form.get("donor_address", "").strip()
                donor_email = request.form.get("donor_email", "").strip()
                amount_yen = request.form.get("amount_yen", "").strip()
                payment_method = request.form.get("payment_method", "").strip()
                status = request.form.get("status", "").strip()
                donated_at_raw = request.form.get("donated_at", "").strip()
                created_at_raw = request.form.get("created_at", "").strip()

                if (
                    not donor_postal_code
                    or not donor_address
                    or not donor_email
                    or not amount_yen
                    or not payment_method
                    or not status
                    or not donated_at_raw
                    or not created_at_raw
                ):
                    raise ValueError("必須項目が未入力です。")
                if payment_method not in ALLOWED_PAYMENT_METHODS:
                    raise ValueError("支払方法は 現金 / 振込 / クレジットカード から選択してください。")
                if assigned_handler not in set(handler_options):
                    raise ValueError("担当者の値が不正です。")

                donated_at = parse_dt(donated_at_raw)
                created_at = parse_dt(created_at_raw)

                cur.execute(
                    """
                    UPDATE donation_receipts
                    SET
                        donor_name=%s,
                        is_name_public=%s,
                        assigned_handler=%s,
                        donor_postal_code=%s,
                        donor_address=%s,
                        donor_email=%s,
                        amount_yen=%s,
                        payment_method=%s,
                        status=%s,
                        donated_at=%s,
                        created_at=%s
                    WHERE id=%s AND is_deleted=0
                    """,
                    (
                        donor_name,
                        is_name_public,
                        assigned_handler,
                        donor_postal_code,
                        donor_address,
                        donor_email,
                        amount_yen,
                        payment_method,
                        status,
                        donated_at,
                        created_at,
                        receipt_id,
                    ),
                )
                conn.commit()
                return redirect(public_admin_path())

            cur.execute(
                """
                SELECT
                    id,
                    certificate_no,
                    donor_name,
                    is_name_public,
                    assigned_handler,
                    donor_postal_code,
                    donor_address,
                    donor_email,
                    amount_yen,
                    payment_method,
                    status,
                    donated_at,
                    created_at
                FROM donation_receipts
                WHERE id=%s AND is_deleted=0
                LIMIT 1
                """,
                (receipt_id,),
            )
            row = cur.fetchone()
            if not row:
                abort(404, description="対象データが見つかりません。")
    except ValueError as exc:
        return render_template(
            "admin_edit.html",
            row=request.form,
            receipt_id=receipt_id,
            handler_options=handler_options,
            error=str(exc),
        ), 400
    except Exception as exc:
        return render_template(
            "admin_edit.html",
            row={},
            receipt_id=receipt_id,
            handler_options=handler_options,
            error=str(exc),
        ), 500
    finally:
        if conn:
            conn.close()

    return render_template(
        "admin_edit.html",
        row=row,
        receipt_id=receipt_id,
        handler_options=handler_options,
        error=None,
    )


@app.route("/account/login", methods=["GET", "POST"])
@app.route("/donation/account/login", methods=["GET", "POST"])
def donor_account_login():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        if not email or not password:
            return render_template("account_login.html", error="メールアドレスとパスワードを入力してください。"), 400

        conn = None
        try:
            conn = get_db_connection()
            ensure_donor_accounts_table(conn)
            donor = authenticate_donor(conn, email=email, raw_password=password)
            if not donor:
                return render_template("account_login.html", error="メールアドレスまたはパスワードが違います。"), 401
            session["donor_email"] = donor["email"]
            session["donor_name"] = donor.get("donor_name", "")
            return redirect(f"{PUBLIC_DONATION_PREFIX}/account")
        except Exception as exc:
            return render_template("account_login.html", error=str(exc)), 500
        finally:
            if conn:
                conn.close()

    if session.get("donor_email"):
        return redirect(f"{PUBLIC_DONATION_PREFIX}/account")
    return render_template("account_login.html", error=None)


@app.route("/account/logout", methods=["POST"])
@app.route("/donation/account/logout", methods=["POST"])
def donor_account_logout():
    session.pop("donor_email", None)
    session.pop("donor_name", None)
    return redirect(f"{PUBLIC_DONATION_PREFIX}/account/login")


@app.route("/account", methods=["GET"])
@app.route("/donation/account", methods=["GET"])
@require_donor_login
def donor_account_dashboard():
    donor_email = session.get("donor_email", "")
    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        rows = get_receipts_by_email(conn, donor_email)
        rows = sync_monthly_statuses_for_donor(conn, rows)
    except Exception as exc:
        return render_template(
            "account_dashboard.html",
            donor_email=donor_email,
            donor_name=session.get("donor_name", ""),
            rows=[],
            db_error=str(exc),
        )
    finally:
        if conn:
            conn.close()

    return render_template(
        "account_dashboard.html",
        donor_email=donor_email,
        donor_name=session.get("donor_name", ""),
        rows=rows,
        db_error=None,
    )


def get_owned_receipt_for_donor(conn, receipt_id: int, donor_email: str) -> dict | None:
    row = get_receipt_by_id(conn, receipt_id)
    if not row:
        return None
    if (row.get("donor_email") or "").lower() != (donor_email or "").lower():
        return None
    return row


@app.route("/account/subscription/<int:receipt_id>/amount", methods=["POST"])
@app.route("/donation/account/subscription/<int:receipt_id>/amount", methods=["POST"])
@require_donor_login
def donor_update_subscription_amount(receipt_id: int):
    donor_email = session.get("donor_email", "")
    raw_amount = request.form.get("amount_yen", "").strip()
    try:
        amount_yen = parse_amount_yen(raw_amount)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    try:
        validate_stripe_ready()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        row = get_owned_receipt_for_donor(conn, receipt_id, donor_email)
        if not row:
            return jsonify({"ok": False, "error": "対象データが見つかりません。"}), 404
        if row.get("donation_plan") != "monthly":
            return jsonify({"ok": False, "error": "月次寄付データではありません。"}), 400
        subscription_id = (row.get("stripe_subscription_id") or "").strip()
        if not subscription_id:
            return jsonify({"ok": False, "error": "サブスクリプションIDが未登録です。"}), 400

        subscription = stripe.Subscription.retrieve(subscription_id)
        subscription_status = (subscription.get("status") or "").strip()
        if subscription_status in {"canceled", "incomplete_expired", "unpaid"}:
            update_receipt_payment_status(
                conn,
                receipt_id=receipt_id,
                status="subscription_canceled",
                subscription_id=subscription_id,
            )
            return jsonify({"ok": False, "error": "この継続寄付は既に解約済みです。"}), 400

        items = subscription.get("items", {}).get("data", [])
        if not items:
            return jsonify({"ok": False, "error": "サブスクリプション項目が見つかりません。"}), 400
        item_id = items[0]["id"]

        stripe.Subscription.modify(
            subscription_id,
            items=[
                {
                    "id": item_id,
                    "price_data": {
                        "currency": STRIPE_CURRENCY,
                        "unit_amount": amount_yen,
                        "recurring": {"interval": "month"},
                        "product_data": {"name": f"継続寄付 ({row['certificate_no']})"},
                    },
                }
            ],
            proration_behavior="none",
        )

        update_receipt_payment_status(
            conn,
            receipt_id=receipt_id,
            status="subscription_active",
            subscription_id=subscription_id,
            amount_yen=amount_yen,
        )
        return jsonify({"ok": True, "amount_yen": amount_yen}), 200
    except Exception as exc:
        app.logger.exception("Failed to update monthly donation amount")
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/account/subscription/<int:receipt_id>/cancel", methods=["POST"])
@app.route("/donation/account/subscription/<int:receipt_id>/cancel", methods=["POST"])
@require_donor_login
def donor_cancel_subscription(receipt_id: int):
    donor_email = session.get("donor_email", "")
    try:
        validate_stripe_ready()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        row = get_owned_receipt_for_donor(conn, receipt_id, donor_email)
        if not row:
            return jsonify({"ok": False, "error": "対象データが見つかりません。"}), 404
        if row.get("donation_plan") != "monthly":
            return jsonify({"ok": False, "error": "月次寄付データではありません。"}), 400
        subscription_id = (row.get("stripe_subscription_id") or "").strip()
        if not subscription_id:
            return jsonify({"ok": False, "error": "サブスクリプションIDが未登録です。"}), 400

        subscription = stripe.Subscription.retrieve(subscription_id)
        subscription_status = (subscription.get("status") or "").strip()
        if subscription_status in {"canceled", "incomplete_expired", "unpaid"}:
            update_receipt_payment_status(
                conn,
                receipt_id=receipt_id,
                status="subscription_canceled",
                subscription_id=subscription_id,
            )
            return jsonify({"ok": True, "already_canceled": True}), 200

        stripe.Subscription.modify(subscription_id, cancel_at_period_end=True)
        update_receipt_payment_status(
            conn,
            receipt_id=receipt_id,
            status="subscription_cancel_scheduled",
            subscription_id=subscription_id,
        )
        return jsonify({"ok": True}), 200
    except Exception as exc:
        app.logger.exception("Failed to cancel monthly donation")
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/account/subscription/<int:receipt_id>/restart", methods=["POST"])
@app.route("/donation/account/subscription/<int:receipt_id>/restart", methods=["POST"])
@require_donor_login
def donor_restart_subscription(receipt_id: int):
    donor_email = session.get("donor_email", "")
    try:
        validate_stripe_ready()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        row = get_owned_receipt_for_donor(conn, receipt_id, donor_email)
        if not row:
            return jsonify({"ok": False, "error": "対象データが見つかりません。"}), 404
        if row.get("donation_plan") != "monthly":
            return jsonify({"ok": False, "error": "月次寄付データではありません。"}), 400
        if normalize_payment_method(row.get("payment_method", "")) != "credit_card":
            return jsonify({"ok": False, "error": "クレジットカード以外は再開できません。"}), 400
        if row.get("status") not in {"subscription_canceled", "subscription_cancel_scheduled"}:
            return jsonify({"ok": False, "error": "解約済みデータのみ再開できます。"}), 400

        amount_yen = parse_amount_yen(row["amount_yen"])
        success_url = STRIPE_SUCCESS_URL or f"{build_public_url('/payment/success')}?session_id={{CHECKOUT_SESSION_ID}}"
        cancel_url = STRIPE_CANCEL_URL or build_public_url("/payment/cancel")

        session_data = stripe.checkout.Session.create(
            mode="subscription",
            payment_method_types=["card"],
            locale="ja",
            customer_email=row["donor_email"],
            metadata={
                "receipt_id": str(row["id"]),
                "certificate_no": row["certificate_no"],
                "donor_email": row["donor_email"],
                "donor_name": row["donor_name"],
                "donation_plan": "monthly",
            },
            line_items=[
                {
                    "quantity": 1,
                    "price_data": {
                        "currency": STRIPE_CURRENCY,
                        "unit_amount": amount_yen,
                        "recurring": {"interval": "month"},
                        "product_data": {"name": f"継続寄付 ({row['certificate_no']})"},
                    },
                }
            ],
            success_url=success_url,
            cancel_url=cancel_url,
        )

        update_receipt_payment_status(
            conn,
            receipt_id=receipt_id,
            status="checkout_created",
            checkout_session_id=session_data.id,
        )
        return jsonify({"ok": True, "checkout_url": session_data.url}), 200
    except Exception as exc:
        app.logger.exception("Failed to restart monthly donation")
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/db-check", methods=["GET"])
def db_check():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok, DATABASE() AS db, CURRENT_USER() AS user")
            row = cur.fetchone()

            cur.execute("SHOW TABLES LIKE 'donation_receipts'")
            table_exists = cur.fetchone() is not None

        return jsonify(
            {
                "ok": True,
                "db_result": row,
                "donation_receipts_exists": table_exists,
            }
        ), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/db-check/receipts", methods=["GET"])
def db_check_receipts():
    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total FROM donation_receipts WHERE is_deleted=0")
            total = cur.fetchone()["total"]
            cur.execute("SELECT COUNT(*) AS total_deleted FROM donation_receipts WHERE is_deleted=1")
            total_deleted = cur.fetchone()["total_deleted"]

            cur.execute(
                """
                SELECT
                    id,
                    certificate_no,
                    donor_name,
                    donor_postal_code,
                    donor_address,
                    donor_email,
                    amount_yen,
                    payment_method,
                    status,
                    is_checked,
                    checked_at,
                    checked_by,
                    is_deleted,
                    deleted_at,
                    deleted_by,
                    donated_at,
                    created_at
                FROM donation_receipts
                WHERE is_deleted=0
                ORDER BY id DESC
                LIMIT 20
                """
            )
            rows = cur.fetchall()

        return jsonify({"ok": True, "total": total, "total_deleted": total_deleted, "rows": rows}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn:
            conn.close()


def validate_stripe_ready() -> None:
    if not STRIPE_SECRET_KEY or not STRIPE_PUBLISHABLE_KEY:
        if STRIPE_MODE == "live":
            raise RuntimeError(
                "Stripe本番設定が未完了です。STRIPE_LIVE_SECRET_KEY / STRIPE_LIVE_PUBLISHABLE_KEY を設定してください。"
            )
        raise RuntimeError(
            "Stripeテスト設定が未完了です。STRIPE_TEST_SECRET_KEY / STRIPE_TEST_PUBLISHABLE_KEY を設定してください。"
        )
    if STRIPE_MODE == "test":
        if not STRIPE_SECRET_KEY.startswith("sk_test_"):
            raise RuntimeError("テストモードでは STRIPE_TEST_SECRET_KEY に sk_test_ を設定してください。")
        if not STRIPE_PUBLISHABLE_KEY.startswith("pk_test_"):
            raise RuntimeError("テストモードでは STRIPE_TEST_PUBLISHABLE_KEY に pk_test_ を設定してください。")
        return
    if not STRIPE_SECRET_KEY.startswith("sk_live_"):
        raise RuntimeError("本番モードでは STRIPE_LIVE_SECRET_KEY に sk_live_ を設定してください。")
    if not STRIPE_PUBLISHABLE_KEY.startswith("pk_live_"):
        raise RuntimeError("本番モードでは STRIPE_LIVE_PUBLISHABLE_KEY に pk_live_ を設定してください。")


@app.route("/api/stripe/checkout-session", methods=["POST"])
@app.route("/donation/api/stripe/checkout-session", methods=["POST"])
def create_checkout_session():
    try:
        validate_stripe_ready()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

    payload = request.get_json(silent=True) or {}
    receipt_id_raw = payload.get("receipt_id")
    certificate_no = str(payload.get("certificate_no", "")).strip()
    receipt_id = None
    if receipt_id_raw not in (None, ""):
        try:
            receipt_id = int(str(receipt_id_raw))
        except Exception:
            return jsonify({"ok": False, "error": "receipt_id は数値で指定してください。"}), 400
    if receipt_id is None and not certificate_no:
        return jsonify({"ok": False, "error": "receipt_id または certificate_no を指定してください。"}), 400

    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        row = get_receipt_by_id(conn, receipt_id) if receipt_id is not None else get_receipt_by_certificate_no(conn, certificate_no)
        if not row:
            return jsonify({"ok": False, "error": "対象の寄付データが見つかりません。"}), 404
        receipt_id = int(row["id"])
        if normalize_payment_method(row["payment_method"]) != "credit_card":
            return jsonify({"ok": False, "error": "クレジットカード決済のデータではありません。"}), 400
        amount_yen = parse_amount_yen(row["amount_yen"])
        donation_plan = normalize_donation_plan(row.get("donation_plan", "one_time"))

        success_url = STRIPE_SUCCESS_URL or f"{build_public_url('/payment/success')}?session_id={{CHECKOUT_SESSION_ID}}"
        cancel_url = STRIPE_CANCEL_URL or build_public_url("/payment/cancel")

        checkout_kwargs = {
            "payment_method_types": ["card"],
            "locale": "ja",
            "customer_email": row["donor_email"],
            "metadata": {
                "receipt_id": str(row["id"]),
                "certificate_no": row["certificate_no"],
                "donor_email": row["donor_email"],
                "donor_name": row["donor_name"],
                "donation_plan": donation_plan,
            },
            "line_items": [
                {
                    "quantity": 1,
                    "price_data": {
                        "currency": STRIPE_CURRENCY,
                        "unit_amount": amount_yen,
                        "product_data": {"name": f"寄付金 ({row['certificate_no']})"},
                    },
                }
            ],
            "success_url": success_url,
            "cancel_url": cancel_url,
        }
        if donation_plan == "monthly":
            checkout_kwargs["mode"] = "subscription"
            checkout_kwargs["line_items"][0]["price_data"]["recurring"] = {"interval": "month"}
        else:
            checkout_kwargs["mode"] = "payment"

        session_data = stripe.checkout.Session.create(**checkout_kwargs)

        update_receipt_payment_status(
            conn,
            receipt_id=receipt_id,
            status="checkout_created",
            checkout_session_id=session_data.id,
        )

        return jsonify(
            {
                "ok": True,
                "checkout_url": session_data.url,
                "session_id": session_data.id,
                "donation_plan": donation_plan,
            }
        ), 200
    except Exception as exc:
        app.logger.exception("Failed to create Stripe Checkout session")
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/api/stripe/webhook", methods=["POST"])
@app.route("/donation/api/stripe/webhook", methods=["POST"])
def stripe_webhook():
    try:
        validate_stripe_ready()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500
    if not STRIPE_WEBHOOK_SECRET:
        return jsonify({"ok": False, "error": "STRIPE_WEBHOOK_SECRET が未設定です。"}), 500

    payload = request.get_data()
    sig_header = request.headers.get("Stripe-Signature", "")
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid payload"}), 400
    except stripe.error.SignatureVerificationError:
        return jsonify({"ok": False, "error": "Invalid signature"}), 400

    event_type = event["type"]
    obj = event["data"]["object"]
    event_id = event.get("id")

    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)

        receipt_id = None
        metadata = obj.get("metadata") or {}
        if metadata.get("receipt_id"):
            try:
                receipt_id = int(metadata["receipt_id"])
            except Exception:
                receipt_id = None

        if event_type == "checkout.session.completed":
            payment_intent_id = obj.get("payment_intent")
            checkout_session_id = obj.get("id")
            subscription_id = obj.get("subscription")
            customer_id = obj.get("customer")
            payment_status = obj.get("payment_status", "")
            metadata_plan = normalize_donation_plan((metadata.get("donation_plan") or "one_time"))
            if receipt_id:
                row = get_receipt_by_id(conn, receipt_id)
                if row and row.get("stripe_last_event_id") != event_id:
                    status = "paid"
                    if metadata_plan == "monthly":
                        status = "subscription_active" if payment_status == "paid" else "subscription_started"
                    update_receipt_payment_status(
                        conn,
                        receipt_id=receipt_id,
                        status=status,
                        checkout_session_id=checkout_session_id,
                        payment_intent_id=payment_intent_id,
                        subscription_id=subscription_id,
                        customer_id=customer_id,
                        stripe_event_id=event_id,
                    )

        elif event_type == "payment_intent.succeeded":
            payment_intent_id = obj.get("id")
            if not receipt_id and payment_intent_id:
                row = get_receipt_by_stripe_payment_intent(conn, payment_intent_id)
                receipt_id = row["id"] if row else None
            if receipt_id:
                row = get_receipt_by_id(conn, receipt_id)
                if row and row.get("stripe_last_event_id") != event_id:
                    update_receipt_payment_status(
                        conn,
                        receipt_id=receipt_id,
                        status="paid",
                        payment_intent_id=payment_intent_id,
                        stripe_event_id=event_id,
                    )

        elif event_type == "payment_intent.payment_failed":
            payment_intent_id = obj.get("id")
            if not receipt_id and payment_intent_id:
                row = get_receipt_by_stripe_payment_intent(conn, payment_intent_id)
                receipt_id = row["id"] if row else None
            if receipt_id:
                row = get_receipt_by_id(conn, receipt_id)
                if row and row.get("stripe_last_event_id") != event_id:
                    update_receipt_payment_status(
                        conn,
                        receipt_id=receipt_id,
                        status="payment_failed",
                        payment_intent_id=payment_intent_id,
                        stripe_event_id=event_id,
                    )

        elif event_type == "invoice.paid":
            subscription_id = obj.get("subscription")
            if subscription_id:
                row = get_receipt_by_stripe_subscription(conn, subscription_id)
                if row and row.get("stripe_last_event_id") != event_id:
                    update_receipt_payment_status(
                        conn,
                        receipt_id=row["id"],
                        status="subscription_active",
                        subscription_id=subscription_id,
                        stripe_event_id=event_id,
                    )

        elif event_type == "invoice.payment_failed":
            subscription_id = obj.get("subscription")
            if subscription_id:
                row = get_receipt_by_stripe_subscription(conn, subscription_id)
                if row and row.get("stripe_last_event_id") != event_id:
                    update_receipt_payment_status(
                        conn,
                        receipt_id=row["id"],
                        status="subscription_payment_failed",
                        subscription_id=subscription_id,
                        stripe_event_id=event_id,
                    )

        elif event_type == "customer.subscription.deleted":
            subscription_id = obj.get("id")
            if subscription_id:
                row = get_receipt_by_stripe_subscription(conn, subscription_id)
                if row and row.get("stripe_last_event_id") != event_id:
                    update_receipt_payment_status(
                        conn,
                        receipt_id=row["id"],
                        status="subscription_canceled",
                        subscription_id=subscription_id,
                        stripe_event_id=event_id,
                    )

        elif event_type == "customer.subscription.updated":
            subscription_id = obj.get("id")
            if subscription_id:
                row = get_receipt_by_stripe_subscription(conn, subscription_id)
                if row and row.get("stripe_last_event_id") != event_id:
                    stripe_status = (obj.get("status") or "").strip()
                    cancel_at_period_end = bool(obj.get("cancel_at_period_end"))
                    next_status = None
                    if stripe_status == "canceled":
                        next_status = "subscription_canceled"
                    elif cancel_at_period_end:
                        next_status = "subscription_cancel_scheduled"
                    elif stripe_status == "active":
                        next_status = "subscription_active"
                    if next_status:
                        update_receipt_payment_status(
                            conn,
                            receipt_id=row["id"],
                            status=next_status,
                            subscription_id=subscription_id,
                            stripe_event_id=event_id,
                        )

    except Exception:
        app.logger.exception("Failed to process Stripe webhook")
        return jsonify({"ok": False, "error": "Webhook handling failed"}), 500
    finally:
        if conn:
            conn.close()

    return jsonify({"ok": True, "received": True}), 200


@app.route("/submit", methods=["POST"])
@app.route("/submit/", methods=["POST"])
@app.route("/donation/submit", methods=["POST"])
@app.route("/donation/submit/", methods=["POST"])
def submit():
    name = request.form.get("name", "匿名").strip() or "匿名"
    is_name_public = 1 if request.form.get("name_public_ok", "0").strip() == "1" else 0
    postal_code = request.form.get("postal_code", "").strip()
    address = request.form.get("address", "").strip()
    email = request.form.get("email", "").strip().lower()
    account_password = request.form.get("account_password", "")
    donation_plan = normalize_donation_plan(request.form.get("donation_plan", "one_time"))
    amount = request.form.get("amount", "").strip()
    payment_method = request.form.get("payment_method", "未指定").strip() or "未指定"

    if not postal_code or not address or not email or not amount:
        abort(400, description="postal_code / address / email / amount は必須です。")
    if payment_method not in ALLOWED_PAYMENT_METHODS:
        abort(400, description="payment_method は 現金 / 振込 / クレジットカード のみ指定できます。")
    if donation_plan not in ALLOWED_DONATION_PLANS:
        abort(400, description="donation_plan は one_time / monthly のみ指定できます。")
    if donation_plan == "monthly" and payment_method != "クレジットカード":
        abort(400, description="継続寄付（月次）はクレジットカードのみ対応です。")
    if not account_password:
        abort(400, description="ログイン用パスワードは必須です。")
    try:
        validate_account_password(account_password)
    except ValueError as exc:
        abort(400, description=str(exc))

    donated_at = datetime.now(JST).replace(tzinfo=None)

    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        ensure_donor_accounts_table(conn)
        receipt_id, certificate_no = create_receipt_record(
            conn=conn,
            name=name,
            is_name_public=is_name_public,
            postal_code=postal_code,
            address=address,
            email=email,
            amount=amount,
            payment_method=payment_method,
            donation_plan=donation_plan,
            donated_at=donated_at,
        )
        upsert_donor_account(conn, email=email, donor_name=name, raw_password=account_password)
    except Exception as exc:
        app.logger.exception("Failed to create receipt record")
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    payment_kind = normalize_payment_method(payment_method)
    token = None
    if payment_kind == "credit_card":
        pdf_bytes = build_receipt_pdf(
            name=name,
            address=address,
            amount=amount,
            payment_method=payment_method,
            donated_at=donated_at,
            certificate_no=certificate_no,
        )
        token = save_receipt(pdf_bytes)
        credit_card_input_url = build_credit_card_input_url(certificate_no, receipt_id, donation_plan=donation_plan)
        try:
            send_receipt_email(
                name=name,
                email=email,
                pdf_bytes=pdf_bytes,
                payment_method=payment_method,
                credit_card_input_url=credit_card_input_url,
            )
        except Exception as exc:
            app.logger.exception("Failed to send receipt email for credit-card donation")
            conn = None
            try:
                conn = get_db_connection()
                update_receipt_status(conn, receipt_id=receipt_id, status="mail_failed", token=token)
            except Exception:
                pass
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
            return jsonify({"ok": False, "error": str(exc)}), 502
        issue_status = "subscription_awaiting_payment" if donation_plan == "monthly" else "awaiting_payment"
    else:
        issue_status = "pending_confirmation"
    conn = None
    try:
        conn = get_db_connection()
        if payment_kind == "credit_card":
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE donation_receipts
                    SET
                        status=%s,
                        download_token=COALESCE(%s, download_token),
                        is_checked=1,
                        checked_at=NOW(),
                        checked_by='system_credit_auto'
                    WHERE id=%s
                    """,
                    (issue_status, token, receipt_id),
                )
            conn.commit()
        else:
            update_receipt_status(conn, receipt_id=receipt_id, status=issue_status)
    except Exception:
        app.logger.exception("Failed to update receipt status")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    if payment_kind == "credit_card":
        return redirect(credit_card_input_url)

    return render_template(
        "thanks.html",
        name=name,
        token="",
        certificate_no=certificate_no,
        payment_method=payment_method,
        donation_plan=donation_plan,
        payment_kind=payment_kind,
        bank_transfer_info=BANK_TRANSFER_INFO,
    )


@app.route("/payment/credit-card", methods=["GET"])
@app.route("/donation/payment/credit-card", methods=["GET"])
def credit_card_input_page():
    certificate_no = request.args.get("certificate_no", "").strip()
    receipt_id = request.args.get("receipt_id", "").strip()
    donation_plan = normalize_donation_plan(request.args.get("donation_plan", "one_time"))
    return render_template(
        "credit_card.html",
        certificate_no=certificate_no,
        receipt_id=receipt_id,
        donation_plan=donation_plan,
    )


@app.route("/payment/success", methods=["GET"])
@app.route("/donation/payment/success", methods=["GET"])
def payment_success():
    session_id = request.args.get("session_id", "").strip()
    certificate_no = ""
    donor_name = "ご寄付者"
    payment_status = ""
    download_token = ""
    donation_plan = "one_time"

    if session_id and STRIPE_SECRET_KEY:
        try:
            session_data = stripe.checkout.Session.retrieve(session_id)
            metadata = session_data.get("metadata") or {}
            certificate_no = metadata.get("certificate_no", "")
            donor_name = metadata.get("donor_name", donor_name)
            payment_status = session_data.get("payment_status", "")
            donation_plan = normalize_donation_plan(metadata.get("donation_plan", "one_time"))
            receipt_id_raw = metadata.get("receipt_id", "")
            if receipt_id_raw:
                try:
                    receipt_id = int(receipt_id_raw)
                    conn = get_db_connection()
                    try:
                        ensure_receipts_table(conn)
                        row = get_receipt_by_id(conn, receipt_id)
                        if row:
                            download_token = row.get("download_token", "")
                    finally:
                        conn.close()
                except Exception:
                    app.logger.exception("Failed to load receipt token for success page")
        except Exception:
            app.logger.exception("Failed to retrieve checkout session for success page")

    return render_template(
        "payment_success.html",
        session_id=session_id,
        certificate_no=certificate_no,
        donor_name=donor_name,
        payment_status=payment_status,
        download_token=download_token,
        donation_plan=donation_plan,
    )


@app.route("/payment/cancel", methods=["GET"])
@app.route("/donation/payment/cancel", methods=["GET"])
def payment_cancel():
    return render_template("payment_cancel.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
