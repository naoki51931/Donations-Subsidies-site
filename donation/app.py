import io
import os
import smtplib
from functools import wraps
from pathlib import Path
from datetime import datetime
from email.message import EmailMessage
from urllib.parse import quote
from uuid import uuid4

import pymysql
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
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfgen import canvas

load_dotenv()

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


def require_basic_auth(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        auth = request.authorization
        if auth and auth.type == "basic" and auth.username == ADMIN_USERNAME and auth.password == ADMIN_PASSWORD:
            return view_func(*args, **kwargs)
        return (
            "認証が必要です。",
            401,
            {"WWW-Authenticate": 'Basic realm="Donation Site", charset="UTF-8"'},
        )

    return wrapped


def public_admin_path(path: str = "") -> str:
    base = PUBLIC_DONATION_PREFIX or ""
    if path and not path.startswith("/"):
        path = f"/{path}"
    return f"{base}/admin{path}"


@app.route("/", methods=["GET"])
@app.route("/index.html", methods=["GET"])
@require_basic_auth
def top_page():
    return send_from_directory(str(PUBLIC_WEB_DIR), "index.html")


@app.route("/style.css", methods=["GET"])
@app.route("/main.js", methods=["GET"])
@app.route("/jquery.min.js", methods=["GET"])
@app.route("/favicon.ico", methods=["GET"])
@require_basic_auth
def top_assets_root():
    return send_from_directory(str(PUBLIC_WEB_DIR), request.path.lstrip("/"))


@app.route("/images/<path:filename>", methods=["GET"])
@require_basic_auth
def top_assets_images(filename: str):
    return send_from_directory(str(PUBLIC_WEB_DIR / "images"), filename)


@app.route("/meishi/<path:filename>", methods=["GET"])
@require_basic_auth
def top_assets_meishi(filename: str):
    return send_from_directory(str(PUBLIC_WEB_DIR / "meishi"), filename)


@app.route("/chirashi/<path:filename>", methods=["GET"])
@require_basic_auth
def top_assets_chirashi(filename: str):
    return send_from_directory(str(PUBLIC_WEB_DIR / "chirashi"), filename)


@app.route("/donation", methods=["GET"])
@app.route("/donation/", methods=["GET"])
@require_basic_auth
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
    text.textLine(f"日付：{donated_at.strftime('%Y年%m月%d日 %H:%M:%S')}")
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


def build_credit_card_input_url(certificate_no: str) -> str:
    base_url = CREDIT_CARD_INPUT_URL or f"{request.url_root.rstrip('/')}/donation/payment/credit-card"
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}certificate_no={quote(certificate_no)}"


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
    )


def ensure_receipts_table(conn) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS donation_receipts (
        id BIGINT NOT NULL AUTO_INCREMENT,
        certificate_no VARCHAR(32) NOT NULL,
        donor_name VARCHAR(255) NOT NULL,
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
    conn.commit()


def create_receipt_record(
    conn,
    name: str,
    postal_code: str,
    address: str,
    email: str,
    amount: str,
    payment_method: str,
    donated_at: datetime,
) -> tuple[int, str]:
    with conn.cursor() as cur:
        # certificate_no is VARCHAR(32), so keep temporary value within 32 chars.
        temp_certificate_no = f"TEMP-{uuid4().hex[:27]}"
        cur.execute(
            """
            INSERT INTO donation_receipts (
                certificate_no, donor_name, donor_postal_code, donor_address, donor_email, amount_yen,
                payment_method, donated_at, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'created')
            """,
            (temp_certificate_no, name, postal_code, address, email, amount, payment_method, donated_at),
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


def save_receipt(pdf_bytes: bytes) -> str:
    # Keep files for a day and clean older ones opportunistically.
    now_ts = datetime.now().timestamp()
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

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
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
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total FROM donation_receipts WHERE is_deleted=0")
            total = cur.fetchone()["total"]
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
                LIMIT 100
                """
            )
            rows = cur.fetchall()
    except Exception as exc:
        return render_template(
            "admin_dashboard.html",
            total=0,
            rows=[],
            current_user=session.get("dashboard_user", ""),
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
                    UPDATE donation_receipts
                    SET is_checked=1, checked_at=NOW(), checked_by=%s
                    WHERE id=%s AND is_deleted=0
                    """,
                    (current_user, receipt_id),
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
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        with conn.cursor() as cur:
            if request.method == "POST":
                donor_name = request.form.get("donor_name", "").strip() or "匿名"
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

                donated_at = parse_dt(donated_at_raw)
                created_at = parse_dt(created_at_raw)

                cur.execute(
                    """
                    UPDATE donation_receipts
                    SET
                        donor_name=%s,
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
        return render_template("admin_edit.html", row=request.form, receipt_id=receipt_id, error=str(exc)), 400
    except Exception as exc:
        return render_template("admin_edit.html", row={}, receipt_id=receipt_id, error=str(exc)), 500
    finally:
        if conn:
            conn.close()

    return render_template("admin_edit.html", row=row, receipt_id=receipt_id, error=None)


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


@app.route("/submit", methods=["POST"])
@app.route("/submit/", methods=["POST"])
@app.route("/donation/submit", methods=["POST"])
@app.route("/donation/submit/", methods=["POST"])
def submit():
    name = request.form.get("name", "匿名").strip() or "匿名"
    postal_code = request.form.get("postal_code", "").strip()
    address = request.form.get("address", "").strip()
    email = request.form.get("email", "").strip()
    amount = request.form.get("amount", "").strip()
    payment_method = request.form.get("payment_method", "未指定").strip() or "未指定"

    if not postal_code or not address or not email or not amount:
        abort(400, description="postal_code / address / email / amount は必須です。")
    if payment_method not in ALLOWED_PAYMENT_METHODS:
        abort(400, description="payment_method は 現金 / 振込 / クレジットカード のみ指定できます。")

    donated_at = datetime.now()

    conn = None
    try:
        conn = get_db_connection()
        ensure_receipts_table(conn)
        receipt_id, certificate_no = create_receipt_record(
            conn=conn,
            name=name,
            postal_code=postal_code,
            address=address,
            email=email,
            amount=amount,
            payment_method=payment_method,
            donated_at=donated_at,
        )
    except Exception as exc:
        app.logger.exception("Failed to create receipt record")
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    pdf_bytes = build_receipt_pdf(
        name=name,
        address=address,
        amount=amount,
        payment_method=payment_method,
        donated_at=donated_at,
        certificate_no=certificate_no,
    )

    credit_card_input_url = build_credit_card_input_url(certificate_no)

    try:
        send_receipt_email(
            name=name,
            email=email,
            pdf_bytes=pdf_bytes,
            payment_method=payment_method,
            credit_card_input_url=credit_card_input_url,
        )
    except Exception as exc:
        app.logger.exception("Failed to send receipt email")
        conn = None
        try:
            conn = get_db_connection()
            update_receipt_status(conn, receipt_id=receipt_id, status="mail_failed")
        except Exception:
            pass
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        return jsonify({"ok": False, "error": str(exc)}), 502

    token = save_receipt(pdf_bytes)
    conn = None
    try:
        conn = get_db_connection()
        update_receipt_status(conn, receipt_id=receipt_id, status="issued", token=token)
    except Exception:
        app.logger.exception("Failed to update receipt status")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    payment_kind = normalize_payment_method(payment_method)
    if payment_kind == "credit_card":
        return redirect(credit_card_input_url)

    return render_template(
        "thanks.html",
        name=name,
        token=token,
        certificate_no=certificate_no,
        payment_method=payment_method,
        payment_kind=payment_kind,
        bank_transfer_info=BANK_TRANSFER_INFO,
    )


@app.route("/payment/credit-card", methods=["GET"])
@app.route("/donation/payment/credit-card", methods=["GET"])
def credit_card_input_page():
    certificate_no = request.args.get("certificate_no", "").strip()
    return render_template("credit_card.html", certificate_no=certificate_no)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
