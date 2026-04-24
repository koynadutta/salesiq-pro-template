"""
SalesIQ PRO  —  app.py
All features unlocked. Customize branding at the top of this file.
"""

import os
import json
import sqlite3
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from functools import wraps
from flask import (
    Flask, render_template, request, session,
    redirect, url_for, flash,
)
from apscheduler.schedulers.background import BackgroundScheduler

# ============================================================
# BRANDING CONFIGURATION — replace per client deployment
# ============================================================
COMPANY_NAME = "SalesIQ"
APP_TITLE    = "AI Sales Intelligence"
# ============================================================

# Credentials — replace per client deployment
CREDENTIALS = {
    "client": "temppass123",
}

DATABASE = "salesiq.db"

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "salesiq-pro-2024-secret-key")

# ── Jinja helpers ─────────────────────────────────────────────────────────────

@app.template_filter("currency")
def currency_filter(v):
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return "$0.00"

@app.template_filter("intcomma")
def intcomma_filter(v):
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return "0"

@app.context_processor
def inject_globals():
    return {
        "company_name": COMPANY_NAME,
        "app_title":    APP_TITLE,
        "current_user": session.get("user", ""),
        "today":        datetime.now().strftime("%b %d, %Y"),
    }

# ── Database ──────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sales_data (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            date           TEXT    NOT NULL,
            customer_email TEXT,
            customer_name  TEXT,
            order_id       TEXT,
            product        TEXT,
            category       TEXT,
            quantity       INTEGER DEFAULT 1,
            unit_price     REAL    DEFAULT 0,
            total_amount   REAL    NOT NULL,
            source         TEXT    DEFAULT 'upload',
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS shopify_config (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            store_url   TEXT NOT NULL,
            api_token   TEXT NOT NULL,
            shop_name   TEXT,
            last_synced TEXT,
            connected   INTEGER DEFAULT 0,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sales_date  ON sales_data(date);
        CREATE INDEX IF NOT EXISTS idx_sales_email ON sales_data(customer_email);
    """)

    defaults = {
        "email_alerts_enabled":     "false",
        "alert_email":              "",
        "revenue_threshold":        "10000",
        "order_threshold":          "100",
        "auto_sync_enabled":        "false",
        "auto_sync_interval_hours": "24",
        "smtp_host":                "",
        "smtp_port":                "587",
        "smtp_user":                "",
        "smtp_password":            "",
    }
    for k, v in defaults.items():
        conn.execute("INSERT OR IGNORE INTO settings (key,value) VALUES (?,?)", (k, v))

    conn.commit()
    conn.close()

def get_setting(key, default=None):
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default

def set_setting(key, value):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key, str(value)))
    conn.commit()
    conn.close()

# ── Auth ──────────────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated

# ── Auth routes ───────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def index():
    return redirect(url_for("dashboard"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if "user" in session:
        return redirect(url_for("dashboard"))
    error = None
    if request.method == "POST":
        u = request.form.get("username", "").strip()
        p = request.form.get("password", "")
        if CREDENTIALS.get(u) == p:
            session["user"] = u
            return redirect(url_for("dashboard"))
        error = "Invalid credentials. Please try again."
    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.route("/dashboard")
@login_required
def dashboard():
    conn = get_db()

    total_revenue = conn.execute(
        "SELECT COALESCE(SUM(total_amount),0) AS v FROM sales_data"
    ).fetchone()["v"]

    total_orders = conn.execute(
        "SELECT COUNT(*) AS v FROM sales_data"
    ).fetchone()["v"]

    total_customers = conn.execute(
        "SELECT COUNT(DISTINCT customer_email) AS v FROM sales_data"
        " WHERE customer_email IS NOT NULL AND customer_email != ''"
    ).fetchone()["v"]

    this_month = datetime.now().strftime("%Y-%m")
    month_revenue = conn.execute(
        "SELECT COALESCE(SUM(total_amount),0) AS v FROM sales_data WHERE date LIKE ?",
        (f"{this_month}%",),
    ).fetchone()["v"]

    thirty_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    daily_rows = conn.execute(
        """SELECT date, SUM(total_amount) AS revenue, COUNT(*) AS orders
           FROM sales_data WHERE date >= ?
           GROUP BY date ORDER BY date""",
        (thirty_ago,),
    ).fetchall()

    top_products = conn.execute(
        """SELECT product, SUM(total_amount) AS revenue, COUNT(*) AS orders
           FROM sales_data WHERE product IS NOT NULL AND product != ''
           GROUP BY product ORDER BY revenue DESC LIMIT 6""",
    ).fetchall()

    top_categories = conn.execute(
        """SELECT category, SUM(total_amount) AS revenue, COUNT(*) AS orders
           FROM sales_data WHERE category IS NOT NULL AND category != ''
           GROUP BY category ORDER BY revenue DESC LIMIT 6""",
    ).fetchall()

    conn.close()

    avg_order = total_revenue / total_orders if total_orders > 0 else 0

    return render_template(
        "dashboard.html",
        has_data=total_orders > 0,
        total_revenue=total_revenue,
        total_orders=total_orders,
        total_customers=total_customers,
        month_revenue=month_revenue,
        avg_order=avg_order,
        chart_dates=json.dumps([r["date"] for r in daily_rows]),
        chart_revenue=json.dumps([round(r["revenue"], 2) for r in daily_rows]),
        top_products=top_products,
        top_categories=top_categories,
    )

# ── Sales Forecast ────────────────────────────────────────────────────────────

@app.route("/forecast")
@login_required
def forecast():
    conn = get_db()
    ninety_ago = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    rows = conn.execute(
        """SELECT date, SUM(total_amount) AS revenue, COUNT(*) AS orders
           FROM sales_data WHERE date >= ?
           GROUP BY date ORDER BY date""",
        (ninety_ago,),
    ).fetchall()
    conn.close()

    if not rows:
        return render_template("forecast.html", has_data=False)

    df = pd.DataFrame([dict(r) for r in rows])
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    df = df.reindex(pd.date_range(df.index.min(), df.index.max()), fill_value=0)

    f7, f14, f30 = _build_forecasts(df)

    hist_dates = [d.strftime("%Y-%m-%d") for d in df.index[-30:]]
    hist_rev   = [round(float(v), 2) for v in df["revenue"].iloc[-30:]]

    return render_template(
        "forecast.html",
        has_data=True,
        forecast_7=f7,
        forecast_14=f14,
        forecast_30=f30,
        total_7=round(sum(x["revenue"] for x in f7), 2),
        total_14=round(sum(x["revenue"] for x in f14), 2),
        total_30=round(sum(x["revenue"] for x in f30), 2),
        hist_dates=json.dumps(hist_dates),
        hist_rev=json.dumps(hist_rev),
        fc_dates=json.dumps([x["date"] for x in f30]),
        fc_7=json.dumps([x["revenue"] if i < 7 else None for i, x in enumerate(f30)]),
        fc_14=json.dumps([x["revenue"] if i < 14 else None for i, x in enumerate(f30)]),
        fc_30=json.dumps([x["revenue"] for x in f30]),
    )


def _build_forecasts(df):
    revenues = df["revenue"].values.astype(float)
    n        = len(revenues)
    recent   = revenues[-min(30, n):]

    if len(recent) > 1:
        x     = np.arange(len(recent), dtype=float)
        trend = float(np.polyfit(x, recent, 1)[0])
    else:
        trend = 0.0

    avg = float(recent[-7:].mean()) if len(recent) >= 7 else float(recent.mean())
    avg = max(avg, 0.0)
    if avg > 0:
        trend = max(min(trend, avg * 0.05), -avg * 0.05)
    else:
        trend = 0.0

    global_avg = float(revenues.mean()) if revenues.mean() > 0 else 1.0

    df_tmp = df.copy()
    df_tmp["dow"] = df_tmp.index.dayofweek
    dow_avg = df_tmp.groupby("dow")["revenue"].mean()
    dow_mul = {
        int(dow): max(min(float(v / global_avg), 2.5), 0.1)
        for dow, v in dow_avg.items()
    }

    start     = df.index.max() + timedelta(days=1)
    forecasts = []

    for i in range(30):
        date      = start + timedelta(days=i)
        base      = max(avg + trend * (i + 1), 0.0)
        mul       = dow_mul.get(date.dayofweek, 1.0)
        predicted = round(base * mul, 2)
        conf      = max(0.05, 0.08 + i * 0.004)

        forecasts.append({
            "date":    date.strftime("%Y-%m-%d"),
            "day":     date.strftime("%a, %b %d"),
            "revenue": predicted,
            "low":     max(round(predicted * (1 - conf), 2), 0),
            "high":    round(predicted * (1 + conf), 2),
        })

    return forecasts[:7], forecasts[:14], forecasts[:30]

# ── Customer LTV ──────────────────────────────────────────────────────────────

@app.route("/ltv")
@login_required
def ltv():
    conn = get_db()
    rows = conn.execute(
        """SELECT customer_email, customer_name,
                  COUNT(*) AS order_count,
                  SUM(total_amount) AS total_spent,
                  AVG(total_amount) AS avg_order,
                  MIN(date) AS first_order,
                  MAX(date) AS last_order
           FROM sales_data
           WHERE customer_email IS NOT NULL AND customer_email != ''
           GROUP BY customer_email
           ORDER BY total_spent DESC""",
    ).fetchall()
    conn.close()

    today     = datetime.now().date()
    customers = []

    for row in rows:
        first         = datetime.strptime(row["first_order"], "%Y-%m-%d").date()
        last          = datetime.strptime(row["last_order"],  "%Y-%m-%d").date()
        days_active   = max((last - first).days, 1)
        days_since    = (today - last).days
        months_active = max(days_active / 30.0, 1.0)
        monthly_spend = row["total_spent"] / months_active

        if days_since <= 30:
            status, sc, churn = "Active",   "green",  1.00
        elif days_since <= 90:
            status, sc, churn = "At Risk",  "yellow", 0.70
        elif days_since <= 180:
            status, sc, churn = "Churning", "orange", 0.40
        else:
            status, sc, churn = "Churned",  "red",    0.10

        ltv_24m = monthly_spend * 24 * churn

        if ltv_24m >= 5000:
            segment, seg_c = "High Value",   "indigo"
        elif ltv_24m >= 1000:
            segment, seg_c = "Medium Value", "blue"
        elif ltv_24m >= 200:
            segment, seg_c = "Low Value",    "slate"
        else:
            segment, seg_c = "At Risk",      "red"

        customers.append({
            "email":         row["customer_email"],
            "name":          (row["customer_name"] or
                              row["customer_email"].split("@")[0].title()),
            "orders":        row["order_count"],
            "total_spent":   round(row["total_spent"], 2),
            "avg_order":     round(row["avg_order"], 2),
            "monthly_spend": round(monthly_spend, 2),
            "ltv_24m":       round(ltv_24m, 2),
            "first_order":   row["first_order"],
            "last_order":    row["last_order"],
            "days_since":    days_since,
            "status":        status,
            "sc":            sc,
            "segment":       segment,
            "seg_c":         seg_c,
        })

    seg_counts = {"High Value": 0, "Medium Value": 0, "Low Value": 0, "At Risk": 0}
    for c in customers:
        if c["segment"] in seg_counts:
            seg_counts[c["segment"]] += 1

    avg_ltv        = round(sum(c["ltv_24m"] for c in customers) / len(customers), 2) if customers else 0
    total_projected = round(sum(c["ltv_24m"] for c in customers), 2)
    high_value_ct  = seg_counts["High Value"]
    at_risk_ct     = sum(1 for c in customers if c["status"] in ("At Risk", "Churning", "Churned"))

    return render_template(
        "ltv.html",
        has_data=bool(customers),
        customers=customers,
        total_customers=len(customers),
        avg_ltv=avg_ltv,
        total_projected=total_projected,
        high_value_ct=high_value_ct,
        at_risk_ct=at_risk_ct,
        seg_labels=json.dumps(list(seg_counts.keys())),
        seg_data=json.dumps(list(seg_counts.values())),
    )

# ── Integrations — Shopify ────────────────────────────────────────────────────

@app.route("/integrations")
@login_required
def integrations():
    conn = get_db()
    cfg  = conn.execute(
        "SELECT * FROM shopify_config ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()

    return render_template(
        "integrations.html",
        connected=bool(cfg and cfg["connected"]),
        shop_name=cfg["shop_name"] if cfg else None,
        store_url=cfg["store_url"] if cfg else "",
        last_synced=cfg["last_synced"] if cfg else None,
    )


@app.route("/integrations/connect", methods=["POST"])
@login_required
def shopify_connect():
    store_url = request.form.get("store_url", "").strip().rstrip("/")
    api_token = request.form.get("api_token", "").strip()

    if not store_url or not api_token:
        flash("Store URL and API token are required.", "error")
        return redirect(url_for("integrations"))

    if not store_url.startswith("http"):
        store_url = f"https://{store_url}"

    try:
        hdrs = {"X-Shopify-Access-Token": api_token}
        resp = requests.get(
            f"{store_url}/admin/api/2024-01/shop.json",
            headers=hdrs,
            timeout=10,
        )
        if resp.status_code == 200:
            shop_name = resp.json().get("shop", {}).get("name", store_url)
            conn = get_db()
            conn.execute("DELETE FROM shopify_config")
            conn.execute(
                "INSERT INTO shopify_config (store_url,api_token,shop_name,connected)"
                " VALUES (?,?,?,1)",
                (store_url, api_token, shop_name),
            )
            conn.commit()
            conn.close()
            flash(f"Connected to {shop_name}!", "success")
            _sync_shopify()
        else:
            flash(
                f"Connection failed (HTTP {resp.status_code}). "
                "Check your store URL and API token.",
                "error",
            )
    except requests.exceptions.RequestException as exc:
        flash(f"Connection error: {exc}", "error")

    return redirect(url_for("integrations"))


@app.route("/integrations/sync", methods=["POST"])
@login_required
def shopify_sync():
    res = _sync_shopify()
    if res["success"]:
        flash(f"Sync complete — {res['orders_imported']} new orders imported.", "success")
    else:
        flash(f"Sync failed: {res['error']}", "error")
    return redirect(url_for("integrations"))


@app.route("/integrations/disconnect", methods=["POST"])
@login_required
def shopify_disconnect():
    conn = get_db()
    conn.execute("DELETE FROM shopify_config")
    conn.commit()
    conn.close()
    flash("Shopify store disconnected.", "info")
    return redirect(url_for("integrations"))


def _sync_shopify():
    conn = get_db()
    cfg  = conn.execute(
        "SELECT * FROM shopify_config WHERE connected=1 ORDER BY id DESC LIMIT 1"
    ).fetchone()

    if not cfg:
        conn.close()
        return {"success": False, "error": "No connected Shopify store."}

    store_url = cfg["store_url"]
    api_token = cfg["api_token"]
    hdrs      = {"X-Shopify-Access-Token": api_token}

    try:
        params = {
            "status": "any",
            "limit":  250,
            "fields": "id,created_at,email,customer,line_items,total_price",
        }
        resp = requests.get(
            f"{store_url}/admin/api/2024-01/orders.json",
            headers=hdrs,
            params=params,
            timeout=30,
        )
        if resp.status_code != 200:
            conn.close()
            return {"success": False, "error": f"Shopify API returned {resp.status_code}"}

        orders   = resp.json().get("orders", [])
        imported = 0

        for o in orders:
            oid    = str(o["id"])
            exists = conn.execute(
                "SELECT id FROM sales_data WHERE order_id=? AND source='shopify'", (oid,)
            ).fetchone()
            if exists:
                continue

            date    = o["created_at"][:10]
            email   = o.get("email", "") or ""
            cust    = o.get("customer") or {}
            name    = f"{cust.get('first_name','')} {cust.get('last_name','')}".strip()
            total   = float(o.get("total_price") or 0)
            items   = o.get("line_items") or []
            product = items[0]["title"] if items else "Shopify Order"

            conn.execute(
                "INSERT INTO sales_data"
                " (date,customer_email,customer_name,order_id,product,total_amount,source)"
                " VALUES (?,?,?,?,?,?,'shopify')",
                (date, email or None, name or None, oid, product, total),
            )
            imported += 1

        conn.execute(
            "UPDATE shopify_config SET last_synced=? WHERE connected=1",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),),
        )
        conn.commit()
        conn.close()
        return {"success": True, "orders_imported": imported}

    except Exception as exc:
        conn.close()
        return {"success": False, "error": str(exc)}

# ── Upload Data ───────────────────────────────────────────────────────────────

@app.route("/upload", methods=["GET", "POST"])
@login_required
def upload():
    if request.method == "GET":
        conn = get_db()
        total_rows   = conn.execute("SELECT COUNT(*) AS v FROM sales_data").fetchone()["v"]
        source_counts = conn.execute(
            "SELECT source, COUNT(*) AS v FROM sales_data GROUP BY source"
        ).fetchall()
        conn.close()
        return render_template("upload.html", total_rows=total_rows, source_counts=source_counts)

    file = request.files.get("file")
    if not file or file.filename == "":
        flash("Please select a CSV file.", "error")
        return redirect(url_for("upload"))
    if not file.filename.lower().endswith(".csv"):
        flash("Only CSV files are supported.", "error")
        return redirect(url_for("upload"))

    try:
        df = pd.read_csv(file)   # PRO: no row limit — full file processed
        original_cols = df.columns.tolist()
        df.columns    = [c.lower().strip().replace(" ", "_") for c in df.columns]
        col_map       = _detect_columns(df.columns.tolist())

        if not col_map.get("date") or not col_map.get("amount"):
            flash(
                f"Could not auto-detect date and amount columns. "
                f"Detected columns: {', '.join(original_cols)}. "
                "Rename them to 'date' and 'amount' (or 'total', 'revenue').",
                "error",
            )
            return redirect(url_for("upload"))

        conn     = get_db()
        inserted = errors = 0

        for _, row in df.iterrows():
            try:
                date_val = pd.to_datetime(str(row[col_map["date"]])).strftime("%Y-%m-%d")
                amount   = float(
                    str(row[col_map["amount"]]).replace(",", "").replace("$", "").strip()
                )

                def sg(key):
                    c = col_map.get(key)
                    if not c:
                        return ""
                    v = str(row.get(c, "")).strip()
                    return "" if v.lower() in ("nan", "none", "") else v

                conn.execute(
                    "INSERT INTO sales_data"
                    " (date,customer_email,customer_name,order_id,product,"
                    "  category,quantity,unit_price,total_amount,source)"
                    " VALUES (?,?,?,?,?,?,?,?,?,'upload')",
                    (
                        date_val,
                        sg("email") or None,
                        sg("name")  or None,
                        sg("order_id") or None,
                        sg("product")  or None,
                        sg("category") or None,
                        int(float(sg("quantity") or 1)),
                        float(sg("unit_price") or 0),
                        amount,
                    ),
                )
                inserted += 1
            except Exception:
                errors += 1

        conn.commit()
        conn.close()

        msg = f"Imported {inserted:,} rows"
        if errors:
            msg += f" ({errors:,} rows skipped due to parse errors)"
        flash(msg, "success")

    except Exception as exc:
        flash(f"Failed to process file: {exc}", "error")

    return redirect(url_for("upload"))


@app.route("/upload/clear", methods=["POST"])
@login_required
def upload_clear():
    source = request.form.get("source", "all")
    conn   = get_db()
    if source == "all":
        conn.execute("DELETE FROM sales_data")
    else:
        conn.execute("DELETE FROM sales_data WHERE source=?", (source,))
    conn.commit()
    conn.close()
    label = "All data" if source == "all" else f"{source.title()} data"
    flash(f"{label} cleared.", "success")
    return redirect(url_for("upload"))


def _detect_columns(cols):
    col_map = {}

    def find(keywords):
        for k in keywords:
            if k in cols:
                return k
        for k in keywords:
            for c in cols:
                if k in c:
                    return c
        return None

    col_map["date"]       = find(["date", "order_date", "created_at", "timestamp",
                                   "sale_date", "transaction_date", "invoice_date"])
    col_map["amount"]     = find(["total_amount", "total_price", "amount", "revenue",
                                   "total", "sales", "price", "order_total", "value"])
    col_map["email"]      = find(["email", "customer_email", "email_address", "buyer_email"])
    col_map["name"]       = find(["customer_name", "name", "full_name", "buyer_name", "customer"])
    col_map["product"]    = find(["product", "product_name", "item", "sku",
                                   "title", "product_title", "item_name"])
    col_map["category"]   = find(["category", "product_category", "type", "department"])
    col_map["order_id"]   = find(["order_id", "order", "id", "transaction_id",
                                   "invoice", "invoice_id", "ref"])
    col_map["quantity"]   = find(["quantity", "qty", "units", "count"])
    col_map["unit_price"] = find(["unit_price", "item_price", "unit_cost"])
    return col_map

# ── Settings ──────────────────────────────────────────────────────────────────

SETTINGS_FIELDS = [
    "email_alerts_enabled", "alert_email", "revenue_threshold", "order_threshold",
    "auto_sync_enabled", "auto_sync_interval_hours",
    "smtp_host", "smtp_port", "smtp_user", "smtp_password",
]
SETTINGS_DEFAULTS = {
    "email_alerts_enabled":     "false",
    "alert_email":              "",
    "revenue_threshold":        "10000",
    "order_threshold":          "100",
    "auto_sync_enabled":        "false",
    "auto_sync_interval_hours": "24",
    "smtp_host":                "",
    "smtp_port":                "587",
    "smtp_user":                "",
    "smtp_password":            "",
}


@app.route("/settings", methods=["GET", "POST"])
@login_required
def settings():
    if request.method == "POST":
        for field in SETTINGS_FIELDS:
            val = request.form.get(field, "")
            if field in ("email_alerts_enabled", "auto_sync_enabled"):
                val = "true" if val == "on" else "false"
            set_setting(field, val)
        flash("Settings saved.", "success")
        return redirect(url_for("settings"))

    cfg = {k: get_setting(k, d) for k, d in SETTINGS_DEFAULTS.items()}
    cfg["email_alerts_enabled"] = cfg["email_alerts_enabled"] == "true"
    cfg["auto_sync_enabled"]    = cfg["auto_sync_enabled"] == "true"

    return render_template("settings.html", cfg=cfg)

# ── APScheduler — auto-sync Shopify every N hours ─────────────────────────────

scheduler = BackgroundScheduler(daemon=True)


def _scheduled_sync():
    if get_setting("auto_sync_enabled") != "true":
        return
    interval_h = int(get_setting("auto_sync_interval_hours", "24"))
    conn = get_db()
    cfg  = conn.execute(
        "SELECT last_synced FROM shopify_config WHERE connected=1 ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    if not cfg:
        return
    if cfg["last_synced"]:
        try:
            last = datetime.strptime(cfg["last_synced"], "%Y-%m-%d %H:%M:%S")
            if (datetime.now() - last).total_seconds() < interval_h * 3600:
                return
        except ValueError:
            pass
    _sync_shopify()


scheduler.add_job(
    _scheduled_sync,
    "interval",
    hours=1,
    id="shopify_auto_sync",
    replace_existing=True,
)

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    scheduler.start()
    try:
        app.run(debug=True, use_reloader=False, port=5000)
    finally:
        scheduler.shutdown()
