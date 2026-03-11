#!/usr/bin/env python3
"""
Monarch Money Daily Digest — light theme, clean net worth math.
"""

import asyncio
import calendar
import os
import smtplib
from collections import defaultdict
from datetime import date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from monarchmoney import MonarchMoney

# ── Config ─────────────────────────────────────────────────────────────────────
MONARCH_EMAIL    = os.environ["MONARCH_EMAIL"]
MONARCH_PASSWORD = os.environ["MONARCH_PASSWORD"]
MONARCH_MFA_KEY  = os.environ.get("MONARCH_MFA_KEY", "")
GMAIL_ADDRESS    = os.environ["GMAIL_ADDRESS"]
GMAIL_APP_PASS   = os.environ["GMAIL_APP_PASSWORD"]
RECIPIENT_EMAIL  = os.environ.get("RECIPIENT_EMAIL", GMAIL_ADDRESS)

HSA_KEYWORDS      = ["medical", "pharmacy", "dental", "vision", "health", "doctor", "hospital"]
TRANSFER_KEYWORDS = ["transfer", "transfers to investments", "paycheck"]

# Colors (light theme)
C_BG       = "#f8f7f5"
C_CARD     = "#ffffff"
C_BORDER   = "#e8e4df"
C_TEXT     = "#2c2825"
C_MUTED    = "#9a9188"
C_LABEL    = "#b0a89e"
C_GREEN    = "#2d7a4f"
C_RED      = "#c0392b"
C_ACCENT   = "#2d7a4f"
# ──────────────────────────────────────────────────────────────────────────────

def fmt(amount: float) -> str:
    return f"${abs(amount):,.2f}"

def pct(value: float) -> str:
    return f"{value:.1f}%"

def is_hsa(cat: str) -> bool:
    return any(k in (cat or "").lower() for k in HSA_KEYWORDS)

def is_transfer(cat: str) -> bool:
    return any(k in (cat or "").lower() for k in TRANSFER_KEYWORDS)

def green(t): return f'<span style="color:{C_GREEN}">{t}</span>'
def red(t):   return f'<span style="color:{C_RED}">{t}</span>'
def muted(t): return f'<span style="color:{C_MUTED}">{t}</span>'


# ── Fetch ──────────────────────────────────────────────────────────────────────

async def fetch_data():
    mm = MonarchMoney()
    await mm.login(
        email=MONARCH_EMAIL,
        password=MONARCH_PASSWORD,
        mfa_secret_key=MONARCH_MFA_KEY if MONARCH_MFA_KEY else None,
        save_session=False,
        use_saved_session=False,
    )

    today       = date.today()
    yesterday   = today - timedelta(days=1)
    month_start = yesterday.replace(day=1)

    print("Fetching transactions...")
    txn_resp = await mm.get_transactions(start_date=str(yesterday), end_date=str(yesterday))
    transactions = txn_resp.get("allTransactions", {}).get("results", [])

    print("Fetching MTD transactions...")
    mtd_resp = await mm.get_transactions(start_date=str(month_start), end_date=str(yesterday))
    mtd_transactions = mtd_resp.get("allTransactions", {}).get("results", [])

    print("Fetching accounts...")
    acct_resp = await mm.get_accounts()
    accounts = acct_resp.get("accounts", [])

    # Debug: print first account to understand shape
    if accounts:
        a = accounts[0]
        print(f"  Sample account: name={a.get('name')} balance={a.get('currentBalance')} isAsset={a.get('isAsset')} includeInNetWorth={a.get('includeInNetWorth')}")

    print("Fetching cashflow...")
    cashflow = {}
    try:
        cashflow = await mm.get_cashflow_summary(
            start_date=str(month_start),
            end_date=str(yesterday),
        )
    except Exception as e:
        print(f"Cashflow failed (non-fatal): {e}")

    return yesterday, transactions, mtd_transactions, accounts, cashflow


# ── Net Worth ──────────────────────────────────────────────────────────────────

def compute_net_worth(accounts):
    """
    Monarch returns currentBalance as:
    - Positive for assets (cash, investments, property, vehicles)
    - Negative for liabilities (loans, credit cards, mortgage)
    So net worth = sum of all signed balances, filtered to includeInNetWorth accounts.
    """
    net_worth = 0.0
    assets = 0.0
    liabilities = 0.0

    for acct in accounts:
        # Only count accounts Monarch includes in net worth
        if not acct.get("includeInNetWorth", True):
            continue

        balance = float(acct.get("currentBalance", 0) or 0)
        is_asset = acct.get("isAsset", True)

        if is_asset:
            assets += balance
            net_worth += balance
        else:
            # Liabilities: balance is positive in API, represents amount owed
            liabilities += balance
            net_worth -= balance

    return net_worth, assets, liabilities


# ── Analysis ───────────────────────────────────────────────────────────────────

def analyze_transactions(transactions, exclude_transfers=False):
    income = expenses = hsa_total = 0.0
    by_category = defaultdict(float)

    for txn in transactions:
        amount = float(txn.get("amount", 0))
        cat    = (txn.get("category") or {}).get("name", "Uncategorized")

        if exclude_transfers and is_transfer(cat):
            continue

        if amount > 0:
            income += amount
        else:
            expenses += abs(amount)
            by_category[cat] += abs(amount)
            if is_hsa(cat):
                hsa_total += abs(amount)

    top_cats = sorted(by_category.items(), key=lambda x: x[1], reverse=True)[:5]
    return income, expenses, top_cats, hsa_total


def get_biggest_txn(transactions):
    expenses = [t for t in transactions
                if float(t.get("amount", 0)) < 0
                and not is_transfer((t.get("category") or {}).get("name", ""))]
    if not expenses:
        return None
    return min(expenses, key=lambda t: float(t.get("amount", 0)))


def extract_cashflow(cashflow):
    if not cashflow:
        return 0.0, 0.0
    for shape in [
        lambda c: (c["summary"][0]["sumIncome"], abs(c["summary"][0]["sumExpense"])) if isinstance(c.get("summary"), list) and c["summary"] else None,
        lambda c: (c["summary"]["sumIncome"], abs(c["summary"]["sumExpense"])) if isinstance(c.get("summary"), dict) else None,
        lambda c: (c["sumIncome"], abs(c["sumExpense"])) if "sumIncome" in c else None,
        lambda c: (c["income"], abs(c["expense"])) if "income" in c else None,
    ]:
        try:
            result = shape(cashflow)
            if result:
                return float(result[0] or 0), float(result[1] or 0)
        except Exception:
            pass
    print(f"Unknown cashflow shape: {list(cashflow.keys())}")
    return 0.0, 0.0


# ── HTML Primitives ────────────────────────────────────────────────────────────

def card(title, badge, content):
    if not (content or "").strip():
        return ""
    badge_html = ""
    if badge:
        badge_html = f'<span style="display:inline-block;background:#edf7f1;color:{C_GREEN};border:1px solid #c8e6d4;border-radius:3px;font-size:9px;letter-spacing:.08em;padding:2px 7px;margin-left:8px;font-family:Georgia,serif">{badge}</span>'
    return f"""
  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;padding:20px 28px">
    <div style="font-size:10px;letter-spacing:.18em;text-transform:uppercase;color:{C_LABEL};margin-bottom:14px;font-family:Georgia,serif">{title}{badge_html}</div>
    {content}
  </div>"""


def row(left, right, left_style="", right_style="", border=True):
    border_css = f"border-bottom:1px solid {C_BORDER};" if border else ""
    return f"""
    <tr>
      <td style="padding:8px 0;{border_css}font-size:13px;color:{C_TEXT};{left_style}">{left}</td>
      <td style="padding:8px 0;{border_css}text-align:right;font-size:13px;{right_style}">{right}</td>
    </tr>"""


# ── Section Builders ───────────────────────────────────────────────────────────

def build_accounts_html(accounts):
    net_worth, assets, liabilities = compute_net_worth(accounts)

    # Show only accounts with non-zero balance
    visible = [a for a in accounts if float(a.get("currentBalance", 0) or 0) != 0]
    sorted_accts = sorted(
        visible,
        key=lambda a: (not a.get("isAsset", True), -abs(float(a.get("currentBalance", 0) or 0)))
    )

    rows = ""
    prev_is_asset = None
    for acct in sorted_accts:
        name     = acct.get("displayName") or acct.get("name", "Unknown")
        inst     = (acct.get("institution") or {}).get("name", "")
        balance  = float(acct.get("currentBalance", 0) or 0)
        is_asset = acct.get("isAsset", True)

        # Section divider
        if prev_is_asset is not None and prev_is_asset and not is_asset:
            rows += f'<tr><td colspan="2" style="padding:4px 0"></td></tr>'
        prev_is_asset = is_asset

        bal_display = fmt(balance) if is_asset else f"-{fmt(balance)}"
        bal_color   = C_TEXT if is_asset else C_RED
        inst_html   = f'<span style="color:{C_LABEL};font-size:11px"> · {inst}</span>' if inst else ""

        rows += f"""
        <tr>
          <td style="padding:7px 0;border-bottom:1px solid {C_BORDER};font-size:13px;color:{C_TEXT}">{name}{inst_html}</td>
          <td style="padding:7px 0;border-bottom:1px solid {C_BORDER};text-align:right;font-size:13px;color:{bal_color}">{bal_display}</td>
        </tr>"""

    rows += f"""
        <tr>
          <td style="padding:12px 0 4px;font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:{C_LABEL};font-family:Georgia,serif">Net Worth</td>
          <td style="padding:12px 0 4px;text-align:right;font-size:17px;font-weight:600;color:{C_TEXT}">{fmt(net_worth)}</td>
        </tr>"""

    return f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>', net_worth


def build_transactions_html(transactions):
    if not transactions:
        return f'<p style="color:{C_MUTED};font-size:13px;margin:0">No transactions recorded yesterday.</p>', 0.0, 0.0, 0.0

    income = expenses = 0.0
    rows = ""
    for txn in sorted(transactions, key=lambda t: float(t.get("amount", 0)), reverse=True):
        merchant = (txn.get("merchant") or {}).get("name") or txn.get("plaidName") or "Unknown"
        cat      = (txn.get("category") or {}).get("name", "Uncategorized")
        amount   = float(txn.get("amount", 0))
        note     = (txn.get("notes") or "").strip()

        if amount > 0:
            income += amount
            amt_html = green(f"+{fmt(amount)}")
        else:
            expenses += abs(amount)
            amt_html = red(f"-{fmt(abs(amount))}")

        badges = ""
        if amount < 0 and is_hsa(cat):
            badges += f'<span style="display:inline-block;background:#edf7f1;color:{C_GREEN};border:1px solid #c8e6d4;border-radius:3px;font-size:9px;padding:1px 5px;margin-left:6px">HSA</span>'
        if is_transfer(cat):
            badges += f'<span style="display:inline-block;background:#f5f3f0;color:{C_MUTED};border:1px solid {C_BORDER};border-radius:3px;font-size:9px;padding:1px 5px;margin-left:6px">transfer</span>'

        note_html = f'<div style="font-size:10px;color:{C_LABEL};margin-top:1px;font-style:italic">{note}</div>' if note else ""

        rows += f"""
        <tr>
          <td style="padding:9px 0;border-bottom:1px solid {C_BORDER}">
            <div style="font-size:13px;color:{C_TEXT}">{merchant}</div>
            <div style="font-size:11px;color:{C_MUTED};margin-top:2px">{cat}{badges}</div>
            {note_html}
          </td>
          <td style="padding:9px 0;border-bottom:1px solid {C_BORDER};text-align:right;font-size:13px;vertical-align:top">{amt_html}</td>
        </tr>"""

    net = income - expenses
    return f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>', income, expenses, net


def build_cashflow_html(cashflow, mtd_transactions, yesterday):
    api_income, api_expense = extract_cashflow(cashflow)
    txn_income, txn_expense, _, _ = analyze_transactions(mtd_transactions, exclude_transfers=True)

    mtd_income  = api_income  if api_income  > 0 else txn_income
    mtd_expense = api_expense if api_expense > 0 else txn_expense

    if mtd_income == 0 and mtd_expense == 0:
        return ""

    savings      = mtd_income - mtd_expense
    savings_rate = (savings / mtd_income * 100) if mtd_income > 0 else 0
    days_elapsed  = yesterday.day
    days_in_month = calendar.monthrange(yesterday.year, yesterday.month)[1]
    month_pct     = days_elapsed / days_in_month * 100
    run_rate      = (mtd_expense / days_elapsed * days_in_month) if days_elapsed > 0 else 0

    sr_color = C_GREEN if savings_rate >= 20 else (C_RED if savings_rate < 5 else "#c07a2a")

    return f"""
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td style="padding:6px 0;font-size:13px;color:{C_TEXT}">MTD Income</td>
        <td style="padding:6px 0;text-align:right;font-size:13px;color:{C_GREEN}">{fmt(mtd_income)}</td>
      </tr>
      <tr>
        <td style="padding:6px 0;font-size:13px;color:{C_TEXT}">MTD Spending <span style="color:{C_LABEL};font-size:11px">(excl. transfers)</span></td>
        <td style="padding:6px 0;text-align:right;font-size:13px;color:{C_RED}">{fmt(mtd_expense)}</td>
      </tr>
      <tr>
        <td style="padding:6px 0;font-size:13px;color:{C_TEXT}">Projected Month-End</td>
        <td style="padding:6px 0;text-align:right;font-size:13px;color:{C_MUTED}">{fmt(run_rate)}</td>
      </tr>
      <tr>
        <td colspan="2" style="padding:12px 0 6px">
          <div style="display:flex;justify-content:space-between;font-size:10px;color:{C_LABEL};margin-bottom:5px;font-family:Georgia,serif;letter-spacing:.08em">
            <span>{yesterday.strftime('%B')} · Day {days_elapsed} of {days_in_month}</span>
            <span>{pct(month_pct)}</span>
          </div>
          <div style="background:#ede9e4;border-radius:2px;height:4px;width:100%">
            <div style="background:{C_GREEN};border-radius:2px;height:4px;width:{min(int(month_pct), 100)}%"></div>
          </div>
        </td>
      </tr>
      <tr>
        <td style="padding:10px 0 4px;font-size:10px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};font-family:Georgia,serif">Savings Rate</td>
        <td style="padding:10px 0 4px;text-align:right;font-size:17px;font-weight:600;color:{sr_color}">{pct(savings_rate)}</td>
      </tr>
    </table>"""


def build_spending_html(mtd_transactions):
    _, _, top_cats, _ = analyze_transactions(mtd_transactions, exclude_transfers=True)
    if not top_cats:
        return ""

    total = sum(v for _, v in top_cats)
    max_v = top_cats[0][1]
    rows  = ""
    for cat, amount in top_cats:
        bar_w = int((amount / max_v) * 140)
        share = (amount / total * 100) if total else 0
        rows += f"""
        <tr>
          <td style="padding:7px 0;border-bottom:1px solid {C_BORDER};font-size:12px;color:{C_TEXT};width:36%">{cat}</td>
          <td style="padding:7px 10px;border-bottom:1px solid {C_BORDER};vertical-align:middle">
            <div style="background:#ede9e4;border-radius:2px;height:3px;width:{bar_w}px;display:inline-block;vertical-align:middle"></div>
          </td>
          <td style="padding:7px 0;border-bottom:1px solid {C_BORDER};text-align:right;font-size:11px;color:{C_MUTED};white-space:nowrap">{pct(share)}</td>
          <td style="padding:7px 0 7px 10px;border-bottom:1px solid {C_BORDER};text-align:right;font-size:12px;color:{C_TEXT};white-space:nowrap">{fmt(amount)}</td>
        </tr>"""

    return f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>'


def build_highlights_html(transactions, accounts):
    items = []

    biggest = get_biggest_txn(transactions)
    if biggest:
        merchant = (biggest.get("merchant") or {}).get("name") or "Unknown"
        amount   = abs(float(biggest.get("amount", 0)))
        items.append(f'<div style="padding:8px 0;border-bottom:1px solid {C_BORDER};font-size:13px;color:{C_TEXT}">💸 Largest purchase: <strong>{merchant}</strong> — {red(fmt(amount))}</div>')

    hsa_txns = [t for t in transactions if float(t.get("amount", 0)) < 0 and is_hsa((t.get("category") or {}).get("name", ""))]
    if hsa_txns:
        total = sum(abs(float(t.get("amount", 0))) for t in hsa_txns)
        items.append(f'<div style="padding:8px 0;border-bottom:1px solid {C_BORDER};font-size:13px;color:{C_TEXT}">🏥 HSA-eligible spend: {green(fmt(total))} — log receipts in Monarch</div>')

    liab = [a for a in accounts if not a.get("isAsset", True) and float(a.get("currentBalance", 0) or 0) > 0]
    if liab:
        total_debt = sum(float(a.get("currentBalance", 0) or 0) for a in liab)
        items.append(f'<div style="padding:8px 0;border-bottom:1px solid {C_BORDER};font-size:13px;color:{C_TEXT}">🎯 Total debt: {red(fmt(total_debt))}</div>')

    return "".join(items)


# ── Email ──────────────────────────────────────────────────────────────────────

def build_email(yesterday, transactions, mtd_transactions, accounts, cashflow):
    date_str  = yesterday.strftime("%A, %B %-d")
    generated = date.today().strftime("%B %-d, %Y")

    acct_html, net_worth            = build_accounts_html(accounts)
    txn_html, income, expenses, net = build_transactions_html(transactions)
    cashflow_html   = build_cashflow_html(cashflow, mtd_transactions, yesterday)
    spending_html   = build_spending_html(mtd_transactions)
    highlights_html = build_highlights_html(transactions, accounts)

    txn_count = len(transactions)
    net_color = C_GREEN if net >= 0 else C_RED
    net_label = f"+{fmt(net)}" if net >= 0 else f"-{fmt(abs(net))}"

    summary_bar = f"""
  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;display:table;width:100%;box-sizing:border-box">
    <div style="display:table-cell;padding:16px 0;text-align:center;border-right:1px solid {C_BORDER}">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};margin-bottom:5px;font-family:Georgia,serif">Income</div>
      <div style="font-size:15px;font-weight:600;color:{C_GREEN}">{fmt(income)}</div>
    </div>
    <div style="display:table-cell;padding:16px 0;text-align:center;border-right:1px solid {C_BORDER}">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};margin-bottom:5px;font-family:Georgia,serif">Spent</div>
      <div style="font-size:15px;font-weight:600;color:{C_RED}">{fmt(expenses)}</div>
    </div>
    <div style="display:table-cell;padding:16px 0;text-align:center">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};margin-bottom:5px;font-family:Georgia,serif">Net</div>
      <div style="font-size:15px;font-weight:600;color:{net_color}">{net_label}</div>
    </div>
  </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Monarch Digest · {date_str}</title>
</head>
<body style="margin:0;padding:32px 16px;background:{C_BG};font-family:Georgia,serif">
<div style="max-width:580px;margin:0 auto">

  <!-- Header -->
  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-radius:6px 6px 0 0;padding:28px 28px 22px">
    <div style="display:table;width:100%">
      <div style="display:table-cell;vertical-align:top">
        <div style="font-size:9px;letter-spacing:.25em;text-transform:uppercase;color:{C_GREEN};margin-bottom:7px">Monarch Daily Digest</div>
        <div style="font-size:24px;color:{C_TEXT};font-weight:400;font-style:italic">{date_str}</div>
      </div>
      <div style="display:table-cell;vertical-align:top;text-align:right">
        <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};margin-bottom:5px">Net Worth</div>
        <div style="font-size:22px;color:{C_TEXT};font-weight:600">{fmt(net_worth)}</div>
      </div>
    </div>
  </div>

  {card("Account Balances", None, acct_html)}
  {card("Yesterday's Transactions", f"{txn_count} transactions", txn_html)}
  {summary_bar}
  {card("Highlights", None, highlights_html)}
  {card("Month-to-Date Cashflow", yesterday.strftime("%B %Y"), cashflow_html)}
  {card("Spending Breakdown", "excl. transfers", spending_html)}

  <!-- Footer -->
  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;border-radius:0 0 6px 6px;padding:12px 28px;display:table;width:100%;box-sizing:border-box">
    <span style="display:table-cell;font-size:10px;color:{C_LABEL}">Monarch Money · {generated} · 7:00 AM ET</span>
    <span style="display:table-cell;font-size:10px;color:{C_GREEN};text-align:right">{RECIPIENT_EMAIL}</span>
  </div>

</div>
</body>
</html>"""


# ── Send ───────────────────────────────────────────────────────────────────────

def send_email(subject, html_body):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_ADDRESS
    msg["To"]      = RECIPIENT_EMAIL
    msg.attach(MIMEText(html_body, "html"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASS)
        server.sendmail(GMAIL_ADDRESS, RECIPIENT_EMAIL, msg.as_string())
    print(f"✓ Sent to {RECIPIENT_EMAIL}")


async def main():
    print("Fetching Monarch data...")
    yesterday, transactions, mtd_transactions, accounts, cashflow = await fetch_data()
    print(f"  {len(transactions)} txns · {len(mtd_transactions)} MTD · {len(accounts)} accounts")

    html = build_email(yesterday, transactions, mtd_transactions, accounts, cashflow)
    net_worth, _, _ = compute_net_worth(accounts)
    date_str = yesterday.strftime("%b %-d")
    subject  = f"💰 Monarch · {date_str} · {len(transactions)} txns · NW {fmt(net_worth)}"
    send_email(subject, html)


if __name__ == "__main__":
    asyncio.run(main())
