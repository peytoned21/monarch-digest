#!/usr/bin/env python3
"""
Monarch Money Daily Digest
Pulls yesterday's transactions, account balances, cashflow, and budget data.
Sends a rich HTML email via Gmail SMTP every morning.
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
TRANSFER_KEYWORDS = ["transfer", "investment transfer", "transfers to investments", "transfers", "paycheck"]
# ──────────────────────────────────────────────────────────────────────────────


def fmt(amount: float) -> str:
    return f"${abs(amount):,.2f}"

def pct(value: float) -> str:
    return f"{value:.1f}%"

def is_hsa(category: str) -> bool:
    return any(k in (category or "").lower() for k in HSA_KEYWORDS)

def is_transfer(category: str) -> bool:
    return any(k in (category or "").lower() for k in TRANSFER_KEYWORDS)

def green(text): return f'<span style="color:#3a7d52">{text}</span>'
def red(text):   return f'<span style="color:#b05040">{text}</span>'
def muted(text): return f'<span style="color:#555">{text}</span>'


# ── Data Fetching ──────────────────────────────────────────────────────────────

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

    print("Fetching month-to-date transactions...")
    mtd_resp = await mm.get_transactions(start_date=str(month_start), end_date=str(yesterday))
    mtd_transactions = mtd_resp.get("allTransactions", {}).get("results", [])

    print("Fetching accounts...")
    acct_resp = await mm.get_accounts()
    accounts = acct_resp.get("accounts", [])

    print("Fetching cashflow...")
    cashflow = {}
    try:
        cashflow = await mm.get_cashflow_summary(
            start_date=str(month_start),
            end_date=str(yesterday),
        )
        print(f"  Cashflow keys: {list(cashflow.keys()) if cashflow else 'empty'}")
    except Exception as e:
        print(f"Cashflow fetch failed (non-fatal): {e}")

    return yesterday, transactions, mtd_transactions, accounts, cashflow


# ── Analysis ───────────────────────────────────────────────────────────────────

def analyze_transactions(transactions, exclude_transfers=False):
    """Returns income, expenses, top categories, hsa_total."""
    income = 0.0
    expenses = 0.0
    by_category = defaultdict(float)
    hsa_total = 0.0

    for txn in transactions:
        amount   = float(txn.get("amount", 0))
        category = (txn.get("category") or {}).get("name", "Uncategorized")

        if exclude_transfers and is_transfer(category):
            continue

        if amount > 0:
            income += amount
        else:
            expenses += abs(amount)
            by_category[category] += abs(amount)
            if is_hsa(category):
                hsa_total += abs(amount)

    top_categories = sorted(by_category.items(), key=lambda x: x[1], reverse=True)[:5]
    return income, expenses, top_categories, hsa_total


def compute_net_worth(accounts):
    assets = liabilities = 0.0
    for acct in accounts:
        balance = float(acct.get("currentBalance", 0) or 0)
        if acct.get("isAsset") is False:
            liabilities += balance
        else:
            assets += balance
    return assets - liabilities, assets, liabilities


def get_biggest_txn(transactions):
    # Exclude transfers for highlights
    expenses = [t for t in transactions
                if float(t.get("amount", 0)) < 0
                and not is_transfer((t.get("category") or {}).get("name", ""))]
    if not expenses:
        return None
    return min(expenses, key=lambda t: float(t.get("amount", 0)))


def extract_cashflow_summary(cashflow):
    """Safely extract MTD income and expense from various response shapes."""
    if not cashflow:
        return 0.0, 0.0

    # Shape 1: {"summary": [{"sumIncome": ..., "sumExpense": ...}]}
    if isinstance(cashflow.get("summary"), list) and cashflow["summary"]:
        s = cashflow["summary"][0]
        return float(s.get("sumIncome", 0) or 0), abs(float(s.get("sumExpense", 0) or 0))

    # Shape 2: {"summary": {"sumIncome": ..., "sumExpense": ...}}
    if isinstance(cashflow.get("summary"), dict):
        s = cashflow["summary"]
        return float(s.get("sumIncome", 0) or 0), abs(float(s.get("sumExpense", 0) or 0))

    # Shape 3: flat {"sumIncome": ..., "sumExpense": ...}
    if "sumIncome" in cashflow:
        return float(cashflow.get("sumIncome", 0) or 0), abs(float(cashflow.get("sumExpense", 0) or 0))

    # Shape 4: {"income": ..., "expense": ...}
    if "income" in cashflow:
        return float(cashflow.get("income", 0) or 0), abs(float(cashflow.get("expense", 0) or 0))

    print(f"  Unknown cashflow shape: {cashflow}")
    return 0.0, 0.0


# ── HTML Builders ──────────────────────────────────────────────────────────────

def section(title, badge_text, content_html):
    if not content_html or not content_html.strip():
        return ""
    badge = ""
    if badge_text:
        badge = f'<span style="display:inline-block;background:#1e2a1e;color:#3a7d52;border:1px solid #2a4030;border-radius:2px;font-size:9px;letter-spacing:.1em;padding:1px 6px;margin-left:8px">{badge_text}</span>'
    return f"""
  <div style="background:#161616;border:1px solid #2a2a2a;border-top:none;padding:20px 32px">
    <div style="font-size:9px;letter-spacing:.2em;text-transform:uppercase;color:#555;margin-bottom:14px">{title}{badge}</div>
    {content_html}
  </div>"""


def build_accounts_html(accounts):
    net_worth, assets, liabilities = compute_net_worth(accounts)

    # Filter: hide $0 accounts (but keep liabilities even at $0 if they have a name suggesting active)
    def should_show(acct):
        balance = float(acct.get("currentBalance", 0) or 0)
        is_liability = acct.get("isAsset") is False
        if balance == 0 and not is_liability:
            return False
        if balance == 0 and is_liability:
            return False  # hide $0 liabilities too (paid off / empty)
        return True

    visible = [a for a in accounts if should_show(a)]
    sorted_accts = sorted(
        visible,
        key=lambda a: (a.get("isAsset") is False, -float(a.get("currentBalance", 0) or 0))
    )

    rows = ""
    for acct in sorted_accts:
        name         = acct.get("displayName") or acct.get("name", "Unknown")
        inst         = (acct.get("institution") or {}).get("name", "")
        balance      = float(acct.get("currentBalance", 0) or 0)
        is_liability = acct.get("isAsset") is False

        bal_display = f"-{fmt(balance)}" if is_liability else fmt(balance)
        bal_color   = "color:#777" if is_liability else "color:#e8e2d9"
        inst_html   = f'<span style="color:#3a3a3a;font-size:11px"> · {inst}</span>' if inst else ""

        rows += f"""
        <tr>
          <td style="padding:8px 0;border-bottom:1px solid #1a1a1a;font-size:13px;color:#b8b0a4">{name}{inst_html}</td>
          <td style="padding:8px 0;border-bottom:1px solid #1a1a1a;text-align:right;font-size:13px;{bal_color}">{bal_display}</td>
        </tr>"""

    rows += f"""
        <tr>
          <td style="padding:10px 0 4px;font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:#444">Net Worth</td>
          <td style="padding:10px 0 4px;text-align:right;font-size:16px;font-weight:500;color:#f0ebe3">{fmt(net_worth)}</td>
        </tr>"""

    return f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>', net_worth


def build_transactions_html(transactions):
    if not transactions:
        return '<p style="color:#444;font-size:12px;margin:0">No transactions recorded yesterday.</p>', 0.0, 0.0, 0.0

    income = expenses = 0.0
    sorted_txns = sorted(transactions, key=lambda t: float(t.get("amount", 0)), reverse=True)
    rows = ""

    for txn in sorted_txns:
        merchant = (txn.get("merchant") or {}).get("name") or txn.get("plaidName") or "Unknown"
        category = (txn.get("category") or {}).get("name", "Uncategorized")
        amount   = float(txn.get("amount", 0))
        note     = (txn.get("notes") or "").strip()

        if amount > 0:
            income += amount
            amt_html = green(f"+{fmt(amount)}")
        else:
            expenses += abs(amount)
            amt_html = red(f"-{fmt(abs(amount))}")

        badges = ""
        if amount < 0 and is_hsa(category):
            badges += '<span style="display:inline-block;background:#1e2a1e;color:#3a7d52;border:1px solid #2a4030;border-radius:2px;font-size:9px;padding:1px 5px;margin-left:6px">HSA</span>'
        if amount < 0 and is_transfer(category):
            badges += '<span style="display:inline-block;background:#1a1a2e;color:#556;border:1px solid #222;border-radius:2px;font-size:9px;padding:1px 5px;margin-left:6px">transfer</span>'

        note_html = f'<div style="font-size:10px;color:#3a3a3a;margin-top:1px;font-style:italic">{note}</div>' if note else ""

        rows += f"""
        <tr>
          <td style="padding:9px 0;border-bottom:1px solid #1a1a1a">
            <div style="font-size:13px;color:#c0b8ae">{merchant}</div>
            <div style="font-size:11px;color:#444;margin-top:2px">{category}{badges}</div>
            {note_html}
          </td>
          <td style="padding:9px 0;border-bottom:1px solid #1a1a1a;text-align:right;font-size:13px;vertical-align:top">{amt_html}</td>
        </tr>"""

    net = income - expenses  # negative when you spent more than you earned
    return f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>', income, expenses, net


def build_spending_breakdown_html(mtd_transactions):
    # Exclude transfers from breakdown — they're not real spending
    _, _, top_cats, _ = analyze_transactions(mtd_transactions, exclude_transfers=True)
    if not top_cats:
        return ""

    total_spend = sum(v for _, v in top_cats)
    max_val = top_cats[0][1]
    rows = ""

    for cat, amount in top_cats:
        bar_width = int((amount / max_val) * 100)
        share = (amount / total_spend * 100) if total_spend else 0
        rows += f"""
        <tr>
          <td style="padding:7px 0;border-bottom:1px solid #1a1a1a;font-size:12px;color:#b8b0a4;width:38%">{cat}</td>
          <td style="padding:7px 12px;border-bottom:1px solid #1a1a1a;vertical-align:middle">
            <div style="background:#2a2a2a;border-radius:1px;height:3px;width:{bar_width}px;display:inline-block;vertical-align:middle"></div>
          </td>
          <td style="padding:7px 0;border-bottom:1px solid #1a1a1a;text-align:right;font-size:11px;color:#555;white-space:nowrap">{pct(share)}</td>
          <td style="padding:7px 0 7px 12px;border-bottom:1px solid #1a1a1a;text-align:right;font-size:12px;color:#c0b8ae;white-space:nowrap">{fmt(amount)}</td>
        </tr>"""

    return f"""
    <div style="font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#333;margin-bottom:10px">Month-to-date · Excluding transfers</div>
    <table width="100%" cellpadding="0" cellspacing="0">{rows}</table>"""


def build_cashflow_html(cashflow, mtd_transactions, yesterday):
    # Use transaction data as fallback (more reliable than API cashflow summary)
    api_income, api_expense = extract_cashflow_summary(cashflow)

    # Always derive from transactions — more reliable
    txn_income, txn_expense, _, _ = analyze_transactions(mtd_transactions, exclude_transfers=True)

    # Prefer API values if they look valid, otherwise fall back to transaction sum
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

    sr_color = "#3a7d52" if savings_rate >= 20 else ("#b05040" if savings_rate < 5 else "#a88c5a")

    return f"""
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td style="padding:6px 0;font-size:12px;color:#b8b0a4">MTD Income</td>
        <td style="padding:6px 0;text-align:right;font-size:12px;color:#3a7d52">{fmt(mtd_income)}</td>
      </tr>
      <tr>
        <td style="padding:6px 0;font-size:12px;color:#b8b0a4">MTD Spending (excl. transfers)</td>
        <td style="padding:6px 0;text-align:right;font-size:12px;color:#b05040">{fmt(mtd_expense)}</td>
      </tr>
      <tr>
        <td style="padding:6px 0;font-size:12px;color:#b8b0a4">Projected Month-End Spend</td>
        <td style="padding:6px 0;text-align:right;font-size:12px;color:#777">{fmt(run_rate)}</td>
      </tr>
      <tr>
        <td style="padding:8px 0 4px;font-size:11px;color:#555">{yesterday.strftime('%B')} · Day {days_elapsed} of {days_in_month}</td>
        <td style="padding:8px 0 4px;text-align:right;font-size:11px;color:#444">{pct(month_pct)}</td>
      </tr>
      <tr>
        <td colspan="2" style="padding:4px 0 10px">
          <div style="background:#222;border-radius:2px;height:3px;width:100%">
            <div style="background:#3a7d52;border-radius:2px;height:3px;width:{min(int(month_pct), 100)}%"></div>
          </div>
        </td>
      </tr>
      <tr>
        <td style="font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:#444">Savings Rate</td>
        <td style="text-align:right;font-size:16px;font-weight:500;color:{sr_color}">{pct(savings_rate)}</td>
      </tr>
    </table>"""


def build_highlights_html(transactions, accounts):
    items = []

    biggest = get_biggest_txn(transactions)
    if biggest:
        merchant = (biggest.get("merchant") or {}).get("name") or "Unknown"
        amount   = abs(float(biggest.get("amount", 0)))
        items.append(f'<div style="padding:8px 0;border-bottom:1px solid #1a1a1a;font-size:12px;color:#b8b0a4">💸 Largest purchase: <strong style="color:#e8e2d9">{merchant}</strong> — {red(fmt(amount))}</div>')

    hsa_txns = [t for t in transactions if float(t.get("amount", 0)) < 0
                and is_hsa((t.get("category") or {}).get("name", ""))]
    if hsa_txns:
        hsa_total = sum(abs(float(t.get("amount", 0))) for t in hsa_txns)
        items.append(f'<div style="padding:8px 0;border-bottom:1px solid #1a1a1a;font-size:12px;color:#b8b0a4">🏥 HSA-eligible spend: {green(fmt(hsa_total))} — log receipts in Monarch</div>')

    liability_accts = [a for a in accounts if a.get("isAsset") is False and float(a.get("currentBalance", 0) or 0) > 0]
    if liability_accts:
        total_debt = sum(float(a.get("currentBalance", 0) or 0) for a in liability_accts)
        items.append(f'<div style="padding:8px 0;border-bottom:1px solid #1a1a1a;font-size:12px;color:#b8b0a4">🎯 Total debt remaining: {red(fmt(total_debt))}</div>')

    invest_name_hints = ["vanguard", "wealthfront", "merrill", "fidelity", "roth", "401", "ira", "529", "brokerage", "s&p", "nasdaq"]
    invest_accts = [a for a in accounts
                    if any(k in (a.get("displayName") or a.get("name", "")).lower() for k in invest_name_hints)
                    and a.get("isAsset") is not False
                    and float(a.get("currentBalance", 0) or 0) > 0]
    if invest_accts:
        inv_total = sum(float(a.get("currentBalance", 0) or 0) for a in invest_accts)
        items.append(f'<div style="padding:8px 0;border-bottom:1px solid #1a1a1a;font-size:12px;color:#b8b0a4">📈 Investment & retirement accounts: {green(fmt(inv_total))}</div>')

    return "".join(items)


# ── Email Assembly ─────────────────────────────────────────────────────────────

def build_email(yesterday, transactions, mtd_transactions, accounts, cashflow):
    date_str  = yesterday.strftime("%A, %B %-d")
    generated = date.today().strftime("%B %-d, %Y")

    acct_html, net_worth         = build_accounts_html(accounts)
    txn_html, income, expenses, net = build_transactions_html(transactions)
    spending_html   = build_spending_breakdown_html(mtd_transactions)
    cashflow_html   = build_cashflow_html(cashflow, mtd_transactions, yesterday)
    highlights_html = build_highlights_html(transactions, accounts)

    txn_count = len(transactions)

    # Net: negative = spent more than earned (normal day with no paycheck)
    net_val   = income - expenses
    net_color = "#3a7d52" if net_val >= 0 else "#b05040"
    net_label = f"+{fmt(net_val)}" if net_val >= 0 else f"-{fmt(abs(net_val))}"

    summary_bar = f"""
  <div style="background:#161616;border:1px solid #2a2a2a;border-top:none;display:table;width:100%;box-sizing:border-box">
    <div style="display:table-cell;padding:16px 0;text-align:center;border-right:1px solid #1e1e1e">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:#444;margin-bottom:6px">Income</div>
      <div style="font-size:15px;font-weight:500;color:#3a7d52">{fmt(income)}</div>
    </div>
    <div style="display:table-cell;padding:16px 0;text-align:center;border-right:1px solid #1e1e1e">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:#444;margin-bottom:6px">Spent</div>
      <div style="font-size:15px;font-weight:500;color:#b05040">{fmt(expenses)}</div>
    </div>
    <div style="display:table-cell;padding:16px 0;text-align:center">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:#444;margin-bottom:6px">Net</div>
      <div style="font-size:15px;font-weight:500;color:{net_color}">{net_label}</div>
    </div>
  </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Monarch Digest · {date_str}</title>
</head>
<body style="margin:0;padding:32px 16px;background:#0f0f0f;font-family:'Courier New',monospace">
<div style="max-width:580px;margin:0 auto">

  <!-- Header -->
  <div style="background:#161616;border:1px solid #2a2a2a;border-radius:4px 4px 0 0;padding:28px 32px 22px">
    <div style="display:table;width:100%">
      <div style="display:table-cell;vertical-align:top">
        <div style="font-size:9px;letter-spacing:.25em;text-transform:uppercase;color:#3a7d52;margin-bottom:6px">Monarch Daily Digest</div>
        <div style="font-size:22px;color:#f0ebe3;font-weight:300">{date_str}</div>
      </div>
      <div style="display:table-cell;vertical-align:top;text-align:right">
        <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:#555;margin-bottom:4px">Net Worth</div>
        <div style="font-size:20px;color:#f0ebe3;font-weight:500">{fmt(net_worth)}</div>
      </div>
    </div>
  </div>

  {section("Account Balances", None, acct_html)}
  {section("Yesterday's Transactions", f"{txn_count} transactions", txn_html)}
  {summary_bar}
  {section("Highlights", None, highlights_html)}
  {section("Month-to-Date Cashflow", yesterday.strftime("%B %Y"), cashflow_html)}
  {section("Spending Breakdown", None, spending_html)}

  <!-- Footer -->
  <div style="background:#111;border:1px solid #2a2a2a;border-top:none;border-radius:0 0 4px 4px;padding:12px 32px;display:table;width:100%;box-sizing:border-box">
    <span style="display:table-cell;font-size:10px;color:#2a2a2a">Monarch Money · {generated} · 7:00 AM ET</span>
    <span style="display:table-cell;font-size:10px;color:#3a6347;text-align:right">{RECIPIENT_EMAIL}</span>
  </div>

</div>
</body>
</html>"""


# ── Send ───────────────────────────────────────────────────────────────────────

def send_email(subject: str, html_body: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_ADDRESS
    msg["To"]      = RECIPIENT_EMAIL
    msg.attach(MIMEText(html_body, "html"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASS)
        server.sendmail(GMAIL_ADDRESS, RECIPIENT_EMAIL, msg.as_string())
    print(f"✓ Digest sent to {RECIPIENT_EMAIL}")


async def main():
    print("Fetching Monarch data...")
    yesterday, transactions, mtd_transactions, accounts, cashflow = await fetch_data()
    print(f"  {len(transactions)} txns yesterday · {len(mtd_transactions)} MTD · {len(accounts)} accounts")

    html = build_email(yesterday, transactions, mtd_transactions, accounts, cashflow)

    net_worth, _, _ = compute_net_worth(accounts)
    date_str = yesterday.strftime("%b %-d")
    subject  = f"💰 Monarch · {date_str} · {len(transactions)} txns · NW {fmt(net_worth)}"
    send_email(subject, html)


if __name__ == "__main__":
    asyncio.run(main())
