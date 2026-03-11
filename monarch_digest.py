#!/usr/bin/env python3
"""
Monarch Money Daily Digest
Pulls yesterday's transactions and account balance changes, sends a formatted HTML email via Gmail SMTP.
"""

import asyncio
import os
import smtplib
from datetime import date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from monarchmoney import MonarchMoney
import monarchmoney.monarchmoney as _mm
_mm.BASE_URL = "https://api.monarch.com"

# ── Config (set these as environment variables or GitHub Actions secrets) ──────
MONARCH_EMAIL    = os.environ["MONARCH_EMAIL"]
MONARCH_PASSWORD = os.environ["MONARCH_PASSWORD"]
MONARCH_MFA_KEY  = os.environ.get("MONARCH_MFA_KEY", "")   # leave blank if no MFA

GMAIL_ADDRESS    = os.environ["GMAIL_ADDRESS"]              # your Gmail
GMAIL_APP_PASS   = os.environ["GMAIL_APP_PASSWORD"]         # Gmail App Password

RECIPIENT_EMAIL  = os.environ.get("RECIPIENT_EMAIL", GMAIL_ADDRESS)

HSA_CATEGORY_KEYWORDS = ["medical", "pharmacy", "dental", "vision", "health"]
# ──────────────────────────────────────────────────────────────────────────────


def fmt_dollars(amount: float, sign: bool = False) -> str:
    prefix = "+" if sign and amount > 0 else ""
    return f"{prefix}${abs(amount):,.2f}"


def delta_arrow(amount: float) -> str:
    if amount > 0.01:
        return f'<span style="color:#3a7d52">▲ {fmt_dollars(amount)}</span>'
    elif amount < -0.01:
        return f'<span style="color:#b05040">▼ {fmt_dollars(abs(amount))}</span>'
    else:
        return '<span style="color:#666">— no change</span>'


def is_hsa_eligible(category_name: str) -> bool:
    name = (category_name or "").lower()
    return any(k in name for k in HSA_CATEGORY_KEYWORDS)


async def fetch_data():
    mm = MonarchMoney()

    await mm.login(
        email=MONARCH_EMAIL,
        password=MONARCH_PASSWORD,
        mfa_secret_key=MONARCH_MFA_KEY if MONARCH_MFA_KEY else None,
        save_session=False,
        use_saved_session=False,
    )

    yesterday = date.today() - timedelta(days=1)
    day_before = yesterday - timedelta(days=1)

    # Transactions from yesterday
    txn_data = await mm.get_transactions(
        start_date=str(yesterday),
        end_date=str(yesterday),
    )
    transactions = txn_data.get("allTransactions", {}).get("results", [])

    # Account balances (returns recent snapshots)
    accounts_data = await mm.get_accounts()
    accounts = accounts_data.get("accounts", [])

    # Balance history for delta calculation
    balance_history = await mm.get_account_type_balances()

    return yesterday, transactions, accounts, balance_history


def build_account_rows(accounts) -> tuple[str, float]:
    """Returns HTML rows and total net worth."""
    rows_html = ""
    net_worth = 0.0

    for acct in accounts:
        name = acct.get("displayName") or acct.get("name", "Unknown")
        inst = acct.get("institution", {}).get("name", "") if acct.get("institution") else ""
        balance = float(acct.get("currentBalance", 0) or 0)
        is_liability = acct.get("isAsset") is False

        # Use signedBalance for net worth calculation
        signed = -balance if is_liability else balance
        net_worth += signed

        # Delta: Monarch doesn't always expose prior-day balance per account cleanly,
        # so we show the today balance. You can extend this with get_account_snapshots.
        delta_placeholder = ""  # extended in pro version with snapshot diff

        balance_display = f"-{fmt_dollars(balance)}" if is_liability else fmt_dollars(balance)
        color = "color:#999" if is_liability else ""

        rows_html += f"""
        <tr>
          <td style="padding:8px 0;border-bottom:1px solid #1e1e1e;font-size:13px;color:#c0b8ae">{name}
            {f'<span style="color:#444;font-size:11px"> · {inst}</span>' if inst else ""}
          </td>
          <td style="padding:8px 0;border-bottom:1px solid #1e1e1e;text-align:right;font-size:13px;{color}">{balance_display}</td>
        </tr>"""

    return rows_html, net_worth


def build_transaction_rows(transactions) -> tuple[str, float, float, float]:
    rows_html = ""
    total_income = 0.0
    total_expense = 0.0

    for txn in transactions:
        merchant = txn.get("merchant", {}).get("name") or txn.get("plaidName") or "Unknown"
        category = txn.get("category", {}).get("name", "Uncategorized")
        amount = float(txn.get("amount", 0))
        is_income = amount > 0  # Monarch: positive = income, negative = expense

        if is_income:
            total_income += amount
            amt_html = f'<span style="color:#3a7d52">+{fmt_dollars(amount)}</span>'
        else:
            total_expense += abs(amount)
            amt_html = f'<span style="color:#b05040">-{fmt_dollars(abs(amount))}</span>'

        hsa_badge = ""
        if not is_income and is_hsa_eligible(category):
            hsa_badge = '<span style="display:inline-block;background:#1e2a1e;color:#3a7d52;border:1px solid #2a4030;border-radius:2px;font-size:9px;letter-spacing:.1em;padding:1px 5px;margin-left:6px">HSA</span>'

        rows_html += f"""
        <tr>
          <td style="padding:9px 0;border-bottom:1px solid #1e1e1e">
            <div style="font-size:13px;color:#c0b8ae">{merchant}</div>
            <div style="font-size:11px;color:#444;margin-top:2px">{category}{hsa_badge}</div>
          </td>
          <td style="padding:9px 0;border-bottom:1px solid #1e1e1e;text-align:right;font-size:13px">{amt_html}</td>
        </tr>"""

    net = total_income - total_expense
    return rows_html, total_income, total_expense, net


def build_email_html(yesterday: date, transactions, accounts, net_worth: float) -> str:
    acct_rows_html, _ = build_account_rows(accounts)
    txn_rows_html, income, expense, net = build_transaction_rows(transactions)
    txn_count = len(transactions)
    date_str = yesterday.strftime("%A, %B %-d")
    generated = date.today().strftime("%B %-d, %Y")
    net_color = "#3a7d52" if net >= 0 else "#b05040"
    net_sign = "+" if net >= 0 else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Monarch Digest · {date_str}</title>
</head>
<body style="margin:0;padding:32px 16px;background:#0f0f0f;font-family:'Courier New',monospace">
<div style="max-width:560px;margin:0 auto">

  <!-- Header -->
  <div style="background:#161616;border:1px solid #2a2a2a;border-radius:4px 4px 0 0;padding:28px 32px 22px;display:flex;justify-content:space-between;align-items:flex-start">
    <div>
      <div style="font-size:9px;letter-spacing:.25em;text-transform:uppercase;color:#3a7d52;margin-bottom:6px">Monarch Daily Digest</div>
      <div style="font-size:22px;color:#f0ebe3;font-weight:300">{date_str}</div>
    </div>
    <div style="text-align:right">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:#555;margin-bottom:4px">Net Worth</div>
      <div style="font-size:20px;color:#f0ebe3;font-weight:500">{fmt_dollars(net_worth)}</div>
    </div>
  </div>

  <!-- Account Balances -->
  <div style="background:#161616;border:1px solid #2a2a2a;border-top:none;padding:20px 32px">
    <div style="font-size:9px;letter-spacing:.2em;text-transform:uppercase;color:#555;margin-bottom:14px">Account Balances</div>
    <table width="100%" cellpadding="0" cellspacing="0">
      {acct_rows_html}
    </table>
  </div>

  <!-- Transactions -->
  <div style="background:#161616;border:1px solid #2a2a2a;border-top:none;padding:20px 32px">
    <div style="font-size:9px;letter-spacing:.2em;text-transform:uppercase;color:#555;margin-bottom:14px">
      Yesterday's Transactions
      <span style="display:inline-block;background:#1e2a1e;color:#3a7d52;border:1px solid #2a4030;border-radius:2px;font-size:9px;letter-spacing:.1em;padding:1px 6px;margin-left:8px">{txn_count} transactions</span>
    </div>
    <table width="100%" cellpadding="0" cellspacing="0">
      {txn_rows_html if txn_rows_html else '<tr><td style="color:#444;font-size:12px;padding:8px 0">No transactions recorded yesterday.</td></tr>'}
    </table>
  </div>

  <!-- Summary Bar -->
  <div style="background:#161616;border:1px solid #2a2a2a;border-top:none;display:flex">
    <div style="flex:1;padding:16px 0;text-align:center;border-right:1px solid #1e1e1e">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:#444;margin-bottom:6px">Income</div>
      <div style="font-size:15px;font-weight:500;color:#3a7d52">{fmt_dollars(income)}</div>
    </div>
    <div style="flex:1;padding:16px 0;text-align:center;border-right:1px solid #1e1e1e">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:#444;margin-bottom:6px">Spent</div>
      <div style="font-size:15px;font-weight:500;color:#b05040">{fmt_dollars(expense)}</div>
    </div>
    <div style="flex:1;padding:16px 0;text-align:center">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:#444;margin-bottom:6px">Net</div>
      <div style="font-size:15px;font-weight:500;color:{net_color}">{net_sign}{fmt_dollars(net)}</div>
    </div>
  </div>

  <!-- Footer -->
  <div style="background:#111;border:1px solid #2a2a2a;border-top:none;border-radius:0 0 4px 4px;padding:12px 32px;display:flex;justify-content:space-between;align-items:center">
    <span style="font-size:10px;color:#333">Pulled from Monarch Money · {generated} at 7:00 AM</span>
    <span style="font-size:10px;color:#3a6347">{RECIPIENT_EMAIL}</span>
  </div>

</div>
</body>
</html>"""


def send_email(subject: str, html_body: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = RECIPIENT_EMAIL
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASS)
        server.sendmail(GMAIL_ADDRESS, RECIPIENT_EMAIL, msg.as_string())
    print(f"✓ Digest sent to {RECIPIENT_EMAIL}")


async def main():
    print("Fetching Monarch data...")
    yesterday, transactions, accounts, balance_history = await fetch_data()

    _, net_worth = build_account_rows(accounts)
    html = build_email_html(yesterday, transactions, accounts, net_worth)

    date_str = yesterday.strftime("%b %-d")
    subject = f"💰 Monarch Digest · {date_str} · {len(transactions)} transactions"
    send_email(subject, html)


if __name__ == "__main__":
    asyncio.run(main())
