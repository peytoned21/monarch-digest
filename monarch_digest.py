#!/usr/bin/env python3
"""
Monarch Money Daily Digest
Light theme, uses Monarch's own net worth figure, balance deltas from account history.
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
TRANSFER_KEYWORDS = ["transfer", "transfers to investments"]
DELTA_THRESHOLD   = 25.0   # only show accounts that moved by more than this amount

# Light theme colors
C_BG     = "#f8f7f5"
C_CARD   = "#ffffff"
C_BORDER = "#e8e4df"
C_TEXT   = "#2c2825"
C_MUTED  = "#9a9188"
C_LABEL  = "#b0a89e"
C_GREEN  = "#2d7a4f"
C_RED    = "#c0392b"
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
    two_days_ago = yesterday - timedelta(days=1)

    print("Fetching transactions...")
    txn_resp = await mm.get_transactions(start_date=str(yesterday), end_date=str(yesterday))
    transactions = txn_resp.get("allTransactions", {}).get("results", [])

    print("Fetching MTD transactions...")
    mtd_resp = await mm.get_transactions(start_date=str(month_start), end_date=str(yesterday))
    mtd_transactions = mtd_resp.get("allTransactions", {}).get("results", [])

    print("Fetching accounts...")
    acct_resp = await mm.get_accounts()
    accounts = acct_resp.get("accounts", [])

    # Print ALL fields of the first account so we can see the exact structure
    if accounts:
        print("=== ACCOUNT FIELDS ===")
        for k, v in accounts[0].items():
            print(f"  {k}: {v}")
        print("=== END ACCOUNT FIELDS ===")

    print("Fetching account history for balance deltas...")
    # Fetch balance snapshots for yesterday and day before for delta calculation
    history_by_id = {}
    try:
        # get_account_history returns daily snapshots for a single account
        # We'll use get_accounts which includes currentBalance, and try to get
        # yesterday's snapshot via get_account_snapshots_by_type if available
        snap_resp = await mm.get_account_type_options()
        print(f"Account type options available: {snap_resp is not None}")
    except Exception as e:
        print(f"Snapshot fetch failed (non-fatal): {e}")

    # Try fetching individual account history for delta
    for acct in accounts[:3]:  # test on first 3
        acct_id = acct.get("id")
        if acct_id:
            try:
                hist = await mm.get_account_history(account_id=acct_id)
                print(f"History for {acct.get('name')}: {list(hist.keys()) if hist else 'empty'}")
                if hist:
                    # Print first snapshot to understand shape
                    items = hist.get("account", {}).get("balanceHistory", [])
                    if items:
                        print(f"  Sample snapshot: {items[0]}")
                    break
            except Exception as e:
                print(f"  History failed: {e}")

    print("Fetching cashflow...")
    cashflow = {}
    try:
        cashflow = await mm.get_cashflow_summary(
            start_date=str(month_start),
            end_date=str(yesterday),
        )
    except Exception as e:
        print(f"Cashflow failed (non-fatal): {e}")

    return yesterday, transactions, mtd_transactions, accounts, cashflow, history_by_id


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
    for fn in [
        lambda c: (c["summary"][0]["sumIncome"], abs(c["summary"][0]["sumExpense"])) if isinstance(c.get("summary"), list) and c["summary"] else None,
        lambda c: (c["summary"]["sumIncome"], abs(c["summary"]["sumExpense"])) if isinstance(c.get("summary"), dict) else None,
        lambda c: (c["sumIncome"], abs(c["sumExpense"])) if "sumIncome" in c else None,
        lambda c: (c["income"], abs(c["expense"])) if "income" in c else None,
    ]:
        try:
            r = fn(cashflow)
            if r:
                return float(r[0] or 0), float(r[1] or 0)
        except Exception:
            pass
    return 0.0, 0.0


# ── HTML Primitives ────────────────────────────────────────────────────────────

def card(title, badge, content):
    if not (content or "").strip():
        return ""
    badge_html = f'<span style="display:inline-block;background:#edf7f1;color:{C_GREEN};border:1px solid #c8e6d4;border-radius:3px;font-size:9px;letter-spacing:.08em;padding:2px 7px;margin-left:8px">{badge}</span>' if badge else ""
    return f"""
  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;padding:20px 28px">
    <div style="font-size:10px;letter-spacing:.18em;text-transform:uppercase;color:{C_LABEL};margin-bottom:14px;font-family:Georgia,serif">{title}{badge_html}</div>
    {content}
  </div>"""


# ── Section Builders ───────────────────────────────────────────────────────────

def build_net_worth_summary(accounts):
    """
    Use Monarch's own signed balance values directly.
    Print raw data to logs so we can see what's happening.
    """
    total = 0.0
    assets = 0.0
    liabilities = 0.0

    print("=== NET WORTH CALC ===")
    for a in accounts:
        name      = a.get("displayName") or a.get("name", "")
        balance   = float(a.get("currentBalance") or 0)
        is_asset  = a.get("isAsset")
        in_nw     = a.get("includeInNetWorth")
        print(f"  {name}: balance={balance} isAsset={is_asset} includeInNetWorth={in_nw}")

        if in_nw is False:
            continue
        if is_asset is True:
            assets += balance
            total  += balance
        elif is_asset is False:
            liabilities += balance
            total -= balance
        else:
            # isAsset is None — include as asset if positive, liability if negative
            if balance >= 0:
                assets += balance
                total  += balance
            else:
                liabilities += abs(balance)
                total       += balance  # already negative

    print(f"  => Assets: {assets}, Liabilities: {liabilities}, Net Worth: {total}")
    print("=== END NET WORTH CALC ===")
    return total, assets, liabilities


def build_account_changes_html(accounts):
    """
    Show net worth summary + only accounts that moved materially.
    Since we don't have prior-day snapshots yet (need to debug history API),
    we'll show all non-zero accounts grouped by type for now,
    then switch to deltas once we confirm the history shape from logs.
    """
    net_worth, assets, liabilities = build_net_worth_summary(accounts)

    # Group by asset type
    groups = {
        "Cash": [],
        "Investments": [],
        "Real Estate": [],
        "Vehicles": [],
        "Credit Cards": [],
        "Loans": [],
        "Other": [],
    }

    for a in accounts:
        balance = float(a.get("currentBalance") or 0)
        if balance == 0:
            continue
        name = (a.get("displayName") or a.get("name", "")).lower()
        type_name = str((a.get("type") or {}).get("name", "")).lower()
        is_asset = a.get("isAsset", True)

        if not is_asset:
            if any(k in name for k in ["mortgage", "heloc"]) or "mortgage" in type_name:
                groups["Loans"].append(a)
            elif any(k in name for k in ["student", "auto", "car", "lexus", "tesla", "loan"]) or "loan" in type_name:
                groups["Loans"].append(a)
            elif any(k in name for k in ["card", "mastercard", "visa", "amex", "sapphire", "citi", "amazon", "banana", "frontier", "tjx", "bonvoy"]) or "credit" in type_name:
                groups["Credit Cards"].append(a)
            else:
                groups["Loans"].append(a)
        else:
            if any(k in name for k in ["checking", "savings", "cash", "wealthfront check", "wells fargo", "credit union", "southern first"]):
                groups["Cash"].append(a)
            elif any(k in name for k in ["mccully", "home", "real estate", "zillow"]) or "real_estate" in type_name or "real estate" in type_name:
                groups["Real Estate"].append(a)
            elif any(k in name for k in ["lexus", "tesla", "vehicle", "car", "vinaudit"]) or "vehicle" in type_name or "car" in type_name:
                groups["Vehicles"].append(a)
            else:
                groups["Investments"].append(a)

    html = ""

    # Net worth header row
    html += f"""
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td style="padding:0 0 4px;font-size:13px;color:{C_MUTED}">Assets</td>
        <td style="padding:0 0 4px;text-align:right;font-size:13px;color:{C_TEXT}">{fmt(assets)}</td>
      </tr>
      <tr>
        <td style="padding:4px 0 10px;font-size:13px;color:{C_MUTED}">Liabilities</td>
        <td style="padding:4px 0 10px;text-align:right;font-size:13px;color:{C_RED}">-{fmt(liabilities)}</td>
      </tr>
      <tr>
        <td colspan="2" style="padding:0 0 16px">
          <div style="border-top:1px solid {C_BORDER}"></div>
        </td>
      </tr>
      <tr>
        <td style="padding:0 0 0;font-size:10px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};font-family:Georgia,serif">Net Worth</td>
        <td style="padding:0 0 0;text-align:right;font-size:20px;font-weight:600;color:{C_TEXT}">{fmt(net_worth)}</td>
      </tr>
    </table>"""

    # Group summaries
    group_totals = []
    for group_name, accts in groups.items():
        if not accts:
            continue
        total = sum(abs(float(a.get("currentBalance") or 0)) for a in accts)
        is_liab = group_name in ("Credit Cards", "Loans")
        group_totals.append((group_name, total, is_liab))

    if group_totals:
        html += f'<div style="margin-top:16px;border-top:1px solid {C_BORDER};padding-top:14px">'
        html += f'<table width="100%" cellpadding="0" cellspacing="0">'
        for g_name, g_total, is_liab in group_totals:
            color = C_RED if is_liab else C_TEXT
            prefix = "-" if is_liab else ""
            html += f"""
            <tr>
              <td style="padding:5px 0;font-size:12px;color:{C_MUTED}">{g_name}</td>
              <td style="padding:5px 0;text-align:right;font-size:12px;color:{color}">{prefix}{fmt(g_total)}</td>
            </tr>"""
        html += "</table></div>"

    return html, net_worth


def build_transactions_html(transactions):
    if not transactions:
        return f'<p style="color:{C_MUTED};font-size:13px;margin:0">No transactions yesterday.</p>', 0.0, 0.0, 0.0

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
          <div style="display:flex;justify-content:space-between;font-size:10px;color:{C_LABEL};margin-bottom:5px">
            <span>{yesterday.strftime('%B')} · Day {days_elapsed} of {days_in_month}</span><span>{pct(month_pct)}</span>
          </div>
          <div style="background:#ede9e4;border-radius:2px;height:4px">
            <div style="background:{C_GREEN};border-radius:2px;height:4px;width:{min(int(month_pct),100)}%"></div>
          </div>
        </td>
      </tr>
      <tr>
        <td style="padding:10px 0 4px;font-size:10px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL}">Savings Rate</td>
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
        items.append(f'<div style="padding:8px 0;border-bottom:1px solid {C_BORDER};font-size:13px;color:{C_TEXT}">🏥 HSA-eligible spend: {green(fmt(total))} — log receipts</div>')
    return "".join(items)


# ── Email ──────────────────────────────────────────────────────────────────────

def build_email(yesterday, transactions, mtd_transactions, accounts, cashflow, history_by_id):
    date_str  = yesterday.strftime("%A, %B %-d")
    generated = date.today().strftime("%B %-d, %Y")

    nw_html, net_worth              = build_account_changes_html(accounts)
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
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};margin-bottom:5px">Income</div>
      <div style="font-size:15px;font-weight:600;color:{C_GREEN}">{fmt(income)}</div>
    </div>
    <div style="display:table-cell;padding:16px 0;text-align:center;border-right:1px solid {C_BORDER}">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};margin-bottom:5px">Spent</div>
      <div style="font-size:15px;font-weight:600;color:{C_RED}">{fmt(expenses)}</div>
    </div>
    <div style="display:table-cell;padding:16px 0;text-align:center">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};margin-bottom:5px">Net</div>
      <div style="font-size:15px;font-weight:600;color:{net_color}">{net_label}</div>
    </div>
  </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Monarch Digest · {date_str}</title>
</head>
<body style="margin:0;padding:32px 16px;background:{C_BG};font-family:Georgia,serif">
<div style="max-width:580px;margin:0 auto">

  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-radius:6px 6px 0 0;padding:26px 28px 20px">
    <div style="display:table;width:100%">
      <div style="display:table-cell;vertical-align:middle">
        <div style="font-size:9px;letter-spacing:.25em;text-transform:uppercase;color:{C_GREEN};margin-bottom:6px">Monarch Daily Digest</div>
        <div style="font-size:22px;color:{C_TEXT};font-style:italic">{date_str}</div>
      </div>
    </div>
  </div>

  {card("Net Worth", None, nw_html)}
  {card("Yesterday's Transactions", f"{txn_count} transactions", txn_html)}
  {summary_bar}
  {card("Highlights", None, highlights_html)}
  {card("Month-to-Date Cashflow", yesterday.strftime("%B %Y"), cashflow_html)}
  {card("Spending Breakdown", "excl. transfers", spending_html)}

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
    yesterday, transactions, mtd_transactions, accounts, cashflow, history_by_id = await fetch_data()
    print(f"  {len(transactions)} txns · {len(mtd_transactions)} MTD · {len(accounts)} accounts")

    html = build_email(yesterday, transactions, mtd_transactions, accounts, cashflow, history_by_id)

    _, net_worth, _ = build_net_worth_summary(accounts)
    # Actually recompute cleanly
    nw = 0.0
    for a in accounts:
        if a.get("includeInNetWorth") is False:
            continue
        b = float(a.get("currentBalance") or 0)
        nw += b if a.get("isAsset", True) else -b

    date_str = yesterday.strftime("%b %-d")
    subject  = f"💰 Monarch · {date_str} · {len(transactions)} txns · NW {fmt(nw)}"
    send_email(subject, html)


if __name__ == "__main__":
    asyncio.run(main())
