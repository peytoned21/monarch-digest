#!/usr/bin/env python3
"""
Monarch Money Daily Digest — Financial Advisor Edition
Sections:
  1. Advisory Brief    — one-paragraph narrative on where you stand
  2. Net Worth         — assets / liabilities + notable account moves
  3. Action Items      — time-sensitive things needing attention today
  4. Yesterday         — transactions with context
  5. Budget Pulse      — categories: spent vs. budget, % used, status
  6. Month-to-Date     — cashflow, savings rate, projection
  7. Upcoming Bills    — next 7 days of recurring transactions
  8. Debt Tracker      — balances for each tracked liability
  9. Spending Breakdown — top categories MTD
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
TRANSFER_KEYWORDS = ["transfer", "transfers to investments", "credit card payment"]
INCOME_KEYWORDS   = ["paycheck", "income", "salary", "bonus", "direct deposit", "reimbursement"]
DELTA_THRESHOLD   = 100.0   # min account move to show in net worth notable moves
BILL_LOOKAHEAD    = 7       # days ahead to show upcoming bills

# Debt accounts — display name substrings (case-insensitive)
DEBT_ACCOUNTS = ["mortgage", "tesla", "lexus", "stanford", "heloc"]

# Colors
C_BG      = "#f5f4f1"
C_CARD    = "#ffffff"
C_BORDER  = "#e5e1db"
C_TEXT    = "#1e1c1a"
C_MUTED   = "#8a8278"
C_LABEL   = "#a89f95"
C_GREEN   = "#1f6b42"
C_LTGREEN = "#edf7f1"
C_BGREEN  = "#c4dfd0"
C_RED     = "#b03020"
C_LTRED   = "#fdf0ee"
C_BRED    = "#e8c2bc"
C_AMBER   = "#9a6200"
C_LTAMBER = "#fdf6e3"
C_BAMBER  = "#e8d9a0"
C_BLUE    = "#1a5276"
C_LTBLUE  = "#eaf2fb"
C_BBLUE   = "#bad4ea"
# ──────────────────────────────────────────────────────────────────────────────

def fmt(amount: float) -> str:
    return f"${abs(amount):,.2f}"

def pct(value: float) -> str:
    return f"{value:.1f}%"

def is_hsa(cat: str) -> bool:
    return any(k in (cat or "").lower() for k in HSA_KEYWORDS)

def is_transfer(cat: str) -> bool:
    return any(k in (cat or "").lower() for k in TRANSFER_KEYWORDS)

def is_income_cat(cat: str) -> bool:
    return any(k in (cat or "").lower() for k in INCOME_KEYWORDS)

def green(t):  return f'<span style="color:{C_GREEN};font-weight:600">{t}</span>'
def red(t):    return f'<span style="color:{C_RED};font-weight:600">{t}</span>'

def badge(text, bg, border, color):
    return (f'<span style="display:inline-block;background:{bg};color:{color};'
            f'border:1px solid {border};border-radius:3px;font-size:9px;'
            f'letter-spacing:.06em;padding:1px 6px;margin-left:6px;vertical-align:middle">'
            f'{text}</span>')

def section_label(text):
    return (f'<div style="font-size:9px;letter-spacing:.22em;text-transform:uppercase;'
            f'color:{C_LABEL};margin-bottom:14px;padding-bottom:8px;'
            f'border-bottom:1px solid {C_BORDER}">{text}</div>')


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
    txn_resp     = await mm.get_transactions(start_date=str(yesterday), end_date=str(yesterday))
    transactions = txn_resp.get("allTransactions", {}).get("results", [])

    print("Fetching MTD transactions...")
    mtd_resp         = await mm.get_transactions(start_date=str(month_start), end_date=str(yesterday))
    mtd_transactions = mtd_resp.get("allTransactions", {}).get("results", [])

    print("Fetching accounts...")
    acct_resp = await mm.get_accounts()
    accounts  = acct_resp.get("accounts", [])

    print("Fetching account history for balance deltas...")
    history_by_id = {}
    two_days_ago  = str(yesterday - timedelta(days=1))
    yest_str      = str(yesterday)
    for acct in accounts:
        acct_id = acct.get("id")
        if not acct_id:
            continue
        try:
            hist      = await mm.get_account_history(account_id=acct_id)
            snapshots = hist if isinstance(hist, list) else (
                hist.get("account", {}).get("balanceHistory") or
                hist.get("balanceHistory") or hist.get("history") or [])
            bal_map = {s.get("date"): float(s.get("balance") or 0)
                       for s in snapshots if isinstance(s, dict)}
            if yest_str in bal_map or two_days_ago in bal_map:
                history_by_id[str(acct_id)] = bal_map
        except Exception:
            pass

    print("Fetching budgets...")
    budgets = {}
    try:
        budgets = await mm.get_budgets(
            start_date=str(month_start),
            end_date=str(yesterday),
        )
        print(f"  Budget keys: {list(budgets.keys()) if isinstance(budgets, dict) else type(budgets)}")
        if isinstance(budgets, dict):
            for k, v in budgets.items():
                sample = v[:1] if isinstance(v, list) and v else v
                print(f"  budget[{k!r}] type={type(v).__name__} sample={str(sample)[:300]}")
    except Exception as e:
        print(f"  Budgets failed (non-fatal): {e}")

    print("Fetching recurring transactions...")
    recurring = []
    try:
        rec_resp  = await mm.get_recurring_transactions()
        print(f"  Recurring keys: {list(rec_resp.keys()) if isinstance(rec_resp, dict) else type(rec_resp)}")
        if isinstance(rec_resp, dict):
            for k, v in rec_resp.items():
                sample = v[:1] if isinstance(v, list) and v else v
                print(f"  recurring[{k!r}] type={type(v).__name__} sample={str(sample)[:300]}")
        elif isinstance(rec_resp, list) and rec_resp:
            print(f"  recurring[0] keys={list(rec_resp[0].keys()) if isinstance(rec_resp[0], dict) else rec_resp[0]}")
            print(f"  recurring[0] sample={str(rec_resp[0])[:400]}")
        recurring = rec_resp if isinstance(rec_resp, list) else (
            rec_resp.get("recurringTransactionItems") or
            rec_resp.get("items") or
            rec_resp.get("recurring") or [])
    except Exception as e:
        print(f"  Recurring failed (non-fatal): {e}")

    print("Fetching cashflow...")
    cashflow = {}
    try:
        cashflow = await mm.get_cashflow_summary(
            start_date=str(month_start),
            end_date=str(yesterday),
        )
    except Exception as e:
        print(f"  Cashflow failed (non-fatal): {e}")

    return (yesterday, transactions, mtd_transactions, accounts,
            cashflow, history_by_id, budgets, recurring)


# ── Net Worth ──────────────────────────────────────────────────────────────────

def compute_net_worth(accounts):
    net_worth = assets = liabilities = 0.0
    for a in accounts:
        if a.get("includeInNetWorth") is False:
            continue
        b = float(a.get("currentBalance") or 0)
        net_worth += b
        if b >= 0:
            assets += b
        else:
            liabilities += abs(b)
    return net_worth, assets, liabilities


# ── Budget Parsing ─────────────────────────────────────────────────────────────

def parse_budgets(budgets_raw):
    result = []
    if not budgets_raw:
        return result
    items = (budgets_raw.get("budgets") or budgets_raw.get("budget") or
             budgets_raw.get("categoryBudgets") or [])
    if not items and isinstance(budgets_raw, list):
        items = budgets_raw
    for item in items:
        try:
            cat_obj  = item.get("category") or item.get("group") or {}
            cat_name = cat_obj.get("name") or item.get("name") or item.get("categoryName", "")
            budgeted = abs(float(item.get("budgetedAmount") or item.get("planned") or item.get("budget") or 0))
            actual   = abs(float(item.get("actualAmount") or item.get("actual") or item.get("spent") or 0))
            if not cat_name or budgeted == 0 or is_transfer(cat_name):
                continue
            remaining = budgeted - actual
            pct_used  = (actual / budgeted * 100) if budgeted > 0 else 0
            result.append({"category": cat_name, "budgeted": budgeted,
                           "actual": actual, "remaining": remaining, "pct_used": pct_used})
        except Exception:
            pass
    return sorted(result, key=lambda x: x["pct_used"], reverse=True)


# ── Recurring / Upcoming Bills ─────────────────────────────────────────────────

def parse_upcoming_bills(recurring_raw, today, lookahead=7):
    upcoming = []
    cutoff   = today + timedelta(days=lookahead)
    if not recurring_raw:
        return upcoming
    for item in recurring_raw:
        try:
            next_date_str = (item.get("nextForecastedDate") or item.get("nextDueDate") or
                             item.get("nextDate") or item.get("date") or
                             (item.get("nextTransaction") or {}).get("date") or "")
            if not next_date_str:
                continue
            next_date = date.fromisoformat(next_date_str[:10])
            if not (today <= next_date <= cutoff):
                continue
            merchant = (item.get("merchant") or {}).get("name") or item.get("name") or "Unknown"
            amount   = abs(float(item.get("amount") or
                                 (item.get("nextTransaction") or {}).get("amount") or 0))
            acct     = ((item.get("account") or {}).get("displayName") or
                        item.get("accountName") or "")
            is_inc   = float(item.get("amount") or 0) > 0
            upcoming.append({"date": next_date, "merchant": merchant,
                             "amount": amount, "account": acct, "is_income": is_inc})
        except Exception:
            pass
    return sorted(upcoming, key=lambda x: x["date"])


# ── Transaction Analysis ───────────────────────────────────────────────────────

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
    top_cats = sorted(by_category.items(), key=lambda x: x[1], reverse=True)[:6]
    return income, expenses, top_cats, hsa_total


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


# ── Advisory Brief ─────────────────────────────────────────────────────────────

def build_advisory_brief(yesterday, mtd_transactions, accounts, budgets_parsed,
                          upcoming_bills, cashflow):
    today         = yesterday + timedelta(days=1)
    days_elapsed  = yesterday.day
    days_in_month = calendar.monthrange(yesterday.year, yesterday.month)[1]
    days_remaining = days_in_month - yesterday.day
    month_name    = yesterday.strftime("%B")

    mtd_income, mtd_expense, _, _ = analyze_transactions(mtd_transactions, exclude_transfers=True)
    api_income, api_expense = extract_cashflow(cashflow)
    income  = api_income  if api_income  > 0 else mtd_income
    expense = api_expense if api_expense > 0 else mtd_expense
    savings_rate = ((income - expense) / income * 100) if income > 0 else 0
    run_rate     = (expense / days_elapsed * days_in_month) if days_elapsed > 0 else 0

    sentences = []

    # Pace sentence
    if income > 0:
        if savings_rate >= 30:
            sentences.append(f"You're {days_elapsed} days into {month_name} tracking at a {pct(savings_rate)} savings rate — well ahead of plan.")
        elif savings_rate >= 15:
            sentences.append(f"You're {days_elapsed} days into {month_name} with a {pct(savings_rate)} savings rate — solid pace.")
        else:
            sentences.append(f"You're {days_elapsed} days into {month_name} at a {pct(savings_rate)} savings rate — spending pace warrants attention.")
    else:
        sentences.append(f"Day {days_elapsed} of {month_name} — no income recorded yet this month.")

    # Budget stress
    over_budget = [b for b in budgets_parsed if b["pct_used"] > 100]
    near_budget = [b for b in budgets_parsed if 80 <= b["pct_used"] <= 100]
    if over_budget:
        names = ", ".join(b["category"] for b in over_budget[:2])
        sentences.append(f"{len(over_budget)} categor{'y' if len(over_budget)==1 else 'ies'} over budget ({names}).")
    elif near_budget:
        names = ", ".join(b["category"] for b in near_budget[:2])
        sentences.append(f"{names} {'is' if len(near_budget)==1 else 'are'} approaching budget limit with {days_remaining} days left in {month_name}.")

    # Upcoming bills
    big_bills = [b for b in upcoming_bills if not b["is_income"] and b["amount"] >= 200]
    if big_bills:
        next_bill = big_bills[0]
        days_away = (next_bill["date"] - today).days
        when = "today" if days_away == 0 else "tomorrow" if days_away == 1 else f"in {days_away} days"
        sentences.append(f"{next_bill['merchant']} ({fmt(next_bill['amount'])}) hits {when}.")

    # HSA reminder
    hsa_txns = [t for t in mtd_transactions
                if float(t.get("amount", 0)) < 0
                and is_hsa((t.get("category") or {}).get("name", ""))]
    if hsa_txns:
        hsa_unlogged = [t for t in hsa_txns if not (t.get("notes") or "").strip()]
        if hsa_unlogged:
            total = sum(abs(float(t.get("amount", 0))) for t in hsa_unlogged)
            sentences.append(f"You have {fmt(total)} in HSA-eligible expenses without receipts logged in Monarch.")

    text = " ".join(sentences)

    return f"""
    <div style="background:linear-gradient(135deg,{C_LTGREEN} 0%,#f0f9f4 100%);
                border:1px solid {C_BGREEN};border-radius:4px;padding:18px 20px;
                font-size:13.5px;line-height:1.75;color:{C_TEXT}">
      <span style="font-size:9px;letter-spacing:.2em;text-transform:uppercase;
                   color:{C_GREEN};display:block;margin-bottom:8px;font-style:normal">&#9679; Advisory Brief</span>
      {text}
    </div>"""


# ── Net Worth ──────────────────────────────────────────────────────────────────

def build_net_worth_html(accounts, history_by_id, yesterday):
    net_worth, assets, liabilities = compute_net_worth(accounts)
    yest_str = str(yesterday)
    prev_str = str(yesterday - timedelta(days=1))

    html = f"""
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:4px">
      <tr>
        <td style="padding:5px 0;font-size:13px;color:{C_MUTED}">Assets</td>
        <td style="padding:5px 0;text-align:right;font-size:13px;color:{C_TEXT}">{fmt(assets)}</td>
      </tr>
      <tr>
        <td style="padding:5px 0 10px;font-size:13px;color:{C_MUTED}">Liabilities</td>
        <td style="padding:5px 0 10px;text-align:right;font-size:13px;color:{C_RED}">-{fmt(liabilities)}</td>
      </tr>
      <tr><td colspan="2" style="padding:0 0 10px">
        <div style="border-top:1px solid {C_BORDER}"></div>
      </td></tr>
      <tr>
        <td style="font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:{C_LABEL}">Net Worth</td>
        <td style="text-align:right;font-size:22px;font-weight:700;color:{C_TEXT}">{fmt(net_worth)}</td>
      </tr>
    </table>"""

    movers = []
    for acct in accounts:
        if acct.get("includeInNetWorth") is False:
            continue
        acct_id    = str(acct.get("id", ""))
        name       = acct.get("displayName") or acct.get("name", "Unknown")
        cur_bal    = float(acct.get("currentBalance") or 0)
        is_asset   = acct.get("isAsset", True)
        hist       = history_by_id.get(acct_id, {})
        prev_bal   = hist.get(prev_str) or hist.get(yest_str)
        if prev_bal is None:
            continue
        delta      = cur_bal - prev_bal
        net_impact = delta  # positive = net worth went up
        if abs(delta) >= DELTA_THRESHOLD:
            movers.append((name, cur_bal, delta, is_asset, net_impact))

    if movers:
        movers.sort(key=lambda x: abs(x[2]), reverse=True)
        rows = ""
        for name, bal, delta, is_asset, net_impact in movers:
            display_bal = bal if is_asset else -abs(bal)
            bal_str   = f"-{fmt(abs(display_bal))}" if display_bal < 0 else fmt(display_bal)
            bal_color = C_RED if display_bal < 0 else C_TEXT
            sign      = "▲" if net_impact > 0 else "▼"
            d_color   = C_GREEN if net_impact > 0 else C_RED
            rows += f"""
            <tr>
              <td style="padding:6px 0;border-bottom:1px solid {C_BORDER};font-size:12px;color:{C_TEXT}">{name}</td>
              <td style="padding:6px 0;border-bottom:1px solid {C_BORDER};text-align:right;font-size:12px;color:{bal_color}">{bal_str}</td>
              <td style="padding:6px 0 6px 14px;border-bottom:1px solid {C_BORDER};text-align:right;font-size:11px;color:{d_color};white-space:nowrap">{sign} {fmt(abs(delta))}</td>
            </tr>"""
        html += f"""
        <div style="margin-top:16px">
          {section_label(f"Notable Moves &nbsp;&middot;&nbsp; >${DELTA_THRESHOLD:.0f} threshold")}
          <table width="100%" cellpadding="0" cellspacing="0">{rows}</table>
        </div>"""

    return html


# ── Action Items ───────────────────────────────────────────────────────────────

def build_action_items(transactions, mtd_transactions, budgets_parsed,
                        upcoming_bills, accounts, yesterday):
    today = yesterday + timedelta(days=1)
    items = []

    # HSA unlogged receipts
    hsa_unlogged = [t for t in mtd_transactions
                    if float(t.get("amount", 0)) < 0
                    and is_hsa((t.get("category") or {}).get("name", ""))
                    and not (t.get("notes") or "").strip()]
    if hsa_unlogged:
        total = sum(abs(float(t.get("amount", 0))) for t in hsa_unlogged)
        items.append(("🏥", f"{fmt(total)} in HSA-eligible expenses — attach receipts in Monarch to preserve tax-free reimbursement", C_LTGREEN, C_BGREEN, C_GREEN))

    # Over-budget categories
    over = [b for b in budgets_parsed if b["pct_used"] > 100]
    for b in over[:2]:
        over_by = b["actual"] - b["budgeted"]
        items.append(("⚠️", f"{b['category']} is {fmt(over_by)} over budget ({pct(b['pct_used'])} used) — review or adjust", C_LTRED, C_BRED, C_RED))

    # Bills due in next 2 days
    urgent = [b for b in upcoming_bills if (b["date"] - today).days <= 2 and not b["is_income"]]
    for b in urgent:
        days_away = (b["date"] - today).days
        when = "today" if days_away == 0 else "tomorrow" if days_away == 1 else "in 2 days"
        items.append(("💳", f"{b['merchant']} payment of {fmt(b['amount'])} due {when}", C_LTAMBER, C_BAMBER, C_AMBER))

    if not items:
        return ""

    rows = ""
    for icon, text, bg, border, color in items:
        rows += f"""
        <div style="display:flex;align-items:flex-start;gap:10px;padding:10px 14px;
                    background:{bg};border:1px solid {border};border-radius:4px;margin-bottom:8px">
          <span style="font-size:15px;line-height:1.5;flex-shrink:0">{icon}</span>
          <span style="font-size:12.5px;color:{C_TEXT};line-height:1.6">{text}</span>
        </div>"""
    return rows


# ── Transactions ───────────────────────────────────────────────────────────────

def build_transactions_html(transactions):
    if not transactions:
        return (f'<p style="color:{C_MUTED};font-size:13px;margin:0">No transactions yesterday.</p>',
                0.0, 0.0, 0.0)
    income = expenses = 0.0
    rows = ""
    for txn in sorted(transactions, key=lambda t: float(t.get("amount", 0)), reverse=True):
        merchant = (txn.get("merchant") or {}).get("name") or txn.get("plaidName") or "Unknown"
        cat      = (txn.get("category") or {}).get("name", "Uncategorized")
        amount   = float(txn.get("amount", 0))
        note     = (txn.get("notes") or "").strip()
        if amount > 0:
            income   += amount
            amt_html  = green(f"+{fmt(amount)}")
        else:
            expenses += abs(amount)
            amt_html  = red(f"-{fmt(abs(amount))}")
        badges_html = ""
        if amount < 0 and is_hsa(cat):
            badges_html += badge("HSA", C_LTGREEN, C_BGREEN, C_GREEN)
        if is_transfer(cat):
            badges_html += badge("transfer", "#f5f3f0", C_BORDER, C_MUTED)
        if amount > 0 and is_income_cat(cat):
            badges_html += badge("income", C_LTBLUE, C_BBLUE, C_BLUE)
        note_html = (f'<div style="font-size:10px;color:{C_LABEL};margin-top:2px;'
                     f'font-style:italic">{note}</div>') if note else ""
        rows += f"""
        <tr>
          <td style="padding:10px 0;border-bottom:1px solid {C_BORDER}">
            <div style="font-size:13px;color:{C_TEXT};font-weight:500">{merchant}</div>
            <div style="font-size:11px;color:{C_MUTED};margin-top:2px">{cat}{badges_html}</div>
            {note_html}
          </td>
          <td style="padding:10px 0;border-bottom:1px solid {C_BORDER};
                     text-align:right;font-size:13px;vertical-align:top;white-space:nowrap">{amt_html}</td>
        </tr>"""
    net = income - expenses
    return f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>', income, expenses, net


# ── Budget Pulse ───────────────────────────────────────────────────────────────

def build_budget_html(budgets_parsed, yesterday):
    if not budgets_parsed:
        return ""
    days_elapsed  = yesterday.day
    days_in_month = calendar.monthrange(yesterday.year, yesterday.month)[1]
    month_pct     = days_elapsed / days_in_month * 100

    rows = ""
    for b in budgets_parsed[:10]:
        p       = b["pct_used"]
        over    = p > 100
        on_pace = abs(p - month_pct) <= 15
        color   = C_RED if over else (C_GREEN if p <= month_pct else C_AMBER)
        bar_bg  = C_LTRED if over else (C_LTGREEN if p <= month_pct else C_LTAMBER)
        status  = "OVER" if over else ("ON TRACK" if on_pace else ("AHEAD" if p < month_pct else "WATCH"))
        s_color = C_RED if over else (C_GREEN if status in ("ON TRACK", "AHEAD") else C_AMBER)
        bar_w   = min(int(p), 100)

        rows += f"""
        <tr>
          <td style="padding:8px 0;border-bottom:1px solid {C_BORDER};font-size:12px;color:{C_TEXT};width:30%">{b['category']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid {C_BORDER};width:36%">
            <div style="background:{bar_bg};border-radius:3px;height:5px">
              <div style="background:{color};border-radius:3px;height:5px;width:{bar_w}%"></div>
            </div>
          </td>
          <td style="padding:8px 0;border-bottom:1px solid {C_BORDER};text-align:right;font-size:11px;color:{C_MUTED};white-space:nowrap">{fmt(b['actual'])} / {fmt(b['budgeted'])}</td>
          <td style="padding:8px 0 8px 12px;border-bottom:1px solid {C_BORDER};text-align:right;font-size:9px;letter-spacing:.08em;color:{s_color};white-space:nowrap">{status}</td>
        </tr>"""

    note = f'<div style="font-size:10px;color:{C_LABEL};margin-bottom:14px;font-style:italic">{pct(month_pct)} of month elapsed · pace reference</div>'
    return note + f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>'


# ── MTD Cashflow ───────────────────────────────────────────────────────────────

def build_cashflow_html(cashflow, mtd_transactions, yesterday):
    api_income, api_expense = extract_cashflow(cashflow)
    txn_income, txn_expense, _, _ = analyze_transactions(mtd_transactions, exclude_transfers=True)
    mtd_income  = api_income  if api_income  > 0 else txn_income
    mtd_expense = api_expense if api_expense > 0 else txn_expense
    if mtd_income == 0 and mtd_expense == 0:
        return ""
    savings        = mtd_income - mtd_expense
    savings_rate   = (savings / mtd_income * 100) if mtd_income > 0 else 0
    days_elapsed   = yesterday.day
    days_in_month  = calendar.monthrange(yesterday.year, yesterday.month)[1]
    month_pct      = days_elapsed / days_in_month * 100
    run_rate       = (mtd_expense / days_elapsed * days_in_month) if days_elapsed > 0 else 0
    proj_savings   = mtd_income - run_rate
    sr_color       = C_GREEN if savings_rate >= 20 else (C_RED if savings_rate < 5 else C_AMBER)
    ps_color       = C_GREEN if proj_savings > 0 else C_RED

    return f"""
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td style="padding:7px 0;font-size:13px;color:{C_TEXT}">MTD Income</td>
        <td style="padding:7px 0;text-align:right;font-size:13px;color:{C_GREEN};font-weight:600">{fmt(mtd_income)}</td>
      </tr>
      <tr>
        <td style="padding:7px 0;font-size:13px;color:{C_TEXT}">MTD Spending <span style="color:{C_LABEL};font-size:11px">(excl. transfers)</span></td>
        <td style="padding:7px 0;text-align:right;font-size:13px;color:{C_RED};font-weight:600">{fmt(mtd_expense)}</td>
      </tr>
      <tr>
        <td style="padding:7px 0;font-size:12px;color:{C_MUTED}">Projected month-end spend</td>
        <td style="padding:7px 0;text-align:right;font-size:12px;color:{C_MUTED}">{fmt(run_rate)}</td>
      </tr>
      <tr>
        <td style="padding:7px 0 14px;font-size:12px;color:{C_MUTED}">Projected month-end savings</td>
        <td style="padding:7px 0 14px;text-align:right;font-size:12px;color:{ps_color};font-weight:600">{fmt(proj_savings)}</td>
      </tr>
      <tr>
        <td colspan="2" style="padding:0 0 6px">
          <div style="display:flex;justify-content:space-between;font-size:10px;color:{C_LABEL};margin-bottom:6px">
            <span>{yesterday.strftime('%B')} · Day {days_elapsed} of {days_in_month}</span>
            <span>{pct(month_pct)} elapsed</span>
          </div>
          <div style="background:#ede9e4;border-radius:3px;height:5px">
            <div style="background:{C_GREEN};border-radius:3px;height:5px;width:{min(int(month_pct),100)}%"></div>
          </div>
        </td>
      </tr>
      <tr>
        <td style="padding:14px 0 4px;font-size:10px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL}">Savings Rate MTD</td>
        <td style="padding:14px 0 4px;text-align:right;font-size:20px;font-weight:700;color:{sr_color}">{pct(savings_rate)}</td>
      </tr>
    </table>"""


# ── Upcoming Bills ─────────────────────────────────────────────────────────────

def build_upcoming_html(upcoming_bills, today):
    if not upcoming_bills:
        return ""
    rows = ""
    for b in upcoming_bills:
        days_away = (b["date"] - today).days
        if days_away == 0:
            when_str, when_color = "Today", C_RED
        elif days_away == 1:
            when_str, when_color = "Tomorrow", C_AMBER
        else:
            when_str, when_color = b["date"].strftime("%a %b %-d"), C_TEXT
        amt_color = C_GREEN if b["is_income"] else C_TEXT
        sign      = "+" if b["is_income"] else "−"
        acct_str  = (f'<div style="font-size:10px;color:{C_LABEL};margin-top:2px">{b["account"]}</div>'
                     if b["account"] else "")
        rows += f"""
        <tr>
          <td style="padding:8px 0;border-bottom:1px solid {C_BORDER};font-size:11px;
                     color:{when_color};font-weight:600;white-space:nowrap;width:78px">{when_str}</td>
          <td style="padding:8px 12px;border-bottom:1px solid {C_BORDER}">
            <div style="font-size:13px;color:{C_TEXT}">{b['merchant']}</div>{acct_str}
          </td>
          <td style="padding:8px 0;border-bottom:1px solid {C_BORDER};text-align:right;
                     font-size:13px;color:{amt_color};white-space:nowrap">{sign} {fmt(b['amount'])}</td>
        </tr>"""
    return f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>'


# ── Debt Tracker ───────────────────────────────────────────────────────────────

def build_debt_html(accounts):
    debts = []
    for a in accounts:
        name = (a.get("displayName") or a.get("name") or "").lower()
        if not any(k in name for k in DEBT_ACCOUNTS):
            continue
        bal = float(a.get("currentBalance") or 0)
        if bal >= 0:
            continue
        debts.append({"name": a.get("displayName") or a.get("name"), "balance": abs(bal)})
    if not debts:
        return ""
    debts.sort(key=lambda x: x["balance"], reverse=True)
    total_debt = sum(d["balance"] for d in debts)
    max_bal    = debts[0]["balance"]

    rows = ""
    for d in debts:
        bar_w = int((d["balance"] / max_bal) * 120)
        rows += f"""
        <tr>
          <td style="padding:8px 0;border-bottom:1px solid {C_BORDER};font-size:12px;color:{C_TEXT};width:44%">{d['name']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid {C_BORDER};vertical-align:middle">
            <div style="background:{C_LTRED};border-radius:3px;height:4px;width:{bar_w}px"></div>
          </td>
          <td style="padding:8px 0;border-bottom:1px solid {C_BORDER};text-align:right;
                     font-size:12px;color:{C_RED};font-weight:600;white-space:nowrap">-{fmt(d['balance'])}</td>
        </tr>"""
    rows += f"""
    <tr>
      <td colspan="2" style="padding:10px 0;font-size:10px;color:{C_LABEL};letter-spacing:.1em;text-transform:uppercase">Total Debt</td>
      <td style="padding:10px 0;text-align:right;font-size:15px;font-weight:700;color:{C_RED}">-{fmt(total_debt)}</td>
    </tr>"""
    return f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>'


# ── Spending Breakdown ─────────────────────────────────────────────────────────

def build_spending_html(mtd_transactions):
    _, _, top_cats, _ = analyze_transactions(mtd_transactions, exclude_transfers=True)
    if not top_cats:
        return ""
    total = sum(v for _, v in top_cats)
    max_v = top_cats[0][1]
    rows  = ""
    for cat, amount in top_cats:
        bar_w = int((amount / max_v) * 120)
        share = (amount / total * 100) if total else 0
        rows += f"""
        <tr>
          <td style="padding:7px 0;border-bottom:1px solid {C_BORDER};font-size:12px;color:{C_TEXT};width:34%">{cat}</td>
          <td style="padding:7px 10px;border-bottom:1px solid {C_BORDER};vertical-align:middle">
            <div style="background:#ede9e4;border-radius:2px;height:3px;width:{bar_w}px;display:inline-block;vertical-align:middle"></div>
          </td>
          <td style="padding:7px 0;border-bottom:1px solid {C_BORDER};text-align:right;font-size:11px;color:{C_MUTED};white-space:nowrap">{pct(share)}</td>
          <td style="padding:7px 0 7px 12px;border-bottom:1px solid {C_BORDER};text-align:right;font-size:12px;color:{C_TEXT};white-space:nowrap">{fmt(amount)}</td>
        </tr>"""
    return f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>'


# ── Card wrapper ───────────────────────────────────────────────────────────────

def card(title, subtitle, content, accent_color=None):
    if not (content or "").strip():
        return ""
    sub_html = (f'<span style="font-size:10px;color:{C_LABEL};margin-left:8px;'
                f'font-style:italic;text-transform:none;letter-spacing:0">{subtitle}</span>') if subtitle else ""
    border_top = f"border-top:3px solid {accent_color};" if accent_color else ""
    return f"""
  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;
              padding:22px 28px;{border_top}">
    <div style="font-size:9px;letter-spacing:.22em;text-transform:uppercase;
                color:{C_LABEL};margin-bottom:16px">{title}{sub_html}</div>
    {content}
  </div>"""


# ── Email Assembly ─────────────────────────────────────────────────────────────

def build_email(yesterday, transactions, mtd_transactions, accounts, cashflow,
                history_by_id, budgets_raw, recurring_raw):
    today      = yesterday + timedelta(days=1)
    date_str   = yesterday.strftime("%A, %B %-d")
    generated  = today.strftime("%B %-d, %Y")
    month_name = yesterday.strftime("%B %Y")

    budgets_parsed = parse_budgets(budgets_raw)
    upcoming_bills = parse_upcoming_bills(recurring_raw, today, BILL_LOOKAHEAD)
    net_worth, _, _ = compute_net_worth(accounts)

    advisory_html = build_advisory_brief(yesterday, mtd_transactions, accounts,
                                         budgets_parsed, upcoming_bills, cashflow)
    nw_html       = build_net_worth_html(accounts, history_by_id, yesterday)
    action_html   = build_action_items(transactions, mtd_transactions, budgets_parsed,
                                        upcoming_bills, accounts, yesterday)
    txn_html, income, expenses, net = build_transactions_html(transactions)
    budget_html   = build_budget_html(budgets_parsed, yesterday)
    cashflow_html = build_cashflow_html(cashflow, mtd_transactions, yesterday)
    upcoming_html = build_upcoming_html(upcoming_bills, today)
    debt_html     = build_debt_html(accounts)
    spending_html = build_spending_html(mtd_transactions)

    txn_count = len(transactions)
    net_color = C_GREEN if net >= 0 else C_RED
    net_label = f"+{fmt(net)}" if net >= 0 else f"-{fmt(abs(net))}"

    summary_bar = f"""
  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;
              display:table;width:100%;box-sizing:border-box">
    <div style="display:table-cell;padding:15px 0;text-align:center;border-right:1px solid {C_BORDER}">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};margin-bottom:5px">Income</div>
      <div style="font-size:15px;font-weight:700;color:{C_GREEN}">{fmt(income)}</div>
    </div>
    <div style="display:table-cell;padding:15px 0;text-align:center;border-right:1px solid {C_BORDER}">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};margin-bottom:5px">Spent</div>
      <div style="font-size:15px;font-weight:700;color:{C_RED}">{fmt(expenses)}</div>
    </div>
    <div style="display:table-cell;padding:15px 0;text-align:center">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};margin-bottom:5px">Net</div>
      <div style="font-size:15px;font-weight:700;color:{net_color}">{net_label}</div>
    </div>
  </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Monarch Digest · {date_str}</title>
</head>
<body style="margin:0;padding:32px 16px;background:{C_BG};font-family:Georgia,'Times New Roman',serif">
<div style="max-width:600px;margin:0 auto">

  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-radius:6px 6px 0 0;padding:26px 28px 22px">
    <div style="font-size:9px;letter-spacing:.28em;text-transform:uppercase;color:{C_GREEN};margin-bottom:7px">Monarch · Daily Digest</div>
    <div style="font-size:24px;color:{C_TEXT};font-style:italic;margin-bottom:4px">{date_str}</div>
    <div style="font-size:11px;color:{C_LABEL}">Net worth &nbsp;&middot;&nbsp; <strong style="color:{C_TEXT}">{fmt(net_worth)}</strong></div>
  </div>

  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;padding:0 28px 22px">
    {advisory_html}
  </div>

  {card("Action Items", None, action_html, C_AMBER) if action_html else ""}
  {card("Net Worth", None, nw_html)}
  {card("Yesterday", f"{txn_count} transactions", txn_html)}
  {summary_bar}
  {card("Budget Pulse", month_name, budget_html)}
  {card("Month-to-Date Cashflow", month_name, cashflow_html)}
  {card("Upcoming Bills &amp; Income", f"next {BILL_LOOKAHEAD} days", upcoming_html)}
  {card("Debt Tracker", None, debt_html)}
  {card("Spending Breakdown", "excl. transfers", spending_html)}

  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;border-radius:0 0 6px 6px;
              padding:12px 28px;display:table;width:100%;box-sizing:border-box">
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
    (yesterday, transactions, mtd_transactions, accounts,
     cashflow, history_by_id, budgets, recurring) = await fetch_data()
    print(f"  {len(transactions)} txns · {len(mtd_transactions)} MTD · "
          f"{len(accounts)} accounts · {len(history_by_id)} with history · "
          f"budgets={'yes' if budgets else 'no'} · "
          f"recurring={len(recurring) if isinstance(recurring, list) else 'error'}")

    net_worth, _, _ = compute_net_worth(accounts)
    print(f"  Net worth: {fmt(net_worth)}")

    html     = build_email(yesterday, transactions, mtd_transactions, accounts,
                           cashflow, history_by_id, budgets, recurring)
    date_str = yesterday.strftime("%b %-d")
    subject  = f"💰 Monarch · {date_str} · NW {fmt(net_worth)} · {len(transactions)} txns"
    send_email(subject, html)


if __name__ == "__main__":
    asyncio.run(main())
