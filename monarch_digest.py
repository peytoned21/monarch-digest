#!/usr/bin/env python3
"""
Monarch Money — Weekly Financial Digest
Runs Monday at 7am ET, covers the prior Mon–Sun week.

Sections:
  1. Weekly Brief       — narrative paragraph: how the week went
  2. Net Worth          — snapshot + week-over-week change
  3. Action Items       — HSA receipts, over-budget alerts
  4. Last Week          — all transactions sorted by amount desc, grouped by day
  5. Week Summary       — income / spent / net bar
  6. Budget Pulse       — MTD actual vs budget with pace indicator
  7. This Week Ahead    — upcoming bills & income next 7 days
  8. Month-to-Date      — cashflow, savings rate, projection
  9. Debt Tracker       — balances for tracked liabilities
"""

import asyncio
import calendar
import os
import smtplib
from collections import defaultdict
from datetime import date, timedelta
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders as email_encoders

from monarchmoney import MonarchMoney

# ── Config ─────────────────────────────────────────────────────────────────────
MONARCH_EMAIL    = os.environ["MONARCH_EMAIL"]
MONARCH_PASSWORD = os.environ["MONARCH_PASSWORD"]
MONARCH_MFA_KEY  = os.environ.get("MONARCH_MFA_KEY", "")
GMAIL_ADDRESS    = os.environ["GMAIL_ADDRESS"]
GMAIL_APP_PASS   = os.environ["GMAIL_APP_PASSWORD"]
RECIPIENT_EMAIL  = os.environ.get("RECIPIENT_EMAIL", GMAIL_ADDRESS)
GITHUB_USER      = os.environ.get("GITHUB_USER", "peytoned21")
GITHUB_REPO      = os.environ.get("GITHUB_REPO", "monarch-digest")
CALC_FILENAME    = "retirement_calculator.html"
CALC_URL         = f"https://{GITHUB_USER}.github.io/{GITHUB_REPO}/{CALC_FILENAME}"

HSA_KEYWORDS      = ["medical", "pharmacy", "dental", "vision", "health", "doctor", "hospital"]
TRANSFER_KEYWORDS = ["transfer", "transfers to investments", "credit card payment"]
INCOME_KEYWORDS   = ["paycheck", "income", "salary", "bonus", "direct deposit", "reimbursement"]
NW_DELTA_THRESHOLD = 100.0   # min account move to show in net worth section
BILL_LOOKAHEAD     = 7       # days ahead for upcoming bills
DEBT_ACCOUNTS      = ["mortgage", "tesla", "lexus", "stanford", "heloc"]

# Fixed monthly obligations — matched against recurring merchant names (case-insensitive)
# These are pulled from Monarch recurring and identified by keyword
FIXED_EXPENSE_KEYWORDS = {
    "mortgage":  {"label": "Mortgage",       "category": "housing"},
    "stanford":  {"label": "Stanford Loan",  "category": "debt"},
    "tesla":     {"label": "Tesla Loan",     "category": "debt"},
    "lexus":     {"label": "Lexus Loan",     "category": "debt"},
    "529":       {"label": "529 Savings",    "category": "savings"},
    "college":   {"label": "529 Savings",    "category": "savings"},
}

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
def muted(t):  return f'<span style="color:{C_MUTED}">{t}</span>'

def badge(text, bg, border, color):
    return (f'<span style="display:inline-block;background:{bg};color:{color};'
            f'border:1px solid {border};border-radius:3px;font-size:9px;'
            f'letter-spacing:.06em;padding:1px 6px;margin-left:6px;vertical-align:middle">'
            f'{text}</span>')

def card(title, subtitle, content, accent_color=None):
    if not (content or "").strip():
        return ""
    sub_html = (f'<span style="font-size:10px;color:{C_LABEL};margin-left:8px;'
                f'font-style:italic;text-transform:none;letter-spacing:0">{subtitle}</span>'
                ) if subtitle else ""
    top_border = f"border-top:3px solid {accent_color};" if accent_color else ""
    return f"""
  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;
              padding:22px 28px;{top_border}">
    <div style="font-size:9px;letter-spacing:.22em;text-transform:uppercase;
                color:{C_LABEL};margin-bottom:16px">{title}{sub_html}</div>
    {content}
  </div>"""

def divider():
    return f'<div style="border-top:1px solid {C_BORDER};margin:14px 0"></div>'

def section_label(text):
    return (f'<div style="font-size:9px;letter-spacing:.2em;text-transform:uppercase;'
            f'color:{C_LABEL};margin-bottom:10px;padding-bottom:8px;'
            f'border-bottom:1px solid {C_BORDER}">{text}</div>')


# ── Date Helpers ───────────────────────────────────────────────────────────────

def get_week_range(today: date):
    """
    Returns (week_start, week_end) for the most recently completed Mon–Sun week.
    Called on Monday, so week_end = yesterday (Sunday), week_start = 7 days prior.
    Also handles running mid-week for testing — always returns last full week.
    """
    # Find most recent Sunday
    days_since_sunday = (today.weekday() + 1) % 7  # Mon=0 so Sun=6 days back
    week_end   = today - timedelta(days=days_since_sunday if days_since_sunday > 0 else 7)
    week_start = week_end - timedelta(days=6)
    return week_start, week_end


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
    week_start, week_end = get_week_range(today)
    month_start = week_end.replace(day=1)

    print(f"Week: {week_start} → {week_end}")

    print("Fetching week transactions...")
    txn_resp     = await mm.get_transactions(
        start_date=str(week_start), end_date=str(week_end))
    week_txns    = txn_resp.get("allTransactions", {}).get("results", [])

    print("Fetching MTD transactions...")
    mtd_resp     = await mm.get_transactions(
        start_date=str(month_start), end_date=str(week_end))
    mtd_txns     = mtd_resp.get("allTransactions", {}).get("results", [])

    print("Fetching accounts...")
    acct_resp    = await mm.get_accounts()
    accounts     = acct_resp.get("accounts", [])

    print("Fetching account history for NW delta...")
    history_by_id = {}
    # We want balance snapshots for week_end and 7 days before week_end
    target_dates  = {str(week_end), str(week_start - timedelta(days=1))}
    debug_done    = False
    for acct in accounts:
        acct_id = acct.get("id")
        if not acct_id or acct.get("includeInNetWorth") is False:
            continue
        try:
            hist      = await mm.get_account_history(account_id=acct_id)
            snapshots = hist if isinstance(hist, list) else (
                hist.get("account", {}).get("balanceHistory") or
                hist.get("balanceHistory") or hist.get("history") or [])
            if not debug_done and snapshots:
                s = snapshots[-1] if snapshots else {}
                print(f"  History snapshot sample: {str(s)[:180]}")
                print(f"  Total snapshots: {len(snapshots)}, looking for: {sorted(target_dates)}")
                debug_done = True
            bal_map = {}
            for s in snapshots:
                if not isinstance(s, dict):
                    continue
                # Try multiple date field names
                d = (s.get("date") or s.get("startDate") or s.get("day") or "")
                if d:
                    bal_map[d[:10]] = float(s.get("balance") or s.get("amount") or 0)
            if bal_map:
                history_by_id[str(acct_id)] = bal_map
        except Exception:
            pass
    matched = sum(1 for v in history_by_id.values()
                  if any(d in v for d in target_dates))
    print(f"  {len(history_by_id)} accounts with history, {matched} with target dates")

    print("Fetching budgets...")
    budgets = {}
    try:
        budgets = await mm.get_budgets(
            start_date=str(month_start),
            end_date=str(week_end),
        )
    except Exception as e:
        print(f"  Budgets failed: {e}")

    print("Fetching recurring transactions...")
    recurring = []
    try:
        rec_resp  = await mm.get_recurring_transactions()
        raw_list  = (rec_resp.get("recurringTransactionItems") if isinstance(rec_resp, dict)
                     else rec_resp if isinstance(rec_resp, list) else [])
        recurring = raw_list or []
    except Exception as e:
        print(f"  Recurring failed: {e}")

    print("Fetching cashflow...")
    cashflow = {}
    try:
        cashflow = await mm.get_cashflow_summary(
            start_date=str(month_start),
            end_date=str(week_end),
        )
    except Exception as e:
        print(f"  Cashflow failed: {e}")

    return (today, week_start, week_end, week_txns, mtd_txns,
            accounts, cashflow, history_by_id, budgets, recurring)


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
    cat_names = {}
    for group in (budgets_raw.get("categoryGroups") or []):
        for cat in (group.get("categories") or []):
            cat_names[cat["id"]] = cat["name"]
    entries = (budgets_raw.get("budgetData") or {}).get("monthlyAmountsByCategory") or []
    for entry in entries:
        try:
            cat_id   = (entry.get("category") or {}).get("id", "")
            cat_name = cat_names.get(cat_id, "")
            if not cat_name or is_transfer(cat_name):
                continue
            budgeted = actual = 0.0
            for m in (entry.get("monthlyAmounts") or []):
                budgeted += abs(float(m.get("plannedCashFlowAmount") or 0))
                actual   += abs(float(m.get("actualAmount") or 0))
            if budgeted == 0:
                continue
            pct_used = (actual / budgeted * 100) if budgeted > 0 else 0
            result.append({"category": cat_name, "budgeted": budgeted,
                           "actual": actual, "remaining": budgeted - actual,
                           "pct_used": pct_used})
        except Exception:
            pass
    return sorted(result, key=lambda x: x["pct_used"], reverse=True)


# ── Recurring / Upcoming Bills ─────────────────────────────────────────────────

def parse_fixed_expenses(recurring_raw):
    """
    Identify fixed monthly obligations from Monarch recurring transactions.
    Returns list of {label, amount, category, merchant} sorted by amount desc.
    """
    fixed = []
    seen_labels = set()
    for item in (recurring_raw or []):
        try:
            stream   = item.get("stream") or {}
            merchant = (stream.get("merchant") or {}).get("name") or ""
            amount   = abs(float(stream.get("amount") or 0))
            if amount == 0:
                continue
            # Skip income items
            if float(stream.get("amount") or 0) > 0:
                continue
            merchant_lower = merchant.lower()
            for keyword, meta in FIXED_EXPENSE_KEYWORDS.items():
                if keyword in merchant_lower:
                    label = meta["label"]
                    if label not in seen_labels:
                        fixed.append({
                            "label":    label,
                            "amount":   amount,
                            "category": meta["category"],
                            "merchant": merchant,
                        })
                        seen_labels.add(label)
                    break
        except Exception:
            pass
    return sorted(fixed, key=lambda x: x["amount"], reverse=True)


def parse_upcoming_bills(recurring_raw, today, lookahead=7):
    upcoming = []
    cutoff   = today + timedelta(days=lookahead)
    for item in (recurring_raw or []):
        try:
            stream   = item.get("stream") or {}
            next_txn = (item.get("nextTransaction") or
                        stream.get("nextForecastedTransaction") or {})
            date_str = (next_txn.get("date") or stream.get("nextForecastedDate") or
                        stream.get("nextDueDate") or "")
            if not date_str:
                continue
            next_date = date.fromisoformat(date_str[:10])
            if not (today <= next_date <= cutoff):
                continue
            merchant = (stream.get("merchant") or {}).get("name") or "Unknown"
            amount   = abs(float(next_txn.get("amount") or stream.get("amount") or 0))
            acct     = ((item.get("account") or {}).get("displayName") or
                        (stream.get("account") or {}).get("displayName") or "")
            is_inc   = float(stream.get("amount") or 0) > 0
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


# ── Section: Weekly Brief ──────────────────────────────────────────────────────

def build_weekly_brief(week_start, week_end, week_txns, mtd_txns,
                       accounts, budgets_parsed, upcoming_bills, cashflow, recurring_raw=None):
    week_income, week_expense, _, _ = analyze_transactions(week_txns, exclude_transfers=True)
    mtd_income, mtd_expense, _, _   = analyze_transactions(mtd_txns, exclude_transfers=True)
    api_income, api_expense          = extract_cashflow(cashflow)
    mtd_income  = api_income  if api_income  > 0 else mtd_income
    mtd_expense = api_expense if api_expense > 0 else mtd_expense

    net_worth, _, _ = compute_net_worth(accounts)
    savings_rate    = ((mtd_income - mtd_expense) / mtd_income * 100) if mtd_income > 0 else 0
    week_net        = week_income - week_expense
    days_elapsed    = week_end.day
    days_in_month   = calendar.monthrange(week_end.year, week_end.month)[1]
    week_label      = f"{week_start.strftime('%b %-d')}–{week_end.strftime('%b %-d')}"
    month_name      = week_end.strftime("%B")

    # Fixed expense context
    fixed_expenses       = parse_fixed_expenses(recurring_raw) if recurring_raw else []
    total_fixed_monthly  = sum(f["amount"] for f in fixed_expenses)
    days_in_month        = calendar.monthrange(week_end.year, week_end.month)[1]
    fixed_weekly         = total_fixed_monthly / (days_in_month / 7)
    week_discretionary   = max(week_expense - fixed_weekly, 0)
    disc_per_day         = week_discretionary / 7

    sentences = []

    # ── Week pattern detection ────────────────────────────────────────────────
    non_transfer    = [t for t in week_txns if not is_transfer((t.get("category") or {}).get("name", ""))]
    over_budget     = [b for b in budgets_parsed if b["pct_used"] > 100]
    big_bills_ct    = sum(1 for b in upcoming_bills if not b["is_income"] and b["amount"] >= 300)
    avg_weekly_spend = (mtd_expense / max(week_end.day, 1)) * 7
    is_income_week  = week_income > 3000
    is_heavy_spend  = week_expense > avg_weekly_spend * 1.3 and week_expense > 500
    is_quiet        = week_expense < 200 and week_income == 0
    is_budget_stress = len(over_budget) > 0
    is_bill_heavy   = big_bills_ct >= 2

    # ── Lead sentence varies by week pattern ─────────────────────────────────
    if is_quiet:
        sentences.append(f"Quiet week — minimal activity {week_label}.")
    elif is_income_week and week_income > week_expense * 1.5:
        net_str = f"+{fmt(week_net)}" if week_net >= 0 else f"-{fmt(abs(week_net))}"
        sentences.append(f"Strong income week: {fmt(week_income)} in, {fmt(week_expense)} out, net {net_str}.")
    elif is_heavy_spend and not is_income_week:
        sentences.append(f"Spend-heavy week — {fmt(week_expense)} out vs. ~{fmt(avg_weekly_spend)} weekly average.")
    elif is_bill_heavy:
        sentences.append(f"Bill-heavy week: {big_bills_ct} payments of $300+ due in the next 7 days.")
    elif is_budget_stress:
        names = ", ".join(b["category"] for b in over_budget[:2])
        sentences.append(f"Budget pressure: {names} {'has' if len(over_budget)==1 else 'have'} exceeded budget this month.")
    else:
        if total_fixed_monthly > 0 and week_expense > 0:
            sentences.append(f"You spent {fmt(week_expense)} last week — {fmt(week_discretionary)} non-fixed ({fmt(disc_per_day)}/day).")
        elif week_income > 0:
            net_str = f"+{fmt(week_net)}" if week_net >= 0 else f"-{fmt(abs(week_net))}"
            sentences.append(f"You brought in {fmt(week_income)} and spent {fmt(week_expense)} last week, netting {net_str}.")
        else:
            sentences.append(f"You spent {fmt(week_expense)} last week across {len(non_transfer)} transactions.")

    # ── MTD context (one sentence max) ───────────────────────────────────────
    if mtd_income > 0:
        if savings_rate >= 30:
            sentences.append(f"{month_name} tracking at {pct(savings_rate)} savings — ahead of plan.")
        elif savings_rate >= 15:
            sentences.append(f"{month_name} savings rate: {pct(savings_rate)}.")
        else:
            sentences.append(f"{month_name} savings rate {pct(savings_rate)} — spending running high.")

    # ── Supporting signals (budget, bills, HSA) ───────────────────────────────
    # Skip budget stress — already in lead if present
    if not is_budget_stress:
        for b in over_budget[:1]:
            over_by = b["actual"] - b["budgeted"]
            sentences.append(f"{b['category']} is {fmt(over_by)} over budget ({pct(b['pct_used'])} used).")

    big_bills = [b for b in upcoming_bills if not b["is_income"] and b["amount"] >= 300]
    if big_bills and not is_bill_heavy:
        b = big_bills[0]
        days_away = (b["date"] - date.today()).days
        when = "today" if days_away == 0 else "tomorrow" if days_away == 1 else b["date"].strftime("%A")
        sentences.append(f"{b['merchant']} ({fmt(b['amount'])}) due {when}.")

    hsa_unlogged = [t for t in mtd_txns
                    if float(t.get("amount", 0)) < 0
                    and is_hsa((t.get("category") or {}).get("name", ""))
                    and not (t.get("notes") or "").strip()]
    if hsa_unlogged:
        total = sum(abs(float(t.get("amount", 0))) for t in hsa_unlogged)
        sentences.append(f"{fmt(total)} in unlogged HSA receipts.")

    text = " ".join(sentences)
    return f"""
    <div style="background:linear-gradient(135deg,{C_LTGREEN} 0%,#f0f9f4 100%);
                border:1px solid {C_BGREEN};border-radius:4px;padding:18px 20px;
                font-size:13.5px;line-height:1.8;color:{C_TEXT}">
      <span style="font-size:9px;letter-spacing:.2em;text-transform:uppercase;
                   color:{C_GREEN};display:block;margin-bottom:8px">&#9679; Weekly Brief</span>
      {text}
    </div>"""


# ── Section: Net Worth ─────────────────────────────────────────────────────────

def build_net_worth_html(accounts, history_by_id, week_start, week_end):
    net_worth, assets, liabilities = compute_net_worth(accounts)

    # Week-over-week: compare current balance to balance 7 days ago
    prior_date = str(week_start - timedelta(days=1))  # end of prior week = day before week_start
    end_date   = str(week_end)

    # Compute prior net worth from history
    prior_nw   = None
    prior_vals = {}
    for acct in accounts:
        if acct.get("includeInNetWorth") is False:
            continue
        acct_id  = str(acct.get("id", ""))
        hist     = history_by_id.get(acct_id, {})
        # Find closest available date at or before prior_date
        prior_bal = hist.get(prior_date)
        if prior_bal is None:
            # Try nearby dates (±2 days)
            for delta in [1, -1, 2, -2]:
                d = str(date.fromisoformat(prior_date) + timedelta(days=delta))
                if d in hist:
                    prior_bal = hist[d]
                    break
        if prior_bal is not None:
            prior_vals[acct_id] = prior_bal

    if len(prior_vals) >= 5:  # only show delta if we have enough accounts
        prior_nw = sum(prior_vals.values())

    nw_delta      = (net_worth - prior_nw) if prior_nw is not None else None
    delta_color   = C_GREEN if (nw_delta or 0) >= 0 else C_RED
    delta_sign    = "▲" if (nw_delta or 0) >= 0 else "▼"
    delta_html    = (f'<span style="font-size:13px;color:{delta_color};margin-left:10px">'
                     f'{delta_sign} {fmt(abs(nw_delta))} this week</span>'
                     ) if nw_delta is not None else ""

    html = f"""
    <table width="100%" cellpadding="0" cellspacing="0">
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
        <td style="text-align:right">
          <span style="font-size:22px;font-weight:700;color:{C_TEXT}">{fmt(net_worth)}</span>
          {delta_html}
        </td>
      </tr>
    </table>"""

    # Notable movers — accounts that moved meaningfully this week
    movers = []
    for acct in accounts:
        if acct.get("includeInNetWorth") is False:
            continue
        acct_id  = str(acct.get("id", ""))
        name     = acct.get("displayName") or acct.get("name", "Unknown")
        cur_bal  = float(acct.get("currentBalance") or 0)
        is_asset = acct.get("isAsset", True)
        hist     = history_by_id.get(acct_id, {})

        # Get balance at start of week
        prior_bal = hist.get(prior_date)
        if prior_bal is None:
            for delta in [1, -1, 2, -2]:
                d = str(date.fromisoformat(prior_date) + timedelta(days=delta))
                if d in hist:
                    prior_bal = hist[d]
                    break
        if prior_bal is None:
            continue

        delta      = cur_bal - prior_bal
        net_impact = delta  # positive = net worth went up
        if abs(delta) >= NW_DELTA_THRESHOLD:
            movers.append((name, cur_bal, delta, is_asset, net_impact))

    if movers:
        movers.sort(key=lambda x: abs(x[2]), reverse=True)
        rows = ""
        for name, bal, delta, is_asset, net_impact in movers[:8]:
            display_bal = bal if is_asset else -abs(bal)
            bal_str     = f"-{fmt(abs(display_bal))}" if display_bal < 0 else fmt(display_bal)
            bal_color   = C_RED if display_bal < 0 else C_TEXT
            sign        = "▲" if net_impact > 0 else "▼"
            d_color     = C_GREEN if net_impact > 0 else C_RED
            rows += f"""
            <tr>
              <td style="padding:6px 0;border-bottom:1px solid {C_BORDER};font-size:12px;color:{C_TEXT}">{name}</td>
              <td style="padding:6px 0;border-bottom:1px solid {C_BORDER};text-align:right;font-size:12px;color:{bal_color}">{bal_str}</td>
              <td style="padding:6px 0 6px 14px;border-bottom:1px solid {C_BORDER};text-align:right;
                         font-size:11px;color:{d_color};white-space:nowrap">{sign} {fmt(abs(delta))}</td>
            </tr>"""
        html += f"""
        <div style="margin-top:18px">
          {section_label("Week-over-Week Moves")}
          <table width="100%" cellpadding="0" cellspacing="0">{rows}</table>
        </div>"""

    return html


# ── Section: Action Items ──────────────────────────────────────────────────────

def build_action_items(week_txns, mtd_txns, budgets_parsed, upcoming_bills):
    today = date.today()
    items = []

    # HSA unlogged
    hsa_unlogged = [t for t in mtd_txns
                    if float(t.get("amount", 0)) < 0
                    and is_hsa((t.get("category") or {}).get("name", ""))
                    and not (t.get("notes") or "").strip()]
    if hsa_unlogged:
        total = sum(abs(float(t.get("amount", 0))) for t in hsa_unlogged)
        items.append(("🏥", f"{fmt(total)} in HSA-eligible expenses this month — log receipts in Monarch to lock in tax-free reimbursement", C_LTGREEN, C_BGREEN, C_GREEN))

    # Over-budget categories
    for b in [b for b in budgets_parsed if b["pct_used"] > 100][:2]:
        over_by = b["actual"] - b["budgeted"]
        items.append(("⚠️", f"{b['category']} is {fmt(over_by)} over budget ({pct(b['pct_used'])} used) — review or adjust", C_LTRED, C_BRED, C_RED))

    # Bills due this week
    urgent = [b for b in upcoming_bills if not b["is_income"] and b["amount"] >= 100]
    for b in urgent[:3]:
        days_away = (b["date"] - today).days
        when = "today" if days_away == 0 else "tomorrow" if days_away == 1 else b["date"].strftime("%A")
        items.append(("💳", f"{b['merchant']} — {fmt(b['amount'])} due {when}", C_LTAMBER, C_BAMBER, C_AMBER))

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


# ── Section: Transactions (grouped by day, sorted by amount within day) ────────

def build_transactions_html(week_txns, week_start, week_end):
    if not week_txns:
        return '<p style="color:#a89f95;font-size:13px;margin:0">No transactions last week.</p>', 0.0, 0.0, 0.0

    # Group by date
    by_date = defaultdict(list)
    for txn in week_txns:
        d = txn.get("date", "")[:10]
        by_date[d].append(txn)

    total_income = total_expenses = 0.0
    html = ""

    # Iterate days in order, most recent first
    for day_str in sorted(by_date.keys(), reverse=True):
        day_txns = sorted(by_date[day_str],
                          key=lambda t: abs(float(t.get("amount", 0))), reverse=True)
        try:
            day_label = date.fromisoformat(day_str).strftime("%A, %b %-d")
        except Exception:
            day_label = day_str

        html += f"""
        <div style="font-size:10px;letter-spacing:.15em;text-transform:uppercase;
                    color:{C_LABEL};margin:18px 0 8px;padding-bottom:6px;
                    border-bottom:1px solid {C_BORDER}">{day_label}</div>"""

        rows = ""
        for txn in day_txns:
            merchant = (txn.get("merchant") or {}).get("name") or txn.get("plaidName") or "Unknown"
            cat      = (txn.get("category") or {}).get("name", "Uncategorized")
            amount   = float(txn.get("amount", 0))
            note     = (txn.get("notes") or "").strip()

            if amount > 0:
                total_income += amount
                amt_html      = green(f"+{fmt(amount)}")
            else:
                total_expenses += abs(amount)
                amt_html        = red(f"-{fmt(abs(amount))}")

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
              <td style="padding:8px 0;border-bottom:1px solid {C_BORDER}">
                <div style="font-size:13px;color:{C_TEXT};font-weight:500">{merchant}</div>
                <div style="font-size:11px;color:{C_MUTED};margin-top:2px">{cat}{badges_html}</div>
                {note_html}
              </td>
              <td style="padding:8px 0;border-bottom:1px solid {C_BORDER};
                         text-align:right;font-size:13px;vertical-align:top;white-space:nowrap">{amt_html}</td>
            </tr>"""

        html += f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>'

    net = total_income - total_expenses
    return html, total_income, total_expenses, net


# ── Section: Budget Pulse ──────────────────────────────────────────────────────

def build_budget_html(budgets_parsed, week_end):
    if not budgets_parsed:
        return ""
    days_elapsed  = week_end.day
    days_in_month = calendar.monthrange(week_end.year, week_end.month)[1]
    month_pct     = days_elapsed / days_in_month * 100

    rows = ""
    for b in budgets_parsed[:10]:
        p       = b["pct_used"]
        over    = p > 100
        on_pace = abs(p - month_pct) <= 15
        color   = C_RED if over else (C_GREEN if p <= month_pct else C_AMBER)
        bar_bg  = C_LTRED if over else (C_LTGREEN if p <= month_pct else C_LTAMBER)
        status  = "OVER" if over else ("AHEAD" if p < month_pct - 15 else ("ON TRACK" if on_pace else "WATCH"))
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
          <td style="padding:8px 0;border-bottom:1px solid {C_BORDER};text-align:right;
                     font-size:11px;color:{C_MUTED};white-space:nowrap">{fmt(b['actual'])} / {fmt(b['budgeted'])}</td>
          <td style="padding:8px 0 8px 12px;border-bottom:1px solid {C_BORDER};text-align:right;
                     font-size:9px;letter-spacing:.08em;color:{s_color};white-space:nowrap">{status}</td>
        </tr>"""

    note = (f'<div style="font-size:10px;color:{C_LABEL};margin-bottom:14px;font-style:italic">'
            f'{pct(month_pct)} of {week_end.strftime("%B")} elapsed · budget pace reference</div>')
    return note + f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>'


# ── Section: Upcoming Bills ────────────────────────────────────────────────────

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


# ── Section: MTD Cashflow ──────────────────────────────────────────────────────

def build_cashflow_html(cashflow, mtd_txns, week_end):
    api_income, api_expense = extract_cashflow(cashflow)
    txn_income, txn_expense, _, _ = analyze_transactions(mtd_txns, exclude_transfers=True)
    mtd_income  = api_income  if api_income  > 0 else txn_income
    mtd_expense = api_expense if api_expense > 0 else txn_expense
    if mtd_income == 0 and mtd_expense == 0:
        return ""

    savings       = mtd_income - mtd_expense
    savings_rate  = (savings / mtd_income * 100) if mtd_income > 0 else 0
    days_elapsed  = week_end.day
    days_in_month = calendar.monthrange(week_end.year, week_end.month)[1]
    month_pct     = days_elapsed / days_in_month * 100
    run_rate      = (mtd_expense / days_elapsed * days_in_month) if days_elapsed > 0 else 0
    proj_savings  = mtd_income - run_rate
    sr_color      = C_GREEN if savings_rate >= 20 else (C_RED if savings_rate < 5 else C_AMBER)
    ps_color      = C_GREEN if proj_savings > 0 else C_RED

    return f"""
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td style="padding:7px 0;font-size:13px;color:{C_TEXT}">MTD Income</td>
        <td style="padding:7px 0;text-align:right;font-size:13px;color:{C_GREEN};font-weight:600">{fmt(mtd_income)}</td>
      </tr>
      <tr>
        <td style="padding:7px 0;font-size:13px;color:{C_TEXT}">MTD Spending
          <span style="color:{C_LABEL};font-size:11px">(excl. transfers)</span></td>
        <td style="padding:7px 0;text-align:right;font-size:13px;color:{C_RED};font-weight:600">{fmt(mtd_expense)}</td>
      </tr>
      <tr>
        <td style="padding:7px 0;font-size:12px;color:{C_MUTED}">Projected month-end spend</td>
        <td style="padding:7px 0;text-align:right;font-size:12px;color:{C_MUTED}">{fmt(run_rate)}</td>
      </tr>
      <tr>
        <td style="padding:7px 0 14px;font-size:12px;color:{C_MUTED}">Projected month-end savings</td>
        <td style="padding:7px 0 14px;text-align:right;font-size:12px;
                   color:{ps_color};font-weight:600">{fmt(proj_savings)}</td>
      </tr>
      <tr>
        <td colspan="2" style="padding:0 0 6px">
          <div style="display:flex;justify-content:space-between;font-size:10px;
                      color:{C_LABEL};margin-bottom:6px">
            <span>{week_end.strftime('%B')} · Day {days_elapsed} of {days_in_month}</span>
            <span>{pct(month_pct)} elapsed</span>
          </div>
          <div style="background:#ede9e4;border-radius:3px;height:5px">
            <div style="background:{C_GREEN};border-radius:3px;height:5px;
                        width:{min(int(month_pct),100)}%"></div>
          </div>
        </td>
      </tr>
      <tr>
        <td style="padding:14px 0 4px;font-size:10px;letter-spacing:.15em;
                   text-transform:uppercase;color:{C_LABEL}">Savings Rate MTD</td>
        <td style="padding:14px 0 4px;text-align:right;font-size:20px;
                   font-weight:700;color:{sr_color}">{pct(savings_rate)}</td>
      </tr>
    </table>"""


# ── Section: Debt Tracker ──────────────────────────────────────────────────────

def build_discretionary_html(fixed_expenses, mtd_txns, week_txns, mtd_income, week_end):
    """
    Fixed vs discretionary breakdown.
    Shows fixed obligations from recurring, then discretionary = total spend - fixed.
    """
    if not fixed_expenses or mtd_income == 0:
        return ""

    total_fixed_monthly = sum(f["amount"] for f in fixed_expenses)
    _, mtd_expense, _, _ = analyze_transactions(mtd_txns, exclude_transfers=True)
    _, week_expense, _, _ = analyze_transactions(week_txns, exclude_transfers=True)

    days_elapsed  = week_end.day
    days_in_month = calendar.monthrange(week_end.year, week_end.month)[1]
    month_fraction = days_elapsed / days_in_month

    # Pro-rate fixed expenses to MTD
    fixed_mtd        = total_fixed_monthly * month_fraction
    discretionary_mtd = max(mtd_expense - fixed_mtd, 0)
    discretionary_pct = (discretionary_mtd / mtd_income * 100) if mtd_income > 0 else 0
    fixed_pct         = (fixed_mtd / mtd_income * 100) if mtd_income > 0 else 0

    # Weekly discretionary
    week_days         = 7
    fixed_weekly      = total_fixed_monthly / (days_in_month / 7)
    disc_weekly       = max(week_expense - fixed_weekly, 0)
    disc_per_day      = disc_weekly / 7

    # Build fixed line items
    rows = ""
    for f in fixed_expenses:
        cat_color = C_BLUE if f["category"] == "savings" else C_RED
        rows += f"""
        <tr>
          <td style="padding:5px 0;border-bottom:1px solid {C_BORDER};font-size:12px;color:{C_TEXT}">{f['label']}</td>
          <td style="padding:5px 0;border-bottom:1px solid {C_BORDER};text-align:right;
                     font-size:12px;color:{cat_color};white-space:nowrap">-{fmt(f['amount'])}/mo</td>
        </tr>"""

    # Summary bar: fixed vs discretionary visual
    fixed_bar_w = min(int(fixed_pct), 100)
    disc_bar_w  = min(int(discretionary_pct), 100)

    return f"""
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:16px">
      {rows}
      <tr>
        <td style="padding:10px 0 4px;font-size:10px;letter-spacing:.1em;
                   text-transform:uppercase;color:{C_LABEL}">Total Fixed / mo</td>
        <td style="padding:10px 0 4px;text-align:right;font-size:14px;
                   font-weight:700;color:{C_TEXT}">-{fmt(total_fixed_monthly)}</td>
      </tr>
    </table>
    <div style="border-top:1px solid {C_BORDER};margin:4px 0 16px"></div>
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td style="padding:6px 0;font-size:13px;color:{C_TEXT}">MTD Income</td>
        <td style="padding:6px 0;text-align:right;font-size:13px;
                   color:{C_GREEN};font-weight:600">{fmt(mtd_income)}</td>
      </tr>
      <tr>
        <td style="padding:6px 0;font-size:13px;color:{C_TEXT}">Fixed obligations MTD
          <span style="font-size:10px;color:{C_LABEL}"> (pro-rated)</span></td>
        <td style="padding:6px 0;text-align:right;font-size:13px;color:{C_RED}">-{fmt(fixed_mtd)}</td>
      </tr>
      <tr>
        <td style="padding:6px 0 14px;font-size:13px;color:{C_TEXT}">Discretionary spend MTD</td>
        <td style="padding:6px 0 14px;text-align:right;font-size:13px;
                   color:{C_AMBER};font-weight:600">-{fmt(discretionary_mtd)}</td>
      </tr>
      <tr>
        <td colspan="2" style="padding:0 0 12px">
          <div style="display:flex;gap:3px;height:6px;border-radius:3px;overflow:hidden;background:{C_BORDER}">
            <div style="background:{C_GREEN};width:{fixed_bar_w}%;border-radius:3px 0 0 3px" title="Fixed"></div>
            <div style="background:{C_AMBER};width:{disc_bar_w}%" title="Discretionary"></div>
          </div>
          <div style="display:flex;justify-content:space-between;font-size:10px;color:{C_LABEL};margin-top:5px">
            <span style="color:{C_GREEN}">&#9632; Fixed {pct(fixed_pct)}</span>
            <span style="color:{C_AMBER}">&#9632; Discretionary {pct(discretionary_pct)}</span>
          </div>
        </td>
      </tr>
      <tr>
        <td style="font-size:12px;color:{C_MUTED}">Last week's discretionary</td>
        <td style="text-align:right;font-size:12px;color:{C_MUTED}">{fmt(disc_weekly)}</td>
      </tr>
      <tr>
        <td style="padding-top:4px;font-size:10px;letter-spacing:.1em;
                   text-transform:uppercase;color:{C_LABEL}">Discretionary / day</td>
        <td style="padding-top:4px;text-align:right;font-size:16px;
                   font-weight:700;color:{C_AMBER}">{fmt(disc_per_day)}</td>
      </tr>
    </table>"""


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
      <td colspan="2" style="padding:10px 0;font-size:10px;color:{C_LABEL};
                             letter-spacing:.1em;text-transform:uppercase">Total Debt</td>
      <td style="padding:10px 0;text-align:right;font-size:15px;font-weight:700;color:{C_RED}">-{fmt(total_debt)}</td>
    </tr>"""
    return f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>'


# ── Email Assembly ─────────────────────────────────────────────────────────────

def build_email(today, week_start, week_end, week_txns, mtd_txns,
                accounts, cashflow, history_by_id, budgets_raw, recurring_raw,
                calc_url=None):

    budgets_parsed  = parse_budgets(budgets_raw)
    upcoming_bills  = parse_upcoming_bills(recurring_raw, today, BILL_LOOKAHEAD)
    fixed_expenses  = parse_fixed_expenses(recurring_raw)
    net_worth, _, _ = compute_net_worth(accounts)

    # MTD income for discretionary calc
    api_income, _    = extract_cashflow(cashflow)
    mtd_income_raw, _, _, _ = analyze_transactions(mtd_txns, exclude_transfers=True)
    mtd_income       = api_income if api_income > 0 else mtd_income_raw

    week_label  = f"{week_start.strftime('%b %-d')}–{week_end.strftime('%b %-d, %Y')}"
    month_name  = week_end.strftime("%B %Y")
    generated   = today.strftime("%B %-d, %Y")

    brief_html    = build_weekly_brief(week_start, week_end, week_txns, mtd_txns,
                                       accounts, budgets_parsed, upcoming_bills, cashflow,
                                       recurring_raw=recurring_raw)
    nw_html       = build_net_worth_html(accounts, history_by_id, week_start, week_end)
    action_html   = build_action_items(week_txns, mtd_txns, budgets_parsed, upcoming_bills)
    txn_html, week_income, week_expense, week_net = build_transactions_html(
        week_txns, week_start, week_end)
    budget_html   = build_budget_html(budgets_parsed, week_end)
    upcoming_html = build_upcoming_html(upcoming_bills, today)
    cashflow_html = build_cashflow_html(cashflow, mtd_txns, week_end)
    disc_html     = build_discretionary_html(fixed_expenses, mtd_txns, week_txns, mtd_income, week_end)
    debt_html     = build_debt_html(accounts)

    # Debt Tracker: show if balance moved >$100, or first week of month
    debt_moved = week_end.day <= 7
    if not debt_moved:
        for acct in accounts:
            name = (acct.get("displayName") or "").lower()
            if not any(k in name for k in DEBT_ACCOUNTS):
                continue
            acct_id = str(acct.get("id", ""))
            hist    = history_by_id.get(acct_id, {})
            p_date  = week_start - timedelta(days=1)
            prev    = hist.get(str(p_date))
            if prev is None:
                for d in [1,-1,2,-2]:
                    k = str(p_date + timedelta(days=d))
                    if k in hist:
                        prev = hist[k]; break
            cur = float(acct.get("currentBalance") or 0)
            if prev is not None and abs(cur - prev) > 100:
                debt_moved = True; break

    txn_count = len(week_txns)
    net_color = C_GREEN if week_net >= 0 else C_RED
    net_label = f"+{fmt(week_net)}" if week_net >= 0 else f"-{fmt(abs(week_net))}"

    summary_bar = f"""
  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;
              display:table;width:100%;box-sizing:border-box">
    <div style="display:table-cell;padding:15px 0;text-align:center;border-right:1px solid {C_BORDER}">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};margin-bottom:5px">Income</div>
      <div style="font-size:15px;font-weight:700;color:{C_GREEN}">{fmt(week_income)}</div>
    </div>
    <div style="display:table-cell;padding:15px 0;text-align:center;border-right:1px solid {C_BORDER}">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};margin-bottom:5px">Spent</div>
      <div style="font-size:15px;font-weight:700;color:{C_RED}">{fmt(week_expense)}</div>
    </div>
    <div style="display:table-cell;padding:15px 0;text-align:center">
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};margin-bottom:5px">Net</div>
      <div style="font-size:15px;font-weight:700;color:{net_color}">{net_label}</div>
    </div>
  </div>"""

    # Calculator link section
    if calc_url:
        calc_section = f"""
  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;padding:20px 28px">
    <div style="font-size:9px;letter-spacing:.22em;text-transform:uppercase;color:{C_LABEL};margin-bottom:14px">Retirement Calculator</div>
    <div style="font-size:13px;color:{C_TEXT};margin-bottom:14px;line-height:1.6;font-family:Georgia,serif">
      Your calculator has been updated with this week&#39;s balances from Monarch.
      Monte Carlo simulation, Roth conversion strategy, HD concentration risk, and personalized retirement age.
    </div>
    <a href="{calc_url}" style="display:inline-block;background:{C_TEXT};color:{C_BG};
       font-family:'Courier New',monospace;font-size:11px;letter-spacing:.1em;text-transform:uppercase;
       padding:10px 20px;border-radius:3px;text-decoration:none">
      Open Retirement Calculator &rarr;
    </a>
    <div style="margin-top:10px;font-family:'Courier New',monospace;font-size:9px;color:{C_LABEL}">
      {calc_url}
    </div>
  </div>"""
    else:
        calc_section = ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Monarch Weekly · {week_label}</title>
</head>
<body style="margin:0;padding:32px 16px;background:{C_BG};font-family:Georgia,'Times New Roman',serif">
<div style="max-width:600px;margin:0 auto">

  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-radius:6px 6px 0 0;padding:26px 28px 22px">
    <div style="font-size:9px;letter-spacing:.28em;text-transform:uppercase;color:{C_GREEN};margin-bottom:7px">Monarch · Weekly Digest</div>
    <div style="font-size:24px;color:{C_TEXT};font-style:italic;margin-bottom:4px">{week_label}</div>
    <div style="font-size:11px;color:{C_LABEL}">Net worth &nbsp;&middot;&nbsp;
      <strong style="color:{C_TEXT}">{fmt(net_worth)}</strong>
      &nbsp;&middot;&nbsp; {txn_count} transactions
    </div>
  </div>

  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;padding:0 28px 22px">
    {brief_html}
  </div>

  {card("Action Items", None, action_html, C_AMBER) if action_html else ""}
  {card("Net Worth", None, nw_html)}
  {card("Last Week", f"{txn_count} transactions", txn_html)}
  {summary_bar}
  {card("Budget Pulse", month_name, budget_html)}
  {card("This Week Ahead", f"next {BILL_LOOKAHEAD} days", upcoming_html)}
  {card("Month-to-Date Cashflow", month_name, cashflow_html)}
  {card("Fixed vs. Non-Fixed Spending", month_name, disc_html) if week_end.day >= 7 else ""}
  {card("Debt Tracker", None, debt_html) if debt_moved else ""}

  {calc_section}
  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;border-radius:0 0 6px 6px;
              padding:12px 28px;display:table;width:100%;box-sizing:border-box">
    <span style="display:table-cell;font-size:10px;color:{C_LABEL}">Monarch Money · {generated} · 7:00 AM ET</span>
    <span style="display:table-cell;font-size:10px;color:{C_GREEN};text-align:right">{RECIPIENT_EMAIL}</span>
  </div>

</div>
</body>
</html>"""


# ── Send ───────────────────────────────────────────────────────────────────────

# ── Retirement Calculator Template ───────────────────────────────────────────

RETIREMENT_CALC_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Retirement Calculator -- Peyton</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
:root {{
  --bg: #0d0d0b;
  --surface: #141412;
  --surface2: #1c1c19;
  --border: #2a2a26;
  --border2: #333330;
  --text: #f0ede8;
  --muted: #7a7870;
  --label: #4a4a46;
  --gold: #c8a84b;
  --gold-lt: #e8c96b;
  --gold-dk: #8a7030;
  --gold-bg: #1a1608;
  --green: #4a9e6e;
  --green-lt: #6abf8e;
  --green-bg: #081a10;
  --red: #c04a3a;
  --red-lt: #e06a5a;
  --red-bg: #1a0808;
  --blue: #4a7ab8;
  --blue-lt: #6a9ad8;
  --blue-bg: #08101a;
  --amber: #c87a28;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  background: var(--bg);
  color: var(--text);
  font-family: 'DM Sans', sans-serif;
  font-size: 14px;
  line-height: 1.6;
  min-height: 100vh;
}}
body::before {{
  content: '';
  position: fixed;
  inset: 0;
  background-image: linear-gradient(var(--border) 1px, transparent 1px), linear-gradient(90deg, var(--border) 1px, transparent 1px);
  background-size: 40px 40px;
  opacity: 0.15;
  pointer-events: none;
  z-index: 0;
}}
.app {{ position: relative; z-index: 1; max-width: 980px; margin: 0 auto; padding: 40px 24px 80px; }}
.header {{ margin-bottom: 32px; }}
.eyebrow {{ font-family: 'DM Mono', monospace; font-size: 10px; letter-spacing: .25em; text-transform: uppercase; color: var(--gold); margin-bottom: 10px; }}
h1 {{ font-family: 'DM Serif Display', serif; font-size: 34px; font-weight: 400; color: var(--text); line-height: 1.15; margin-bottom: 8px; }}
h1 em {{ font-style: italic; color: var(--gold); }}
.header-sub {{ font-size: 11px; color: var(--muted); font-family: 'DM Mono', monospace; }}
.layout {{ display: grid; grid-template-columns: 320px 1fr; gap: 20px; align-items: start; }}
.panel {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }}
.panel-hd {{ padding: 13px 18px; border-bottom: 1px solid var(--border); font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .22em; text-transform: uppercase; color: var(--muted); display: flex; justify-content: space-between; align-items: center; }}
.panel-bd {{ padding: 18px; }}
.reset-btn {{ font-family: 'DM Mono', monospace; font-size: 8.5px; letter-spacing: .1em; text-transform: uppercase; color: var(--gold-dk); background: none; border: 1px solid var(--gold-dk); border-radius: 3px; padding: 3px 8px; cursor: pointer; transition: all .15s; }}
.reset-btn:hover {{ color: var(--gold); border-color: var(--gold); }}
.ctrl {{ margin-bottom: 15px; }}
.ctrl:last-child {{ margin-bottom: 0; }}
.ctrl-lbl {{ font-size: 10px; color: var(--muted); font-family: 'DM Mono', monospace; letter-spacing: .06em; text-transform: uppercase; margin-bottom: 6px; }}
.ctrl-row {{ display: flex; align-items: center; gap: 8px; }}
input[type=range] {{ -webkit-appearance: none; flex: 1; height: 3px; background: var(--border2); border-radius: 2px; outline: none; cursor: pointer; }}
input[type=range]::-webkit-slider-thumb {{ -webkit-appearance: none; width: 13px; height: 13px; background: var(--gold); border-radius: 50%; cursor: pointer; transition: transform .15s; }}
input[type=range]::-webkit-slider-thumb:hover {{ transform: scale(1.3); }}
.num-in {{ width: 72px; background: var(--surface2); border: 1px solid var(--border2); border-radius: 3px; color: var(--gold); font-family: 'DM Mono', monospace; font-size: 11px; padding: 3px 6px; text-align: right; outline: none; }}
.num-in:focus {{ border-color: var(--gold-dk); }}
.tog-grp {{ display: flex; gap: 4px; }}
.tog {{ flex: 1; padding: 6px 4px; background: transparent; border: 1px solid var(--border2); border-radius: 4px; color: var(--muted); font-family: 'DM Mono', monospace; font-size: 10px; cursor: pointer; transition: all .15s; text-align: center; }}
.tog.active {{ background: var(--gold-bg); border-color: var(--gold-dk); color: var(--gold); }}
.tog.active-blue {{ background: var(--blue-bg); border-color: var(--blue); color: var(--blue-lt); }}
.sec {{ border-top: 1px solid var(--border); margin: 14px 0; padding-top: 14px; }}
.sec-lbl {{ font-family: 'DM Mono', monospace; font-size: 8.5px; letter-spacing: .2em; text-transform: uppercase; color: var(--label); margin-bottom: 12px; }}
.right-col {{ display: flex; flex-direction: column; gap: 16px; }}
.hero {{ background: var(--surface); border: 1px solid var(--border); border-top: 3px solid var(--gold); border-radius: 8px; padding: 22px 24px 20px; }}
.hero-lbl {{ font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .22em; text-transform: uppercase; color: var(--muted); margin-bottom: 6px; }}
.hero-num {{ font-family: 'DM Serif Display', serif; font-size: 46px; line-height: 1; color: var(--gold); margin-bottom: 4px; }}
.hero-sub {{ font-size: 11px; color: var(--muted); }}
.stat-row {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-top: 18px; padding-top: 18px; border-top: 1px solid var(--border); }}
.stat {{ text-align: center; }}
.sv {{ font-family: 'DM Serif Display', serif; font-size: 18px; color: var(--text); line-height: 1.1; }}
.sv.g {{ color: var(--green-lt); }}
.sv.r {{ color: var(--red-lt); }}
.sv.gold {{ color: var(--gold); }}
.sv.am {{ color: var(--amber); }}
.sl {{ font-family: 'DM Mono', monospace; font-size: 8px; letter-spacing: .12em; text-transform: uppercase; color: var(--muted); margin-top: 3px; }}
.success-def {{ font-family: 'DM Mono', monospace; font-size: 8.5px; color: var(--label); margin-top: 10px; padding-top: 10px; border-top: 1px solid var(--border); line-height: 1.6; }}
.chart-wrap {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 20px; }}
.chart-hd {{ display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 14px; }}
.chart-title {{ font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .18em; text-transform: uppercase; color: var(--muted); }}
.chart-note {{ font-family: 'DM Mono', monospace; font-size: 8px; color: var(--label); margin-top: 3px; }}
.legend {{ display: flex; gap: 12px; }}
.leg-item {{ display: flex; align-items: center; gap: 5px; font-family: 'DM Mono', monospace; font-size: 9px; color: var(--muted); }}
.leg-dot {{ width: 7px; height: 7px; border-radius: 50%; }}
canvas {{ display: block; width: 100% !important; }}
.scenarios {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 20px; }}
.sc-card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 6px; padding: 14px; cursor: pointer; transition: border-color .15s, background .15s; }}
.sc-card:hover {{ border-color: var(--border2); }}
.sc-card.active {{ border-color: var(--gold-dk); background: var(--gold-bg); }}
.sc-name {{ font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .15em; text-transform: uppercase; color: var(--muted); margin-bottom: 4px; }}
.sc-age {{ font-family: 'DM Serif Display', serif; font-size: 28px; color: var(--gold); line-height: 1; margin-bottom: 2px; }}
.sc-desc {{ font-size: 10px; color: var(--muted); }}
.sc-prob {{ margin-top: 8px; font-family: 'DM Mono', monospace; font-size: 11px; }}
.ph {{ color: var(--green-lt); }} .pm {{ color: var(--gold); }} .pl {{ color: var(--red-lt); }}
.mc-bar-wrap {{ background: var(--border); border-radius: 2px; height: 5px; overflow: hidden; margin-top: 5px; }}
.mc-bar {{ height: 5px; border-radius: 2px; transition: width .5s ease; }}
.buckets {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 20px; }}
.brow {{ display: flex; align-items: center; gap: 12px; margin-bottom: 10px; }}
.brow:last-child {{ margin-bottom: 0; }}
.bnm {{ font-family: 'DM Mono', monospace; font-size: 10px; color: var(--muted); width: 90px; flex-shrink: 0; }}
.bbar-wrap {{ flex: 1; background: var(--border); border-radius: 2px; height: 6px; overflow: hidden; }}
.bbar {{ height: 6px; border-radius: 2px; transition: width .4s ease; }}
.bval {{ font-family: 'DM Mono', monospace; font-size: 10px; color: var(--text); text-align: right; width: 70px; flex-shrink: 0; }}
.insights {{ display: flex; flex-direction: column; gap: 8px; }}
.insight {{ background: var(--surface); border: 1px solid var(--border); border-left: 3px solid; border-radius: 0 6px 6px 0; padding: 12px 14px; display: flex; gap: 10px; align-items: flex-start; }}
.ins-icon {{ font-size: 14px; flex-shrink: 0; line-height: 1.5; }}
.ins-text {{ font-size: 12px; color: var(--muted); line-height: 1.65; }}
.ins-text strong {{ color: var(--text); font-weight: 500; }}
.strat-tabs {{ display: flex; gap: 5px; margin-bottom: 12px; }}
.stab {{ padding: 5px 10px; background: transparent; border: 1px solid var(--border2); border-radius: 3px; color: var(--muted); font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .1em; text-transform: uppercase; cursor: pointer; transition: all .15s; }}
.stab.active {{ background: var(--blue-bg); border-color: var(--blue); color: var(--blue-lt); }}
.roth-tl {{ display: flex; gap: 3px; margin-top: 10px; flex-wrap: wrap; }}
.ry {{ height: 30px; min-width: 30px; flex: 1; border-radius: 2px; display: flex; align-items: center; justify-content: center; font-family: 'DM Mono', monospace; font-size: 7px; }}
.risk-meter {{ display: flex; gap: 2px; margin-top: 8px; }}
.rblk {{ flex: 1; height: 6px; border-radius: 1px; }}
.footnote {{ margin-top: 32px; font-family: 'DM Mono', monospace; font-size: 8.5px; color: var(--label); line-height: 1.8; border-top: 1px solid var(--border); padding-top: 16px; }}
@keyframes fadeUp {{ from {{ opacity: 0; transform: translateY(10px); }} to {{ opacity: 1; transform: translateY(0); }} }}
.panel, .hero, .chart-wrap, .buckets {{ animation: fadeUp .35s ease both; }}
</style>
</head>
<body>
<div class="app">

<div class="header">
  <div class="eyebrow">Retirement Projection &middot; Peyton Edwards &middot; Age 38 &middot; As of <span id="as-of">{as_of}</span></div>
  <h1>How early can you <em>actually</em> retire?</h1>
  <div class="header-sub">Monte Carlo &middot; 1,000 simulations &middot; Account-type aware &middot; All values in today's dollars</div>
</div>

<div class="scenarios">
  <div class="sc-card active" onclick="setScenario(55)" id="sc-55">
    <div class="sc-name">Aggressive</div><div class="sc-age">55</div>
    <div class="sc-desc">17 years away</div>
    <div class="sc-prob" id="prob-55">--</div>
    <div class="mc-bar-wrap"><div class="mc-bar" id="bar-55" style="background:var(--amber);width:0%"></div></div>
  </div>
  <div class="sc-card" onclick="setScenario(60)" id="sc-60">
    <div class="sc-name">Balanced</div><div class="sc-age">60</div>
    <div class="sc-desc">22 years away</div>
    <div class="sc-prob" id="prob-60">--</div>
    <div class="mc-bar-wrap"><div class="mc-bar" id="bar-60" style="background:var(--green);width:0%"></div></div>
  </div>
  <div class="sc-card" onclick="setScenario(65)" id="sc-65">
    <div class="sc-name">Conservative</div><div class="sc-age">65</div>
    <div class="sc-desc">27 years away</div>
    <div class="sc-prob" id="prob-65">--</div>
    <div class="mc-bar-wrap"><div class="mc-bar" id="bar-65" style="background:var(--green);width:0%"></div></div>
  </div>
</div>

<div class="layout">
<div>
<div class="panel">
  <div class="panel-hd">Your Numbers <button class="reset-btn" onclick="resetAll()">Reset</button></div>
  <div class="panel-bd">

    <div class="sec-lbl">Current Portfolio</div>
    <div class="ctrl"><div class="ctrl-lbl">Roth IRA</div><div class="ctrl-row"><input type="range" min="0" max="800000" step="5000" id="r-roth" oninput="sl('roth',this.value)"><input type="text" class="num-in" id="i-roth" onchange="si('roth',this.value)"></div></div>
    <div class="ctrl"><div class="ctrl-lbl">401(k) Pre-Tax</div><div class="ctrl-row"><input type="range" min="0" max="800000" step="5000" id="r-k401" oninput="sl('k401',this.value)"><input type="text" class="num-in" id="i-k401" onchange="si('k401',this.value)"></div></div>
    <div class="ctrl"><div class="ctrl-lbl">Taxable Brokerage</div><div class="ctrl-row"><input type="range" min="0" max="500000" step="5000" id="r-taxable" oninput="sl('taxable',this.value)"><input type="text" class="num-in" id="i-taxable" onchange="si('taxable',this.value)"></div></div>
    <div class="ctrl"><div class="ctrl-lbl">HD Vested Stock</div><div class="ctrl-row"><input type="range" min="0" max="500000" step="5000" id="r-rsus" oninput="sl('rsus',this.value)"><input type="text" class="num-in" id="i-rsus" onchange="si('rsus',this.value)"></div></div>
    <div class="ctrl"><div class="ctrl-lbl">HSA (Investment)</div><div class="ctrl-row"><input type="range" min="0" max="100000" step="1000" id="r-hsa" oninput="sl('hsa',this.value)"><input type="text" class="num-in" id="i-hsa" onchange="si('hsa',this.value)"></div></div>

    <div class="sec"><div class="sec-lbl">Annual Contributions</div>
    <div class="ctrl"><div class="ctrl-lbl">401(k) / yr</div><div class="ctrl-row"><input type="range" min="0" max="69000" step="500" id="r-c401k" oninput="sl('c401k',this.value)"><input type="text" class="num-in" id="i-c401k" onchange="si('c401k',this.value)"></div></div>
    <div class="ctrl"><div class="ctrl-lbl">Roth IRA / yr</div><div class="ctrl-row"><input type="range" min="0" max="7000" step="500" id="r-croth" oninput="sl('croth',this.value)"><input type="text" class="num-in" id="i-croth" onchange="si('croth',this.value)"></div></div>
    <div class="ctrl"><div class="ctrl-lbl">HSA / yr</div><div class="ctrl-row"><input type="range" min="0" max="8300" step="100" id="r-chsa" oninput="sl('chsa',this.value)"><input type="text" class="num-in" id="i-chsa" onchange="si('chsa',this.value)"></div></div>
    <div class="ctrl"><div class="ctrl-lbl">Taxable / yr</div><div class="ctrl-row"><input type="range" min="0" max="100000" step="2000" id="r-ctax" oninput="sl('ctax',this.value)"><input type="text" class="num-in" id="i-ctax" onchange="si('ctax',this.value)"></div></div>
    <div class="ctrl"><div class="ctrl-lbl">Annual RSU Grant</div><div class="ctrl-row"><input type="range" min="0" max="150000" step="2500" id="r-rsugrant" oninput="sl('rsugrant',this.value)"><input type="text" class="num-in" id="i-rsugrant" onchange="si('rsugrant',this.value)"></div></div>
    <div class="ctrl"><div class="ctrl-lbl">RSU Sell % at Vest</div><div class="ctrl-row"><input type="range" min="0" max="100" step="10" id="r-rsusell" oninput="sl('rsusell',this.value)"><input type="text" class="num-in" id="i-rsusell" onchange="si('rsusell',this.value)"></div></div>
    </div>

    <div class="sec"><div class="sec-lbl">Retirement Assumptions</div>
    <div class="ctrl"><div class="ctrl-lbl">Annual Spending (today $)</div><div class="ctrl-row"><input type="range" min="50000" max="250000" step="5000" id="r-spend" oninput="sl('spend',this.value)"><input type="text" class="num-in" id="i-spend" onchange="si('spend',this.value)"></div></div>
    <div class="ctrl"><div class="ctrl-lbl">Lumpy Expenses (every 8yr)</div><div class="ctrl-row"><input type="range" min="0" max="150000" step="5000" id="r-lumpy" oninput="sl('lumpy',this.value)"><input type="text" class="num-in" id="i-lumpy" onchange="si('lumpy',this.value)"></div></div>
    <div class="ctrl"><div class="ctrl-lbl">Social Security / mo</div><div class="ctrl-row"><input type="range" min="500" max="5000" step="100" id="r-ss" oninput="sl('ss',this.value)"><input type="text" class="num-in" id="i-ss" onchange="si('ss',this.value)"></div></div>
    <div class="ctrl"><div class="ctrl-lbl">SS Claim Age</div><div class="ctrl-row"><input type="range" min="62" max="70" step="1" id="r-ssage" oninput="sl('ssage',this.value)"><input type="text" class="num-in" id="i-ssage" onchange="si('ssage',this.value)"></div></div>
    <div class="ctrl"><div class="ctrl-lbl">HD Concentration %</div><div class="ctrl-row"><input type="range" min="0" max="80" step="5" id="r-hdconc" oninput="sl('hdconc',this.value)"><input type="text" class="num-in" id="i-hdconc" onchange="si('hdconc',this.value)"></div></div>
    </div>

    <div class="sec"><div class="sec-lbl">Legacy Goal</div>
    <div class="tog-grp">
      <button class="tog" onclick="setLegacy('zero')" id="leg-zero">Die with Zero</button>
      <button class="tog active" onclick="setLegacy('modest')" id="leg-modest">$500K Estate</button>
      <button class="tog" onclick="setLegacy('rich')" id="leg-rich">$1M+ Estate</button>
    </div></div>

    <div class="sec"><div class="sec-lbl">Market Scenario</div>
    <div class="tog-grp">
      <button class="tog" onclick="setMarket('bear')" id="m-bear">Bear</button>
      <button class="tog active" onclick="setMarket('base')" id="m-base">Base</button>
      <button class="tog" onclick="setMarket('bull')" id="m-bull">Bull</button>
    </div>
    <div style="font-family:'DM Mono',monospace;font-size:8.5px;color:var(--muted);margin-top:8px" id="mkt-desc">Base: 7% equity / 4% bond &middot; 2.5% inflation &middot; &sigma;=14%</div>
    </div>

  </div>
</div>
</div>

<div class="right-col">

  <div class="hero">
    <div class="hero-lbl">Median Portfolio at Retirement (Today's Dollars)</div>
    <div class="hero-num" id="hero-num">--</div>
    <div class="hero-sub" id="hero-sub">calculating...</div>
    <div class="stat-row">
      <div class="stat"><div class="sv gold" id="st-prob">--</div><div class="sl">Success Rate</div></div>
      <div class="stat"><div class="sv g" id="st-safe">--</div><div class="sl">4% Withdrawal</div></div>
      <div class="stat"><div class="sv" id="st-fire">--</div><div class="sl">FIRE Number</div></div>
      <div class="stat"><div class="sv am" id="st-estate">--</div><div class="sl">Est. Estate (95)</div></div>
    </div>
    <div class="success-def" id="success-def">Success = portfolio above floor through age 95 across 1,000 simulated market sequences.</div>
  </div>

  <div class="chart-wrap">
    <div class="chart-hd">
      <div>
        <div class="chart-title">Portfolio Trajectory &middot; 1,000 Simulations &middot; Today's Dollars</div>
        <div class="chart-note">Shaded band = 25th&ndash;75th percentile &middot; Dashed line = legacy floor</div>
      </div>
      <div class="legend">
        <div class="leg-item"><div class="leg-dot" style="background:var(--gold)"></div>Median</div>
        <div class="leg-item"><div class="leg-dot" style="background:var(--green);opacity:.6"></div>75th</div>
        <div class="leg-item"><div class="leg-dot" style="background:var(--red);opacity:.6"></div>25th</div>
      </div>
    </div>
    <canvas id="mc" height="210"></canvas>
  </div>

  <div class="buckets">
    <div class="panel-hd" style="padding:0 0 12px;border:none">Account Mix at Retirement</div>
    <div id="brows"></div>
  </div>

  <div class="panel">
    <div class="panel-hd">Roth Conversion Strategy</div>
    <div class="panel-bd">
      <div class="strat-tabs">
        <button class="stab active" onclick="setStrat('guardrails')" id="st-guardrails">Guardrails</button>
        <button class="stab" onclick="setStrat('fixed')" id="st-fixed">Fixed 4%</button>
        <button class="stab" onclick="setStrat('dynamic')" id="st-dynamic">Dynamic</button>
      </div>
      <div style="font-size:12px;color:var(--muted);line-height:1.7;margin-bottom:14px" id="strat-desc">Guardrails: cut spending 10% when portfolio drops 20%, raise 10% when up 20%. Improves success rate ~8% vs fixed withdrawal.</div>
      <div class="sec-lbl">Roth Conversion Window &middot; Gap Years Before SS</div>
      <div class="roth-tl" id="roth-tl"></div>
      <div style="display:flex;gap:14px;margin-top:8px;font-family:'DM Mono',monospace;font-size:8.5px">
        <span style="display:flex;align-items:center;gap:4px;color:var(--muted)"><span style="display:inline-block;width:8px;height:8px;background:var(--blue);border-radius:1px"></span>12% bracket</span>
        <span style="display:flex;align-items:center;gap:4px;color:var(--muted)"><span style="display:inline-block;width:8px;height:8px;background:var(--amber);border-radius:1px"></span>22% bracket</span>
        <span style="display:flex;align-items:center;gap:4px;color:var(--muted)"><span style="display:inline-block;width:8px;height:8px;background:var(--green);border-radius:1px"></span>SS active</span>
      </div>
      <div style="margin-top:14px;padding-top:12px;border-top:1px solid var(--border);font-family:'DM Mono',monospace;font-size:10px;color:var(--gold)" id="roth-advice"></div>
    </div>
  </div>

  <div class="panel">
    <div class="panel-hd">HD Employer Concentration Risk</div>
    <div class="panel-bd">
      <div style="font-size:12px;color:var(--muted);margin-bottom:12px;line-height:1.7" id="conc-desc"></div>
      <div class="sec-lbl">Risk Level</div>
      <div class="risk-meter" id="risk-meter"></div>
      <div style="margin-top:12px;font-family:'DM Mono',monospace;font-size:10px;color:var(--gold)" id="conc-adv"></div>
    </div>
  </div>

  <div class="insights" id="insights"></div>

</div>
</div>

<div class="footnote">
  All values in today's inflation-adjusted dollars. Monte Carlo: historical US equity return distribution adjusted to market scenario. Success = portfolio stays above floor through age 95.
  Pre-tax 401(k) withdrawals subject to estimated blended income tax at drawdown. Roth and HSA withdrawals tax-free. Lumpy expenses modeled as periodic shocks every 8 years.
  SS benefit estimated from user input; actual benefit depends on full earnings history. This tool is for educational planning purposes -- not professional financial advice.
</div>
</div>

<script>
// ---- State ----
var BASE = {{
  roth:{roth}, k401:{k401}, taxable:{taxable}, rsus:{rsus}, hsa:{hsa},
  c401k:23000, croth:7000, chsa:8300, ctax:18000,
  rsugrant:50000, rsusell:80,
  spend:110000, lumpy:60000, ss:3200, ssage:67, hdconc:35,
  market:'base', strat:'guardrails', retireAge:55,
  legacy:'modest', legacyFloor:500000, currentAge:38
}};
var S = Object.assign({{}}, BASE);

// ---- Format ----
function fmtK(v) {{
  var a = Math.abs(v);
  if (a >= 1e6) return '$' + (v/1e6).toFixed(2) + 'M';
  if (a >= 1000) return '$' + Math.round(v/1000) + 'K';
  return '$' + Math.round(v);
}}
function fmtPct(v) {{ return Math.round(v) + '%'; }}
function dispVal(k, v) {{
  var pctKeys = ['rsusell','hdconc'];
  var ageKeys = ['ssage'];
  v = parseFloat(v);
  if (pctKeys.indexOf(k) >= 0) return v + '%';
  if (ageKeys.indexOf(k) >= 0) return '' + v;
  return fmtK(v);
}}
function parseInput(k, raw) {{
  var s = raw.replace(/[$,]/g, '').trim();
  var mult = 1;
  if (s.toUpperCase().indexOf('M') >= 0) {{ mult = 1000000; s = s.replace(/[Mm]/g,''); }}
  else if (s.toUpperCase().indexOf('K') >= 0) {{ mult = 1000; s = s.replace(/[Kk]/g,''); }}
  s = s.replace('%','');
  var v = parseFloat(s);
  return isNaN(v) ? null : v * mult;
}}

// ---- Sync ----
function sl(k, v) {{
  S[k] = parseFloat(v);
  var el = document.getElementById('i-' + k);
  if (el) el.value = dispVal(k, v);
  recalc();
}}
function si(k, raw) {{
  var v = parseInput(k, raw);
  if (v === null) return;
  S[k] = v;
  var r = document.getElementById('r-' + k);
  if (r) r.value = v;
  var inp = document.getElementById('i-' + k);
  if (inp) inp.value = dispVal(k, v);
  recalc();
}}
function initControls() {{
  var keys = ['roth','k401','taxable','rsus','hsa','c401k','croth','chsa','ctax','rsugrant','rsusell','spend','lumpy','ss','ssage','hdconc'];
  keys.forEach(function(k) {{
    var r = document.getElementById('r-' + k);
    var i = document.getElementById('i-' + k);
    if (r) r.value = S[k];
    if (i) i.value = dispVal(k, S[k]);
  }});
  document.getElementById('as-of').textContent = new Date().toLocaleDateString('en-US',{{month:'long',day:'numeric',year:'numeric'}});
}}
function resetAll() {{
  Object.assign(S, BASE);
  initControls();
  setMarket('base');
  setStrat('guardrails');
  setLegacy('modest');
  setScenario(55);
}}

// ---- Toggles ----
function setScenario(age) {{
  S.retireAge = age;
  [55,60,65].forEach(function(a) {{ document.getElementById('sc-'+a).classList.toggle('active', a===age); }});
  recalc();
}}
var MKT = {{
  bear: {{eq:0.055, bond:0.025, inf:0.035, sigma:0.16, lbl:'Bear: 5.5% equity / 2.5% bond &middot; 3.5% inflation &middot; &sigma;=16%'}},
  base: {{eq:0.07,  bond:0.04,  inf:0.025, sigma:0.14, lbl:'Base: 7% equity / 4% bond &middot; 2.5% inflation &middot; &sigma;=14%'}},
  bull: {{eq:0.09,  bond:0.05,  inf:0.02,  sigma:0.12, lbl:'Bull: 9% equity / 5% bond &middot; 2% inflation &middot; &sigma;=12%'}}
}};
function setMarket(m) {{
  S.market = m;
  ['bear','base','bull'].forEach(function(k) {{ document.getElementById('m-'+k).classList.toggle('active', k===m); }});
  document.getElementById('mkt-desc').innerHTML = MKT[m].lbl;
  recalc();
}}
var STRAT_DESC = {{
  guardrails: 'Guardrails: cut spending 10% when portfolio drops 20%, raise 10% when up 20%. Improves success rate ~8% vs fixed withdrawal.',
  fixed: 'Fixed 4%: withdraw 4% of initial portfolio annually, inflation-adjusted. Simple but inflexible -- can fail in bad sequence years.',
  dynamic: 'Dynamic: withdraw 3.5% in down years, up to 5% in strong years. Balances lifestyle with longevity protection.'
}};
function setStrat(s) {{
  S.strat = s;
  ['guardrails','fixed','dynamic'].forEach(function(k) {{ document.getElementById('st-'+k).classList.toggle('active', k===s); }});
  document.getElementById('strat-desc').textContent = STRAT_DESC[s];
  recalc();
}}
var LEGACY_FLOORS = {{zero:10000, modest:500000, rich:1000000}};
function setLegacy(l) {{
  S.legacy = l;
  S.legacyFloor = LEGACY_FLOORS[l];
  ['zero','modest','rich'].forEach(function(k) {{ document.getElementById('leg-'+k).classList.toggle('active', k===l); }});
  recalc();
}}

// ---- Math ----
function randn() {{
  var u=0, v=0;
  while(!u) u=Math.random();
  while(!v) v=Math.random();
  return Math.sqrt(-2*Math.log(u))*Math.cos(2*Math.PI*v);
}}

function project() {{
  var m = MKT[S.market];
  var rr = m.eq - m.inf;
  var yA = S.retireAge - S.currentAge;
  var yD = 95 - S.retireAge;
  var ssAnn = S.ss * 12;
  var floor = S.legacyFloor;
  var N = 1000;
  var success = 0;
  var allPaths = [];
  var finals = [];

  for (var sim = 0; sim < N; sim++) {{
    var sig = m.sigma;
    var roth = S.roth, pre = S.k401, tax = S.taxable, hsa = S.hsa;
    var hdV = S.rsus, hdF = 0;

    // Accumulation
    for (var y = 0; y < yA; y++) {{
      var hdSig = sig * (1 + S.hdconc / 100 * 0.8);
      roth = Math.max(0, roth * (1 + rr + sig*randn()) + S.croth);
      pre  = Math.max(0, pre  * (1 + rr + sig*randn()) + S.c401k);
      tax  = Math.max(0, tax  * (1 + rr + sig*randn()) + S.ctax);
      hsa  = Math.max(0, hsa  * (1 + rr + sig*randn()) + S.chsa);
      hdV  = Math.max(0, hdV  * (1 + rr + hdSig*randn()));
      var vest = S.rsugrant * 0.25;
      var sold = vest * (S.rsusell / 100);
      hdF  = Math.max(0, hdF * (1 + rr + hdSig*randn()) + vest - sold);
      tax  = Math.max(0, tax + sold);
    }}

    var totalRet = roth + pre + tax + hsa + hdV + hdF;
    var port = totalRet;
    var path = [port];
    var baseSpend = S.spend;
    var curSpend = baseSpend;
    var hw = port;
    var preFrac = pre / Math.max(totalRet, 1);

    // Drawdown
    for (var d = 0; d < yD; d++) {{
      var age = S.retireAge + d;
      var ss = age >= S.ssage ? ssAnn : 0;
      var lump = (d > 0 && d % 8 === 0) ? S.lumpy : 0;
      var need = Math.max(0, curSpend - ss) + lump;
      var taxDrag = need * preFrac * 0.20;
      var wd = need + taxDrag;

      if (port <= 0) {{ path.push(0); continue; }}

      port = Math.max(0, port * (1 + rr + sig*randn()) - wd);

      if (S.strat === 'guardrails') {{
        if (port < hw * 0.80) curSpend = Math.max(baseSpend * 0.80, curSpend * 0.90);
        else if (port > hw * 1.20) curSpend = Math.min(baseSpend * 1.25, curSpend * 1.05);
      }} else if (S.strat === 'dynamic') {{
        var wdr = wd / Math.max(port, 1);
        if (wdr > 0.05) curSpend = Math.max(baseSpend * 0.80, curSpend * 0.97);
        else if (wdr < 0.035) curSpend = Math.min(baseSpend * 1.25, curSpend * 1.02);
      }}
      if (port > hw) hw = port;
      path.push(port);
    }}
    if (port >= floor) success++;
    finals.push(Math.max(port, 0));
    allPaths.push(path);
  }}

  var tot = yD + 1;
  var p25=[],p50=[],p75=[];
  for (var i = 0; i < tot; i++) {{
    var vs = allPaths.map(function(p) {{ return p[Math.min(i, p.length-1)]; }}).sort(function(a,b){{return a-b;}});
    p25.push(vs[Math.floor(N*0.25)]);
    p50.push(vs[Math.floor(N*0.50)]);
    p75.push(vs[Math.floor(N*0.75)]);
  }}

  finals.sort(function(a,b){{return a-b;}});
  var medFinal = finals[Math.floor(N*0.5)];

  // Bucket estimates (deterministic)
  function grow(v,c,y) {{ return v*Math.pow(1+rr,y)+c*((Math.pow(1+rr,y)-1)/Math.max(rr,0.001)); }}
  var soldPerYr = S.rsugrant * 0.25 * (S.rsusell/100);
  var bRoth = Math.max(0, grow(S.roth, S.croth, yA));
  var bPre  = Math.max(0, grow(S.k401, S.c401k, yA));
  var bTax  = Math.max(0, grow(S.taxable, S.ctax + soldPerYr, yA));
  var bHSA  = Math.max(0, grow(S.hsa, S.chsa, yA));
  // HD bucket: vested stock grows + future grants that aren't sold accumulate
  var unsoldPerYr = S.rsugrant * 0.25 * (1 - S.rsusell/100);
  var bHD   = Math.max(0, S.rsus * Math.pow(1+rr, yA) + grow(0, unsoldPerYr, yA));

  return {{
    sr: success/N, p25:p25, p50:p50, p75:p75,
    portAtRet: p50[0], medFinal:medFinal,
    bkt:{{roth:bRoth, k401:bPre, taxable:bTax, hsa:bHSA, hd:bHD}},
    yA:yA, yD:yD
  }};
}}

function quickSR(retAge) {{
  var m = MKT[S.market];
  var rr = m.eq - m.inf;
  var yA = retAge - S.currentAge;
  var now = S.roth+S.k401+S.taxable+S.hsa+S.rsus;
  var tc = S.croth+S.c401k+S.chsa+S.ctax;
  var proj = now*Math.pow(1+rr,yA)+tc*((Math.pow(1+rr,yA)-1)/Math.max(rr,0.001));
  var fire = S.spend / 0.04;
  var ratio = proj / fire;
  var base = 1/(1+Math.exp(-5*(ratio-0.9)));
  var bonus = S.strat==='guardrails'?0.05:S.strat==='dynamic'?0.025:0;
  return Math.min(0.97, Math.max(0.04, base+bonus));
}}

// ---- Chart ----
function drawChart(p25, p50, p75, retAge) {{
  var cv = document.getElementById('mc');
  var ctx = cv.getContext('2d');
  var dpr = window.devicePixelRatio || 1;
  var W = cv.offsetWidth, H = 210;
  cv.width = W*dpr; cv.height = H*dpr;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0,0,W,H);

  var PAD = {{t:10, r:20, b:28, l:58}};
  var cW = W-PAD.l-PAD.r, cH = H-PAD.t-PAD.b;
  var n = p50.length;
  var maxV = 0;
  for (var i=0;i<p75.length;i++) if (isFinite(p75[i]) && p75[i]>maxV) maxV=p75[i];
  maxV *= 1.08;
  if (maxV < 1) maxV = 1;

  function xOf(i) {{ return PAD.l + (i/(n-1))*cW; }}
  function yOf(v) {{ return PAD.t + cH - (Math.max(v,0)/maxV)*cH; }}

  // Grid
  for (var g=0; g<=4; g++) {{
    var gy = PAD.t + (g/4)*cH;
    var gv = maxV*(1-g/4);
    ctx.strokeStyle='#222220'; ctx.lineWidth=1;
    ctx.beginPath(); ctx.moveTo(PAD.l,gy); ctx.lineTo(W-PAD.r,gy); ctx.stroke();
    ctx.fillStyle='#4a4a46'; ctx.font='9px monospace'; ctx.textAlign='right';
    ctx.fillText(gv>=1e6?'$'+(gv/1e6).toFixed(1)+'M':'$'+Math.round(gv/1000)+'K', PAD.l-6, gy+3);
  }}

  // X labels
  ctx.fillStyle='#4a4a46'; ctx.textAlign='center';
  [0,10,20,95-retAge].forEach(function(off) {{
    var idx = Math.min(off, n-1);
    ctx.fillText('Age '+(retAge+off), xOf(idx), H-5);
  }});

  // Band
  ctx.beginPath();
  ctx.moveTo(xOf(0), yOf(p25[0]));
  for (var i=1;i<n;i++) ctx.lineTo(xOf(i),yOf(p25[i]));
  for (var i=n-1;i>=0;i--) ctx.lineTo(xOf(i),yOf(p75[i]));
  ctx.closePath();
  ctx.fillStyle='rgba(74,158,110,0.07)'; ctx.fill();

  // p75
  ctx.beginPath(); ctx.strokeStyle='rgba(106,191,142,0.35)'; ctx.lineWidth=1.5;
  ctx.moveTo(xOf(0),yOf(p75[0]));
  for (var i=1;i<n;i++) ctx.lineTo(xOf(i),yOf(p75[i]));
  ctx.stroke();

  // p25
  ctx.beginPath(); ctx.strokeStyle='rgba(192,74,58,0.35)'; ctx.lineWidth=1.5;
  ctx.moveTo(xOf(0),yOf(p25[0]));
  for (var i=1;i<n;i++) ctx.lineTo(xOf(i),yOf(p25[i]));
  ctx.stroke();

  // Median
  ctx.beginPath(); ctx.strokeStyle='#c8a84b'; ctx.lineWidth=2.5;
  ctx.shadowColor='rgba(200,168,75,0.3)'; ctx.shadowBlur=8;
  ctx.moveTo(xOf(0),yOf(p50[0]));
  for (var i=1;i<n;i++) ctx.lineTo(xOf(i),yOf(p50[i]));
  ctx.stroke(); ctx.shadowBlur=0;

  // Floor line
  if (S.legacyFloor > 0 && S.legacyFloor < maxV) {{
    var fy = yOf(S.legacyFloor);
    ctx.strokeStyle='rgba(200,122,40,0.4)';
    ctx.setLineDash([4,5]); ctx.lineWidth=1;
    ctx.beginPath(); ctx.moveTo(PAD.l,fy); ctx.lineTo(W-PAD.r,fy); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle='rgba(200,122,40,0.55)'; ctx.font='8px monospace'; ctx.textAlign='left';
    ctx.fillText(fmtK(S.legacyFloor)+' floor', PAD.l+4, fy-3);
  }}
}}

// ---- Roth Timeline ----
function buildRothTL(retAge, ssAge) {{
  var el = document.getElementById('roth-tl');
  el.innerHTML = '';
  var gap = ssAge - retAge;
  var show = Math.min(gap+4, 20);
  for (var y=0; y<show; y++) {{
    var age = retAge+y;
    var d = document.createElement('div');
    d.className = 'ry';
    if (age >= ssAge) {{
      d.style.cssText = 'background:var(--green-bg);border:1px solid var(--green);color:var(--green-lt)';
    }} else if (y <= 2) {{
      d.style.cssText = 'background:var(--blue-bg);border:1px solid var(--blue);color:var(--blue-lt)';
    }} else {{
      d.style.cssText = 'background:rgba(200,122,40,0.12);border:1px solid var(--amber);color:var(--amber)';
    }}
    d.textContent = age;
    el.appendChild(d);
  }}
  var amt = Math.round(55000/1000)*1000;
  document.getElementById('roth-advice').textContent =
    'Convert ~$'+amt.toLocaleString()+'/yr during '+gap+'-year gap (age '+retAge+'--'+ssAge+') at 12-22% vs 32%+ when RMDs force conversions later.';
}}

// ---- Concentration Risk ----
function buildConc(pct) {{
  var meter = document.getElementById('risk-meter');
  var desc  = document.getElementById('conc-desc');
  var adv   = document.getElementById('conc-adv');
  meter.innerHTML = '';
  for (var i=0; i<20; i++) {{
    var d = document.createElement('div');
    d.className = 'rblk';
    if (i < Math.round(pct/100*20)) {{
      var t = i/20;
      d.style.background = 'rgb('+Math.round(74+t*118)+','+Math.round(158-t*120)+','+Math.round(110-t*76)+')';
    }} else {{
      d.style.background = 'var(--border)';
    }}
    meter.appendChild(d);
  }}
  var lvl, col, advice;
  if (pct < 15)      {{ lvl='Low';      col='var(--green-lt)'; advice='Within acceptable range. Continue systematic diversification at each vest.'; }}
  else if (pct < 30) {{ lvl='Moderate'; col='var(--gold)';     advice='Sell RSUs into strength. Target below 15%. Keep discipline regardless of stock outlook.'; }}
  else if (pct < 50) {{ lvl='Elevated'; col='var(--amber)';    advice='RSUs + 401(k) + salary all correlated to HD. Diversify at every vest event.'; }}
  else               {{ lvl='High';     col='var(--red-lt)';   advice='Critical. A 40% HD decline materially impacts retirement timeline. Sell all vests immediately into index funds.'; }}
  desc.innerHTML = '<span style="color:'+col+';font-weight:500">'+lvl+' concentration</span> -- '+pct+'% of investable assets tied to Home Depot. Salary, RSUs, ESPP, and 401(k) all correlated.';
  adv.textContent = advice;
}}

// ---- Insights ----
function buildInsights(res, fireNum) {{
  var el = document.getElementById('insights');
  el.innerHTML = '';
  var total = res.portAtRet;
  var gap = total - fireNum;
  var items = [];

  if (gap >= 0) {{
    items.push({{icon:'&#10022;', col:'var(--green)', text:'Projected <strong>'+fmtK(total)+'</strong> exceeds FIRE number of <strong>'+fmtK(fireNum)+'</strong> by <strong>'+fmtK(gap)+'</strong>. You have a buffer -- consider retiring earlier or spending more.'}});
  }} else {{
    var extra = Math.round(Math.abs(gap)/res.yA/1000)*1000;
    items.push({{icon:'&#9672;', col:'var(--amber)', text:'Portfolio is <strong>'+fmtK(Math.abs(gap))+'</strong> short of FIRE number at '+S.retireAge+'. Adding <strong>$'+extra.toLocaleString()+'/yr</strong> closes the gap, or retire at '+(S.retireAge+2)+' instead.'}});
  }}

  // Recommended age -- scan using full simulation at key ages
  var recAge = null;
  var recSR = 0;
  var scanAges = [52,54,56,58,60,62,64,66,68];
  var origAge = S.retireAge;
  for (var ai=0; ai<scanAges.length; ai++) {{
    S.retireAge = scanAges[ai];
    var scanRes = project();
    if (scanRes.sr >= 0.85 && recAge === null) {{
      recAge = scanAges[ai];
      recSR = Math.round(scanRes.sr * 100);
    }}
  }}
  S.retireAge = origAge;
  if (recAge !== null) {{
    var yrsAway = recAge - S.currentAge;
    items.push({{icon:'&#9670;', col:'var(--gold)',
      text:'<strong>Age '+recAge+'</strong> is your earliest retirement age with an 85%+ success rate (<strong>'+recSR+'%</strong> in simulation). That is '+yrsAway+' years away under '+S.market+' market conditions.'}});
  }} else {{
    items.push({{icon:'&#9670;', col:'var(--red-lt)',
      text:'Success rate stays below 85% through age 68 under current assumptions. Try increasing contributions, reducing spending, or switching to Guardrails withdrawal strategy.'}});
  }}

  var ss62 = Math.round(S.ss*0.70*12);
  var ss70 = Math.round(S.ss*1.24*12);
  items.push({{icon:'&#9711;', col:'var(--blue)', text:'SS timing: at 62 = <strong>'+fmtK(ss62)+'/yr</strong>, at 70 = <strong>'+fmtK(ss70)+'/yr</strong>. That is a <strong>'+fmtK(ss70-ss62)+'</strong> annual gap. Breakeven vs claiming at 62 is roughly age 80. In early retirement, draw taxable first and delay SS.'}});

  items.push({{icon:'&#9733;', col:'var(--gold)', text:'Roth IRA projected at <strong>'+fmtK(res.bkt.roth)+'</strong> at retirement. No RMDs, tax-free growth, passes to heirs tax-free. Protect it: draw taxable accounts first so Roth compounds longer.'}});

  // Tradeoff: work 2 more years
  var origAge2 = S.retireAge;
  S.retireAge = origAge2 + 2;
  var res2 = project();
  var sr2 = Math.round(res2.sr * 100);
  var sr0 = Math.round(res.sr * 100);
  S.retireAge = origAge2;
  var srDiff = sr2 - sr0;
  if (srDiff > 0) {{
    items.push({{icon:'&#9650;', col:'var(--muted)',
      text:'Working 2 more years (to age '+(origAge2+2)+') raises success rate from <strong>'+sr0+'%</strong> to <strong>'+sr2+'%</strong> -- a <strong>+'+srDiff+' point</strong> improvement. That buys the most safety per year of any lever.'}});
  }}

  // Tradeoff: cut spending $10K
  var origSpend = S.spend;
  S.spend = origSpend - 10000;
  var res3 = project();
  var sr3 = Math.round(res3.sr * 100);
  S.spend = origSpend;
  var srDiff3 = sr3 - sr0;
  if (srDiff3 > 0 && origSpend > 70000) {{
    items.push({{icon:'&#9660;', col:'var(--muted)',
      text:'Cutting spending by $10K/yr (to <strong>'+fmtK(origSpend-10000)+'</strong>) raises success rate by <strong>+'+srDiff3+' points</strong>. Spending is your most flexible lever in retirement -- even a 10% cut in bad years changes outcomes significantly.'}});
  }}

  items.forEach(function(ins) {{
    var d = document.createElement('div');
    d.className = 'insight';
    d.style.borderLeftColor = ins.col;
    d.innerHTML = '<span class="ins-icon" style="color:'+ins.col+'">'+ins.icon+'</span><span class="ins-text">'+ins.text+'</span>';
    el.appendChild(d);
  }});
}}

// ---- Main recalc ----
var _t = null;
function recalc() {{ clearTimeout(_t); _t = setTimeout(_recalc, 100); }}
function _recalc() {{
  var res = project();
  var tot = res.portAtRet;
  var fireNum = S.spend / 0.04;
  var sp = Math.round(res.sr * 100);

  document.getElementById('hero-num').textContent = fmtK(tot);
  document.getElementById('hero-sub').textContent = 'Median at age '+S.retireAge+' -- '+res.yA+' years of accumulation';

  var pe = document.getElementById('st-prob');
  pe.textContent = sp+'%';
  pe.className = 'sv '+(sp>=85?'g':sp>=70?'gold':'r');

  document.getElementById('st-safe').textContent = fmtK(tot*0.04)+'/yr';
  document.getElementById('st-fire').textContent = fmtK(fireNum);

  var ee = document.getElementById('st-estate');
  ee.textContent = fmtK(res.medFinal);
  ee.className = 'sv '+(res.medFinal>=1e6?'g':res.medFinal>=200000?'am':'r');

  document.getElementById('success-def').textContent =
    'Success = portfolio stays above '+fmtK(S.legacyFloor)+' floor'+' through age 95 across 1,000 simulated sequences. Includes lumpy expenses every 8 years.';

  drawChart(res.p25, res.p50, res.p75, S.retireAge);

  var b = res.bkt;
  var bt = b.roth+b.k401+b.taxable+b.hsa+b.hd;
  var defs = [
    {{k:'roth', lbl:'Roth IRA', tax:'Tax-free', col:'var(--green)'}},
    {{k:'k401', lbl:'401(k)', tax:'Pre-tax', col:'var(--gold)'}},
    {{k:'taxable', lbl:'Taxable', tax:'Cap gains', col:'var(--blue)'}},
    {{k:'hsa', lbl:'HSA', tax:'Triple-tax', col:'var(--green-lt)'}},
    {{k:'hd', lbl:'HD Stock', tax:'Concentrated', col:'var(--amber)'}}
  ];
  var brows = document.getElementById('brows');
  brows.innerHTML = '';
  defs.forEach(function(d) {{
    var v = b[d.k], p = bt>0?v/bt*100:0;
    var row = document.createElement('div');
    row.className = 'brow';
    row.innerHTML = '<div class="bnm">'+d.lbl+'<br><span style="font-size:7.5px;color:var(--label)">'+d.tax+'</span></div>'+
      '<div class="bbar-wrap"><div class="bbar" style="width:'+p.toFixed(1)+'%;background:'+d.col+'"></div></div>'+
      '<div class="bval">'+fmtK(v)+'</div>';
    brows.appendChild(row);
  }});

  [55,60,65].forEach(function(age) {{
    var r = quickSR(age), p = Math.round(r*100);
    var el = document.getElementById('prob-'+age);
    var bar = document.getElementById('bar-'+age);
    el.textContent = p+'% success rate';
    el.className = 'sc-prob '+(p>=85?'ph':p>=70?'pm':'pl');
    bar.style.width = p+'%';
  }});

  buildRothTL(S.retireAge, S.ssage);
  buildConc(S.hdconc);
  buildInsights(res, fireNum);
}}

window.addEventListener('resize', function() {{
  var r = project();
  drawChart(r.p25, r.p50, r.p75, S.retireAge);
}});

initControls();
recalc();
</script>
</body>
</html>
"""



# ── Retirement Calculator Generator ───────────────────────────────────────────

def build_retirement_calculator(accounts, week_end):
    """
    Generate a personalized retirement_calculator.html with current balances
    injected from Monarch. Returns the HTML string.
    """
    # Extract current balances by account name keyword
    def find_balance(keywords):
        for a in accounts:
            name = (a.get("displayName") or a.get("name") or "").lower()
            if any(k in name for k in keywords):
                bal = float(a.get("currentBalance") or 0)
                if bal > 0:
                    return bal
        return 0

    roth    = find_balance(["roth ira", "peyton - roth"])
    k401    = find_balance(["401(k)", "home depot 401", "vanguard - deloitte"])
    taxable = find_balance(["taxable brokerage", "vanguard - taxable"])
    hsa     = find_balance(["hsa investment", "hsa deposit"])
    rsus    = find_balance(["rsu", "vested shares", "espp", "employee stock"])

    # Net worth for reference
    net_worth, _, _ = compute_net_worth(accounts)

    as_of = week_end.strftime("%B %-d, %Y")

    # Read the template and inject values
    calc_html = RETIREMENT_CALC_TEMPLATE.format(
        as_of=as_of,
        roth=int(roth),
        k401=int(k401),
        taxable=int(taxable),
        hsa=int(hsa),
        rsus=int(rsus),
        net_worth=int(net_worth),
    )
    return calc_html


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
    (today, week_start, week_end, week_txns, mtd_txns,
     accounts, cashflow, history_by_id, budgets, recurring) = await fetch_data()

    net_worth, _, _ = compute_net_worth(accounts)
    print(f"  NW: {fmt(net_worth)} · {len(week_txns)} week txns · "
          f"{len(mtd_txns)} MTD · budgets={'yes' if budgets else 'no'} · "
          f"recurring={len(recurring)}")

    print("Building weekly digest...")
    # html rebuilt below after calc_url is known

    print("Building retirement calculator...")
    calc_html = build_retirement_calculator(accounts, week_end)

    # Write calculator to disk so the workflow can git commit it to the repo
    # GitHub Pages will then serve it at CALC_URL
    with open(CALC_FILENAME, "w", encoding="utf-8") as f:
        f.write(calc_html)
    print(f"  Wrote {CALC_FILENAME} ({len(calc_html):,} chars)")

    week_label = f"{week_start.strftime('%b %-d')}–{week_end.strftime('%-d')}"
    subject    = f"💰 Monarch Weekly · {week_label} · NW {fmt(net_worth)}"
    html = build_email(today, week_start, week_end, week_txns, mtd_txns,
                       accounts, cashflow, history_by_id, budgets, recurring,
                       calc_url=CALC_URL)
    send_email(subject, html)


if __name__ == "__main__":
    asyncio.run(main())
