#!/usr/bin/env python3
"""
Monarch Money — Daily Balance Sync + Weekly Financial Digest
Daily (7am ET):  fetches all Monarch accounts → writes balances.json + appends nw_history.json
Monday (7am ET): also sends the weekly HTML digest email

Files committed to GitHub:
  balances.json    — current account balances, pre-computed dashboard fields
  nw_history.json  — daily net worth snapshots for trend charting
"""

import asyncio
import calendar
import json
import os
import subprocess
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
GITHUB_USER      = os.environ.get("GITHUB_USER", "peytoned21")
GITHUB_REPO      = os.environ.get("GITHUB_REPO", "monarch-digest")
DASHBOARD_URL    = f"https://{GITHUB_USER}.github.io/{GITHUB_REPO}/dashboard.html"

# Peyton's birthdate for age calculation
PEYTON_BIRTHDATE = date(1987, 8, 24)

HSA_KEYWORDS      = ["medical", "pharmacy", "dental", "vision", "health", "doctor", "hospital"]
TRANSFER_KEYWORDS = ["transfer", "transfers to investments", "credit card payment"]
INCOME_KEYWORDS   = ["paycheck", "income", "salary", "bonus", "direct deposit", "reimbursement"]
NW_DELTA_THRESHOLD = 100.0
BILL_LOOKAHEAD     = 7
DEBT_ACCOUNTS      = ["mortgage", "tesla", "lexus", "stanford", "heloc"]

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


# ── Helpers ────────────────────────────────────────────────────────────────────

def fmt(amount: float) -> str:
    return f"${abs(amount):,.2f}"

def pct(value: float) -> str:
    return f"{value:.1f}%"

def current_age(birthdate: date = PEYTON_BIRTHDATE) -> int:
    today = date.today()
    return today.year - birthdate.year - (
        (today.month, today.day) < (birthdate.month, birthdate.day)
    )

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
    days_since_sunday = (today.weekday() + 1) % 7
    week_end   = today - timedelta(days=days_since_sunday if days_since_sunday > 0 else 7)
    week_start = week_end - timedelta(days=6)
    return week_start, week_end


# ── Account Helpers ────────────────────────────────────────────────────────────

def find_balance(accounts, keywords, must_be_positive=True):
    """Find account balance by name keywords."""
    for a in accounts:
        acct_name = (a.get("displayName") or a.get("name") or "").lower()
        if any(k.lower() in acct_name for k in keywords):
            bal = float(a.get("currentBalance") or a.get("balance") or 0)
            if must_be_positive and bal > 0:
                return int(bal)
            if not must_be_positive and bal < 0:
                return int(abs(bal))
    return 0

def sum_by_type(accounts, type_names, positive_only=False, negative_only=False):
    """Sum balances for accounts matching given type names."""
    total = 0.0
    for a in accounts:
        t = (a.get("type") or {}).get("name", "")
        if t not in type_names:
            continue
        bal = float(a.get("currentBalance") or 0)
        if positive_only and bal < 0:
            continue
        if negative_only and bal > 0:
            continue
        total += bal
    return total

def compute_net_worth(accounts):
    net_worth = assets = liabilities = 0.0
    for a in accounts:
        if a.get("includeInNetWorth") is False:
            continue
        b = float(a.get("currentBalance") or a.get("balance") or 0)
        net_worth += b
        if b >= 0:
            assets += b
        else:
            liabilities += abs(b)
    return net_worth, assets, liabilities


def get_account_type(a):
    """Monarch returns type as a dict {"name": "brokerage", ...} — extract the string."""
    t = a.get("type")
    if isinstance(t, dict):
        return t.get("name", "")
    return t or ""


# ── Balances JSON Builder ──────────────────────────────────────────────────────

def build_balances_json(accounts, today: date) -> dict:
    """
    Build the complete balances.json snapshot.
    Includes pre-computed dashboard fields AND full account list.

    Monarch returns these type strings (confirmed from live data):
      "brokerage"   — all investment accounts (Roth, 401k, taxable, HSA, 529, RSUs, etc.)
      "depository"  — checking and savings accounts
      "loan"        — mortgages, auto loans, student loans
      "credit"      — credit cards
      "vehicle"     — car values (KBB via VinAudit)
      "real_estate" — home value (Zillow)
    """
    net_worth, assets, liabilities = compute_net_worth(accounts)

    # Helper: get name string for an account
    def name(a):
        return (a.get("displayName") or a.get("name") or "").lower()

    def bal(a):
        return float(a.get("currentBalance") or 0)

    def atype(a):
        """Extract type string — Monarch returns type as a dict {'name': 'brokerage', ...}"""
        t = a.get("type")
        if isinstance(t, dict):
            return t.get("name", "")
        return t or ""

    # Names that disqualify a brokerage account from being "taxable"
    NON_TAXABLE_KEYWORDS = [
        "roth", "401", "hsa", "liia", "home depot rsu", "the home depot rsu",
        "eleanor", "arthur", "529", "espp", "vested shares", "bond portfolio",
        "wealthfront investment", "nasdaq", "staging", "deloitte", "rollover",
        "traditional ira", "thd employee", "employee stock purchase",
    ]

    # Taxable brokerage = all brokerage accounts that aren't retirement/HD/529/HSA
    taxable_total = sum(
        bal(a) for a in accounts
        if atype(a) == "brokerage"
        and bal(a) > 0
        and not any(k in name(a) for k in NON_TAXABLE_KEYWORDS)
    )

    # HSA = both HSA accounts combined
    hsa_total = sum(
        bal(a) for a in accounts
        if "hsa" in name(a) and bal(a) > 0
    )

    # Cash = all depository accounts, positive, excluding Roth staging
    cash_total = sum(
        bal(a) for a in accounts
        if atype(a) == "depository"
        and bal(a) > 0
        and "roth" not in name(a)
    )

    # Vehicles = type "vehicle", positive
    vehicle_total = sum(
        bal(a) for a in accounts
        if atype(a) == "vehicle" and bal(a) > 0
    )

    # Credit cards = type "credit", negative balances
    cc_total = abs(sum(
        bal(a) for a in accounts
        if atype(a) == "credit" and bal(a) < 0
    ))

    # Full account list — flatten type dict to plain string for dashboard use
    account_list = []
    for a in accounts:
        account_list.append({
            "id":            a.get("id"),
            "name":          a.get("displayName") or a.get("name"),
            "type":          atype(a),   # plain string e.g. "brokerage"
            "balance":       round(bal(a), 2),
            "include_in_nw": a.get("includeInNetWorth", True),
            "is_asset":      a.get("isAsset", True),
            "institution":   (a.get("institution") or {}).get("name") or a.get("institution"),
        })

    return {
        "as_of":        today.strftime("%B %-d, %Y"),
        "generated_at": today.isoformat(),
        "_cached":      False,

        # ── Retirement accounts ────────────────────────────────────────
        "roth":         find_balance(accounts, ["peyton - roth"]),
        "groth":        find_balance(accounts, ["grace - roth"]),
        "k401":         find_balance(accounts, ["home depot 401"]),
        "taxable":      int(taxable_total),
        "hsa":          int(hsa_total),
        "hd_vested":    find_balance(accounts, ["liia-", "liia "]),
        "hd_rsus":      find_balance(accounts, ["the home depot rsus"]),

        # ── Cash ───────────────────────────────────────────────────────
        "roth_staging": find_balance(accounts, ["roth staging"]),
        "total_cash":   int(cash_total),

        # ── 529s ───────────────────────────────────────────────────────
        "eleanor_529":  find_balance(accounts, ["eleanor"]),
        "arthur_529":   find_balance(accounts, ["arthur"]),

        # ── Debt ───────────────────────────────────────────────────────
        "mortgage_bal": find_balance(accounts, ["mccully", "mortgage"], must_be_positive=False),
        "stanford_bal": find_balance(accounts, ["stanford"], must_be_positive=False),
        "lexus_bal":    find_balance(accounts, ["gx460"], must_be_positive=False),
        "tesla_bal":    find_balance(accounts, ["tesla", "model 3"], must_be_positive=False),
        "cc_total":     int(cc_total),

        # ── Real estate & vehicles ─────────────────────────────────────
        "home_value":   find_balance(accounts, ["mccully", "3149"]),
        "vehicle_total": int(vehicle_total),
        "lexus_value":  find_balance(accounts, ["lexus gx base", "2019 lexus gx"]),
        "tesla_value":  find_balance(accounts, ["tesla model 3 base", "2023 tesla"]),

        # ── Summary ────────────────────────────────────────────────────
        "net_worth":    int(net_worth),
        "total_assets": int(assets),
        "total_debt":   int(liabilities),

        # ── Full account list ──────────────────────────────────────────
        "accounts":     account_list,

        # ── Meta ───────────────────────────────────────────────────────
        "peyton_age":   current_age(),
    }


# ── Net Worth History ──────────────────────────────────────────────────────────

def update_nw_history(net_worth: float, today: date, history_path="nw_history.json") -> list:
    """
    Load existing history, append today's snapshot if not already present,
    return updated list. Keeps rolling 3-year window.
    """
    history = []
    if os.path.exists(history_path):
        try:
            with open(history_path) as f:
                history = json.load(f)
        except Exception:
            history = []

    today_str = today.isoformat()

    # Update or append today's entry
    existing = next((h for h in history if h["date"] == today_str), None)
    if existing:
        existing["nw"] = int(net_worth)
    else:
        history.append({"date": today_str, "nw": int(net_worth)})

    # Keep rolling 3-year window (1095 days)
    cutoff = (today - timedelta(days=1095)).isoformat()
    history = [h for h in history if h["date"] >= cutoff]

    # Sort by date
    history.sort(key=lambda x: x["date"])

    with open(history_path, "w") as f:
        json.dump(history, f, indent=2)

    print(f"  NW history: {len(history)} snapshots, latest {today_str} = ${net_worth:,.0f}")
    return history


# ── Git Commit ─────────────────────────────────────────────────────────────────

def git_commit(files: list, message: str):
    """Stage, commit, and push specified files."""
    try:
        subprocess.run(["git", "config", "user.email", "action@github.com"], check=True)
        subprocess.run(["git", "config", "user.name", "GitHub Action"], check=True)
        for f in files:
            subprocess.run(["git", "add", f], check=True)
        result = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if result.returncode != 0:
            subprocess.run(["git", "commit", "-m", message], check=True)
            subprocess.run(["git", "push"], check=True)
            print(f"✓ Committed: {', '.join(files)}")
        else:
            print("  No changes to commit")
    except subprocess.CalledProcessError as e:
        print(f"  Git error: {e}")


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

    print("Fetching accounts...")
    acct_resp    = await mm.get_accounts()
    accounts     = acct_resp.get("accounts", [])

    # Only fetch transaction/budget data on Monday (digest day) to save API calls
    is_monday = today.weekday() == 0
    week_txns = mtd_txns = []
    cashflow  = {}
    budgets   = {}
    recurring = []
    history_by_id = {}

    if is_monday:
        print("Fetching week transactions...")
        txn_resp  = await mm.get_transactions(
            start_date=str(week_start), end_date=str(week_end))
        week_txns = txn_resp.get("allTransactions", {}).get("results", [])

        print("Fetching MTD transactions...")
        mtd_resp  = await mm.get_transactions(
            start_date=str(month_start), end_date=str(week_end))
        mtd_txns  = mtd_resp.get("allTransactions", {}).get("results", [])

        print("Fetching account history...")
        target_dates = {str(week_end), str(week_start - timedelta(days=1))}
        debug_done   = False
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
                    print(f"  History sample: {str(snapshots[-1])[:120]}")
                    debug_done = True
                bal_map = {}
                for s in snapshots:
                    if not isinstance(s, dict):
                        continue
                    d = (s.get("date") or s.get("startDate") or s.get("day") or "")
                    if d:
                        bal_map[d[:10]] = float(s.get("balance") or s.get("amount") or 0)
                if bal_map:
                    history_by_id[str(acct_id)] = bal_map
            except Exception:
                pass

        print("Fetching budgets...")
        try:
            budgets = await mm.get_budgets(
                start_date=str(month_start), end_date=str(week_end))
        except Exception as e:
            print(f"  Budgets failed: {e}")

        print("Fetching recurring...")
        try:
            rec_resp  = await mm.get_recurring_transactions()
            raw_list  = (rec_resp.get("recurringTransactionItems") if isinstance(rec_resp, dict)
                         else rec_resp if isinstance(rec_resp, list) else [])
            recurring = raw_list or []
        except Exception as e:
            print(f"  Recurring failed: {e}")

        print("Fetching cashflow...")
        try:
            cashflow = await mm.get_cashflow_summary(
                start_date=str(month_start), end_date=str(week_end))
        except Exception as e:
            print(f"  Cashflow failed: {e}")

    return (today, week_start, week_end, week_txns, mtd_txns,
            accounts, cashflow, history_by_id, budgets, recurring)


# ── Budget / Transaction helpers (used by digest) ─────────────────────────────

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


def parse_fixed_expenses(recurring_raw):
    fixed = []
    seen_labels = set()
    for item in (recurring_raw or []):
        try:
            stream   = item.get("stream") or {}
            merchant = (stream.get("merchant") or {}).get("name") or ""
            amount   = abs(float(stream.get("amount") or 0))
            if amount == 0 or float(stream.get("amount") or 0) > 0:
                continue
            merchant_lower = merchant.lower()
            for keyword, meta in FIXED_EXPENSE_KEYWORDS.items():
                if keyword in merchant_lower:
                    label = meta["label"]
                    if label not in seen_labels:
                        fixed.append({"label": label, "amount": amount,
                                      "category": meta["category"], "merchant": merchant})
                        seen_labels.add(label)
                    break
        except Exception:
            pass
    return sorted(fixed, key=lambda x: x["amount"], reverse=True)


# ── Digest Email Sections ──────────────────────────────────────────────────────

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
    month_name      = week_end.strftime("%B")
    week_label      = f"{week_start.strftime('%b %-d')}–{week_end.strftime('%b %-d')}"

    fixed_expenses      = parse_fixed_expenses(recurring_raw) if recurring_raw else []
    total_fixed_monthly = sum(f["amount"] for f in fixed_expenses)
    days_in_month       = calendar.monthrange(week_end.year, week_end.month)[1]
    fixed_weekly        = total_fixed_monthly / (days_in_month / 7)
    week_discretionary  = max(week_expense - fixed_weekly, 0)
    disc_per_day        = week_discretionary / 7

    sentences = []
    non_transfer    = [t for t in week_txns if not is_transfer((t.get("category") or {}).get("name", ""))]
    over_budget     = [b for b in budgets_parsed if b["pct_used"] > 100]
    big_bills_ct    = sum(1 for b in upcoming_bills if not b["is_income"] and b["amount"] >= 300)
    avg_weekly_spend = (mtd_expense / max(week_end.day, 1)) * 7
    is_income_week  = week_income > 3000
    is_heavy_spend  = week_expense > avg_weekly_spend * 1.3 and week_expense > 500
    is_quiet        = week_expense < 200 and week_income == 0
    is_bill_heavy   = big_bills_ct >= 2

    if is_quiet:
        sentences.append(f"Quiet week — minimal activity {week_label}.")
    elif is_income_week and week_income > week_expense * 1.5:
        net_str = f"+{fmt(week_net)}" if week_net >= 0 else f"-{fmt(abs(week_net))}"
        sentences.append(f"Strong income week: {fmt(week_income)} in, {fmt(week_expense)} out, net {net_str}.")
    elif is_heavy_spend and not is_income_week:
        sentences.append(f"Spend-heavy week — {fmt(week_expense)} out vs. ~{fmt(avg_weekly_spend)} weekly average.")
    elif is_bill_heavy:
        sentences.append(f"Bill-heavy week: {big_bills_ct} payments of $300+ due in the next 7 days.")
    elif over_budget:
        names = ", ".join(b["category"] for b in over_budget[:2])
        sentences.append(f"Budget pressure: {names} {'has' if len(over_budget)==1 else 'have'} exceeded budget.")
    else:
        if total_fixed_monthly > 0 and week_expense > 0:
            sentences.append(f"You spent {fmt(week_expense)} last week — {fmt(week_discretionary)} non-fixed ({fmt(disc_per_day)}/day).")
        elif week_income > 0:
            net_str = f"+{fmt(week_net)}" if week_net >= 0 else f"-{fmt(abs(week_net))}"
            sentences.append(f"You brought in {fmt(week_income)} and spent {fmt(week_expense)}, netting {net_str}.")
        else:
            sentences.append(f"You spent {fmt(week_expense)} last week across {len(non_transfer)} transactions.")

    if mtd_income > 0:
        if savings_rate >= 30:
            sentences.append(f"{month_name} tracking at {pct(savings_rate)} savings — ahead of plan.")
        elif savings_rate >= 15:
            sentences.append(f"{month_name} savings rate: {pct(savings_rate)}.")
        else:
            sentences.append(f"{month_name} savings rate {pct(savings_rate)} — spending running high.")

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


def build_net_worth_html(accounts, history_by_id, week_start, week_end):
    net_worth, assets, liabilities = compute_net_worth(accounts)
    prior_date = str(week_start - timedelta(days=1))
    prior_vals = {}
    for acct in accounts:
        if acct.get("includeInNetWorth") is False:
            continue
        acct_id  = str(acct.get("id", ""))
        hist     = history_by_id.get(acct_id, {})
        prior_bal = hist.get(prior_date)
        if prior_bal is None:
            for delta in [1, -1, 2, -2]:
                d = str(date.fromisoformat(prior_date) + timedelta(days=delta))
                if d in hist:
                    prior_bal = hist[d]
                    break
        if prior_bal is not None:
            prior_vals[acct_id] = prior_bal

    prior_nw   = sum(prior_vals.values()) if len(prior_vals) >= 5 else None
    nw_delta   = (net_worth - prior_nw) if prior_nw is not None else None
    delta_color = C_GREEN if (nw_delta or 0) >= 0 else C_RED
    delta_sign  = "▲" if (nw_delta or 0) >= 0 else "▼"
    delta_html  = (f'<span style="font-size:13px;color:{delta_color};margin-left:10px">'
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
    return html


def build_action_items(week_txns, mtd_txns, budgets_parsed, upcoming_bills):
    today = date.today()
    items = []
    hsa_unlogged = [t for t in mtd_txns
                    if float(t.get("amount", 0)) < 0
                    and is_hsa((t.get("category") or {}).get("name", ""))
                    and not (t.get("notes") or "").strip()]
    if hsa_unlogged:
        total = sum(abs(float(t.get("amount", 0))) for t in hsa_unlogged)
        items.append(("🏥", f"{fmt(total)} in HSA-eligible expenses — log receipts in Monarch", C_LTGREEN, C_BGREEN, C_GREEN))
    for b in [b for b in budgets_parsed if b["pct_used"] > 100][:2]:
        over_by = b["actual"] - b["budgeted"]
        items.append(("⚠️", f"{b['category']} is {fmt(over_by)} over budget ({pct(b['pct_used'])} used)", C_LTRED, C_BRED, C_RED))
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


def build_transactions_html(week_txns, week_start, week_end):
    if not week_txns:
        return '<p style="color:#a89f95;font-size:13px;margin:0">No transactions last week.</p>', 0.0, 0.0, 0.0
    by_date = defaultdict(list)
    for txn in week_txns:
        by_date[txn.get("date", "")[:10]].append(txn)
    total_income = total_expenses = 0.0
    html = ""
    for day_str in sorted(by_date.keys(), reverse=True):
        day_txns = sorted(by_date[day_str], key=lambda t: abs(float(t.get("amount", 0))), reverse=True)
        try:
            day_label = date.fromisoformat(day_str).strftime("%A, %b %-d")
        except Exception:
            day_label = day_str
        html += f'<div style="font-size:10px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL};margin:18px 0 8px;padding-bottom:6px;border-bottom:1px solid {C_BORDER}">{day_label}</div>'
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
            note_html = (f'<div style="font-size:10px;color:{C_LABEL};margin-top:2px;font-style:italic">{note}</div>') if note else ""
            rows += f"""
            <tr>
              <td style="padding:8px 0;border-bottom:1px solid {C_BORDER}">
                <div style="font-size:13px;color:{C_TEXT};font-weight:500">{merchant}</div>
                <div style="font-size:11px;color:{C_MUTED};margin-top:2px">{cat}{badges_html}</div>
                {note_html}
              </td>
              <td style="padding:8px 0;border-bottom:1px solid {C_BORDER};text-align:right;font-size:13px;vertical-align:top;white-space:nowrap">{amt_html}</td>
            </tr>"""
        html += f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>'
    net = total_income - total_expenses
    return html, total_income, total_expenses, net


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
          <td style="padding:8px 0;border-bottom:1px solid {C_BORDER};text-align:right;font-size:11px;color:{C_MUTED};white-space:nowrap">{fmt(b['actual'])} / {fmt(b['budgeted'])}</td>
          <td style="padding:8px 0 8px 12px;border-bottom:1px solid {C_BORDER};text-align:right;font-size:9px;letter-spacing:.08em;color:{s_color};white-space:nowrap">{status}</td>
        </tr>"""
    note = (f'<div style="font-size:10px;color:{C_LABEL};margin-bottom:14px;font-style:italic">'
            f'{pct(month_pct)} of {week_end.strftime("%B")} elapsed</div>')
    return note + f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>'


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
          <td style="padding:8px 0;border-bottom:1px solid {C_BORDER};font-size:11px;color:{when_color};font-weight:600;white-space:nowrap;width:78px">{when_str}</td>
          <td style="padding:8px 12px;border-bottom:1px solid {C_BORDER}">
            <div style="font-size:13px;color:{C_TEXT}">{b['merchant']}</div>{acct_str}
          </td>
          <td style="padding:8px 0;border-bottom:1px solid {C_BORDER};text-align:right;font-size:13px;color:{amt_color};white-space:nowrap">{sign} {fmt(b['amount'])}</td>
        </tr>"""
    return f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>'


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
        <td style="padding:7px 0;font-size:13px;color:{C_TEXT}">MTD Spending</td>
        <td style="padding:7px 0;text-align:right;font-size:13px;color:{C_RED};font-weight:600">{fmt(mtd_expense)}</td>
      </tr>
      <tr>
        <td style="padding:7px 0;font-size:12px;color:{C_MUTED}">Projected month-end spend</td>
        <td style="padding:7px 0;text-align:right;font-size:12px;color:{C_MUTED}">{fmt(run_rate)}</td>
      </tr>
      <tr>
        <td style="padding:7px 0 14px;font-size:12px;color:{C_MUTED}">Projected savings</td>
        <td style="padding:7px 0 14px;text-align:right;font-size:12px;color:{ps_color};font-weight:600">{fmt(proj_savings)}</td>
      </tr>
      <tr>
        <td style="padding:14px 0 4px;font-size:10px;letter-spacing:.15em;text-transform:uppercase;color:{C_LABEL}">Savings Rate MTD</td>
        <td style="padding:14px 0 4px;text-align:right;font-size:20px;font-weight:700;color:{sr_color}">{pct(savings_rate)}</td>
      </tr>
    </table>"""


def build_debt_html(accounts):
    debts = []
    for a in accounts:
        acct_name = (a.get("displayName") or a.get("name") or "").lower()
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
          <td style="padding:8px 0;border-bottom:1px solid {C_BORDER};text-align:right;font-size:12px;color:{C_RED};font-weight:600;white-space:nowrap">-{fmt(d['balance'])}</td>
        </tr>"""
    rows += f"""
    <tr>
      <td colspan="2" style="padding:10px 0;font-size:10px;color:{C_LABEL};letter-spacing:.1em;text-transform:uppercase">Total Debt</td>
      <td style="padding:10px 0;text-align:right;font-size:15px;font-weight:700;color:{C_RED}">-{fmt(total_debt)}</td>
    </tr>"""
    return f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>'


# ── Digest Email Assembly ──────────────────────────────────────────────────────

def build_email(today, week_start, week_end, week_txns, mtd_txns,
                accounts, cashflow, history_by_id, budgets_raw, recurring_raw,
                dashboard_url=None):

    budgets_parsed  = parse_budgets(budgets_raw)
    upcoming_bills  = parse_upcoming_bills(recurring_raw, today, BILL_LOOKAHEAD)
    net_worth, _, _ = compute_net_worth(accounts)

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
    debt_html     = build_debt_html(accounts)

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

    dashboard_section = ""
    if dashboard_url:
        dashboard_section = f"""
  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;padding:20px 28px">
    <div style="font-size:9px;letter-spacing:.22em;text-transform:uppercase;color:{C_LABEL};margin-bottom:14px">Financial Dashboard</div>
    <div style="font-size:13px;color:{C_TEXT};margin-bottom:14px;line-height:1.6;font-family:Georgia,serif">
      Balances updated from Monarch. Net worth, retirement projections, college planning, and tax analysis.
    </div>
    <a href="{dashboard_url}" style="display:inline-block;background:{C_TEXT};color:{C_BG};
       font-family:'Courier New',monospace;font-size:11px;letter-spacing:.1em;text-transform:uppercase;
       padding:10px 20px;border-radius:3px;text-decoration:none">
      Open Dashboard &rarr;
    </a>
  </div>"""

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
  {card("Debt Tracker", None, debt_html)}
  {dashboard_section}
  <div style="background:{C_CARD};border:1px solid {C_BORDER};border-top:none;border-radius:0 0 6px 6px;
              padding:12px 28px;display:table;width:100%;box-sizing:border-box">
    <span style="display:table-cell;font-size:10px;color:{C_LABEL}">Monarch Money · {generated} · 7:00 AM ET</span>
    <span style="display:table-cell;font-size:10px;color:{C_GREEN};text-align:right">{RECIPIENT_EMAIL}</span>
  </div>
</div>
</body>
</html>"""


# ── Email Send ─────────────────────────────────────────────────────────────────

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


# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    today = date.today()
    is_monday = today.weekday() == 0

    print(f"Running {'Monday digest' if is_monday else 'daily balance sync'} for {today}")

    print("Fetching Monarch data...")
    (today, week_start, week_end, week_txns, mtd_txns,
     accounts, cashflow, history_by_id, budgets, recurring) = await fetch_data()

    net_worth, assets, liabilities = compute_net_worth(accounts)
    print(f"  NW: {fmt(net_worth)} · assets: {fmt(assets)} · debt: {fmt(liabilities)}")
    print(f"  Accounts: {len(accounts)}")

    # ── Always: write balances.json ──────────────────────────────────────────
    print("Building balances.json...")
    balances = build_balances_json(accounts, today)
    with open("balances.json", "w") as f:
        json.dump(balances, f, indent=2)
    print("✓ balances.json written")

    # ── Always: update nw_history.json ──────────────────────────────────────
    print("Updating nw_history.json...")
    update_nw_history(net_worth, today, "nw_history.json")

    # ── Commit both files ────────────────────────────────────────────────────
    git_commit(
        ["balances.json", "nw_history.json"],
        f"Daily balance sync {today} · NW ${net_worth:,.0f}"
    )

    # ── Monday only: send digest email ───────────────────────────────────────
    if is_monday:
        print("Building weekly digest email...")
        week_label = f"{week_start.strftime('%b %-d')}\u2013{week_end.strftime('%-d')}"
        subject    = f"\U0001f4b0 Monarch Weekly \u00b7 {week_label} \u00b7 NW {fmt(net_worth)}"
        html = build_email(
            today, week_start, week_end, week_txns, mtd_txns,
            accounts, cashflow, history_by_id, budgets, recurring,
            dashboard_url=DASHBOARD_URL
        )
        send_email(subject, html)
    else:
        print("Skipping digest email (not Monday)")

    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
