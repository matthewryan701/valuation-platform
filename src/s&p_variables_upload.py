#!/usr/bin/env python3
"""
s&p_variables_upload.py

- Reads S&P 500 universe from SP500.csv (ticker, company name, sector)
- Scrapes DCF-relevant items for EXACTLY four fiscal years: 2024, 2023, 2022, 2021
- Drops a ticker if ANY required datapoint is missing for any of those years
- Flattens into one row per ticker with per-year columns
- Upserts rows into Supabase table: S&P500_DCF_Variables

Requirements (install in your venv):
  pip install yfinance pandas numpy python-dotenv supabase requests lxml html5lib
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from supabase import Client, create_client

# =========================
# Config
# =========================

YEARS = [2024, 2023, 2022, 2021]
TABLE_NAME = "S&P500_DCF_Variables"
CSV_PATH = "SP500.csv"
RATE_LIMIT_SECONDS = 0.4  # be gentle to Yahoo

# Label aliases (lower-cased) for resilience to Yahoo index changes
REV_ALIASES = ["total revenue", "revenue", "totalrevenue"]
OI_ALIASES = ["operating income", "operatingincome", "ebit"]
NI_ALIASES = ["net income", "netincome"]
OCF_ALIASES = [
    "operating cash flow",
    "total cash from operating activities",
    "totalcashfromoperatingactivities",
]
CAPEX_ALIASES = [
    "capital expenditure",
    "capitalexpenditures",
    "investments in property plant and equipment",
]
FCF_ALIASES = ["free cash flow", "freecashflow"]
DEBT_ALIASES = ["total debt", "long term debt", "longtermdebt", "short long term debt"]
CASH_ALIASES = ["cash and cash equivalents", "cashandcashequivalents"]
EQUITY_ALIASES = [
    "stockholders equity",
    "total stockholder equity",
    "totalstockholdersequity",
]
GROSS_PROFIT_ALIASES = ["gross profit", "grossprofit"]
CUR_ASSETS_ALIASES   = ["total current assets", "current assets", "totalcurrentassets"]
CUR_LIABS_ALIASES    = ["total current liabilities", "current liabilities", "totalcurrentliabilities"]

# =========================
# Env & Supabase
# =========================

def require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing {name}. Set it in .env next to this file.")
    return val

def init_supabase() -> Client:
    # Load .env located next to this script, regardless of CWD
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
    url = require_env("SUPABASE_URL")
    key = require_env("SUPABASE_KEY")
    return create_client(url, key)

# =========================
# CSV helpers
# =========================

CSV_TICKER_COLS = ["Symbol", "Ticker", "Tickers", "symbol"]
CSV_NAME_COLS = ["Security", "Company", "Company Name", "Name"]
CSV_SECTOR_COLS = ["GICS Sector", "Sector"]

def normalize_ticker(t: str) -> str:
    if not isinstance(t, str):
        return ""
    # Yahoo uses dashes instead of dots for share classes (e.g., BRK-B)
    return t.strip().upper().replace(".", "-")

def pick_column(df: pd.DataFrame, candidates: List[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    if df.shape[1] == 1:
        # Single-column CSVs: assume it’s the one
        return df.columns[0]
    raise ValueError(f"Could not find any of {candidates} in CSV. Columns: {list(df.columns)}")

def load_universe(csv_path: str = CSV_PATH) -> pd.DataFrame:
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(f"CSV not found at: {p.resolve()}")

    df = pd.read_csv(p)
    if df.empty:
        raise ValueError(f"CSV is empty: {p.resolve()}")

    c_ticker = pick_column(df, CSV_TICKER_COLS)
    c_name = pick_column(df, CSV_NAME_COLS)
    c_sector = pick_column(df, CSV_SECTOR_COLS)

    out = (
        df[[c_ticker, c_name, c_sector]]
        .rename(columns={c_ticker: "ticker_raw", c_name: "company_name", c_sector: "sector"})
        .assign(ticker=lambda d: d["ticker_raw"].astype(str).map(normalize_ticker))
        .drop(columns=["ticker_raw"])
        .dropna()
        .drop_duplicates(subset=["ticker"])
    )

    # Filter obviously bad tickers
    out = out[(out["ticker"].str.len() >= 1) & (out["ticker"].str.len() <= 6)]
    # Ensure strings
    out["company_name"] = out["company_name"].astype(str).str.strip()
    out["sector"] = out["sector"].astype(str).str.strip()
    return out[["ticker", "company_name", "sector"]]

# =========================
# yfinance helpers
# =========================

def _index_map(df: Optional[pd.DataFrame]) -> Dict[str, str]:
    if df is None or df.empty:
        return {}
    return {str(i).strip().lower(): i for i in df.index}

def _get_row_value(df: Optional[pd.DataFrame], aliases: List[str], col_obj) -> Optional[float]:
    if df is None or df.empty:
        return None
    idx = _index_map(df)
    for a in aliases:
        key = a.lower()
        if key in idx:
            try:
                val = df.loc[idx[key], col_obj]
                if pd.isna(val):
                    return None
                return float(val)
            except Exception:
                return None
    return None

def _columns_by_year(df: Optional[pd.DataFrame]) -> Dict[int, object]:
    """
    Map fiscal period columns to calendar year (int).
    Returns {year: column_object}.
    """
    if df is None or df.empty:
        return {}
    out: Dict[int, object] = {}
    for c in df.columns:
        try:
            y = int(pd.to_datetime(c).year)
            out[y] = c
        except Exception:
            # If column isn't a parseable date, skip it
            continue
    return out

def extract_annuals(stock: yf.Ticker) -> Optional[Dict[str, float]]:
    """
    Extract EXACTLY the YEARS specified. If any required datapoint for any year is
    missing in the three statements, return None (drop ticker).
    Now includes: gross profit, margins, current assets/liabilities/ratio, debt/equity.
    """
    inc = stock.financials
    bs  = stock.balance_sheet
    cf  = stock.cashflow

    inc_cols = _columns_by_year(inc)
    bs_cols  = _columns_by_year(bs)
    cf_cols  = _columns_by_year(cf)

    out: Dict[str, float] = {}

    for y in YEARS:
        col_i = inc_cols.get(y)
        col_b = bs_cols.get(y)
        col_c = cf_cols.get(y)
        if col_i is None or col_b is None or col_c is None:
            return None  # missing a statement for that year

        # Income statement
        revenue = _get_row_value(inc, REV_ALIASES,          col_i)
        op_inc  = _get_row_value(inc, OI_ALIASES,           col_i)
        net_inc = _get_row_value(inc, NI_ALIASES,           col_i)
        gross_p = _get_row_value(inc, GROSS_PROFIT_ALIASES, col_i)

        # Cash flow
        ocf  = _get_row_value(cf,  OCF_ALIASES,  col_c)
        capx = _get_row_value(cf,  CAPEX_ALIASES, col_c)
        fcf  = _get_row_value(cf,  FCF_ALIASES,   col_c)
        if fcf is None and (ocf is not None and capx is not None):
            fcf = ocf + capx  # CapEx usually negative

        # Balance sheet
        debt = _get_row_value(bs,  DEBT_ALIASES,  col_b)
        cash = _get_row_value(bs,  CASH_ALIASES,  col_b)
        eqty = _get_row_value(bs,  EQUITY_ALIASES, col_b)
        ca   = _get_row_value(bs,  CUR_ASSETS_ALIASES,  col_b)
        cl   = _get_row_value(bs,  CUR_LIABS_ALIASES,   col_b)

        # Require base values
        base = {
            f"revenue_{y}": revenue,
            f"operating_income_{y}": op_inc,
            f"net_income_{y}": net_inc,
            f"operating_cash_flow_{y}": ocf,
            f"capital_expenditure_{y}": capx,
            f"free_cash_flow_{y}": fcf,
            f"total_debt_{y}": debt,
            f"cash_and_equivalents_{y}": cash,
            f"total_equity_{y}": eqty,
            f"gross_profit_{y}": gross_p,
            f"current_assets_{y}": ca,
            f"current_liabilities_{y}": cl,
        }
        if any(v is None for v in base.values()):
            return None

        # Derived margins & ratios
        # Avoid division by zero
        gm  = (gross_p / revenue) if revenue else None
        om  = (op_inc  / revenue) if revenue else None
        nm  = (net_inc / revenue) if revenue else None
        fcfm= (fcf     / revenue) if revenue else None
        ocfm= (ocf     / revenue) if revenue else None
        dte = (debt / eqty)       if eqty else None
        cr  = (ca / cl)           if (ca and cl) else None

        derived = {
            f"gross_margin_{y}": gm,
            f"operating_margin_{y}": om,
            f"net_margin_{y}": nm,
            f"fcf_margin_{y}": fcfm,
            f"ofc_margin_{y}": ocfm,
            f"debt_to_equity_{y}": dte,
            f"current_ratio_{y}": cr,
        }
        if any(v is None for v in derived.values()):
            return None

        out.update(base)
        out.update(derived)

    return out

def scrape_company_row(ticker: str, company_name: str, sector: str) -> Optional[Dict[str, object]]:
    stock = yf.Ticker(ticker)

    # Point-in-time stats
    current_price = shares_out = market_cap = beta = tax_rate = None
    trailing_pe = forward_pe = p_pcf = None

    try:
        fi = stock.fast_info
        current_price = fi.get("last_price") or fi.get("lastPrice")
        shares_out    = fi.get("shares")
        market_cap    = fi.get("market_cap")
    except Exception:
        pass

    try:
        info = stock.info
        current_price = current_price or info.get("currentPrice") or info.get("regularMarketPrice")
        shares_out    = shares_out or info.get("sharesOutstanding")
        market_cap    = market_cap or info.get("marketCap")
        beta          = info.get("beta")
        tax_rate      = info.get("effectiveTaxRate")
        trailing_pe   = info.get("trailingPE")
        forward_pe    = info.get("forwardPE")
    except Exception:
        pass

    annuals = extract_annuals(stock)
    if annuals is None:
        return None

    # EPS per year (required)
    if not shares_out or shares_out == 0:
        return None
    for y in YEARS:
        ni = annuals.get(f"net_income_{y}")
        if ni is None:
            return None
        annuals[f"eps_{y}"] = ni / shares_out

    # P/PCF (point-in-time; not drop-critical)
    # Use latest year (2024) OCF per share as denominator
    ocf_2024 = annuals.get("operating_cash_flow_2024")
    if current_price and shares_out and ocf_2024:
        ocf_per_share = ocf_2024 / shares_out
        if ocf_per_share:  # avoid div by zero
            p_pcf = current_price / ocf_per_share

    row = {
        "ticker": ticker,
        "company_name": company_name,
        "sector": sector,
        "last_updated": datetime.utcnow().isoformat(),
        "current_price": current_price,
        "shares_outstanding": shares_out,
        "market_cap": market_cap,
        "beta": beta,
        "tax_rate": tax_rate,
        "pe_trailing": trailing_pe,
        "pe_forward": forward_pe,
        "p_pcf": p_pcf,
        **annuals,
    }

    # Enforce none-missing on core annuals + EPS only (P/E & P/PCF are optional)
    required_keys = []
    for y in YEARS:
        required_keys += [
            f"revenue_{y}", f"operating_income_{y}", f"net_income_{y}",
            f"operating_cash_flow_{y}", f"capital_expenditure_{y}", f"free_cash_flow_{y}",
            f"total_debt_{y}", f"cash_and_equivalents_{y}", f"total_equity_{y}",
            f"gross_profit_{y}", f"gross_margin_{y}", f"operating_margin_{y}", f"net_margin_{y}",
            f"fcf_margin_{y}", f"ofc_margin_{y}", f"debt_to_equity_{y}",
            f"current_assets_{y}", f"current_liabilities_{y}", f"current_ratio_{y}",
            f"eps_{y}"
        ]
    if any(row.get(k) is None for k in required_keys):
        return None

    return row

# =========================
# Supabase I/O
# =========================

def upsert_row(sb: Client, row: Dict[str, object]) -> bool:
    try:
        # Because ticker is PRIMARY KEY in the SQL we provided, upsert will replace/insert correctly
        sb.table(TABLE_NAME).upsert(row).execute()
        return True
    except Exception as e:
        print(f"  ✗ Supabase upsert error for {row.get('ticker')}: {e}")
        return False

# =========================
# Main
# =========================

def main():
    print("Loading S&P 500 universe from CSV…")
    universe = load_universe(CSV_PATH)
    print(f"Found {len(universe)} tickers in CSV")

    sb = init_supabase()

    ok = 0
    skipped = 0
    failed = 0

    for i, rec in enumerate(universe.to_dict(orient="records"), start=1):
        t = rec["ticker"]
        nm = rec["company_name"]
        sc = rec["sector"]
        print(f"\n[{i}/{len(universe)}] {t} — {nm} ({sc})")

        try:
            row = scrape_company_row(t, nm, sc)
        except Exception as e:
            failed += 1
            print(f"  ✗ Scrape error: {e}")
            time.sleep(RATE_LIMIT_SECONDS)
            continue

        if row is None:
            skipped += 1
            print("  ↷ Skipped (missing one or more required datapoints for 2021–2024)")
            time.sleep(RATE_LIMIT_SECONDS)
            continue

        if upsert_row(sb, row):
            ok += 1
            print("  ✓ Upserted")
        else:
            failed += 1
            print("  ✗ Upload failed")

        time.sleep(RATE_LIMIT_SECONDS)

    print("\n" + "=" * 60)
    print(f"Done. Upserted: {ok} | Skipped (incomplete): {skipped} | Failed: {failed}")
    print("=" * 60)

if __name__ == "__main__":
    main()
