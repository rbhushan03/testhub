#!/usr/bin/env python3
"""
Ref: https://fabtrader.in/a-simple-peaceful-etf-rotation-strategy-that-delivered-32-cagr/
ETF rotation strategy using local NSE bhavcopy files.

Loads the most recent 350 trading days from local bhavcopy CSVs
(`sec_bhavdata_full_YYYYMMDD.csv`), computes 200-day SMA per ETF,
applies SMA trend filter, computes ROC over a lookback (months × 21
trading days), ranks ETFs by ROC, and prints a tidy table.

Notes on lookback indexing:
 - The script sorts per-ETF data in descending date order (most recent first).
 - For lookback = 1 month (≈21 trading days), the price used is the 21st
   row in that descending series (1-based). Implementation uses index
   (lookback_days - 1) (0-based), i.e., 21st row → index 20.

Prerequisites
- Python 3.8+
- Install dependencies (recommended into a venv):

PowerShell (Windows):

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install pandas tabulate
```

Usage:
Running the script
- Default (1-month ROC):


python .\src\main\common\etf_rotation_strategy.py

- 2-month ROC:
python .\src\main\common\etf_rotation_strategy.py --lookback 2

- 3-month ROC, show top 5:
python .\src\main\common\etf_rotation_strategy.py --lookback 3 --top 5


"""
from __future__ import annotations

import argparse
import glob
import os
import re
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd
from tabulate import tabulate


# ETF universe
ETF_LIST = [
    'SILVERBEES', 'GOLDBEES', 'NIFTYBEES', 'MID150BEES', 'ITBEES',
    'BANKBEES', 'HDFCSML250', 'JUNIORBEES', 'MON100', 'CPSEETF',
    'PSUBNKBEES', 'MODEFENCE', 'PVTBANIETF', 'PHARMABEES', 'ENERGY',
    'MOM30IETF', 'FMCGIETF', 'METAL', 'BFSI', 'ICICIB22',
    'HNGSNGBEES', 'AUTOBEES', 'MOREALTY', 'MOMENTUM50', 'ALPL30IETF',
    'SENSEXIETF', 'OILIETF', 'INFRAIETF', 'GROWWPOWER', 'MIDSMALL',
    'CONSUMBEES', 'LOWVOLIETF', 'HEALTHIETF', 'EVINDIA', 'CHEMICAL',
    'TNIDETF', 'MNC', 'MAKEINDIA', 'ECAPINSURE', 'MOTOUR',
    'MSCIINDIA', 'ESG'
]


DEFAULT_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'bhavcopy'))


def parse_date_from_filename(path: str) -> Optional[datetime]:
    base = os.path.basename(path)
    m = re.search(r"sec_bhavdata_full_(\d{8})\.csv", base)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d")
    except Exception:
        return None


def discover_files(data_dir: str, years: List[str]) -> List[str]:
    files: List[str] = []
    for y in years:
        pattern = os.path.join(data_dir, y, 'sec_bhavdata_full_*.csv')
        files.extend(glob.glob(pattern))
    # keep only files that parse
    files = [f for f in files if parse_date_from_filename(f) is not None]
    files.sort()
    return files


def read_and_normalize(path: str) -> Optional[pd.DataFrame]:
    date = parse_date_from_filename(path)
    if date is None:
        return None
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception as e:
        print(f"Warning: failed to read {path}: {e}")
        return None

    df.columns = [c.strip().upper() for c in df.columns]
    if 'SYMBOL' not in df.columns:
        print(f"Warning: {path} missing SYMBOL column; skipping")
        return None

    close_col = None
    for cand in ('CLOSE_PRICE', 'CLOSE', 'LAST_PRICE', 'CLOSEP'):
        if cand in df.columns:
            close_col = cand
            break
    if close_col is None:
        print(f"Warning: {path} missing CLOSE column; skipping")
        return None

    open_col = None
    for cand in ('OPEN_PRICE', 'OPEN'):
        if cand in df.columns:
            open_col = cand
            break

    out = pd.DataFrame()
    out['SYMBOL'] = df['SYMBOL'].astype(str).str.strip()
    out['OPEN_PRICE'] = pd.to_numeric(df[open_col], errors='coerce') if open_col else pd.NA
    out['CLOSE_PRICE'] = pd.to_numeric(df[close_col], errors='coerce')
    out['DATE'] = pd.to_datetime(date)
    return out


def load_recent_days(data_dir: str, years: List[str], max_trading_days: int = 350) -> Tuple[pd.DataFrame, List[datetime]]:
    files = discover_files(data_dir, years)
    if not files:
        raise FileNotFoundError(f"No bhavcopy files found in {data_dir} for years {years}")

    parts: List[pd.DataFrame] = []
    file_dates: List[datetime] = []
    for f in files:
        d = parse_date_from_filename(f)
        if d is None:
            continue
        file_dates.append(d)
    file_dates = sorted(set(file_dates))
    if not file_dates:
        raise RuntimeError("No valid bhavcopy dates found")

    recent_dates = file_dates[-max_trading_days:]

    # read only files whose date is in recent_dates
    for f in files:
        d = parse_date_from_filename(f)
        if d is None:
            continue
        if d not in recent_dates:
            continue
        df = read_and_normalize(f)
        if df is None:
            continue
        parts.append(df)

    if not parts:
        raise RuntimeError("No bhavcopy data read for recent dates")

    all_df = pd.concat(parts, ignore_index=True)
    all_df['DATE'] = pd.to_datetime(all_df['DATE'])
    all_df = all_df.sort_values(['DATE', 'SYMBOL']).reset_index(drop=True)
    return all_df, recent_dates


def compute_strategy(
    all_df: pd.DataFrame,
    recent_dates: List[datetime],
    etf_list: List[str],
    lookback_months: int = 1,
    sma_period: int = 200,
) -> Tuple[pd.DataFrame, List[str]]:
    """Return ranked DataFrame and warnings list.

    Lookback days = months * 21. For lookback_months=1, price_n = 21st row
    in descending-sorted per-symbol series → index lookback_days-1.
    """
    warnings: List[str] = []
    lookback_days = int(lookback_months * 21)

    # prepare calendar
    dates_sorted = sorted(set(all_df['DATE'].dt.normalize()))
    if not dates_sorted:
        raise RuntimeError("No trading dates available in data")
    start_date = dates_sorted[0]
    end_date = dates_sorted[-1]

    df_etf = all_df[all_df['SYMBOL'].isin(etf_list)].copy()
    if df_etf.empty:
        raise RuntimeError("No ETF rows found in the data for provided universe")

    results = []

    # Group per symbol and sort descending
    for sym, g in df_etf.groupby('SYMBOL'):
        sym_df = g.sort_values('DATE', ascending=False).reset_index(drop=True)
        close = sym_df['CLOSE_PRICE'].astype(float).reset_index(drop=True)

        if len(close) < sma_period:
            warnings.append(f"{sym}: insufficient history (<{sma_period}) to compute SMA{str(sma_period)}; ignored")
            continue

        latest_close = float(close.iloc[0])
        sma200 = float(close.iloc[:sma_period].mean())

        if latest_close < sma200:
            warnings.append(f"{sym}: latest close {latest_close:.2f} < SMA{sma_period} {sma200:.2f}; excluded by trend filter")
            continue

        # Ensure we have the lookback row
        if len(close) < lookback_days:
            warnings.append(f"{sym}: insufficient bars ({len(close)}) for lookback {lookback_days} days; ignored")
            continue

        # Use 1-based 21st row → index lookback_days-1 (0-based)
        price_n = float(close.iloc[lookback_days - 1])
        if price_n == 0 or pd.isna(price_n):
            warnings.append(f"{sym}: invalid lookback price ({price_n}) at index {lookback_days - 1}; ignored")
            continue

        roc = (latest_close / price_n - 1.0) * 100.0

        results.append({
            'ETF': sym,
            'ROC': roc,
            'LastPrice': latest_close,
            'SMA200': sma200,
            'HistoryDays': len(close),
        })

    if not results:
        raise RuntimeError("No ETFs qualified after applying filters; see warnings")

    res_df = pd.DataFrame(results)
    res_df = res_df.sort_values('ROC', ascending=False).reset_index(drop=True)
    res_df.index = res_df.index + 1
    res_df.insert(0, 'Rank', res_df.index)

    # Add meta warnings for start/end dates
    meta = [f"StartDate={start_date.date()}", f"EndDate={end_date.date()}"]
    return res_df, warnings + meta


def print_results(res_df: pd.DataFrame, lookback_months: int, warnings: List[str], top_n: Optional[int] = None) -> None:
    # Separate meta
    meta = [w for w in warnings if w.startswith('StartDate=') or w.startswith('EndDate=')]
    issues = [w for w in warnings if w not in meta]

    if meta:
        print('Data range: ' + ' | '.join(meta))

    if issues:
        print('\nIgnored / issues:')
        for w in issues:
            print(' -', w)

    print()
    display = res_df.copy()
    if top_n and top_n > 0:
        display = display.head(top_n)

    display_table = display[['Rank', 'ETF']].copy()
    display_table['Lookback'] = f"{lookback_months}M"
    display_table['ROC (%)'] = display['ROC'].map(lambda x: f"{x:.2f}")
    display_table['Last Price'] = display['LastPrice'].map(lambda x: f"{x:.2f}")
    display_table['SMA 200'] = display['SMA200'].map(lambda x: f"{x:.2f}")

    print(tabulate(display_table, headers='keys', tablefmt='rounded_grid', showindex=False))


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description='ETF rotation using local bhavcopy CSVs')
    p.add_argument('--data-dir', default=DEFAULT_DATA_DIR, help='Root data/bhavcopy directory')
    p.add_argument('--years', nargs='+', default=['2025', '2026'], help='Years folders to include')
    p.add_argument('--lookback', type=int, default=1, help='Lookback in months (1M ≈ 21 trading days)')
    p.add_argument('--top', type=int, default=0, help='Show top N ETFs (0 = all)')
    args = p.parse_args(argv)

    try:
        all_df, recent_dates = load_recent_days(args.data_dir, args.years, max_trading_days=350)
    except Exception as e:
        print(f"Error loading bhavcopy data: {e}")
        return 2

    try:
        top_n = args.top if args.top > 0 else None
        res_df, warnings = compute_strategy(all_df, recent_dates, ETF_LIST, lookback_months=args.lookback)
    except Exception as e:
        print(f"Error computing strategy: {e}")
        return 3

    print_results(res_df, args.lookback, warnings, top_n=top_n)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
