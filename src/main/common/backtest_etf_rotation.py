#!/usr/bin/env python3
"""Backtest ETF Rotation Strategy (monthly) using local bhavcopy files.

Rules implemented:
 - Monthly execution: buy on first trading day OPEN, sell on last trading day CLOSE
 - Signals computed using data up to previous trading day (no lookahead)
 - Selection: ETFs must pass 200-day SMA, ROC > 0, pick top 5 by ROC
 - Equal-weight allocation across selected ETFs; initial capital 500000

Outputs:
 - Summary performance printed
 - CSV of transactions and monthly/yearly performance

Usage:
    python backtest_etf_rotation.py --data-dir <data/bhavcopy> --years 2024 2025 2026
"""
from __future__ import annotations

import argparse
import importlib.util
import math
import os
from collections import defaultdict
from datetime import datetime
from typing import List, Tuple

import numpy as np
import pandas as pd
from tabulate import tabulate


def load_etf_universe_from_module(module_path: str) -> List[str]:
    """Dynamically import etf_rotation_strategy and return ETF_LIST."""
    spec = importlib.util.spec_from_file_location("etf_rotation_module", module_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return getattr(mod, 'ETF_LIST')


def load_bhavcopy_all(data_dir: str, years: List[str]) -> pd.DataFrame:
    """Read all bhavcopy CSVs for given years into a single DataFrame with DATE parsed from filename."""
    files = []
    for y in years:
        pattern = os.path.join(data_dir, y, 'sec_bhavdata_full_*.csv')
        files.extend(sorted([f for f in __import__('glob').glob(pattern)]))

    if not files:
        raise FileNotFoundError(f"No files found in {data_dir} for years {years}")

    parts = []
    for f in files:
        # parse date from filename
        m = __import__('re').search(r"sec_bhavdata_full_(\d{8})\.csv", os.path.basename(f))
        if not m:
            continue
        dt = datetime.strptime(m.group(1), "%Y%m%d")
        try:
            df = pd.read_csv(f, dtype=str)
        except Exception:
            continue

        df.columns = [c.strip().upper() for c in df.columns]
        if 'SYMBOL' not in df.columns:
            continue
        # determine close and open column names
        close_col = None
        for cand in ('CLOSE_PRICE', 'CLOSE', 'LAST_PRICE', 'CLOSEP'):
            if cand in df.columns:
                close_col = cand
                break
        if close_col is None:
            continue
        open_col = None
        for cand in ('OPEN_PRICE', 'OPEN'):
            if cand in df.columns:
                open_col = cand
                break

        out = pd.DataFrame()
        out['SYMBOL'] = df['SYMBOL'].astype(str).str.strip()
        out['OPEN_PRICE'] = pd.to_numeric(df[open_col], errors='coerce') if open_col else pd.NA
        out['CLOSE_PRICE'] = pd.to_numeric(df[close_col], errors='coerce')
        out['DATE'] = pd.to_datetime(dt)
        parts.append(out)

    if not parts:
        raise RuntimeError('No bhavcopy data read')

    all_df = pd.concat(parts, ignore_index=True)
    all_df['DATE'] = pd.to_datetime(all_df['DATE'])
    all_df = all_df.sort_values(['DATE', 'SYMBOL']).reset_index(drop=True)
    return all_df


def get_month_calendar(dates: List[pd.Timestamp]) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Return list of (first_trading_day, last_trading_day) for each month present in dates (sorted)."""
    df = pd.DataFrame({'DATE': pd.to_datetime(dates)})
    df['YEAR'] = df['DATE'].dt.year
    df['MONTH'] = df['DATE'].dt.month
    groups = df.groupby(['YEAR', 'MONTH'])['DATE']
    months = []
    for (y, m), g in groups:
        dates_sorted = sorted(g.tolist())
        months.append((dates_sorted[0], dates_sorted[-1]))
    months.sort()
    return months


def generate_signals_for_date(all_df: pd.DataFrame, as_of_date: pd.Timestamp, etf_list: List[str], lookback_months: int = 1) -> pd.DataFrame:
    """Generate ETF signals using data up to as_of_date (inclusive).

    Returns DataFrame of ETFs passing SMA200 and ROC with ROC value.
    """

    lookback_days = lookback_months * 21
    results = []
    df_hist = all_df[all_df['DATE'] <= as_of_date].copy()
    # group by symbol and compute
    for sym, g in df_hist.groupby('SYMBOL'):
        if sym not in etf_list:
            continue
        series = g.sort_values('DATE', ascending=False)['CLOSE_PRICE'].astype(float).reset_index(drop=True)
        if len(series) < 200:
            continue
        latest_close = float(series.iloc[0])
        sma200 = float(series.iloc[:200].mean())
        if latest_close < sma200:
            continue
        if len(series) < lookback_days:
            continue
        price_n = float(series.iloc[lookback_days - 1])
        if price_n == 0 or pd.isna(price_n):
            continue
        roc = (latest_close / price_n - 1.0) * 100.0
        if roc <= 0:
            continue
        results.append({'ETF': sym, 'ROC': roc})

    if not results:
        return pd.DataFrame(columns=['ETF', 'ROC'])
    res_df = pd.DataFrame(results).sort_values('ROC', ascending=False).reset_index(drop=True)
    return res_df


def run_backtest(data_dir: str, years: List[str], lookback_months: int = 1, top_n: int = 5, initial_capital: float = 500000.0):
    # load ETF list from existing module (same folder)
    module_path = os.path.join(os.path.dirname(__file__), 'etf_rotation_strategy.py')
    ETF_LIST = load_etf_universe_from_module(module_path)

    all_df = load_bhavcopy_all(data_dir, years)
    unique_dates = sorted(all_df['DATE'].dt.normalize().unique())
    if not unique_dates:
        raise RuntimeError('No trading dates found in data')

    months = get_month_calendar(unique_dates)

    # Strategy start cutoff: ignore months before 1-Jan-2024 (no trading before this)
    strategy_start = datetime(2024, 1, 1)
    months = [(f, l) for (f, l) in months if pd.Timestamp(f) >= pd.Timestamp(strategy_start)]

    capital = float(initial_capital)
    equity_curve = []  # monthly end capital
    monthly_returns = []  # (year, month, return)
    transactions = []
    trade_stats = []  # per trade pnl

    first_trade_date = None
    last_trade_date = None

    for first_day, last_day in months:
        # capital at start of month
        capital_before = capital
        # decision uses data up to previous trading day before first_day
        idx = unique_dates.index(pd.Timestamp(first_day))
        if idx == 0:
            # no past data to make decision
            monthly_returns.append((first_day.year, first_day.month, 0.0))
            equity_curve.append(capital)
            continue
        prev_date = unique_dates[idx - 1]

        # generate signals using data up to prev_date
        signals = generate_signals_for_date(all_df, pd.Timestamp(prev_date), ETF_LIST, lookback_months=lookback_months)
        if signals.empty:
            # no investment this month
            monthly_returns.append((first_day.year, first_day.month, 0.0))
            equity_curve.append(capital)
            continue

        selected = signals.head(top_n)['ETF'].tolist()
        # get ENTRY open prices on first_day
        entry_prices = {}
        for etf in selected:
            row = all_df[(all_df['DATE'] == pd.Timestamp(first_day)) & (all_df['SYMBOL'] == etf)]
            if row.empty:
                # cannot enter this ETF
                continue
            entry_prices[etf] = float(row.iloc[0]['OPEN_PRICE'])

        if not entry_prices:
            monthly_returns.append((first_day.year, first_day.month, 0.0))
            equity_curve.append(capital)
            continue

        # allocate equally across available entry ETFs
        per_etf_cap = capital / len(entry_prices)
        positions = {}
        total_entry_cost = 0.0
        for etf, price in entry_prices.items():
            qty = math.floor(per_etf_cap / price) if price > 0 else 0
            if qty <= 0:
                continue
            cost = qty * price
            total_entry_cost += cost
            positions[etf] = {'qty': qty, 'entry_price': price}
            transactions.append({'Txn Date': first_day.date(), 'Txn Type': 'Buy', 'Symbol': etf, 'Qty': qty, 'Price': price, 'Profit': ''})

        # If no positions opened (e.g., prices too high), skip month
        if not positions:
            monthly_returns.append((first_day.year, first_day.month, 0.0))
            equity_curve.append(capital)
            continue

        # Compute exit on last_day at CLOSE price
        total_exit_proceeds = 0.0
        month_pnl = 0.0
        for etf, pos in positions.items():
            # find close on last_day; if missing, use most recent <= last_day
            row = all_df[(all_df['DATE'] == pd.Timestamp(last_day)) & (all_df['SYMBOL'] == etf)]
            exit_price = None
            if not row.empty and not pd.isna(row.iloc[0]['CLOSE_PRICE']):
                exit_price = float(row.iloc[0]['CLOSE_PRICE'])
            else:
                # find most recent price <= last_day
                hist = all_df[(all_df['SYMBOL'] == etf) & (all_df['DATE'] <= pd.Timestamp(last_day))].sort_values('DATE', ascending=False)
                if not hist.empty and not pd.isna(hist.iloc[0]['CLOSE_PRICE']):
                    exit_price = float(hist.iloc[0]['CLOSE_PRICE'])

            if exit_price is None:
                # cannot close; treat exit at zero (skip)
                continue

            qty = pos['qty']
            entry_price = pos['entry_price']
            proceeds = qty * exit_price
            total_exit_proceeds += proceeds
            pnl = (exit_price - entry_price) * qty
            month_pnl += pnl
            transactions.append({'Txn Date': last_day.date(), 'Txn Type': 'Sell', 'Symbol': etf, 'Qty': qty, 'Price': exit_price, 'Profit': pnl})
            trade_stats.append({'Symbol': etf, 'Entry': entry_price, 'Exit': exit_price, 'Qty': qty, 'PnL': pnl})

        # Update capital: capital - entry_costs + exit_proceeds
        # Note: we assumed capital was fully available; use actual entry costs
        capital = capital - total_entry_cost + total_exit_proceeds
        if first_trade_date is None and positions:
            first_trade_date = first_day
        if positions:
            last_trade_date = last_day

        # monthly return based on capital before and after trades
            monthly_ret = (capital / capital_before - 1.0) if capital_before != 0 else 0.0
            monthly_returns.append((first_day.year, first_day.month, monthly_ret))
            equity_curve.append(capital)

    # Build transactions DataFrame
    tx_df = pd.DataFrame(transactions)
    # Ensure Profit column numeric for sells; keep blank for buys
    if 'Profit' in tx_df.columns:
        tx_df['Profit'] = tx_df['Profit'].replace('', pd.NA)
    tx_df.to_csv('etf_rotation_transactions.csv', index=False)

    # Monthly performance table
    mf = pd.DataFrame(monthly_returns, columns=['Year', 'Month', 'Return'])
    mf['ReturnPct'] = mf['Return'] * 100.0
    # pivot table Year x Month
    perf_table = mf.pivot(index='Year', columns='Month', values='ReturnPct').fillna(0.0).sort_index()
    perf_table['YOY'] = perf_table.sum(axis=1)
    # Format to 2 decimals for CSV output
    perf_table_fmt = perf_table.round(2)
    perf_table_fmt.to_csv('etf_rotation_monthly_performance.csv')

    # Performance metrics
    starting_balance = initial_capital
    ending_balance = capital
    total_pnl = ending_balance - starting_balance
    total_pnl_pct = (ending_balance / starting_balance - 1.0) * 100.0

    # CAGR
    if first_trade_date is None or last_trade_date is None or first_trade_date == last_trade_date:
        cagr = 0.0
    else:
        years = (last_trade_date - first_trade_date).days / 365.25
        cagr = (ending_balance / starting_balance) ** (1.0 / years) - 1.0 if years > 0 else 0.0

    # Equity curve series for drawdown (use monthly equity_curve)
    eq = np.array(equity_curve)
    peaks = np.maximum.accumulate(eq)
    drawdowns = (peaks - eq)
    max_dd_amt = drawdowns.max() if len(drawdowns) else 0.0
    max_dd_pct = (max_dd_amt / peaks.max() * 100.0) if peaks.max() > 0 else 0.0

    # Trades stats
    trades = [t for t in trade_stats]
    num_trades = len(trades)
    wins = [t for t in trades if t['PnL'] > 0]
    losses = [t for t in trades if t['PnL'] <= 0]
    win_rate = (len(wins) / num_trades * 100.0) if num_trades else 0.0
    avg_win_pct = (np.mean([ (t['Exit']/t['Entry'] -1.0)*100.0 for t in wins ]) ) if wins else 0.0
    avg_loss_pct = (np.mean([ (t['Exit']/t['Entry'] -1.0)*100.0 for t in losses ]) ) if losses else 0.0
    avg_win_amt = np.mean([t['PnL'] for t in wins]) if wins else 0.0
    avg_loss_amt = np.mean([t['PnL'] for t in losses]) if losses else 0.0
    rr_ratio = (abs(avg_win_pct) / abs(avg_loss_pct)) if avg_loss_pct != 0 else float('inf')

    # Sharpe & Sortino (annualized) using monthly returns
    monthly_rets = mf['Return'].values if not mf.empty else np.array([])
    if monthly_rets.size > 1:
        ann_mean = np.mean(monthly_rets) * 12.0
        ann_std = np.std(monthly_rets, ddof=1) * (12.0 ** 0.5)
        sharpe = ann_mean / ann_std if ann_std != 0 else 0.0
        # sortino: downside deviation
        downside = monthly_rets[monthly_rets < 0]
        dd = np.std(downside, ddof=1) * (12.0 ** 0.5) if downside.size > 0 else 0.0
        sortino = ann_mean / dd if dd != 0 else 0.0
    else:
        sharpe = 0.0
        sortino = 0.0

    # Print summary
    print('\n=== Backtest Summary ===')
    print('Strategy: ETF Rotation Strategy')
    print('Start Date:', first_trade_date.date() if first_trade_date is not None else 'N/A')
    print('End Date:', last_trade_date.date() if last_trade_date is not None else 'N/A')
    print('Start Balance:', f"{starting_balance:.2f}")
    print('End Balance:', f"{ending_balance:.2f}")
    print('Total Net PnL:', f"{total_pnl:.2f} ({total_pnl_pct:.2f}%)")
    print('CAGR:', f"{cagr*100:.2f}%")
    print('Max Drawdown (%):', f"{max_dd_pct:.2f}%")
    print('Max Drawdown (Amount):', f"{max_dd_amt:.2f}")
    print('Number of Trades:', num_trades)
    print('Win Rate (%):', f"{win_rate:.2f}%")
    print('Average Win (%):', f"{avg_win_pct:.2f}%")
    print('Average Loss (%):', f"{avg_loss_pct:.2f}%")
    print('Average Win Amount:', f"{avg_win_amt:.2f}")
    print('Average Loss Amount:', f"{avg_loss_amt:.2f}")
    print('Risk/Reward Ratio:', f"{rr_ratio:.2f}")
    print('Sharpe Ratio:', f"{sharpe:.2f}")
    print('Sortino Ratio:', f"{sortino:.2f}")

    # Print monthly/yearly table nicely rounded to 2 decimals
    print('\nMonthly / Yearly Performance:')
    # Create display table with month names
    month_names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    disp = perf_table.copy()
    # ensure all months present
    for m in range(1,13):
        if m not in disp.columns:
            disp[m] = 0.0
    # Reorder columns 1..12 then YOY if present
    cols = list(range(1,13))
    if 'YOY' in disp.columns:
        cols.append('YOY')
    disp = disp[cols]
    # rename month number columns to month names
    rename_map = {i: month_names[i-1] for i in range(1,13)}
    if 'YOY' in disp.columns:
        rename_map['YOY'] = 'YOY'
    disp = disp.rename(columns=rename_map)
    disp = disp.round(2)
    print(tabulate(disp.reset_index(), headers='keys', tablefmt='rounded_grid', showindex=False))

    print('\nMonthly/Yearly performance saved to etf_rotation_monthly_performance.csv')
    print('Transactions saved to etf_rotation_transactions.csv')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Backtest ETF Rotation monthly')
    p.add_argument('--data-dir', default=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'bhavcopy')), help='Root data/bhavcopy')
    p.add_argument('--years', nargs='+', default=['2024', '2025', '2026'], help='Years to include')
    p.add_argument('--lookback', type=int, default=1, help='Lookback in months (1M ≈ 21 trading days)')
    p.add_argument('--top', type=int, default=5, help='Top N ETFs to select each month')
    p.add_argument('--capital', type=float, default=500000.0, help='Initial capital')
    args = p.parse_args()

    run_backtest(args.data_dir, args.years, lookback_months=args.lookback, top_n=args.top, initial_capital=args.capital)
