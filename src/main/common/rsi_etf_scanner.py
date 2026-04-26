#!/usr/bin/env python3
r"""
RSI ETF Scanner

Objective:
- Read the latest available bhavcopy CSV file from a folder
- Load historical data (14+ trading days)
- Calculate RSI(14) for a predefined list of ETFs
- Return Top 10 ETFs with the lowest RSI values

Usage:
    python .\src\main\common\rsi_etf_scanner.py
    python .\src\main\common\rsi_etf_scanner.py --period 14 --top 10
    python .\src\main\common\rsi_etf_scanner.py --base-folder data/bhavcopy/2026/ --period 14 --top 10
"""

import os
import glob
import re
from datetime import datetime
import pandas as pd
import numpy as np
from tabulate import tabulate


ETF_LIST = [
    'ITBEES','SILVERBEES','GOLDBEES','PHARMABEES','METALIETF','NIFTYBEES','MOCAPITAL','GROWWPOWER','MODEFENCE','OILIETF',
    'PVTBANIETF','SMALLCAP','PSUBNKBEES','HDFCSML250','FMCGIETF','MON100','BSE500IETF','SETFNIF50','ABSLPSE','ENERGY',
    'CPSEETF','MOREALTY','SMALL250','HDFCMID150','ALPHA','MONIFTY500','MID150BEES','ALPHAETF','MAFANG','MOMENTUM50',
    'BANKBEES','HEALTHY','MIDSMALL','GROWWRAIL','JUNIORBEES','MOM100','MOENERGY','EVINDIA','MULTICAP','MNC',
    'MOMENTUM30','INTERNET','MASPTOP50','BFSI','AUTOBEES','MOVALUE','CHEMICAL','MAKEINDIA','MONQ50','MAHKTECH',
    'GROWWHOSPI','TNIDETF','HNGSNGBEES','CONSUMBEES','MOMNC','HDFCSENSEX','EMULTIMQ','QUAL30IETF','HDFCGROWTH',
    'COMMOIETF','ECAPINSURE','INFRA','SENSEXETF','ESG','NIFTYQLITY','MSCIINDIA','MOIPO','ABSLBANETF','MOTOUR'
]


def parse_date_from_filename(filename):
    """Extract date from filename format: sec_bhavdata_full_YYYYMMDD.csv"""
    match = re.search(r'sec_bhavdata_full_(\d{8})\.csv', filename)
    if match:
        date_str = match.group(1)
        return datetime.strptime(date_str, '%Y%m%d')
    return None


def find_latest_file(base_folder):
    """Find the latest bhavcopy file based on filename date."""
    pattern = os.path.join(base_folder, 'sec_bhavdata_full_*.csv')
    files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError(f"No bhavcopy files found in {base_folder}")
    
    # Sort by date from filename
    files_with_dates = []
    for f in files:
        date = parse_date_from_filename(os.path.basename(f))
        if date:
            files_with_dates.append((date, f))
    
    if not files_with_dates:
        raise FileNotFoundError("Could not parse dates from bhavcopy files")
    
    files_with_dates.sort(key=lambda x: x[0])
    latest_date, latest_file = files_with_dates[-1]
    
    return latest_file, latest_date


def load_historical_data(base_folder, num_days=50):
    """
    Load the latest N trading days of bhavcopy data.
    
    Args:
        base_folder: Path to folder containing bhavcopy CSVs
        num_days: Number of trading days to load (default 50 for proper RSI warmup)
    
    Returns:
        DataFrame with columns: SYMBOL, CLOSE, DATE
    """
    pattern = os.path.join(base_folder, 'sec_bhavdata_full_*.csv')
    files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError(f"No bhavcopy files found in {base_folder}")
    
    # Sort by date from filename
    files_with_dates = []
    for f in files:
        date = parse_date_from_filename(os.path.basename(f))
        if date:
            files_with_dates.append((date, f))
    
    files_with_dates.sort(key=lambda x: x[0])
    
    # Take the last N files
    latest_files = files_with_dates[-num_days:]
    
    print(f"Loading {len(latest_files)} files from {latest_files[0][0].date()} to {latest_files[-1][0].date()}")
    
    dfs = []
    for date, filepath in latest_files:
        try:
            df = pd.read_csv(filepath)
            # Normalize column names
            df.columns = [col.strip().upper() for col in df.columns]
            
            # Extract required columns
            if 'SYMBOL' not in df.columns:
                print(f"  Warning: SYMBOL column not found in {os.path.basename(filepath)}")
                continue
            
            # Find close price column (handle variants)
            close_col = None
            for candidate in ['CLOSE_PRICE', 'CLOSE', 'LAST_PRICE', 'CLOSEP']:
                if candidate in df.columns:
                    close_col = candidate
                    break
            
            if close_col is None:
                print(f"  Warning: No close price column found in {os.path.basename(filepath)}")
                continue
            
            # Build output dataframe with normalized columns
            out_df = pd.DataFrame()
            out_df['SYMBOL'] = df['SYMBOL'].astype(str).str.strip()
            out_df['CLOSE'] = pd.to_numeric(df[close_col], errors='coerce')
            out_df['DATE'] = pd.to_datetime(date)
            
            dfs.append(out_df)
        except Exception as e:
            print(f"  Error reading {os.path.basename(filepath)}: {e}")
            continue
    
    if not dfs:
        raise RuntimeError("No valid data loaded from bhavcopy files")
    
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df = combined_df.dropna(subset=['CLOSE'])
    combined_df = combined_df.sort_values(['SYMBOL', 'DATE']).reset_index(drop=True)
    
    return combined_df


def calculate_rsi(prices, period=14):
    """
    Calculate RSI using Classic Wilder's method (SMA + Smoothing).
    This matches TradingView's RSI calculation.
    
    Args:
        prices: pd.Series of prices (must have at least period+1 values)
        period: RSI period (default 14)
    
    Returns:
        RSI value or NaN if insufficient data
    """
    if len(prices) < period + 1:
        return np.nan
    
    # Calculate price changes
    deltas = prices.diff()
    
    # Separate gains and losses
    gains = deltas.where(deltas > 0, 0.0)
    losses = -deltas.where(deltas < 0, 0.0)
    
    # First average gain/loss (SMA of first 'period' values)
    first_avg_gain = gains.iloc[1:period+1].mean()
    first_avg_loss = losses.iloc[1:period+1].mean()
    
    # Initialize with NaN for first 'period' bars
    avg_gains = [np.nan] * period
    avg_losses = [np.nan] * period
    
    # Set first average at position period-1
    avg_gains[period-1] = first_avg_gain
    avg_losses[period-1] = first_avg_loss
    
    # Subsequent averages use Wilder's smoothing: (prior_avg * (period-1) + current) / period
    for i in range(period, len(gains)):
        avg_gains.append(
            (avg_gains[i-1] * (period - 1) + gains.iloc[i]) / period
        )
        avg_losses.append(
            (avg_losses[i-1] * (period - 1) + losses.iloc[i]) / period
        )
    
    # Calculate RS and RSI
    avg_gains = np.array(avg_gains)
    avg_losses = np.array(avg_losses)
    
    # Avoid division by zero
    rs = np.where(avg_losses != 0, avg_gains / avg_losses, 0)
    rsi = 100 - (100 / (1 + rs))
    
    # Return the last RSI value
    return float(rsi[-1]) if not np.isnan(rsi[-1]) else np.nan


def scan_etfs(data_df, etf_list, period=14):
    """
    Calculate RSI for each ETF in the list using the latest data.
    
    Args:
        data_df: DataFrame with SYMBOL, CLOSE, DATE
        etf_list: List of ETF symbols to scan
        period: RSI period (default 14)
    
    Returns:
        DataFrame with ETF, Close, RSI sorted by RSI ascending
    """
    results = []
    
    for symbol in etf_list:
        # Filter data for this symbol
        symbol_data = data_df[data_df['SYMBOL'] == symbol].sort_values('DATE')
        
        if symbol_data.empty:
            # ETF not found in data
            continue
        
        prices = symbol_data['CLOSE'].values
        
        # Check if we have enough data
        if len(prices) < period + 1:
            # Not enough data for RSI calculation
            continue
        
        # Get latest close
        latest_close = prices[-1]
        
        # Calculate RSI
        rsi_value = calculate_rsi(pd.Series(prices), period=period)
        
        if not np.isnan(rsi_value):
            results.append({
                'ETF': symbol,
                'Close': round(latest_close, 2),
                'RSI(14)': round(rsi_value, 1)
            })
    
    # Convert to DataFrame and sort by RSI ascending
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        return result_df
    
    result_df = result_df.sort_values('RSI(14)').reset_index(drop=True)
    result_df.insert(0, 'Rank', range(1, len(result_df) + 1))
    
    return result_df


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='RSI ETF Scanner')
    parser.add_argument('--base-folder', default='data/bhavcopy/2026/', 
                       help='Base folder containing bhavcopy CSVs (default: data/bhavcopy/2026/)')
    parser.add_argument('--period', type=int, default=14, 
                       help='RSI period (default: 14)')
    parser.add_argument('--top', type=int, default=10, 
                       help='Number of top ETFs to return (default: 10)')
    
    args = parser.parse_args()
    
    try:
        # Load historical data
        print(f"\nScanning ETFs from {args.base_folder}")
        print(f"Using RSI period: {args.period}\n")
        
        data_df = load_historical_data(args.base_folder, num_days=50)
        
        # Scan ETFs
        results_df = scan_etfs(data_df, ETF_LIST, period=args.period)
        
        if results_df.empty:
            print("No ETFs found with sufficient data for RSI calculation")
            return
        
        # Display top N
        top_n = min(args.top, len(results_df))
        print(f"\nTop {top_n} ETFs with Lowest RSI({args.period}):\n")
        
        # Format output as bordered table
        display_df = results_df.head(top_n).copy()
        table_data = display_df.values.tolist()
        headers = display_df.columns.tolist()
        
        print(tabulate(table_data, headers=headers, tablefmt='grid', floatfmt='.1f'))
        
        print(f"\n(Scanned {len(results_df)} ETFs total with sufficient data)")
    
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
