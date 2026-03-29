# -------------------------------------------------------------------------
#              Most Liquid ETF by Category Finder
# -------------------------------------------------------------------------
# This script fetches the latest list of ETFs from the NSE website, 
# filters out non-equity based ETFs, and identifies the most liquid ETF 
# in each category based on trading volume. The resulting mapping of categories 
# to their most liquid ETFs is printed and saved to a CSV file for easy access.
# CONTACT: rbhushan03@gmail.com
# ------------------------------------------------------------------------

import os
import pandas as pd
import requests


pd.set_option("display.max_rows", None, "display.max_columns", None)

# Function that fetches ETF master from NSE website and filters only equity based etfs

df = pd.DataFrame()
try:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36',
        'Upgrade-Insecure-Requests': "1",
        "DNT": "1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*,q=0.8",
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    }

    session = requests.Session()
    session.get("http://nseindia.com", headers=headers)
    cookies = session.cookies.get_dict()

    # print("Starting to download ETF List from NSE")

    ref_url = 'https://www.nseindia.com/market-data/exchange-traded-funds-etf'
    ref = requests.get(ref_url, headers=headers)
    url = 'https://www.nseindia.com/api/etf'
    response = session.get(url, headers=headers, cookies=ref.cookies.get_dict())
    data = response.json()  # Convert response to JSON
    # Convert JSON data to a DataFrame
    df = pd.DataFrame(data['data'])  # Extract the main data list

    if not df.empty:
        df = df[['symbol', 'assets', 'open', 'high', 'low', 'ltP', 'qty']]
        df.columns = ['ETF', 'Underlying', 'Open', 'High', 'Low', 'Close', 'Volume']

except Exception as e:
    print("Error while extracting ETF List from NSE")
    print(e)


# Process the downloaded NSE file
try:

    if not df.empty:
        df = df[df['Volume'] != '-']  # remove invalid rows from datasets
        # ensure numeric conversion (invalid parsing becomes NaN)
        df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')

        # create new column for Weigh / Turnover
        df['Weight'] = df['Close'] * df['Volume']
        # Filter #1 : Filter all etfs that have less than 10K volume
        df = df[df['Volume'] >= 10000]

        # Filter #2 : Remove all debt/ipo related ETFs
        filter_string = [
            'Bond', 'GSEC', 'G-Sec', 'GILT', 'LIQ', 'Liquid', 'IPO'  # Remove debt and other non-equity based etfs from list
        ]
        for scanString in filter_string:
            df = df[~df.Underlying.str.contains('|'.join([scanString]))]
            df = df[~df.ETF.str.contains('|'.join([scanString]))]

        # Filter #3 : Remove one-off / unwanted etfs from list
        filter_etfs = [
            'GROWWNET', 'INTERNET', 'SHARIABEES', 'GROWWNXT50'
        ]
        df = df[~df['ETF'].isin(filter_etfs)]

        # Cross-reference dictionary (search string -> value)
        cross_ref = {
            "Midcap 150": "MIDCAP",
            "Nifty 100": "NIFTY 100",
            "MIDCAP 100": "MIDCAP",
            "Nifty 200": "NIFTY 200",
            "Nifty 500": "NIFTY 500",
            "Nifty 50": "NIFTY 50",
            "Silver": "SILVER",
            "Gold": "GOLD",
            "S&P": "S&P",
            "Next 50": "NIFTY NEXT 50",
            "Junior": "NIFTY NEXT 50",
            "Nifty50": "NIFTY 50",
            "Nifty200": "NIFTY 200",
            "Nifty Total Market Index": "NIFTY 500",
            "Private Bank": "PRIVATE BANK",
            "Oil & Gas": "OIL & GAS",
            "PSU": "PSU",
            "Metal": "METAL",
            "MNC": "MNC",
            "Consumption": "CONSUMPTION",
            "Healthcare": "HEALTHCARE",
            "Financial Services": "FIN SERVICES",
            "FMCG": "FMCG",
            "EV": "EV",
            "Nifty Bank": "BANK NIFTY",
            "Auto": "AUTO",
            "Low-Volatility 30 ": "LOW VOL 30",
            "Momentum 50": "MOMENTUM 50",
            "Nasdaq": "NASDAQ",
            "NYSE": "NASDAQ",
            "Alpha 50": "NIFTY 50",
            "Smallcap 250": "SMALLCAP",
            "Realty": "REALTY",
            "MidSmallcap400": "MID SMALL CAP",
            "Low Vol 30": "LOW VOL 30",
            "Infrastructure": "INFRA",
            "Hang Seng": "HANG SENG",
            "Midcap 150": "MIDCAP",
            "CPSE": "PSU",
            "PSE": "PSU",
            "Commodity": "GOLD",
            "Power": "POWER",
            "Defence": "DEFENCE",
            "Pharma": "PHARMA",
            " IT": "IT",
            "SENSEX": "SENSEX",
            "Nifty Top 10 Equal Weight": "NIFTY 50",
            "Insurance": "INSURANCE",
            "Manufacturing": "MANUFACTURING",
            "Digital": "DIGITAL",
            "Capital Market": "NIFTY 500",
            "NIFTY Growth": "NIFTY 500",
            "50 TRI": "NIFTY 50",
            "Nifty Top 15 Equal Weight": "NIFTY 50",
            "BSE 200": "NIFTY 200",
            "ESG": "ESG",
            "Commodities": "GOLD",
            "Infra": "INFRA",
            "NIFTY100": "NIFTY 100",
            "Tourism": "TOURISM",
            "Midcap 50": "MIDCAP",
            "Top 20 Equal Weight": "NIFTY 50",
            "MSCI": "LARGE MID CAP",
            "Nifty Smallcap 100 Index": "SMALLCAP",
            "Nifty Index 50 Index": "NIFTY 50",
            "Nifty Chemicals Index (TRI)": "CHEMICAL",
            "Nifty Chemicals Index - TRI": "CHEMICAL",
            "Nifty Energy": "ENERGY",
            "Nifty Energy Index": "ENERGY"
        }

        # Function to map values
        def get_benchmark(underlying):
            for search_str, value in cross_ref.items():
                if search_str.upper() in underlying.upper():  # substring match
                    return value
            print("No associated Category found for underlying. Default value-Unknown-assigned : ", underlying)
            # return underlying  # fallback to original value if no match
            return 'Unknown'  # If no match found, mark as 'Unknown', later remove from list

        # Apply function to create Category column
        df["Category"] = df["Underlying"].apply(get_benchmark)

        # Remove ETFs that do not have a valid Category
        df = df[~df['Category'].isin(['Unknown'])]

        # Retain only the top  most ETFs in each underlying category based on highest weight
        df = df.sort_values(['Category', 'Weight'], ascending=False).groupby('Category').head(1)
        df.sort_values(by="Weight", ascending=False, inplace=True)
        df.reset_index(inplace=True, drop=True)

        # Retain only the top 30 ETFs from the final list based on highest weight / turnover
        # df = df.head(35)
        print(df)
        eligible_etfs = df["ETF"].tolist()
        print(eligible_etfs)

        # Create a simple Category | ETF Symbol mapping and print/save it
        result_df = df[['Category', 'ETF']].copy()
        result_df = result_df.rename(columns={'ETF': 'ETF Symbol'})

        # Print in requested format
        print('\nCategory | ETF Symbol')
        print(result_df.to_string(index=False))

        # Save mapping next to this script for easy access
        out_file = os.path.join(os.path.dirname(__file__), 'eligible_etfs_by_category.csv')
        try:
            result_df.to_csv(out_file, index=False)
            print(f"\nSaved Category->ETF mapping to: {out_file}")
        except Exception as e:
            print("Failed to save mapping to CSV:", e)
    else:
        print("Extracted NSE ETF File is empty")
except Exception as e:
    print("Error while processing NSE ETF file")
    print(e)