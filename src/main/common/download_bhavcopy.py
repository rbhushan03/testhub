# This script downloads the bhavcopy file from the specified URL and saves it locally.
# URL: https://archives.nseindia.com/products/content/sec_bhavdata_full_29092025.csv  (ddmmyyyy format)

import sys
import pandas as pd
import urllib.request
from datetime import datetime, timedelta
import time
import csv

'''
if len(sys.argv) < 2:
    print("Usage: python download_bhavcopy.py <date in yyyymmdd format>")
    sys.exit(1)

date_str = sys.argv[1]   #02122025 means 2nd Dec 2025
new_date = date_str[6:8] + date_str[4:6] + date_str[0:4]  #ddmmyyyy
print(new_date)

# Define variables
base_url = "https://archives.nseindia.com/products/content/sec_bhavdata_full_"
local_folder = "c:/Users/Ravi/workspace/testhub/"
base_folder = "assets/data/bhavcopy/2025/"

out_file = f"{local_folder}{base_folder}sec_bhavdata_full_{date_str}.csv"
print(out_file)

url = f"{base_url}{new_date}.csv"
print(f"Downloading bhavcopy of {new_date} from URL: {url}")

# Read tp csv file
try:
    df = pd.read_csv(url)
except Exception as e:
    print(f"Error downloading, data not found for {new_date}: {e}")
    sys.exit(1)

# Writing output file
df.to_csv(out_file, index=False)
print(f"Bhavcopy saved to {out_file}")
print(df.head())

# Last updated time
marker_file = f"{local_folder}assets/data/bhavcopy/Last_Updated_Date.txt"
with open(marker_file, 'w') as f:
    f.write(f"Last Updated Date: {date_str}\n")
    f.write(f"Timestamp: {time.ctime()}\n")
'''

# Define variables
base_url = "https://archives.nseindia.com/products/content/sec_bhavdata_full_"
local_folder = "c:/Users/Ravi/workspace/testhub/"
base_folder = "data/bhavcopy/2025/"

# Download data in loop for multiple dates (uncomment to use)
# start_date = datetime.date(2025, 12, 1)
# end_date = datetime.date(2025, 12, 10)
# delta = datetime.timedelta(days=1)    

start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 10, 24)   #datetime(2025, 10, 24)

for i in range((end_date - start_date).days + 1):
    date = start_date + timedelta(days=i)
    # weekday(): Monday=0, Tuesday=1, ..., Sunday=6
    if date.weekday() in (5, 6):   # 5 = Saturday, 6 = Sunday
        continue  # skip weekends
    print(date.strftime("%Y-%m-%d"))
    new_date = date.strftime("%d%m%Y")
    date_str = date.strftime("%Y%m%d")

    url = f"{base_url}{new_date}.csv"
    print(f"\nDownloading bhavcopy of {new_date} from URL: {url}")

    # Read tp csv file
    try:
        df = pd.read_csv(url)
    except Exception as e:
        print(f"Error downloading, data not found for {new_date}: {e}")
        continue

    out_file = f"{local_folder}{base_folder}sec_bhavdata_full_{date_str}.csv"
    #print(out_file)
    # Writing output file
    df.to_csv(out_file, index=False)
    print(f"Bhavcopy saved to {out_file}")
    #print(df.head())

    # Last updated time
    marker_file = f"{local_folder}data/bhavcopy/Last_Updated_Date.txt"
    with open(marker_file, 'w') as f:
        f.write(f"Last Updated Date: {date_str}\n")
        f.write(f"Timestamp: {time.ctime()}\n")

