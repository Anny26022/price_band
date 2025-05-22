import pandas as pd
import requests
import io
from datetime import datetime, timedelta
import pytz
import sys

def get_latest_nse_csv_url():
    """
    Attempts to fetch the latest available NSE securities CSV from the past 7 days.
    Skips weekends and handles session headers and cookie setup.
    Returns: (url, raw_csv_text, used_date) or (None, None, None) if not found.
    """
    IST = pytz.timezone('Asia/Kolkata')
    today_ist = datetime.now(IST)
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept": "text/csv,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.nseindia.com/",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    }
    session.headers.update(headers)
    for url in ["https://www.nseindia.com/", "https://beta.nseindia.com/"]:
        try:
            session.get(url, timeout=10)
        except Exception:
            continue

    for i in range(7):
        check_date = today_ist - timedelta(days=i)
        if check_date.weekday() >= 5:  # Skip weekends
            continue
        date_str = check_date.strftime("%d%m%Y")
        url = f"https://nsearchives.nseindia.com/content/equities/sec_list_{date_str}.csv"
        try:
            resp = session.get(url, timeout=10)
            if resp.status_code == 200 and len(resp.text) > 1000:
                return url, resp.text, check_date
        except Exception:
            continue
    return None, None, None

def fetch_nse_price_bands_df():
    """
    Fetches and parses the most recent NSE securities list into a DataFrame.
    Returns: (DataFrame, raw_csv_text, used_date)
    """
    url, raw_csv, used_date = get_latest_nse_csv_url()
    if url is None or raw_csv is None:
        raise Exception("No NSE securities list file found in the last 7 days.")
    df = pd.read_csv(io.StringIO(raw_csv))
    df.columns = [c.strip().upper() for c in df.columns]
    return df, raw_csv, used_date

if __name__ == "__main__":
    try:
        df, raw_csv, used_date = fetch_nse_price_bands_df()
        print(f"Data for: {used_date.strftime('%Y-%m-%d')}")
        print(df.head())
        date_str = used_date.strftime("%d%m%Y")
        df.to_csv(f"{date_str}.csv", index=False)
        sys.exit(0)
    except Exception as e:
        print(f"No new CSV found: {e}")
        sys.exit(1) 