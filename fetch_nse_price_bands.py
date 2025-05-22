import pandas as pd
import httpx
import io
from datetime import datetime, timedelta
import pytz
import sys
from typing import Tuple, Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

def get_latest_nse_csv_url() -> Tuple[Optional[str], Optional[str], Optional[datetime]]:
    """
    Attempts to fetch the latest available NSE securities CSV from the past 7 days.
    Skips weekends and handles session headers and cookie setup.
    Returns: (url, raw_csv_text, used_date) or (None, None, None) if not found.
    """
    IST = pytz.timezone('Asia/Kolkata')
    today_ist = datetime.now(IST)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept": "text/csv,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.nseindia.com/",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    }
    with httpx.Client(headers=headers, timeout=10, follow_redirects=True) as client:
        for url in ["https://www.nseindia.com/", "https://beta.nseindia.com/"]:
            try:
                logging.info(f"Priming session with {url}")
                client.get(url)
            except Exception as e:
                logging.warning(f"Failed to prime session with {url}: {e}")
                continue

        for i in range(7):
            check_date = today_ist - timedelta(days=i)
            if check_date.weekday() >= 5:  # Skip weekends
                logging.info(f"Skipping weekend: {check_date.strftime('%A, %d-%m-%Y')}")
                continue
            date_str = check_date.strftime("%d%m%Y")
            url = f"https://nsearchives.nseindia.com/content/equities/sec_list_{date_str}.csv"
            logging.info(f"Trying URL: {url}")
            try:
                resp = client.get(url)
                logging.info(f"Response status: {resp.status_code}, Content length: {len(resp.text)}")
                if resp.status_code == 200 and len(resp.text) > 1000:
                    logging.info(f"Found valid CSV for date: {check_date.strftime('%d-%m-%Y')}")
                    return url, resp.text, check_date
                else:
                    logging.info(f"No valid CSV at {url}")
            except Exception as e:
                logging.error(f"Error fetching {url}: {e}")
                continue
    logging.error("No NSE securities list file found in the last 7 days.")
    return None, None, None

def fetch_nse_price_bands_df() -> Tuple[pd.DataFrame, str, datetime]:
    """
    Fetches and parses the most recent NSE securities list into a DataFrame.
    Returns: (DataFrame, raw_csv_text, used_date)
    """
    url, raw_csv, used_date = get_latest_nse_csv_url()
    if url is None or raw_csv is None:
        raise Exception("No NSE securities list file found in the last 7 days.")
    try:
        df = pd.read_csv(io.StringIO(raw_csv))
        df.columns = [c.strip().upper() for c in df.columns]
        logging.info(f"CSV loaded into DataFrame with {len(df)} rows and {len(df.columns)} columns.")
        return df, raw_csv, used_date
    except Exception as e:
        logging.error(f"Error parsing CSV: {e}")
        raise

if __name__ == "__main__":
    try:
        df, raw_csv, used_date = fetch_nse_price_bands_df()
        logging.info(f"Data for: {used_date.strftime('%Y-%m-%d')}")
        print(df.head())
        date_str = used_date.strftime("%d%m%Y")
        df.to_csv(f"{date_str}.csv", index=False)
        logging.info(f"CSV saved as {date_str}.csv")
        sys.exit(0)
    except Exception as e:
        logging.error(f"No new CSV found: {e}")
        sys.exit(1) 
