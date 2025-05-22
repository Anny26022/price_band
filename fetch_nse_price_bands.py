import pandas as pd
import requests
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

SCRAPER_API_KEY = "492fed55ee317f3d46a5336e5bda77b8"  # your ScraperAPI key

def get_latest_nse_csv_url() -> Tuple[Optional[str], Optional[str], Optional[datetime]]:
    """
    Fetches the latest available NSE securities CSV via ScraperAPI.
    Skips weekends and returns first valid CSV found in last 7 days.
    Returns: (url, raw_csv_text, used_date) or (None, None, None) if not found.
    """
    IST = pytz.timezone('Asia/Kolkata')
    today_ist = datetime.now(IST)

    for i in range(7):
        check_date = today_ist - timedelta(days=i)
        if check_date.weekday() >= 5:  # Skip weekends
            logging.info(f"Skipping weekend: {check_date.strftime('%A, %d-%m-%Y')}")
            continue
        date_str = check_date.strftime("%d%m%Y")
        url = f"https://nsearchives.nseindia.com/content/equities/sec_list_{date_str}.csv"
        logging.info(f"Trying URL via ScraperAPI: {url}")

        payload = {
            "api_key": SCRAPER_API_KEY,
            "url": url,
            "country_code": "in",
            "device_type": "desktop",
            "keep_headers": "true"
        }

        try:
            r = requests.get("https://api.scraperapi.com/", params=payload, timeout=30)
            logging.info(f"Response status: {r.status_code}, Content length: {len(r.text)}")
            if r.status_code == 200 and len(r.text) > 1000:
                logging.info(f"Valid CSV found for date: {check_date.strftime('%d-%m-%Y')}")
                return url, r.text, check_date
        except Exception as e:
            logging.error(f"Error fetching CSV via ScraperAPI for {url}: {e}")
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
