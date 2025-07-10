import logging
import requests
import itertools
import os
import json
import time
from urllib.parse import urlencode

LOG = logging.getLogger(__name__)

__all__ = ['NASDAQOptionsScraper']

class NASDAQOptionsScraper:
    """
    Scrapes NASDAQ options chain data by hitting the official NASDAQ API endpoint,
    with built-in file-based caching.
    """
    def __init__(self, cache_dir='cache'):
        self.base_url = "https://api.nasdaq.com/api/quote/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'
        })
        self.cache_dir = cache_dir
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
    
    # In options_scraper/scraper.py, inside the NASDAQOptionsScraper class

    def get_stock_info(self, ticker: str):
        """Fetches summary data for a given stock ticker, including the last price."""
        LOG.info(f"Fetching stock info for {ticker.upper()}...")
        # This endpoint provides summary details for the ticker.
        url = f"{self.base_url}{ticker}/info?assetclass=stocks"
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            # The primary quote is usually found in this part of the response.
            primary_data = data.get('data', {}).get('primaryData', {})
            if primary_data and primary_data.get('lastSalePrice'):
                return {
                    "last_price": float(primary_data['lastSalePrice'].replace('$', ''))
                }
            return None
        except requests.exceptions.RequestException as e:
            LOG.error(f"Failed to fetch stock info for {ticker}: {e}")
            return None

    def get_filter_options(self, ticker: str):
        url = f"{self.base_url}{ticker}/option-chain?assetclass=stocks"
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data.get('data', {}).get('filterlist', {})
        except requests.exceptions.RequestException as e:
            LOG.error(f"Failed to fetch filter options for {ticker}: {e}")
            return None

    def get_expiration_dates(self, ticker: str):
        """Quickly fetches the list of available expiration dates."""
        LOG.info(f"Fetching available expiration dates for {ticker.upper()}...")
        filter_list = self.get_filter_options(ticker)
        if not filter_list:
            LOG.error("Could not retrieve filter list for expiration dates.")
            return []
        
        # Extract date values (e.g., '2025-09-19|2025-09-19')
        dates_raw = [f['value'] for f in filter_list.get('fromdate', {}).get('filter', [])]
        # Parse to 'YYYY-MM-DD' format
        parsed_dates = [d.split('|')[0] for d in dates_raw if '|' in d]
        return parsed_dates

    @staticmethod
    def parse_json_records(json_data, ticker):
        rows = json_data.get('data', {}).get('table', {}).get('rows', [])
        # We will standardize on 'YYYY-MM-DD' which comes from the API parameters.
        # The 'expiryDate' field in the raw JSON is less reliable.
        expiry_date = json_data.get('data', {}).get('filters', {}).get('fromdate', {}).get('value', '').split('|')[0]
        
        for row in rows:
            if not row.get('strike'):
                continue
            if row.get('c_Last') and row['c_Last'] != '--':
                yield {
                    'Root': ticker.upper(), 'Calls': row.get('drillDownURL', '').split('/')[-1],
                    'Last': row.get('c_Last'), 'Chg': row.get('c_Change'), 'Bid': row.get('c_Bid'),
                    'Ask': row.get('c_Ask'), 'Vol': row.get('c_Volume'), 'Open Int': row.get('c_Openinterest'),
                    'Strike': row.get('strike'), 'Puts': None, 'Expiry Date': expiry_date,
                }
            if row.get('p_Last') and row['p_Last'] != '--':
                yield {
                    'Root': ticker.upper(), 'Puts': row.get('drillDownURL', '').replace('C', 'P').split('/')[-1],
                    'Last': row.get('p_Last'), 'Chg': row.get('p_Change'), 'Bid': row.get('p_Bid'),
                    'Ask': row.get('p_Ask'), 'Vol': row.get('p_Volume'), 'Open Int': row.get('p_Openinterest'),
                    'Strike': row.get('strike'), 'Calls': None, 'Expiry Date': expiry_date,
                }

    def __call__(self, ticker, expiry=None, **kwargs):
        """
        Main method to scrape options data.
        If 'expiry' is provided, fetches data only for that date.
        Otherwise, it fetches for all available dates.
        """
        LOG.info(f"Fetching available filters for {ticker.upper()}...")
        filter_list = self.get_filter_options(ticker)
        if not filter_list:
            LOG.error("Could not retrieve filter list. Aborting.")
            return

        # --- UPDATED: Logic to handle a single expiry date ---
        if expiry:
            LOG.info(f"Fetching on-demand for single expiration: {expiry}")
            # Format the single date to match the API's 'from|to' requirement
            dates = [f'{expiry}|{expiry}']
        else:
            # Fallback to fetching all dates if no specific expiry is given
            dates = [f['value'] for f in filter_list.get('fromdate', {}).get('filter', [])]

        types = [f['value'] for f in filter_list.get('type', {}).get('filter', [])]
        moneyness = [f['value'] for f in filter_list.get('money', {}).get('filter', [])]
        
        combinations = list(itertools.product(dates, types, moneyness))
        total_combos = len(combinations)
        LOG.info(f"Found {total_combos} filter combinations to process.")

        for i, combo in enumerate(combinations):
            date_range, type_val, money_val = combo
            
            try:
                from_date, to_date = date_range.split('|')
            except ValueError:
                LOG.warning(f"Skipping invalid date range: {date_range}")
                continue

            cache_filename = f"{ticker}_{from_date}_{to_date}_{type_val}_{money_val}.json"
            cache_filepath = os.path.join(self.cache_dir, cache_filename)

            if os.path.exists(cache_filepath):
                LOG.info(f"Cache HIT for combo {i+1}/{total_combos}. Loading from file.")
                with open(cache_filepath, 'r') as f:
                    json_data = json.load(f)
            else:
                LOG.info(f"Cache MISS for combo {i+1}/{total_combos}. Fetching from API.")
                params = {
                    'assetclass': 'stocks', 'fromdate': from_date, 'todate': to_date,
                    'type': type_val, 'money': money_val, 'limit': 10000
                }
                full_url = f"{self.base_url}{ticker}/option-chain?{urlencode(params)}"
                
                try:
                    response = self.session.get(full_url, timeout=20)
                    response.raise_for_status()
                    json_data = response.json()
                    
                    with open(cache_filepath, 'w') as f:
                        json.dump(json_data, f)
                    
                    time.sleep(1) 
                except requests.exceptions.RequestException as e:
                    LOG.error(f"Failed to scrape URL {full_url}: {e}")
                    continue
            
            for record in self.parse_json_records(json_data, ticker):
                yield record

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s :: [%(levelname)s] :: %(message)s")
    test_ticker = 'AMD'
    print(f"--- Running Individual Test for {test_ticker.upper()} ---")
    scraper = NASDAQOptionsScraper()

    # --- Test new on-demand fetching for a single date ---
    test_expiry_date = '2025-07-11'
    print(f"\n--- Testing fetch for single expiry: {test_expiry_date} ---")
    record_count = 0
    for record in scraper(test_ticker, expiry=test_expiry_date):
        print(json.dumps(record, indent=4))
        record_count += 1
    print(f"--- Test Complete: Found {record_count} records for {test_expiry_date} ---")