#scraper.py
import logging
import requests
import itertools
import os
import json
import time
import datetime
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
        """Fetches summary data for a given stock ticker, with detailed logging."""
        LOG.info(f"--- Running get_stock_info for {ticker.upper()} ---")
        url = f"{self.base_url}{ticker}/info?assetclass=stocks"
        LOG.info(f"Requesting stock info from URL: {url}")
        try:
            response = self.session.get(url, timeout=10)
            LOG.info(f"Received response with status code: {response.status_code}")
            response.raise_for_status()
            
            raw_data = response.json()
            main_data = raw_data.get('data', raw_data)
            
            if not main_data:
                LOG.error("After handling API structure, the main_data object is empty.")
                return None

            primary_data = main_data.get('primaryData', {})
            if primary_data and primary_data.get('lastSalePrice'):
                price_str = primary_data['lastSalePrice']
                LOG.info(f"Successfully found lastSalePrice: {price_str}")
                return {"last_price": float(price_str.replace('$', ''))}

            LOG.warning(f"Could not find 'lastSalePrice' in the response for {ticker}.")
            LOG.info(f"Dumping main_data object for debugging: {json.dumps(main_data, indent=2)}")
            return None
            
        except requests.exceptions.RequestException as e:
            LOG.error(f"Failed to fetch stock info for {ticker}: {e}")
            return None
        except json.JSONDecodeError:
            LOG.error(f"Failed to decode JSON for stock info. Raw response was: {response.text}")
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
        
    def _parse_date(self, date_str: str) -> str:
        """
        Parses a date string that could be in one of several formats
        (e.g., 'MM/DD/YYYY' or 'Mon DD') and returns it as 'YYYY-MM-DD'.
        This is now a method of the class.
        """
        try:
            # First, try the full 'MM/DD/YYYY' format
            return datetime.datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
        except ValueError:
            # If that fails, try the 'Mon DD' format (e.g., 'Jan 16')
            try:
                today = datetime.date.today()
                parsed_date = datetime.datetime.strptime(date_str, '%b %d').date()
                # If the parsed month is less than the current month, it's for the next year
                if parsed_date.month < today.month:
                    return parsed_date.replace(year=today.year + 1).strftime('%Y-%m-%d')
                else:
                    return parsed_date.replace(year=today.year).strftime('%Y-%m-%d')
            except ValueError:
                # If both formats fail, return None to be filtered out
                return None

    def get_expiration_dates(self, ticker: str):
        """Fetches expiration dates, with detailed step-by-step logging."""
        LOG.info(f"--- Running get_expiration_dates for {ticker.upper()} ---")
        url = f"{self.base_url}{ticker}/option-chain?assetclass=stocks"
        LOG.info(f"Requesting expiration dates from URL: {url}")
        
        try:
            response = self.session.get(url, timeout=15)
            LOG.info(f"Received response with status code: {response.status_code}")
            response.raise_for_status()

            raw_text = response.text
            if not raw_text:
                LOG.error("API response body for expiration dates is empty.")
                return []
            
            LOG.info("API response received, attempting to parse JSON...")
            raw_data = response.json()

            main_data = raw_data.get('data', raw_data)
            if not main_data:
                LOG.error("After handling API structure, the main_data object for expirations is empty.")
                LOG.info(f"Original JSON was: {json.dumps(raw_data, indent=2)}")
                return []

            filter_list = main_data.get('filterlist', {})
            if not filter_list:
                LOG.error("Could not find 'filterlist' in the main data object.")
                LOG.info(f"The main_data object was: {json.dumps(main_data, indent=2)}")
                return []
            LOG.info("'filterlist' was found successfully.")

            dates_raw = [f['value'] for f in filter_list.get('fromdate', {}).get('filter', [])]
            if not dates_raw:
                LOG.warning("The 'filterlist' exists but contains no expiration dates.")
                return []
            
            all_dates = {d.split('|')[0] for d in dates_raw if '|' in d}
            LOG.info(f"Successfully parsed {len(all_dates)} unique expiration dates.")
            return sorted(list(all_dates))

        except requests.exceptions.HTTPError as e:
            LOG.error(f"HTTP Error for {ticker}: {e}")
            LOG.error(f"Response Body: {response.text}")
            return []
        except requests.exceptions.RequestException as e:
            LOG.error(f"A network error occurred for {ticker}: {e}")
            return []
        except json.JSONDecodeError:
            LOG.error(f"Failed to decode JSON for {ticker}. Raw response text was:")
            LOG.error(response.text)
            return []
            
    # Note: The __call__ and parse_json_records methods rely on fetching the option chain
    # for a *specific date*, and those responses appear to be consistent. 
    # Therefore, no changes are needed there. The issue is with the initial metadata calls.
    # We will update the options chain endpoint in server.py instead for completeness.

            
    # Make sure you have the fallback function from before as well
    def _get_fallback_expiration_dates(self, ticker: str):
        # ... (the fallback code remains the same)
        LOG.info(f"Using fallback method to get expiration dates for {ticker.upper()}.")
        url = f"{self.base_url}{ticker}/option-chain?assetclass=stocks"
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            filter_list = data.get('data', {}).get('filterlist', {})

            if not filter_list:
                return []
            
            dates_raw = [f['value'] for f in filter_list.get('fromdate', {}).get('filter', [])]
            all_dates = {d.split('|')[0] for d in dates_raw if '|' in d}
            
            LOG.info(f"Found {len(all_dates)} dates via fallback method.")
            return sorted(list(all_dates))

        except requests.exceptions.RequestException as e:
            LOG.error(f"Fallback method also failed for {ticker}: {e}")
            return []


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
        Main method to scrape options data. Makes a single, comprehensive request
        for the specified expiration date.
        """
        if not expiry:
            LOG.error("An expiration date must be provided to fetch the options chain.")
            return

        LOG.info(f"Fetching all options for {ticker.upper()} on {expiry} in a single request.")

        # The cache filename will be simple: just the ticker and the expiry date.
        cache_filename = f"{ticker}_{expiry}_all.json"
        cache_filepath = os.path.join(self.cache_dir, cache_filename)

        if os.path.exists(cache_filepath):
            LOG.info(f"Cache HIT for {ticker} on {expiry}. Loading from file.")
            with open(cache_filepath, 'r') as f:
                json_data = json.load(f)
        else:
            LOG.info(f"Cache MISS for {ticker} on {expiry}. Fetching from API.")
            
            # These parameters ask for ALL options: calls and puts, all strike prices.
            params = {
                'assetclass': 'stocks',
                'fromdate': expiry,
                'todate': expiry,
                'excode': 'oprac',
                'callput': 'callput',
                'money': 'all',
                'type': 'all',
                'limit': 10000  # A high limit to get all strikes
            }
            full_url = f"{self.base_url}{ticker}/option-chain?{urlencode(params)}"
            
            try:
                response = self.session.get(full_url, timeout=20)
                response.raise_for_status()
                json_data = response.json()
                
                # Save the complete data to the cache for next time
                with open(cache_filepath, 'w') as f:
                    json.dump(json_data, f)
                
            except requests.exceptions.RequestException as e:
                LOG.error(f"Failed to scrape URL {full_url}: {e}")
                return # Stop execution if the API call fails
        
        # Once data is fetched (from cache or API), parse and yield the records.
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