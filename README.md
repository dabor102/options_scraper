NASDAQ Options Chain Scraper
This is a Python-based options chain scraper for the new NASDAQ website: https://nasdaq.com. It allows you to scrape NASDAQ options chain data by hitting the official NASDAQ API endpoint.

This version includes several key improvements over the original:

File-based caching: Caches API responses to avoid re-downloading data.

Targeted data fetching: You can now fetch options data for a specific expiration date.

Expiration date listing: A function to quickly fetch and list all available expiration dates for a ticker.

Standardized data: The output data now uses a 'YYYY-MM-DD' date format.

Install
Bash
pip install options-scraper
API Usage
You can use the API to get scraped data records as Python objects.

Basic Scraping

Python
from options_scraper.scraper import NASDAQOptionsScraper
from options_scraper.utils import batched

scraper = NASDAQOptionsScraper()
ticker_symbol = 'AMD'

# Get all options data
records_generator = scraper(ticker_symbol)
for item in records_generator:
    print(item)
Scraping for a Specific Expiration Date

To make your scraping more efficient, you can fetch data for a single expiration date.

Python
from options_scraper.scraper import NASDAQOptionsScraper

scraper = NASDAQOptionsScraper()
ticker_symbol = 'AMD'
expiry_date = '2025-07-11' # Make sure this is a valid date

# Get options data for a single expiration date
records_generator = scraper(ticker_symbol, expiry=expiry_date)
for item in records_generator:
    print(item)
Listing all Available Expiration Dates

You can also fetch a list of all available expiration dates for a given ticker.

Python
from options_scraper.scraper import NASDAQOptionsScraper

scraper = NASDAQOptionsScraper()
ticker_symbol = 'AMD'

# Get a list of all expiration dates
expiration_dates = scraper.get_expiration_dates(ticker_symbol)
print(expiration_dates)
Output

Each scraped record will have the following structure:

Python
{
    "Root": "AMD",
    "Calls": "AMD250711C00050000",
    "Last": "119.50",
    "Chg": "0.00",
    "Bid": "118.85",
    "Ask": "120.35",
    "Vol": "0",
    "Open Int": "1",
    "Strike": "50.00",
    "Puts": null,
    "Expiry Date": "2025-07-11"
}
Console Script
You can also use the command-line script to scrape records and save them to either a CSV or JSON file.

Bash
options-scraper --help
Examples

Get all option chain data for XOM and save as CSV
This will save the data in batches of 1000 records per file.

Bash
options-scraper -t XOM -o /path/to/your/data -b 1000 -s csv
Get all option chain data for MSFT and save as JSON

Bash
options-scraper -t MSFT -o /path/to/your/data -b 10 -s json
Get all put options with weekly expiry

Bash
options-scraper -t XOM -c put -x week -o /path/to/your/data
