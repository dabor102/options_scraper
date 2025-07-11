import logging
import time
from datetime import datetime
import os

from apscheduler.schedulers.background import BackgroundScheduler
from pytz import timezone

# Assuming the scraper and serializer are accessible from this path
from options_scraper.scraper import NASDAQOptionsScraper
from options_scraper.serializer import NASDAQOptionsSerializer

# --- Configuration ---
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s :: [%(levelname)s] :: %(message)s")

TICKER = "AMD"
OUTPUT_DIR = "scheduled_data"
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 60  # 1 minute

# --- Core Functions ---

def scrape_and_save(market_session: str):
    """
    Fetches options data for a ticker and saves snapshots for each expiration date.
    
    Args:
        market_session: A label for the trading session (e.g., 'pre_market', 'post_market').
    """
    LOG.info(f"--- Starting {market_session} scrape for {TICKER} ---")
    scraper = NASDAQOptionsScraper()

    for i in range(MAX_RETRIES):
        try:
            expiration_dates = scraper.get_expiration_dates(TICKER)
            if not expiration_dates:
                LOG.warning(f"No expiration dates found for {TICKER}.")
                raise ConnectionError("Could not fetch expiration dates.")

            LOG.info(f"Found {len(expiration_dates)} expiration dates for {TICKER}.")

            # Create a directory for this specific run, e.g., 'scheduled_data/AMD_pre_market_2023-10-27_09-30-00'
            run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            session_dir = os.path.join(OUTPUT_DIR, f"{TICKER}_{market_session}_{run_timestamp}")
            if not os.path.exists(session_dir):
                os.makedirs(session_dir)
            
            for expiry in expiration_dates:
                LOG.info(f"Fetching data for expiry: {expiry}")
                
                # Scrape all records for the given ticker and expiry date
                records = list(scraper(TICKER, expiry=expiry))
                
                if not records:
                    LOG.warning(f"No records found for {TICKER} on {expiry}.")
                    continue

                # Save the data to a file, e.g., 'AMD_2025-07-11.json'
                file_name = f"{TICKER}_{expiry}.json"
                output_file = os.path.join(session_dir, file_name)
                
                # Use the existing serializer to save the data as JSON
                NASDAQOptionsSerializer._to_json(records, output_file)
                LOG.info(f"Successfully saved {len(records)} records to {output_file}")
            
            LOG.info(f"--- Completed {market_session} scrape for {TICKER} ---")
            return  # Success, so we exit the retry loop

        except Exception as e:
            LOG.error(f"An error occurred during scraping: {e}")
            if i < MAX_RETRIES - 1:
                LOG.info(f"Retrying in {RETRY_DELAY_SECONDS} seconds...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                LOG.error("Max retries reached. The scraping process has failed.")

def start_scheduler():
    """Initializes and starts the APScheduler to run the scraping tasks."""
    scheduler = BackgroundScheduler(timezone=timezone('US/Eastern'))
    
    # Schedule the scraping function to run at 9:30 AM and 4:00 PM EST on weekdays
    scheduler.add_job(scrape_and_save, 'cron', day_of_week='mon-fri', hour=9, minute=30, args=['pre_market'])
    scheduler.add_job(scrape_and_save, 'cron', day_of_week='mon-fri', hour=16, minute=0, args=['post_market'])
    
    scheduler.start()
    LOG.info("Scheduler started. Waiting for the next scheduled run.")
    
    # Keep the main thread alive to allow the background scheduler to run
    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        LOG.info("Scheduler shut down successfully.")

# --- Main Execution ---

if __name__ == '__main__':
    # You can uncomment the line below to run a scrape immediately for testing
    # scrape_and_save('manual_test_run')
    
    start_scheduler()