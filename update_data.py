import ccxt
import datetime
import time
import sqlite3
import logging

# Logging setup
logging.basicConfig(filename='upbit_ohlcv.log', level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Connect to Upbit
upbit = ccxt.upbit()

# Load all trading pairs
upbit.load_markets()
all_pairs = upbit.symbols
logging.info(f'Loaded {len(all_pairs)} trading pairs from Upbit.')

# Database connection (SQLite for example)
conn = sqlite3.connect('ohlcv.db')
cursor = conn.cursor()

# Function to sanitize table names
def sanitize_table_name(symbol):
    return 't' + ''.join('_' if not c.isalnum() else c for c in symbol)

# Function to create a table for a given symbol
def create_table_for_symbol(symbol):
    table_name = sanitize_table_name(symbol)
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS "{table_name}" 
                     (timestamp DATETIME PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume REAL)''')

# Function to fetch the most recent timestamp from the database
def get_most_recent_timestamp(symbol):
    table_name = sanitize_table_name(symbol)
    cursor.execute(f'SELECT MAX(timestamp) FROM "{table_name}"')
    result = cursor.fetchone()
    return result[0] if result and result[0] else int(datetime.datetime(2020, 1, 1).timestamp() * 1000)

# Function to fetch and store data
def fetch_and_store(symbol, since):
    table_name = sanitize_table_name(symbol)
    try:
        ohlcv = upbit.fetch_ohlcv(symbol, '1h', since)
        logging.info(f'Fetched {len(ohlcv)} data points for {symbol}.')

        if len(ohlcv) > 0:
            for candle in ohlcv:
                cursor.execute(f'INSERT OR IGNORE INTO "{table_name}" VALUES (?, ?, ?, ?, ?, ?)', candle)
            conn.commit()

            return ohlcv[-1][0] + 1  # Return the timestamp of the last fetched candle
        else:
            return since  # Return the original timestamp if no new data

    except ccxt.NetworkError as e:
        logging.error(f'Network error: {e} for symbol {symbol}')
    except ccxt.ExchangeError as e:
        logging.error(f'Exchange error: {e} for symbol {symbol}')
    except Exception as e:
        logging.error(f'An unexpected error occurred: {e} for symbol {symbol}')

# Initial setup
last_fetch_times = {}
for pair in all_pairs:
    create_table_for_symbol(pair)
    last_fetch_times[pair] = get_most_recent_timestamp(pair)

# Main loop
while True:
    for pair in all_pairs:
        logging.info(f'Checking for new data for {pair}.')
        last_fetch_times[pair] = fetch_and_store(pair, last_fetch_times[pair])
    logging.info('Waiting for one minute before the next check.')
    time.sleep(1800)

# Note: This script will run indefinitely. Stop it manually when needed.

