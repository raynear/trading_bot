import ccxt
import datetime
import time
import sqlite3
import logging

# Logging setup
logging.basicConfig(filename='upbit_ohlcv.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    # Prefix with 't' and replace non-alphanumeric characters with '_'
    return 't' + ''.join('_' if not c.isalnum() else c for c in symbol)

# Function to create a table for a given symbol
def create_table_for_symbol(symbol):
    table_name = sanitize_table_name(symbol)
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS "{table_name}" 
                     (timestamp DATETIME PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume REAL)''')

# Function to fetch and store data
def fetch_and_store(symbol, since):
    table_name = sanitize_table_name(symbol)
    while True:
        try:
            ohlcv = upbit.fetch_ohlcv(symbol, '1h', since)
            logging.info(f'Fetched {len(ohlcv)} data points for {symbol}.')

            if len(ohlcv) == 0:
                logging.info(f'No more data to fetch for {symbol}')
                break

            for candle in ohlcv:
                cursor.execute(f'INSERT OR IGNORE INTO "{table_name}" VALUES (?, ?, ?, ?, ?, ?)', candle)
            conn.commit()

            since = ohlcv[-1][0] + 1
            time.sleep(upbit.rateLimit / 1000)

        except ccxt.NetworkError as e:
            logging.error(f'Network error: {e} for symbol {symbol}')
            time.sleep(1)
        except ccxt.ExchangeError as e:
            logging.error(f'Exchange error: {e} for symbol {symbol}')
            break
        except Exception as e:
            logging.error(f'An unexpected error occurred: {e} for symbol {symbol}')
            break

# Fetch and store OHLCV data for each trading pair
start_date = datetime.datetime(2020, 1, 1)
timestamp = int(start_date.timestamp() * 1000)

for pair in all_pairs:
    create_table_for_symbol(pair)
    logging.info(f'Fetching data for {pair}.')
    fetch_and_store(pair, timestamp)

# Close database connection
conn.close()
logging.info('Database connection closed.')
