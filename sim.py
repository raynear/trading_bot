import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Database Connection
engine = create_engine('your_database_connection_string')
query = "SELECT * FROM your_ohlcv_table"
data = pd.read_sql(query, engine)

# Volatility Breakthrough Strategy
data['target'] = data['High'].shift(1)  # assuming breakout level is previous day's high
data['buy_signal'] = data['Close'] > data['target']

# Simulate Trades (simplified, not accounting for holding period, etc.)
data['returns'] = data['Close'].pct_change()
data['strategy_returns'] = data['returns'] * data['buy_signal'].shift(1)

# Account for Slippage and Fees
slippage = 0.0005  # example slippage
fees = 0.001  # example fee per trade
data['strategy_returns_net'] = data['strategy_returns'] - slippage - fees

# Cumulative Returns
data['cumulative_returns'] = (1 + data['strategy_returns_net']).cumprod()

# Plotting
plt.figure(figsize=(12, 6))
plt.plot(data['cumulative_returns'], label='Strategy Returns')
plt.plot((1 + data['returns']).cum
