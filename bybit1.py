###################################################### Import Libraries ###############################################
import os
import json
import time
import logging
import threading
from datetime import datetime
from collections import deque

import pandas as pd
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from flask import Flask, render_template_string
from flask_sock import Sock
from pytz import timezone
import websocket  # Requires the "websocket-client" package
from apscheduler.schedulers.background import BackgroundScheduler

###################################################### Constants #######################################################
DEQUE_MAXLEN = 10
tick_data = deque(maxlen=DEQUE_MAXLEN)  # Global deque to store tick data

###################################################### Flask Setup #######################################################
app = Flask(__name__)
sock = Sock(app)

###################################################### Logging Configuration ###############################################
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

###################################################### SQL Setup ###########################################################
# PostgreSQL Connection Pool
CONNECTION_STRING = "postgresql://neondb_owner:npg_Mr7uaZH1pGBP@ep-morning-art-a9w8mj9y-pooler.gwc.azure.neon.tech/neondb?sslmode=require"
db_pool = SimpleConnectionPool(1, 10, dsn=CONNECTION_STRING)

def create_table_if_not_exists():
    """Creates the 'trades' table if it does not exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS trades (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL,
        price DECIMAL(18,8) NOT NULL
    );
    """
    conn = None
    cursor = None
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        logging.info("Table 'trades' ensured in the database.")
    except Exception as error:
        logging.error(f"Error creating table: {error}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)

def push_tick_data_to_db(ticks):
    """Bulk inserts tick data into the database."""
    if not ticks:
        return

    insert_query = """
    INSERT INTO trades (timestamp, price)
    VALUES (%s, %s);
    """
    conn = None
    cursor = None
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        tick_values = [(tick["timestamp"], tick["price"]) for tick in ticks]
        cursor.executemany(insert_query, tick_values)
        conn.commit()
        logging.info(f"{len(ticks)} records uploaded to the database.")
    except Exception as error:
        logging.error(f"Error uploading data: {error}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)

###################################################### WebSocket Client Setup ###############################################
# Global variable to hold the WebSocketApp instance.
ws_app = None

def ws_client_connect(stop_event):
    """
    Connects to Bybit WebSocket and processes tick data.
    Runs until stop_event is set.
    """
    global ws_app
    ws_url = "wss://stream.bybit.com/v5/public/linear"

    def on_message(ws, message):
        tick = json.loads(message)
        if tick.get("topic") == "publicTrade.BTCUSDT":
            trade_data = tick["data"][0]
            tick_time = datetime.fromtimestamp(trade_data["T"] / 1000)
            price = float(trade_data["p"])
            tick_data.append({"timestamp": tick_time, "price": price})
            
            # Flush to CSV if deque is full
            if len(tick_data) >= DEQUE_MAXLEN:
                logging.info("Deque reached maximum capacity. Flushing to CSV...")
                ist = timezone('Asia/Kolkata')
                current_date = datetime.now(ist).strftime('%b%d').lower()
                csv_filename = f'{current_date}.csv'
                df = pd.DataFrame(list(tick_data))
                df.to_csv(csv_filename, mode='a', header=False, index=False)
                tick_data.clear()
            logging.info("Tick data added at %s", tick_time)

    def on_open(ws):
        subscribe_message = json.dumps({"op": "subscribe", "args": ["publicTrade.BTCUSDT"]})
        ws.send(subscribe_message)
        logging.info("WebSocket connected and subscribed.")

    # Create the WebSocketApp instance and store it globally.
    ws_app = websocket.WebSocketApp(ws_url, on_message=on_message, on_open=on_open)

    # Loop until the stop_event is set.
    while not stop_event.is_set():
        try:
            ws_app.run_forever()  # Blocks until the connection is closed.
        except Exception as e:
            logging.error("WebSocket encountered an error: %s", e)
        # If the stop event isn’t set, wait a moment before attempting to reconnect.
        time.sleep(1)
    logging.info("ws_client_connect: Stop event set. Exiting thread.")

###################################################### Candlestick Conversion ###############################################
def tick_to_candle(tick_data_deque, timeframe="1min"):
    """Converts tick data to OHLC candlestick format."""
    if not tick_data_deque:
        return pd.DataFrame()
    df = pd.DataFrame(list(tick_data_deque))
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    ohlc = df.resample(timeframe, on="timestamp").agg({"price": ["first", "max", "min", "last"]})
    ohlc.columns = ["Open", "High", "Low", "Close"]
    return ohlc.dropna().reset_index()

###################################################### Flask WebSocket Server ###############################################
@sock.route("/ws")
def push_candlestick_data(ws):
    """Pushes real-time candlestick data to WebSocket clients."""
    while True:
        if len(tick_data) > 0:
            ohlc_df = tick_to_candle(tick_data)
            if not ohlc_df.empty:
                ws.send(ohlc_df.to_json(orient="records"))
        time.sleep(1)  # Adjust the sleep time as needed

###################################################### Frontend Setup #######################################################
@app.route("/")
def index():
    """Serves the frontend chart."""
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Live BTC/USDT Chart</title>
        <script src="https://cdn.jsdelivr.net/gh/parth-royale/cdn@main/lightweight-charts.standalone.production.js"></script>
    </head>
    <body>
        <h1>Live BTC/USDT Candlestick Chart</h1>
        <div id="chart" style="width: 100%; height: 500px;"></div>
        <script>
            const chart = LightweightCharts.createChart(document.getElementById('chart'), {
                width: window.innerWidth,
                height: window.innerHeight,
                priceScale: { borderColor: '#cccccc' },
                timeScale: { borderColor: '#cccccc', timeVisible: true, secondsVisible: true }
            });
            const candleSeries = chart.addCandlestickSeries();
            const ws = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws");
            ws.onmessage = function(event) {
                const data = JSON.parse(event.data);
                const formattedData = data.map(candle => ({
                    time: new Date(candle.timestamp).getTime() / 1000,
                    open: candle.Open,
                    high: candle.High,
                    low: candle.Low,
                    close: candle.Close
                }));
                candleSeries.setData(formattedData);
                console.log(formattedData);
            };
        </script>
    </body>
    </html>
    """
    return render_template_string(html)

###################################################### Main Flow ###########################################################
# Global variables to manage the WebSocket thread and its stop signal.
ws_thread = None
ws_stop_event = threading.Event()

def main():
    """Starts the WebSocket thread."""
    global ws_thread, ws_stop_event
    create_table_if_not_exists()
    ws_stop_event.clear()  # Ensure the stop event is not set
    ws_thread = threading.Thread(target=ws_client_connect, args=(ws_stop_event,), daemon=True)
    ws_thread.start()
    logging.info("WebSocket thread started.")

def stop_main():
    """Signals the WebSocket thread to stop gracefully."""
    global ws_thread, ws_stop_event, ws_app
    logging.info("Stopping WebSocket thread gracefully...")
    ws_stop_event.set()  # Signal the thread to stop

    # Close the WebSocket connection if it exists so that run_forever() exits.
    if ws_app:
        ws_app.close()
        logging.info("WebSocket connection closed.")

    # Wait for the thread to finish.
    if ws_thread:
        ws_thread.join(timeout=10)
        logging.info("WebSocket thread stopped.")
    else:
        logging.info("No WebSocket thread running.")

###################################################### Scheduler Setup ####################################################
scheduler = BackgroundScheduler(daemon=True)

# Schedule main() to start the WebSocket thread (example: 12:55 IST)
scheduler.add_job(
    main,
    'cron',
    day_of_week='mon-fri',
    hour=13,
    minute=3,
    timezone='Asia/Kolkata'
)

# Schedule stop_main() to stop the WebSocket thread (example: 12:56 IST)
scheduler.add_job(
    stop_main,
    'cron',
    day_of_week='mon-fri',
    hour=13,
    minute=4,
    timezone='Asia/Kolkata',
    id='stop_main'
)

scheduler.start()

###################################################### Start Flask App ####################################################
port = int(os.getenv('PORT', 80))
logging.info('Listening on port %s', port)
app.run(debug=False, host="0.0.0.0", port=port)
