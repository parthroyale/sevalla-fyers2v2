from flask import Flask, render_template_string
from flask_sock import Sock
import json
from datetime import datetime
from collections import deque
import pandas as pd
import threading
import logging
import time
import psycopg2
from psycopg2 import sql
from psycopg2.pool import SimpleConnectionPool
import os 

# Flask Setup
app = Flask(__name__)
sock = Sock(app)

# Deque to store tick data for processing
DEQUE_MAXLEN = 100000 
tick_data = deque(maxlen=DEQUE_MAXLEN)  # Adjust maxlen as needed

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

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



def ws_client_connect():
    """Connects to Bybit WebSocket and processes tick data."""
    import websocket

    ws_url = "wss://stream.bybit.com/v5/public/linear"

    def on_message(ws, message):
        tick = json.loads(message)
        if tick.get("topic") == "publicTrade.BTCUSDT":
            trade_data = tick["data"][0]
            tick_time = datetime.fromtimestamp(trade_data["T"] / 1000)
            price = float(trade_data["p"])
            tick_data.append({"timestamp": tick_time, "price": price})

            # Flush if deque reaches max length
            if len(tick_data) >= DEQUE_MAXLEN:
                ticks_to_flush = list(tick_data)  # Copy before clearing
                tick_data.clear()
                push_tick_data_to_db(ticks_to_flush)
                logging.info("Deque flushed and reset after reaching max length.")

            threading.Event().wait(0.5)  # More efficient than time.sleep()

            # logging.info(
            #     "Append #%d: deque size = %d\nLast 5 ticks:\n%s\n",
            #     len(tick_data),
            #     len(tick_data),
            #     json.dumps(list(tick_data)[-5:], indent=4, default=str),
            # )
            
            
            # logging.info(f"Tick data added: {tick_time}, {price}")
            # logging.info(f"Deque contents (last 5): {json.dumps(list(tick_data)[-5:], indent=4, default=str)}")
           
           
           
           
            logging.info(
                "Tick data added: %s, %f\nAppend #%d: deque size = %d\nLast 5 ticks:\n%s\n",
                tick_time.strftime("%Y-%m-%d %H:%M:%S.%f"), #.%f: Microsecond as a zero-padded six-digit number (e.g., .123456).
                price,
                len(tick_data),
                len(tick_data),  # This is the second time len(tick_data) is being used
                json.dumps(list(tick_data)[-5:], indent=4, default=str)
            )


            
           

    
    def on_open(ws):
        ws.send(json.dumps({"op": "subscribe", "args": ["publicTrade.BTCUSDT"]}))
        logging.info("WebSocket connected and subscribed.")

    ws = websocket.WebSocketApp(ws_url, on_message=on_message)
    ws.on_open = on_open
    ws.run_forever()  # Blocks, but now handles both receiving and flushing

def tick_to_candle(tick_data_deque, timeframe="1min"):
    """Converts tick data to OHLC candlestick format."""
    df = pd.DataFrame(list(tick_data_deque), columns=["timestamp", "price"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    ohlc = df.resample(timeframe, on="timestamp").agg({"price": ["first", "max", "min", "last"]})
    ohlc.columns = ["Open", "High", "Low", "Close"]
    return ohlc.dropna().reset_index()


@sock.route("/ws")
def push_candlestick_data(ws):
    """Pushes real-time candlestick data to WebSocket clients."""
    while True:
        if len(tick_data) > 0:
            ohlc_df = tick_to_candle(tick_data)
            if not ohlc_df.empty:
                ws.send(ohlc_df.to_json(orient="records"))


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
            // const ws = new WebSocket('ws://' + location.host + '/ws');
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


def main():
    """Main function to start the WebSocket, Flask server, and background tasks."""
    create_table_if_not_exists()

    threading.Thread(target=ws_client_connect, daemon=True).start()

    port = int(os.getenv('PORT', 80))
    print('Listening on port %s' % (port))

    app.run(debug=False, host="0.0.0.0", port=port)  # Bind to 0.0.0.0 for Heroku


main()
