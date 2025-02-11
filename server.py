
from flask import Flask, render_template_string
from flask_sock import Sock
import json
from datetime import datetime
from collections import deque
import pandas as pd
import threading

app = Flask(__name__)
sock = Sock(app)

# Global deque to store tick data for processing
DEQUE_MAXLEN = 5000000
tick_data = deque(maxlen=DEQUE_MAXLEN)  # Adjust maxlen as needed deque of dictionaries


import logging


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


# Log initial state of the deque
logging.info(f"Initial tick_data: {list(tick_data)}")


def ws_client_connect():
    """
    Connect to Bybit WebSocket and populate the tick_data deque.
    """
    append_counter = 0  # Now a local variable inside ws_client_connect()

    
    import os
    import requests, time, base64, struct, hmac
    from fyers_apiv3 import fyersModel
    from urllib.parse import urlparse, parse_qs 
    pin = '8894'
    class FyesApp:
        def __init__(self) -> None:
            self.__username= 'XP12325' 
            self.__totp_key='Q2HC7F57FHMHPRT2VRLPRWA4ORWPK34E'
            self.__pin='8894'
            self.__client_id="M6EQ9SEMLM-100"
            self.__secret_key="22NRKYLP40"
            self.__redirect_uri='http://127.0.0.1:8081'
            self.__access_token=None

        def enable_app(self):
            appSession= fyersModel.SessionModel(
                client_id= self.__client_id,
                redirect_uri= self.__redirect_uri,
                response_type= 'code',
                state= 'state',
                secret_key=self.__secret_key,
                grant_type='authorization_code'
                
            )
            return appSession.generate_authcode()

        #private function for authenticator app function for authentacation using totp
        def __totp(self, key, time_step=30, digits=6, digest="sha1"):
            key = base64.b32decode(key.upper() + "=" * ((8 - len(key)) % 8))
            counter = struct.pack(">Q", int(time.time() / time_step))
            mac = hmac.new(key, counter, digest).digest()
            offset = mac[-1] & 0x0F
            binary = struct.unpack(">L", mac[offset : offset + 4])[0] & 0x7FFFFFFF
            return str(binary)[-digits:].zfill(digits)

        def get_token(self, refresh=False):
            if self.__access_token == None and refresh !=False:
                return #?

            headers = {
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
            }

            s = requests.Session()
            s.headers.update(headers)

            data1 = f'{{"fy_id":"{base64.b64encode(f"{self.__username}".encode()).decode()}","app_id":"2"}}'
            r1 = s.post("https://api-t2.fyers.in/vagator/v2/send_login_otp_v2", data=data1)

            request_key = r1.json()["request_key"]
            data2 = f'{{"request_key":"{request_key}","otp":{self.__totp(self.__totp_key)}}}'
            r2 = s.post("https://api-t2.fyers.in/vagator/v2/verify_otp", data=data2)
            assert r2.status_code == 200, f"Error in r2:\n {r2.text}"

            request_key = r2.json()["request_key"]
            data3 = f'{{"request_key":"{request_key}","identity_type":"pin","identifier":"{base64.b64encode(f"{pin}".encode()).decode()}"}}'
            r3 = s.post("https://api-t2.fyers.in/vagator/v2/verify_pin_v2", data=data3)
            assert r3.status_code == 200, f"Error in r3:\n {r3.json()}"

            headers = {"authorization": f"Bearer {r3.json()['data']['access_token']}", "content-type": "application/json; charset=UTF-8"}
            data4 = f'{{"fyers_id":"{self.__username}","app_id":"{self.__client_id[:-4]}","redirect_uri":"{self.__redirect_uri}","appType":"100","code_challenge":"","state":"abcdefg","scope":"","nonce":"","response_type":"code","create_cookie":true}}'
            r4 = s.post("https://api.fyers.in/api/v2/token", headers=headers, data=data4)
            assert r4.status_code == 308, f"Error in r4:\n {r4.json()}"

            parsed = urlparse(r4.json()["Url"])
            auth_code = parse_qs(parsed.query)["auth_code"][0]

            session = fyersModel.SessionModel(
                client_id=self.__client_id, 
                secret_key=self.__secret_key, 
                redirect_uri=self.__redirect_uri, 
                response_type="code", 
                grant_type="authorization_code"
                )

            session.set_token(auth_code)
            response = session.generate_token()
            self.__access_token =  response["access_token"]
            return self.__access_token

    app = FyesApp()
    access_token = app.get_token()
    print(f'AcessTOKEN: {access_token}')


    client_id = "M6EQ9SEMLM-100"

    
    from fyers_apiv3.FyersWebsocket import data_ws





    
    def onmessage(message):
        """
        Callback function for handling incoming messages from Fyers WebSocket.
        """
        nonlocal append_counter  # Refers to the variable inside ws_client_connect
        # print(f"Received message: {message}")
        
        # If message is a string, then deserialize it to a dictionary
        if isinstance(message, str):
            tick = json.loads(message)
            # print(f"data: {tick}")
        else:
            tick = message  # If message is already a dictionary, use it directly
            # print(f"data: {tick}")


        # if 'ltp' in tick:
        #     print(f"tick: {tick['ltp']}")
           
        if "ltp" in tick:
            price = tick["ltp"]
            tick_time = datetime.now()
            
            # Append data to deque
            tick_data.append({'timestamp': tick_time, 'price': price})
            
            logging.info(f"Tick data added: {tick_time}, {price}")
            
            # Optionally, log deque contents (last 5 items)
            logging.info(f"Deque contents (last 5): {json.dumps(list(tick_data)[-5:], indent=4, default=str)}")
            
            # Check if deque is full and handle it (saving data to CSV, flushing, etc.)
            if len(tick_data) == tick_data.maxlen:
                logging.info("Deque reached maximum capacity. Flushing data...")
                
                # Save to CSV file
                df = pd.DataFrame(list(tick_data))
                df.to_csv('tick_data.csv', mode='a', header=False, index=False)
                
                # Optionally, clear the deque after saving
                tick_data.clear()



    def onerror(message):
        """

        Callback function to handle WebSocket errors.

        Parameters:
            message (dict): The error message received from the WebSocket.


        """
        print("Error:", message)


    def onclose(message):
        """
        Callback function to handle WebSocket connection close events.
        """
        print("Connection closed:", message)


    def onopen():
        """
        Callback function to subscribe to data type and symbols upon WebSocket connection.

        """
        # Specify the data type and symbols you want to subscribe to
        data_type = "SymbolUpdate"
        # data_type = "DepthUpdate"


        # Subscribe to the specified symbols and data type
        symbols = ['NSE:NIFTY50-INDEX']
        fyers.subscribe(symbols=symbols, data_type=data_type)

        # Keep the socket running to receive real-time data
        fyers.keep_running()




    # Create a FyersDataSocket instance with the provided parameters
    fyers = data_ws.FyersDataSocket(
        access_token=access_token,       # Access token in the format "appid:accesstoken"
        log_path="",                     # Path to save logs. Leave empty to auto-create logs in the current directory.
        litemode=True,                  # Lite mode disabled. Set to True if you want a lite response.
        write_to_file=False,              # Save response in a log file instead of printing it.
        reconnect=True,                  # Enable auto-reconnection to WebSocket on disconnection.
        on_connect=onopen,               # Callback function to subscribe to data upon connection.
        on_close=onclose,                # Callback function to handle WebSocket connection close events.
        on_error=onerror,                # Callback function to handle WebSocket errors.
        on_message=onmessage             # Callback function to handle incoming messages from the WebSocket.
    )

    # Establish a connection to the Fyers WebSocket
    fyers.connect()

# Start WebSocket client in a separate thread
threading.Thread(target=ws_client_connect, daemon=True).start()





import time 

@sock.route("/ws")
def push_latest_tick(ws):
    """Pushes only the latest tick data to WebSocket clients."""
    while True:
        if tick_data:
            latest_tick = tick_data[-1]  # Get the most recent tick
            ws.send(json.dumps(latest_tick, default=str))
        time.sleep(0.5)  # Adjust frequency if needed


# Serve the frontend

@app.route("/")
def index():
    """Serves the frontend chart."""
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Live BTC/USDT Tick Data</title>
        <script src="https://cdn.jsdelivr.net/gh/parth-royale/cdn@main/lightweight-charts.standalone.production.js"></script>
    </head>
    <body>
        <h1>Live BTC/USDT Tick Chart</h1>
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

    let lastCandle = null;

    ws.onmessage = function(event) {
        const tick = JSON.parse(event.data);
        const tickTime = new Date(tick.timestamp).getTime() / 1000;

        if (!lastCandle || tickTime >= lastCandle.time + 60) {
            // Create a new candle every 1 min
            lastCandle = {
                time: tickTime,
                open: tick.price,
                high: tick.price,
                low: tick.price,
                close: tick.price
            };
            candleSeries.update(lastCandle);
        } else {
            // Update current candle
            lastCandle.high = Math.max(lastCandle.high, tick.price);
            lastCandle.low = Math.min(lastCandle.low, tick.price);
            lastCandle.close = tick.price;
            candleSeries.update(lastCandle);
        }
    };
</script>

    </body>
    </html>
    """
    return render_template_string(html)

if __name__ == '__main__':
    app.run()
# wscat -c ws://127.0.0.1:5000/ws
