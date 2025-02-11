import os 

data_dir = '/var/lib/data'
data_dir = '/c/Users/acer/Documents/y2025/jan6/sevalla-fyers/data'
# check if data_dir exists
if not os.path.exists(data_dir):
    print(f"Data directory {data_dir} does not exist.")
else:
    print(f"Data directory {data_dir} exists.")
