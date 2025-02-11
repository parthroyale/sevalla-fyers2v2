import os 
from flask import Flask, render_template_string
import pandas as pd

data_dir = '/var/lib/data'
# data_dir = 'C:/Users/acer/Documents/y2025/jan6/sevalla-fyers/data'


app = Flask(__name__)


htmlx= """
<!DOCTYPE html>
<html>
<head>
    <title>CSV Data</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <h2>CSV Data</h2>
        {{ table | safe }}
    </div>
</body>
</html>
"""
@app.route('/')
def index():
    # Read the CSV file
    csv_file = os.path.join(data_dir, 'test.csv')
    df = pd.read_csv(csv_file)
    
    # Convert DataFrame to HTML table
    table_html = df.to_html(classes='table table-striped', index=False)
    
    return render_template_string(htmlx, table=table_html)


def main():
    # check if data_dir exists
    if not os.path.exists(data_dir):
        print(f"Data directory {data_dir} does not exist.")
    else:
        print(f"Data directory {data_dir} exists.")
        
        # Create a test CSV file
        test_file = os.path.join(data_dir, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('name,age,city\nJohn,30,New York\nJane,25,Boston')
        
        # Show the directory contents
        print("\nDirectory contents:")
        for item in os.listdir(data_dir):
            print(f"- {item}")
        
        # Show the contents of the file line by line
        print("\nContents of test.csv (line by line):")
        with open(test_file, 'r') as f:
            for line in f:
                print(line.strip())

main()


port = int(os.getenv('PORT', 80))
print('Listening on port %s' % (port))
app.run(host="0.0.0.0", port=port)

