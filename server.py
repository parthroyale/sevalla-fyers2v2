import os 

data_dir = '/var/lib/data'
# data_dir = '/c/Users/acer/Documents/y2025/jan6/sevalla-fyers/data'
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
