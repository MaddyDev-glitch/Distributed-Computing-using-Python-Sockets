import socket
import pickle
import struct
import time
import numpy as np
import pandas as pd
def process_data(data):
    return data.apply(lambda x: x*2 )

def compute_intensive_operations(df):
    """
    Perform compute-intensive operations on a DataFrame with the following steps:
    1. Add two new columns:
       - 'Column6' as the square of 'Column1'
       - 'Column7' as the exponential of 'Column2' minus 1
    2. Sort the DataFrame based on 'Column6' in descending order.
    3. Create a 'CumulativeMax' column that represents the cumulative maximum of 'Column3'.
    4. Apply a rolling window operation on 'Column4':
       - Compute the rolling mean with a window size of 5, then multiply by 'Column5'.
    5. Filter the DataFrame to keep rows where 'CumulativeMax' is greater than the median of 'CumulativeMax'.
    6. Return the modified DataFrame.
    """
    
    # Step 1: Add new columns
    df['Column6'] = df['A'] ** 2
    df['Column7'] = np.exp(df['B'] - 1)
    
    # Step 2: Sort the DataFrame
    df.sort_values(by='Column6', ascending=False, inplace=True)
    
    # Step 3: Cumulative maximum
    df['CumulativeMax'] = df['C'].cummax()
    
    # Step 4: Rolling operation
    df['RollingOperation'] = df['D'].rolling(window=5).mean() * df['E']
    
    # Step 5: Filter based on 'CumulativeMax'
    median_cummax = df['CumulativeMax'].median()
    filtered_df = df[df['CumulativeMax'] > median_cummax]
    
    return filtered_df
def send_data_and_function_to_worker(code_string, data):
    pickled_data = pickle.dumps(data)
    combined_data = pickle.dumps((code_string, pickled_data))
    
    # Prefix each message with a 4-byte length (network byte order)
    message = struct.pack('>I', len(combined_data)) + combined_data
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('192.168.1.105', 12345))
        s.sendall(message)
        
        # Receive response
        length = struct.unpack('>I', s.recv(4))[0]
        response = s.recv(length)
        
    result = pickle.loads(response)
    return result

def worker_function():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('0.0.0.0', 12345))
        print("Worker listening on port 12345s")
        s.listen()
        conn, addr = s.accept()
        with conn:
            start_time = time.time()

            # Receive message length
            raw_msglen = recvall(conn, 4)
            if not raw_msglen:
                return None
            msglen = struct.unpack('>I', raw_msglen)[0]
 
            # Receive the actual message
            received_data = recvall(conn, msglen)
 
            # Process data
            code_string, pickled_data = pickle.loads(received_data)
            func = pickle.loads(code_string)
            print(code_string)
            print (func)
            data = pickle.loads(pickled_data)
            result = func(data)
            print(result)
            # Send result back
            pickled_result = pickle.dumps(result)
            response = struct.pack('>I', len(pickled_result)) + pickled_result
            conn.sendall(response)
            end_time = time.time()
            print(f"Time taken to execute the function: {end_time - start_time:.5f} seconds")

 
def recvall(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data
 

worker_function()

