import socket
import pickle
import pandas as pd
import numpy as np
import struct
import time
def process_data(data):
    processed_data = data.apply(lambda x: x * 2)  # Example: Multiply each value by 2
    return processed_data


def compute_intensive_operations(df):
    """
    1. Add two new columns:
       - 'Column6' as the square of 'A'
       - 'Column7' as the exponential of 'B' minus 1
    2. Sort the DataFrame based on 'Column6' in descending order.
    3. Create a 'CumulativeMax' column that represents the cumulative maximum of 'C'.
    4. Apply a rolling window operation on 'D':
       - Compute the rolling mean with a window size of 5, then multiply by 'E'.
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
        
        # Receive the length of the incoming response
        raw_length = s.recv(4)
        if len(raw_length) != 4:
            raise ValueError("Received an incomplete message length")
        length = struct.unpack('>I', raw_length)[0]
        
        # Receive the response based on the length
        response = b""
        while len(response) < length:
            part = s.recv(length - len(response))
            if not part:
                raise IOError("Connection closed by server before complete data was received")
            response += part
        
    result = pickle.loads(response)
    return result

# Serialize the function to be executed on the worker
function_code = pickle.dumps(compute_intensive_operations)

# Example usage
data = pd.read_csv('input_data.csv')
start_time = time.time()
result = send_data_and_function_to_worker(function_code, data)
print("Result:", result)
end_time = time.time()

print(f"Time taken to execute the function: {end_time - start_time:.5f} seconds")

