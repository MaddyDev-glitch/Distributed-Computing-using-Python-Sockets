import pandas as pd
def process_data(data):
    processed_data = data.apply(lambda x: x * 2)  # Example: Multiply each value by 2
    return processed_data