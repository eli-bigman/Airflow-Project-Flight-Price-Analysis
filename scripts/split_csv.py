import pandas as pd
import os
import math

SOURCE_FILE = r'C:\Users\RichardElinamNutsuga\Documents\xcode\Airflow Project-Flight Price Analysis\data\Flight_Price_Dataset_of_Bangladesh.csv'
OUTPUT_DIR = r'C:\Users\RichardElinamNutsuga\Documents\xcode\Airflow Project-Flight Price Analysis\data\test_parts'

def split_csv_in_5_parts():
    """
    Reads the main dataset and splits it into 5 roughly equal CSV files.
    """
    if not os.path.exists(SOURCE_FILE):
        print(f"Error: Source file not found: {SOURCE_FILE}")
        return

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")

    print(f"Reading {SOURCE_FILE}...")
    try:
        df = pd.read_csv(SOURCE_FILE)
    except Exception as e:
        print(f"Failed to read CSV: {e}")
        return

    total_rows = len(df)
    print(f"Total rows: {total_rows}")

    # Split into 5 parts
    num_parts = 5
    chunk_size = math.ceil(total_rows / num_parts)

    for i in range(num_parts):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size
        
        # Slice
        df_part = df.iloc[start_idx:end_idx]
        
        # Save
        filename = f"part_{i+1}.csv"
        file_path = os.path.join(OUTPUT_DIR, filename)
        
        df_part.to_csv(file_path, index=False)
        print(f"Saved {filename}: {len(df_part)} rows to {file_path}")

    print("Splitting complete! You can now move these files to data/input one by one to test incremental loading.")

if __name__ == "__main__":
    split_csv_in_5_parts()
