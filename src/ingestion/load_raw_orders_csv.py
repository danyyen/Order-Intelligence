


import pandas as pd
from pathlib import Path
import time


# CONFIG

from config.paths import RAW_CSV_DIR



# FUNCTIONS

def get_latest_order_file(folder_path):
    """
    Get latest order history CSV file
    """

    files = sorted(folder_path.glob("order_history_20*.csv"))

    if not files:
        raise FileNotFoundError(
            "No order history files found."
        )

    return files[-1]


def load_csv(file_path):
    """
    Load CSV into pandas dataframe
    """

    df = pd.read_csv(
        file_path,
        low_memory=False
    )

    return df


def standardize_columns(df):
    """
    Standardize column names
    """

    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
        .str.replace("-", "_")
    )

    return df


def print_data_summary(df):
    """
    Print profiling summary
    """

    print("\n-------> DATA SUMMARY <-------")

    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns):,}")

    print("\n-------> COLUMN NAMES <-------")

    for col in df.columns:
        print(col)

    print("\n-------> DATA TYPES <-------")

    print(df.dtypes)



# MAIN PIPELINE


def main():

    start_time = time.time()

    latest_file = get_latest_order_file(RAW_CSV_DIR)

    print(f"\nReading latest file:")
    print(latest_file)

    df = load_csv(latest_file)

    df = standardize_columns(df)

    print_data_summary(df)

    print(
        f"\nCompleted in "
        f"{(time.time() - start_time):.2f} seconds"
    )



# ENTRY POINT


if __name__ == "__main__":
    main()

