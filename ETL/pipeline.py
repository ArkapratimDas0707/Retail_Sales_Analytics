# run_chip_etl.py

from extract import extract_data
from transform import transform_data
from summary import create_summary_tables
from load import load_dataframes


# ----------------------------
# Configuration
# ----------------------------
TRANSACTION_PATH = "./Data/Raw/QVI_transaction_data.xlsx"
CUSTOMER_PATH = "./Data/Raw/QVI_purchase_behaviour.csv"

# PostgreSQL config

DB_CONFIG = {
    'user': 'postgres',
    'password': 'Postgres%4070',
    'host': 'localhost',
    'port': '5432',
    'database': 'chip_sales'
}

# ----------------------------
# Run ETL Pipeline
# ----------------------------
def run_pipeline():
    print("[INFO] Starting chip sales ETL pipeline...")

    # Extract
    df_transactions, df_customers = extract_data(TRANSACTION_PATH, CUSTOMER_PATH)
    print("[INFO] Data extracted successfully.")

    # Transform
    df_cleaned = transform_data(df_transactions, df_customers)
    print("[INFO] Data transformed successfully.")

    # Summarize
    df_summary, df_prod, df_sales = create_summary_tables(df_cleaned)
    print("[INFO] Summary tables created.")

    # Load
    df_dict = {
        'chip_transactions': df_cleaned,
        'customer_summary': df_summary,
        'brand_sales_summary': df_prod,
        'daily_sales': df_sales
    }

    # Choose how to load the data
    load_dataframes(df_dict, db_config=DB_CONFIG)


    print("[INFO] ETL pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
