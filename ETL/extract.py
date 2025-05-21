import pandas as pd

def extract_data(transaction_path: str, customer_path: str):
    """
    Extract transaction and customer data from provided file paths.
    """
    try:
        df_transactions = pd.read_excel(transaction_path)
        df_customers = pd.read_csv(customer_path)
        return df_transactions, df_customers
    except Exception as e:
        raise RuntimeError(f"[ERROR] Failed to extract data: {e}")