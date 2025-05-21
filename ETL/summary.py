
import pandas as pd



def create_summary_tables(df_merged):
    """
    Generate summary tables for:
    - Customer transactions summary
    - Brand-level sales summary
    - Daily total sales
    """
    # Customer-level summary
    df_summary = df_merged.groupby('LYLTY_CARD_NBR').agg({
        'TXN_ID': 'count',          # Number of transactions
        'DATE': ['min', 'max'],     # First and last purchase dates
        'PROD_QTY': 'sum'           # Total quantity purchased
    }).reset_index()
    df_summary.columns = ['LYLTY_CARD_NBR', 'NUM_TRANSACTIONS', 'FIRST_PURCHASE', 'LAST_PURCHASE', 'TOTAL_QTY']

    # Brand-level summary
    df_prod = df_merged.groupby('BRAND_STD').agg({
        'PROD_QTY': 'sum',
        'TOT_SALES': 'sum',
        'PRODUCT': lambda x: x.value_counts().idxmax()  # Most frequent product name per brand
    }).reset_index()
    df_prod.columns = ['BRAND_STD', 'TOTAL_QTY', 'TOTAL_SALES', 'TOP_PRODUCT']

    # Daily sales summary
    df_sales = df_merged.groupby('DATE')['TOT_SALES'].sum().reset_index().sort_values('DATE')

    return df_summary, df_prod, df_sales
