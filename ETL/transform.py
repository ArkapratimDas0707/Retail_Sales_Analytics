import re
import difflib
import pandas as pd

def transform_data(df_transactions: pd.DataFrame, df_customers: pd.DataFrame) -> pd.DataFrame:
    """
    Transform transaction and customer data:
    - Filter for chips
    - Convert dates
    - Standardize brand names
    - Extract pack size and clean product name
    - Merge with customer demographics
    """
    # Filter only chip products
    df_transactions = df_transactions[df_transactions['PROD_NAME'].str.contains(r'\bchips?\b', case=False, regex=True)]

    # Convert date from Excel serial
    df_transactions['DATE'] = pd.to_datetime(df_transactions['DATE'], origin='1899-12-30', unit='D')

    # Extract pack size
    df_transactions['PACK_SIZE'] = df_transactions['PROD_NAME'].str.extract(r'(\d+)[ ]?g', expand=False).astype(float)

    # Extract brand name (first word)
    df_transactions['BRAND'] = df_transactions['PROD_NAME'].str.strip().str.split().str[0]

    # Auto-standardize similar brand names
    unique_brands = df_transactions['BRAND'].unique()
    brand_mapping_auto = {}
    for brand in unique_brands:
        matches = difflib.get_close_matches(brand, unique_brands, n=5, cutoff=0.8)
        standard = matches[0]
        for match in matches:
            brand_mapping_auto[match] = standard
    df_transactions['BRAND_STD'] = df_transactions['BRAND'].replace(brand_mapping_auto)

    # Manual corrections for better consistency
    manual_corrections = {
        'Dorito': 'Doritos', 'Infuzions': 'Infuzions', 'Infzns': 'Infuzions',
        'Smith': 'Smiths', 'GrnWves': 'GrainWaves', 'Grain': 'GrainWaves',
        'Natural': 'Natural Chip Co', 'NCC': 'Natural Chip Co',
        'WW': 'Woolworths', 'WoolWorths': 'Woolworths', 'Snbts': 'Sunbites'
    }
    df_transactions['BRAND_STD'] = df_transactions['BRAND_STD'].replace(manual_corrections)

    # Clean product names (remove pack size and brand)
    df_transactions['PRODUCT'] = df_transactions['PROD_NAME'].str.replace(r'\d+\s?g', '', regex=True).str.strip()
    df_transactions['PRODUCT'] = df_transactions.apply(
        lambda x: re.sub(rf'^{re.escape(x["BRAND"])}\s*', '', x['PRODUCT'], flags=re.IGNORECASE),
        axis=1
    )

    # Add age group mapping
    def map_lifestage(lifestage: str) -> str:
        lifestage = lifestage.upper()
        if 'YOUNG' in lifestage:
            return 'Young'
        elif 'MIDAGE' in lifestage or 'OLDER' in lifestage:
            return 'Middle Aged'
        elif 'RETIREES' in lifestage:
            return 'Old'
        return 'Unknown'

    df_customers['AGE_GROUP'] = df_customers['LIFESTAGE'].apply(map_lifestage)

    # Merge transactions with customer data
    df_merged = pd.merge(df_transactions, df_customers, how='left', on='LYLTY_CARD_NBR')

    return df_merged
