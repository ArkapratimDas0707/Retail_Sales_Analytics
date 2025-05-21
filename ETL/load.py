from sqlalchemy import create_engine
import os

def load_dataframes(df_dict, db_config=None, output_dir=None):
    """
    Load multiple DataFrames either to a PostgreSQL database or as CSV files.
    
    Parameters:
        df_dict (dict): Dictionary of {table_name: DataFrame}
        db_config (dict, optional): PostgreSQL connection parameters.
            Required keys: 'user', 'password', 'host', 'port', 'database'
        output_dir (str, optional): Folder path to save CSVs if no database is used.
    """
    
    if db_config:
        # Load to PostgreSQL
        print("[INFO] Connecting to PostgreSQL...")
        try:
            engine = create_engine(
                f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
                f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
            )
            for name, df in df_dict.items():
                df.to_sql(name, engine, if_exists='replace', index=False)
                print(f"[INFO] Loaded table: {name}")
        except Exception as e:
            raise RuntimeError(f"[ERROR] Failed to load to database: {e}")
    elif output_dir:
        # Save as CSV
        os.makedirs(output_dir, exist_ok=True)
        for name, df in df_dict.items():
            csv_path = os.path.join(output_dir, f"{name}.csv")
            df.to_csv(csv_path, index=False)
            print(f"[INFO] Saved CSV: {csv_path}")
    else:
        raise ValueError("You must provide either db_config or output_dir.")


