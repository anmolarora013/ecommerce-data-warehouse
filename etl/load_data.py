import os
import sys
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# -------------------------
# Configuration & env load
# -------------------------
def load_environment(env_search: bool = True):
    """Load environment variables from a .env file (if present)."""
    if env_search:
        env_path = find_dotenv()
        if env_path:
            load_dotenv(env_path)
            print(f"Loaded .env from: {env_path}")
        else:
            print("No .env found via find_dotenv(). Proceeding with system env.")
    else:
        print("Skipping .env load (env_search=False).")

def get_db_config():
    """Read DB configuration from environment (with sensible defaults)."""
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    db_name = os.getenv("POSTGRES_DB", "appdb")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    return dict(user=db_user, password=db_password, name=db_name, host=db_host, port=db_port)

# -------------------------
# Engine creation
# -------------------------
def create_db_engine(db_conf: dict):
    """Create SQLAlchemy engine from db config dict."""
    db_url = f"postgresql://{db_conf['user']}:{db_conf['password']}@{db_conf['host']}:{db_conf['port']}/{db_conf['name']}"
    engine = create_engine(db_url, future=True)
    print("Database engine created.")
    return engine

# -------------------------
# CSV loading
# -------------------------
def load_csvs(data_dir: str):
    """Load all required CSVs and return as DataFrames dict."""
    def read_csv_safe(fname):
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            raise FileNotFoundError(f"CSV file not found: {path}")
        return pd.read_csv(path)

    print("Loading CSV files from:", data_dir)
    df_customer = read_csv_safe("dim_customer.csv")
    df_product = read_csv_safe("dim_products.csv")   # ensure filename matches your files
    df_payment = read_csv_safe("dim_payment.csv")
    df_orders = read_csv_safe("fact_orders.csv")
    df_date = read_csv_safe("dim_date.csv")
    print("CSV loading complete.")
    return dict(
        df_customer=df_customer,
        df_product=df_product,
        df_payment=df_payment,
        df_orders=df_orders,
        df_date=df_date,
    )

# -------------------------
# Truncate tables
# -------------------------
def truncate_tables(engine, table_names: list):
    """Truncate given tables using RESTART IDENTITY CASCADE in a transaction."""
    if not table_names:
        return
    try:
        with engine.begin() as conn:
            for t in table_names:
                sql = text(f"TRUNCATE TABLE {t} RESTART IDENTITY CASCADE;")
                conn.execute(sql)
        print("Truncate complete for tables:", table_names)
    except SQLAlchemyError as e:
        print("Error truncating tables:", e)
        raise

# -------------------------
# Insert dimensions
# -------------------------
def insert_dimensions(engine, dfs: dict, mapping: dict, if_exists="append", chunksize=1000):
    """
    Insert dimension DataFrames into DB.
    mapping: dict of dataframe-key -> target_table_name
    e.g. {'df_customer': 'dim_customer', 'df_product': 'dim_product'}
    """
    try:
        for df_key, table_name in mapping.items():
            df = dfs.get(df_key)
            if df is None:
                raise KeyError(f"DataFrame '{df_key}' not found in dfs.")
            print(f"Inserting {df_key} -> {table_name} (rows={len(df)})")
            df.to_sql(table_name, engine, if_exists=if_exists, index=False, method="multi", chunksize=chunksize)
        print("Dimension inserts completed.")
    except Exception as e:
        print("Error inserting dimensions:", e)
        raise

# -------------------------
# Fetch surrogate keys
# -------------------------
def fetch_surrogate_keys(engine):
    """
    Read surrogate key mapping tables from the DB and return DataFrames.
    Adjust SQL if your key/ID column names differ.
    """
    try:
        df_customers_db = pd.read_sql("SELECT customer_key, customer_id FROM dim_customer;", engine)
        df_products_db = pd.read_sql("SELECT product_key, product_id FROM dim_product;", engine)
        df_payments_db = pd.read_sql("SELECT payment_key, payment_method FROM dim_payment;", engine)
        print("Fetched surrogate key mappings from DB.")
        return dict(
            df_customers_db=df_customers_db,
            df_products_db=df_products_db,
            df_payments_db=df_payments_db,
        )
    except Exception as e:
        print("Error fetching surrogate keys:", e)
        raise

# -------------------------
# Merge surrogates into fact
# -------------------------
def merge_surrogates(df_orders, surrogate_dfs: dict):
    """Merge surrogate keys into orders DataFrame and return merged DataFrame."""
    df = df_orders.copy()
    df = df.merge(surrogate_dfs["df_customers_db"], on="customer_id", how="left")
    df = df.merge(surrogate_dfs["df_products_db"], on="product_id", how="left")
    df = df.merge(surrogate_dfs["df_payments_db"], on="payment_method", how="left")
    print("Merged surrogate keys into orders.")
    return df

# -------------------------
# Prepare final fact dataframe
# -------------------------
def prepare_fact_orders(df_orders_merged, expected_cols=None):
    """Select and validate final fact table columns."""
    if expected_cols is None:
        expected_cols = [
            "order_id",
            "customer_key",
            "product_key",
            "payment_key",
            "date_key",
            "quantity",
            "price",
            "discount",
            "total_amount",
        ]
    missing = [c for c in expected_cols if c not in df_orders_merged.columns]
    if missing:
        raise KeyError(f"Missing expected columns in merged orders: {missing}")
    df_final = df_orders_merged[expected_cols].copy()
    print("Prepared final fact_orders DataFrame.")
    return df_final

# -------------------------
# Insert fact table
# -------------------------
def insert_fact_orders(engine, df_fact, table_name="fact_orders", if_exists="append", chunksize=1000):
    try:
        print(f"Inserting fact table {table_name} (rows={len(df_fact)})")
        df_fact.to_sql(table_name, engine, if_exists=if_exists, index=False, method="multi", chunksize=chunksize)
        print("Fact table insert completed.")
    except Exception as e:
        print("Error inserting fact table:", e)
        raise

# -------------------------
# Main orchestration
# -------------------------
def main():
    try:
        # load env & config
        load_environment()
        db_conf = get_db_config()

        # compute directories
        script_dir =  os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_dir = os.path.join(script_dir, "data")  # data folder next to script

        # create engine
        engine = create_db_engine(db_conf)

        # load csvs
        dfs = load_csvs(data_dir)

        # truncate tables (order doesn't strictly matter because CASCADE used)
        truncate_tables(engine, [
            "dim_customer",
            "dim_product",
            "dim_payment",
            "fact_orders",
            "dim_date",
        ])

        # insert dimension tables
        insert_dimensions(engine, dfs, {
            "df_customer": "dim_customer",
            "df_product": "dim_product",
            "df_payment": "dim_payment",
            "df_date": "dim_date",
        })

        # fetch surrogate keys and merge
        surrogate_dfs = fetch_surrogate_keys(engine)
        df_orders_merged = merge_surrogates(dfs["df_orders"], surrogate_dfs)

        # prepare and insert fact
        df_fact_final = prepare_fact_orders(df_orders_merged)
        insert_fact_orders(engine, df_fact_final)

        print("ETL completed successfully.")

    except Exception as exc:
        print("ETL failed:", str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()