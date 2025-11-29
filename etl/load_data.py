import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv, find_dotenv


env_path = find_dotenv()
load_dotenv(env_path)

print("Reading Environment Variables...")
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_NAME = os.getenv('POSTGRES_DB')
DB_HOST = 'localhost'
DB_PORT = '5432'
print("Database User:", DB_USER)
print("Database Name:", DB_NAME)
print("Database Pass:", DB_PASSWORD)
print("Database Host:", DB_HOST)
print("Database Port:", DB_PORT)

print("Reading Environment Variables Completed...")

print("Creating Database Engine...")
DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(DB_URL)
print("Database Engine Created...")

print("Loading Data from CSV files...")
df_customer = pd.read_csv(os.path.join(DATA_DIR, 'dim_customer.csv'))
df_product = pd.read_csv(os.path.join(DATA_DIR, 'dim_products.csv'))
df_payment = pd.read_csv(os.path.join(DATA_DIR, 'dim_payment.csv'))
df_orders = pd.read_csv(os.path.join(DATA_DIR, 'fact_orders.csv'))
df_date = pd.read_csv(os.path.join(DATA_DIR, 'dim_date.csv'))
print("Data Loaded from CSV files...")

print("Truncating Existing Data...")
with engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE dim_customer RESTART IDENTITY CASCADE;"))
    conn.execute(text("TRUNCATE TABLE dim_product RESTART IDENTITY CASCADE;"))
    conn.execute(text("TRUNCATE TABLE dim_payment RESTART IDENTITY CASCADE;"))
    conn.execute(text("TRUNCATE TABLE fact_orders RESTART IDENTITY CASCADE;"))
    conn.execute(text("TRUNCATE TABLE dim_date RESTART IDENTITY CASCADE;"))
print("Existing Data Truncated...")

print("Inserting Data into Database tables...")
df_customer.to_sql('dim_customer', engine, if_exists='append', index=False)
df_product.to_sql('dim_product', engine, if_exists='append', index=False)
df_payment.to_sql('dim_payment', engine, if_exists='append', index=False)
df_date.to_sql('dim_date', engine, if_exists='append', index=False)
print("Data Insertion Completed...")

print("Fetch surrogate keys from db")
df_customers_db = pd.read_sql("SELECT customer_key, customer_id FROM dim_customer;", engine)
df_products_db = pd.read_sql("SELECT product_key, product_id FROM dim_product;", engine)
df_payments_db = pd.read_sql("SELECT payment_key, payment_method FROM dim_payment;",engine)
print("Fetched surrogate keys from db")

print("Merging surrogate keys into fact_orders...")
df_orders = df_orders.merge(df_customers_db, on='customer_id', how='left')
df_orders = df_orders.merge(df_products_db, on='product_id', how='left')
df_orders = df_orders.merge(df_payments_db, on='payment_method', how='left')
print("Merged surrogate keys into fact_orders...")

print("Preparing final fact_orders dataframe...")
df_orders_final = df_orders[['order_id', 'customer_key', 'product_key', 'payment_key', 'date_key', 'quantity','price','discount', 'total_amount']]
print("Prepared final fact_orders dataframe...")

print("Inserting Data into fact_orders...")
df_orders_final.to_sql('fact_orders', engine, if_exists='append', index=False)
print("Data Insertion into fact_orders Completed...")

