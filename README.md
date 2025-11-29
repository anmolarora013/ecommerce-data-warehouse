# 📦 E-Commerce Data Warehouse (Data Engineering Project)

A complete end-to-end Data Engineering mini-project built using:

- Docker (PostgreSQL + pgAdmin)
- PostgreSQL (Star Schema Data Warehouse)
- Pandas + SQLAlchemy (ETL pipeline)
- Python virtual environment
- Modern data-engineering folder structure

This project simulates how an e-commerce company stores, models, and analyzes operational and sales data.

## 🚀 Project Overview

### ✔ Data Modeling  
- Designed a Star Schema with 4 dimension tables and 1 fact table  
- Surrogate keys used for warehouse-style analytics

### ✔ Database Setup with Docker  
- PostgreSQL and pgAdmin running in containers  
- Auto schema creation using SQL init scripts

### ✔ ETL Pipeline (Python + Pandas)  
- Read raw CSV files  
- Load dimension tables  
- Fetch surrogate keys  
- Transform fact data  
- Insert into fact table  
- Clean, structured ETL pipeline

### ✔ Analytics & SQL  
- Revenue analytics  
- Customer insights  
- Product performance  
- Payment method analysis  
- Window functions for ranking  

## 🏗️ Architecture

Raw CSV → Pandas ETL → PostgreSQL Warehouse → Analytics (pgAdmin)

## 🗂️ Folder Structure
```txt
ecommerce-data-warehouse-project/
├── docker-compose.yml
├── db/
│   └── init/
│       └── Tables_Creation.sql
├── data/
│   ├── dim_customer.csv
│   ├── dim_product.csv
│   ├── dim_payment.csv
│   ├── dim_date.csv
│   └── fact_orders.csv
├── etl/
│   └── load_data.py
├── sql/
│   ├── sales_analysis.sql
│   ├── customer_analysis.sql
│   ├── product_analysis.sql
│   └── payment_analysis.sql
└── README.md
```

## 📊 Star Schema Data Model

dim_customer → fact_orders → dim_product  
                      ↘ dim_date  
                      ↘ dim_payment  

## 🐳 Running the Project

### 1. Start Database Containers
docker compose up -d

### 2. Open pgAdmin  
http://localhost:8080

Email: admin@example.com  
Password: admin  

### 3. Connect pgAdmin to Postgres  
Host: db  
User: de_user  
Password: de_password  
DB: ecommerce_dw  

## 🐍 Running the ETL

Activate virtual environment  
Install dependencies  
Run ETL

python etl/load_sample_data_pandas.py

## 📈 Example SQL Queries

SELECT d.full_date, SUM(f.total_amount)
FROM fact_orders f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date;

## 🔧 Future Enhancements

- Incremental ETL  
- Parquet pipeline  
- Airflow DAG  
- Dashboard  
- Logging  
- dbt models

## ⭐ Why This Project Matters

This project demonstrates real-world DE skills:
data modeling, ETL, Docker, SQL analytics, clean repo structure.
