"""
ETL Pipeline for Superstore Data
--------------------------------
Extracts data from 'superstore.csv', transforms it (normalization, feature engineering),
and loads it into SQL Server 'db_superstore' using a Star Schema.

Features:
- Idempotent: Can be run multiple times without error.
- Modular: Separated Extract, Transform, and Load steps.
- Robust: Handles constraints and schema updates.
"""

import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine, text
import urllib.parse
import sys
import os

# Configuration
SERVER = 'localhost'
DATABASE = 'db_superstore'
DRIVER = 'ODBC Driver 17 for SQL Server'
CSV_PATH = 'superstore.csv' # Assumes script is run from project root or path is relative

def get_db_connection():
    """Establishes connection to SQL Server."""
    try:
        params = urllib.parse.quote_plus(
            f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
        )
        conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
        engine = create_engine(conn_str)
        print("Connected to SQL Server.")
        return engine
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

def extract_data(file_path):
    """Reads the CSV file with error handling for encoding."""
    print(f"Extracting data from {file_path}...")
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        sys.exit(1)
        
    try:
        df = pd.read_csv(file_path, encoding='latin1')
    except Exception:
        print("Latin1 encoding failed, trying default...")
        df = pd.read_csv(file_path)
    
    print(f"Extracted {len(df)} rows.")
    return df

def transform_data(df):
    """Applies normalization and feature engineering."""
    print("Transforming data...")
    
    # 1. Normalization
    df.columns = df.columns.str.lower().str.replace('.', '_').str.replace(' ', '_').str.replace('-', '_')
    
    # Normalize ID columns to uppercase for SQL case-insensitivity
    if 'customer_id' in df.columns:
        df['customer_id'] = df['customer_id'].astype(str).str.upper()
    if 'product_id' in df.columns:
        df['product_id'] = df['product_id'].astype(str).str.upper()
    
    # 2. Type Conversion
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    df['ship_date'] = pd.to_datetime(df['ship_date'], errors='coerce')
    
    # 3. Feature Engineering from Notebook
    # Shipping Days
    df['shipping_days'] = (df['ship_date'] - df['order_date']).dt.days
    df['shipping_days'] = df['shipping_days'].fillna(0)
    
    # Profit Margin
    with np.errstate(divide='ignore', invalid='ignore'):
        df['profit_margin'] = (df['profit'] / df['sales']) * 100
    df['profit_margin'] = df['profit_margin'].replace([np.inf, -np.inf], 0).fillna(0)
    
    # Is Profitable
    df['is_profitable'] = np.where(df['profit'] > 0, 1, 0)
    
    # Discount Category
    def categorize_discount(discount):
        if discount == 0: return 'No Discount'
        elif discount < 0.2: return 'Low'
        else: return 'High'
    df['discount_category'] = df['discount'].apply(categorize_discount)
    
    # Order Value Segment
    sales_threshold = df['sales'].quantile(0.75)
    df['order_value_segment'] = np.where(df['sales'] > sales_threshold, 'High Value', 'Standard Value')
    
    # Extract Dimensional Integers (for DateKey)
    df['date_key'] = df['order_date'].dt.strftime('%Y%m%d').fillna(0).astype(np.int64)

    # Truncate string columns to fit schema (NVARCHAR(255))
    str_cols = df.select_dtypes(include=['object']).columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.slice(0, 255)

    return df

def insert_data(engine, df, table_name, chunksize=1000):
    print(f"Loading {table_name} ({len(df)} rows) via custom insert...")
    columns = df.columns.tolist()
    placeholders = ",".join(["?" for _ in columns])
    sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
    
    # Convert df to list of tuples, handling NaN/None
    # Replace NaN with None for SQL NULL
    data = df.where(pd.notnull(df), None).values.tolist()
    
    conn = engine.raw_connection()
    cursor = conn.cursor()
    try:
        total = len(data)
        for i in range(0, total, chunksize):
            chunk = data[i:i+chunksize]
            cursor.executemany(sql, chunk)
            conn.commit()
        print(f"Successfully loaded {table_name}")
    except Exception as e:
        print(f"Failed to load {table_name}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()

def create_dimensions(df, engine):
    """Creates and loads dimension tables."""
    print("Creating Dimensions...")
    
    # Helper to load safely
    def load_dim(data, table_name, pk_col):
        insert_data(engine, data, table_name, chunksize=1000)

    # --- Dim_Date ---
    min_date = df['order_date'].min()
    max_date = df['order_date'].max()
    date_range = pd.date_range(start=min_date, end=max_date)
    dim_date = pd.DataFrame({'Date': date_range})
    dim_date['DateKey'] = dim_date['Date'].dt.strftime('%Y%m%d').astype(int)
    dim_date['Year'] = dim_date['Date'].dt.year
    dim_date['Quarter'] = dim_date['Date'].dt.quarter
    dim_date['Month'] = dim_date['Date'].dt.month
    dim_date['MonthName'] = dim_date['Date'].dt.month_name()
    dim_date['Day'] = dim_date['Date'].dt.day
    dim_date['Weekday'] = dim_date['Date'].dt.day_name()
    dim_date['Weekday'] = dim_date['Date'].dt.day_name()
    dim_date['IsWeekend'] = dim_date['Date'].dt.dayofweek.isin([5, 6])
    
    # Add scalar "Unknown" date
    unknown_date = pd.DataFrame([{
        'DateKey': 0, 'Date': None, 'Year': None, 'Quarter': None, 'Month': None,
        'MonthName': 'Unknown', 'Day': None, 'Weekday': None, 'IsWeekend': None
    }])
    dim_date = pd.concat([unknown_date, dim_date], ignore_index=True)
    
    # Assert Uniqueness
    assert dim_date['DateKey'].is_unique, "Dim_Date DateKey is not unique!"
    
    # Dim_Date handled slightly differently (replace logic)
    load_dim(dim_date, 'Dim_Date', 'DateKey')

    # --- Dim_Customer ---
    dim_customer = df[['customer_id', 'customer_name', 'segment']].drop_duplicates()
    dim_customer.columns = ['CustomerID', 'CustomerName', 'Segment']
    # Deduplicate by PK to avoid IntegrityError
    dim_customer = dim_customer.drop_duplicates(subset=['CustomerID'])
    load_dim(dim_customer, 'Dim_Customer', 'CustomerID')

    # --- Dim_Product ---
    dim_product = df[['product_id', 'product_name', 'category', 'sub_category']].drop_duplicates()
    dim_product.columns = ['ProductID', 'ProductName', 'Category', 'SubCategory']
    # Deduplicate by PK
    dim_product = dim_product.drop_duplicates(subset=['ProductID'])
    load_dim(dim_product, 'Dim_Product', 'ProductID')

    # --- Dim_ShipMode ---
    dim_shipmode = df[['ship_mode']].drop_duplicates().reset_index(drop=True)
    dim_shipmode['ShipModeID'] = dim_shipmode.index + 1
    dim_shipmode.rename(columns={'ship_mode': 'ShipMode'}, inplace=True)
    dim_shipmode = dim_shipmode[['ShipModeID', 'ShipMode']] # Reorder columns
    load_dim(dim_shipmode, 'Dim_ShipMode', 'ShipModeID')

    # --- Dim_Location ---
    # Smart handling of location columns
    loc_cols = ['city', 'state', 'country', 'region', 'market', 'postal_code']
    available_cols = [c for c in loc_cols if c in df.columns]
    
    dim_location = df[available_cols].drop_duplicates().reset_index(drop=True)
    dim_location['LocationID'] = dim_location.index + 1
    
    if 'postal_code' in dim_location.columns:
        dim_location.rename(columns={'postal_code': 'PostalCode'}, inplace=True)
    
    # Map to schema names
    schema_map = {'city': 'City', 'state': 'State', 'country': 'Country', 'region': 'Region', 'market': 'Market'}
    dim_location.rename(columns=schema_map, inplace=True)
    
    # Order columns
    cols = ['LocationID'] + [c for c in dim_location.columns if c != 'LocationID']
    dim_location = dim_location[cols]
    
    load_dim(dim_location, 'Dim_Location', 'LocationID')
    
    return dim_shipmode, dim_location

def load_face_sales(df, dim_shipmode, dim_location, engine):
    """Creates and loads the Fact_Sales table."""
    print("Creating Fact_Sales...")
    
    fact = df.copy()
    
    # Join Keys
    fact = fact.merge(dim_shipmode, left_on='ship_mode', right_on='ShipMode', how='left')
    
    loc_cols = ['city', 'state', 'country', 'region', 'market', 'postal_code']
    left_keys = [c for c in loc_cols if c in df.columns]
    
    schema_map = {'city': 'City', 'state': 'State', 'country': 'Country', 'region': 'Region', 'market': 'Market'}
    right_keys = []
    for k in left_keys:
        if k == 'postal_code': right_keys.append('PostalCode')
        elif k in schema_map: right_keys.append(schema_map[k])
        else: right_keys.append(k)
        
    fact = fact.merge(dim_location, left_on=left_keys, right_on=right_keys, how='left')
    
    fact.rename(columns={
        'row_id': 'RowID', 'order_id': 'OrderID', 'sales': 'Sales', 'quantity': 'Quantity',
        'discount': 'Discount', 'profit': 'Profit', 'shipping_cost': 'ShippingCost',
        'date_key': 'DateKey', 'customer_id': 'CustomerID', 'product_id': 'ProductID'
    }, inplace=True)
    
    # Deduplicate RowID for PK constraint
    fact = fact.drop_duplicates(subset=['RowID'])
    
    fact_cols = [
        'RowID', 'OrderID', 'DateKey', 'CustomerID', 'ProductID', 'LocationID', 'ShipModeID',
        'Sales', 'Quantity', 'Discount', 'Profit', 'ShippingCost',
        'shipping_days', 'profit_margin', 'is_profitable', 'discount_category', 'order_value_segment'
    ]
    
    final_fact = fact[fact_cols]
    
    # Custom Insert for Fact
    insert_data(engine, final_fact, 'Fact_Sales', chunksize=1000)

def execute_schema_script(engine, script_path):
    """Executes the DDL script to create tables."""
    print(f"Executing schema script {script_path}...")
    try:
        with open(script_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
            
        sql_script = sql_script.replace('\ufeff', '')
        statements = sql_script.split(';')
        
        conn = engine.raw_connection()
        try:
            cursor = conn.cursor()
            for statement in statements:
                if statement.strip():
                    cursor.execute(statement)
            conn.commit()
            print("Schema created successfully.")
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Schema execution failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def main():
    print("Starting ETL Process...")
    
    # 0. Connect
    engine = get_db_connection()
    
    # 1. Initialize Schema (Drop & Create)
    # This will drop tables first because schema.sql has DROP if exists
    # 1. Initialize Schema (Drop & Create)
    # This will drop tables first because schema.sql has DROP if exists
    print("Initializing Database Schema...")
    try:
        # Re-implementing schema execution directly here for standalone script
        # based on successful setup_db.py logic
        
        script_path = 'sql/schema.sql'
        if not os.path.exists(script_path):
             # Handle case where run from root
             if os.path.exists(os.path.join('sql', 'schema.sql')):
                 script_path = os.path.join('sql', 'schema.sql')
             elif os.path.exists(os.path.join('..', 'sql', 'schema.sql')):
                 script_path = os.path.join('..', 'sql', 'schema.sql')

        with open(script_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # Remove BOM if present
        sql_script = sql_script.replace('\ufeff', '')
        statements = sql_script.split(';')
        
        conn = engine.raw_connection()
        cursor = conn.cursor()
        try:
            for statement in statements:
                if statement.strip():
                    cursor.execute(statement)
            conn.commit()
            print("Schema created successfully.")
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Schema setup failed: {e}")
        sys.exit(1)

    # 2. Extract
    df = extract_data(CSV_PATH)
    
    # 3. Transform
    df_transformed = transform_data(df)
    
    # 4. Load Dimensions
    dim_shipmode, dim_location = create_dimensions(df_transformed, engine)
    
    # 5. Load Fact
    load_face_sales(df_transformed, dim_shipmode, dim_location, engine)
    
    print("ETL Pipeline Completed Successfully.")

if __name__ == "__main__":
    main()
