import mysql.connector
import pandas as pd

db_password = None  # This should be replaced with the actual password or fetched securely

# MySQL connection config

def run_query(query):
    config = {
    'host': '173.212.200.22',         # e.g., 'localhost' or '127.0.0.1'
    'user': 'root',     # e.g., 'root'
    'password': db_password,
    'database': 'new_indexdb'  # e.g., 'stock_db'
    }

    try:
        connection = mysql.connector.connect(**config)
        # Load results into DataFrame
        df = pd.read_sql(query, con=connection)
        return df

    finally:
        if connection.is_connected():
            connection.close()


