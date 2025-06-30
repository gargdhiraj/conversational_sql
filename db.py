import mysql.connector
import pandas as pd

# MySQL connection config
config = {
    'host': '173.212.200.22',         # e.g., 'localhost' or '127.0.0.1'
    'user': 'root',     # e.g., 'root'
    'password': 'Allindex123',
    'database': 'new_indexdb'  # e.g., 'stock_db'
}

def run_query(query):
    # try:
    #     connection = mysql.connector.connect(**config)
    #     cursor = connection.cursor()
    #     cursor.execute(query)
    #     results = cursor.fetchall()
    #     return results
    # except mysql.connector.Error as err:
    #     print(f"Error: {err}")
    # finally:
    #     if connection.is_connected():
    #         cursor.close()
    #         connection.close()
    try:
        connection = mysql.connector.connect(**config)
        # Load results into DataFrame
        df = pd.read_sql(query, con=connection)
        return df

    finally:
        if connection.is_connected():
            connection.close()


