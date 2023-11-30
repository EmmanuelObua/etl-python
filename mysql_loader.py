import mysql.connector
from mysql.connector import errorcode

def load_csv_to_mysql(username, password, host, port, database_name, table_name, csv_file_path):

    # MySQL connection configuration
    config = {
        'user': username,
        'password': password,
        'host': host,
        'port': port,
        'database': database_name,
        'raise_on_warnings': True,
        'allow_local_infile': True
    }

    try:
        # Connect to MySQL
        cnx = mysql.connector.connect(**config)

        # Create a cursor
        cursor = cnx.cursor()

        # Load CSV data into the MySQL table
        load_data_query = f"""
        LOAD DATA INFILE '{csv_file_path}' 
        INTO TABLE {table_name} 
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        """

        cursor.execute(load_data_query)

        cnx.commit()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Access denied. Check your MySQL username and password.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist.")
        else:
            print(f"Error: {err}")
    finally:
        # Close the connections
        cursor.close()
        cnx.close()