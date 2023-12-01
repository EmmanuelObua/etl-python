import pymysql
import mysql.connector
from dotenv import dotenv_values
from mysql.connector import errorcode
import pandas as pd
import os

def file_path():

	current_directory = os.path.abspath(os.path.dirname(__file__))
	return f'{current_directory}\\cdr_cpc.csv'

def extract(table_name,odbc_conn):

	SQLCommand = f"SELECT * FROM {table_name}"

	print("Extracting data from the data source")
	df = pd.read_sql(SQLCommand, odbc_conn)

	return df

def transform(df,file_path):

	selected_columns = ['FIELD_2', 'FIELD_7', 'FIELD_8', 'FIELD_9']

	selected_df = df[selected_columns]

	# Exclude headers and index
	selected_df.to_csv(file_path, header=False, index=False)

def remove_file(file_path):

	if os.path.exists(file_path):
	    os.remove(file_path)
	    print(f"File {file_path} deleted successfully.")
	else:
	    print(f"File {file_path} does not exist.")

def load_csv_to_mysql(table_name, csv_file_path):

	env = dotenv_values(".env")

	try:
		# Connect to MySQL
	   
		mysql_conn = pymysql.connect(
								host = env['MYSQL_HOST'],
								user = env['MYSQL_USER_NAME'],
								password = env['MYSQL_PASSWORD'],
								database = env['MYSQL_DATABASE'],
								charset='utf8mb4',
								cursorclass=pymysql.cursors.DictCursor
							)

		cursor = mysql_conn.cursor();

		with open(csv_file_path, 'r') as file:

			for line in file:

				values = line.strip().split(',')
				
				# Assuming the order of values in the CSV file matches the order of columns in the table
				placeholders = ', '.join(['%s' for _ in values])

				insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"

				cursor.execute(insert_query, values)

			mysql_conn.commit()

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
		mysql_conn.close()