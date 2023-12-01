import time
import pyodbc
import schedule
import warnings
import pandas as pd
from dotenv import dotenv_values
from datetime import datetime, timedelta
from etl_pipeline import load_csv_to_mysql, extract, transform, remove_file, file_path

def run_etl():

	with warnings.catch_warnings():
		warnings.simplefilter("ignore")

		env = dotenv_values(".env")

		# Oracle Variables

		username = env['USER_NAME']
		password = env['PASSWORD']
		host = env['HOST']
		port = env['PORT']
		service_name = env['SERVICE_NAME']

		connection_string = f"DRIVER={{Oracle in OraClient11g_home1}};DBQ={host}:{port}/{service_name};UID={username};PWD={password}"

		try:

			odbc_conn = pyodbc.connect(connection_string)

			odbc_conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
			odbc_conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
			odbc_conn.setencoding(encoding='utf-8')

			csv_file_path = file_path()
			table_name = 'cdr_cpc'
			
			df = extract(table_name, odbc_conn)
			time.sleep(0.5)

			print("Cleaning, transforming and storing data to csv")
			transform(df, csv_file_path)
			time.sleep(0.5)

			print("Loading data from the csv to the mysql database")
			load_csv_to_mysql(table_name, csv_file_path)
			time.sleep(0.5)
			
			print("Removing the csv file from the system")
			remove_file(csv_file_path)


		except Exception as e:
			print(f"Error: {e}")

		finally:

			odbc_conn.close()

# Schedule tasks
schedule.every().day.at('00:00').do(run_etl).tag('etl pipline')
# schedule.every(5).seconds.do(run_etl).tag('etl pipline')

# Run the scheduled tasks continuously
while True:
    schedule.run_pending()
    time.sleep(1)

