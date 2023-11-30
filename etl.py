import time
import pyodbc
import warnings
import pandas as pd
from tqdm import tqdm
from dotenv import dotenv_values
from mysql_loader import load_csv_to_mysql

with warnings.catch_warnings():
	warnings.simplefilter("ignore")

	config = dotenv_values(".env")

	# Oracle Variables

	username = config['USER_NAME']
	password = config['PASSWORD']
	host = config['HOST']
	port = config['PORT']
	service_name = config['SERVICE_NAME']

	# Mysql Variables

	mysql_username = config['MYSQL_USER_NAME']
	mysql_password = config['MYSQL_PASSWORD']
	mysql_host = config['MYSQL_HOST']
	mysql_port = config['MYSQL_PORT']
	mysql_database = config['MYSQL_DATABASE']
	mysql_table = config['MYSQL_TABLE']

	connection_string = f"DRIVER={{Oracle in OraClient11g_home1}};DBQ={host}:{port}/{service_name};UID={username};PWD={password}"

	try:

		connection = pyodbc.connect(connection_string)

		connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
		connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
		connection.setencoding(encoding='utf-8')

		total_steps = 1

		# Use tqdm to create a progress bar
		for step in tqdm(range(total_steps), desc="Processing ..."):

			# SQLCommand = 'SELECT * FROM emmab.cdr_cpc'

			# df = pd.read_sql(SQLCommand, connection)

			# selected_columns = ['FIELD_2', 'FIELD_7', 'FIELD_8', 'FIELD_9']

			# selected_df = df[selected_columns]

			csv_file = f'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\cdr_cpc.csv'

			# # Exclude headers and index
			# selected_df.to_csv(csv_file, header=False, index=False)

			print("Loading data from the csv to the mysql database ...")

			load_csv_to_mysql(
				mysql_username, 
				mysql_password, 
				mysql_host, 
				mysql_port, 
				mysql_database, 
				mysql_table, 
				'cdr_cpc.csv'
			)

		
		time.sleep(0.5)

	except Exception as e:
		print(f"Error: {e}")

	finally:

		connection.close()

