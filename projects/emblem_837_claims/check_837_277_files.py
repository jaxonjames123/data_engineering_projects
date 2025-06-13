import pandas as pd
import pyodbc

conn = pyodbc.connect(dsn='somos_redshift_etl')
cursor = conn.cursor()

query_837 = "select distinct file_name from risk_datagov.eh_submitted_837_header"
query_277 = "select distinct file_name from risk_datagov.eh_submitted_277_parsed"

file_list_hn = pd.read_excel('response_file_list.xlsx', usecols="B").apply(lambda x: x.astype(str).str.upper())
file_list_tr = pd.read_excel('response_file_list.xlsx', usecols="A").apply(lambda x: x.astype(str).str.upper())

parsed_tr = pd.read_sql(query_837, conn).apply(lambda x: x.astype(str).str.upper())
parsed_hn = pd.read_sql(query_277, conn).apply(lambda x: x.astype(str).str.upper())
missing_hn = file_list_hn.merge(parsed_hn, how='left', left_on='responsefilename', right_on='file_name')
missing_tr = file_list_tr.merge(parsed_tr, how='left', left_on='filename', right_on='file_name')
missing_tr.to_csv('missing_837_files.csv')
missing_hn.to_csv('missing_277_files.csv')
