import pyodbc
import csv
import re
import pandas as pd
import atexit

# Intialize Global SQL Objects
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=LAPTOP-2QA6D1K8\MSSQL;'
                      'Database=randr;'
                      'Trusted_Connection=yes;')



pd.read_sql(sql='SELECT DISTINCT * FROM TypesALLSmall ORDER BY type', con=conn).to_csv("example_table.csv", index=False, sep=',', quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')
