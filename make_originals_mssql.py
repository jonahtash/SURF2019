import pyodbc
import csv

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=LAPTOP-2QA6D1K8\MSSQL;'
                      'Database=randr;'
                      'Trusted_Connection=yes;')
cur = conn.cursor()

def populate_originals(in_csv, out_table):
    for row in csv.reader(open(in_csv, 'r')):
        delims = re.findall(r':[0-9]+:', row[0])
        if not delims:
            delims = [':0:']
        max_level = max(set(delims), key = lambda s: int(s[1:-1]))
        cur.execute('INSERT INTO '+out_table+' VALUES (?, ?, ?, ?)', (row[0], row[2], int(max_level[1:-1]), row[3]))
