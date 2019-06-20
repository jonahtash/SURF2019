import pyodbc

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=LAPTOP-2QA6D1K8\MSSQL;'
                      'Database=randr;'
                      'Trusted_Connection=yes;')
cur = conn.cursor()

def make_index(table, index_name, columns):
    cur.execute('CREATE INDEX 'index_name' on '+table+'('+columns+')')
    conn.commit()
    cur.close()
    conn.close()
