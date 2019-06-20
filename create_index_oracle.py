import cx_Oracle

conn = cx_Oracle.connect('username/password@localhost')
cur = conn.cursor()

def make_index(table, index_name, columns):
    cur.execute('CREATE INDEX 'index_name' on '+table+'('+columns+')')
    conn.commit()
    cur.close()
    conn.close()
