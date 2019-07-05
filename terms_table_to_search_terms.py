import pyodbc
import re

conn1 = pyodbc.connect('Driver={SQL Server};'
                      'Server=LAPTOP-2QA6D1K8\MSSQL;'
                      'Database=randr;'
                      'Trusted_Connection=yes;')
cur1 = conn1.cursor()
conn2 = pyodbc.connect('Driver={SQL Server};'
                      'Server=LAPTOP-2QA6D1K8\MSSQL;'
                      'Database=AutoFill;'
                      'Trusted_Connection=yes;')
cur2 = conn2.cursor()

def make_search_term_table(in_table, new_table_prefix):
    cur2.execute("if not exists (select * from sysobjects where name='"+new_table_prefix+"_db_autofill' and xtype='U') create table "+new_table_prefix+
                "_db_autofill (FINAL_TERM nvarchar(255), UPPER_TERM nvarchar(255), ID nvarchar(255), term_from nvarchar(MAX), type int,  PK float, length_term_from float, length_FINAL_TERM int)")
    i = 1
    cur1.execute('SELECT DISTINCT randr_fragment, snippet_randr_found, type, ID FROM '+in_table+' WHERE type<2')
    for row in cur1.fetchall():
        final = re.split(r':[0-9]+:', row[0])
        final_joined = ' '.join(final)
        cur2.execute('INSERT INTO '+new_table_prefix+'_db_autofill VALUES (?, ?, ?,  ?, ?, ?, ?, ?)', (final_joined, final_joined, row[3], row[1], row[2], i, row[1].count(' '), len(final)))
        i += 1
    conn2.commit()
    cur2.close()
    conn2.close()
if __name__ == '__main__':
    make_search_term_table('TypesAll', 'pub')
