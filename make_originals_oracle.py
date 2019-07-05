import cx_Oracle
import csv

conn = cx_Oracle.connect('username/password@localhost')
cur = conn.cursor()

def populate_originals(in_csv, out_table):
    cur.execute('create table "'+out_table+'" (terms varchar(900), sentence varchar(900), max_level INTEGER, ID varchar(900), snippet varchar(900)); exception when others then if SQLCODE = -955 then null; else raise; end if;')
    for row in csv.reader(open(in_csv, 'r')):
        delims = re.findall(r':[0-9]+:', row[0])
        if not delims:
            delims = [':0:']
        max_level = max(set(delims), key = lambda s: int(s[1:-1]))
        cur.execute('INSERT INTO '+in_table+' VALUES (?, ?, ?, ?, ?)', (row[0], row[2], int(max_level[1:-1]), row[3], row[1]))
    conn.commit()
    conn.close()
