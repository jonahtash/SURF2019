import cx_Oracle
import csv
import re
import pandas as pd
import atexit

# Intialize Global SQL Objects

# LAPTOP-2QA6D1K8\MSSQL
conn = cx_Oracle.connect('username/password@localhost')
cur = conn.cursor()
cur = conn.cursor()
#atexit.register(conn.commit)
#atexit.register(conn.close)
# Create 2 tables, Originals and Terms
# 4 Coluns: Originals
# 11 Columns: Terms


#cur.execute('CREATE TABLE Originals (terms varchar(900), sentence varchar(900), max_level INTEGER, ID varchar(900))')
#cur.execute('CREATE TABLE Terms (piece varchar(900), alphabetical varchar(900), normalized_piece varchar(900), freq INTEGER, norm_freq INTEGER, break_level INTEGER, type INTEGER, original varchar(900),\
#            full_normalized varchar(900), sentence varchar(900), ID varchar(900))')
#cur.execute('CREATE INDEX idx ON Terms (alphabetical, break_level)')


def _make_alph(s, l):
    li = s.split(':'+str(l)+':')
    li.sort()
    return (':'+str(l)+':').join(li)

def _no_dups(s, level):
    if level < 1:
        return s
    li = s.split(':'+str(level)+':')
    nd = []
    for item in li:
        n = _no_dups(item, level-1)
        if n not in nd:
            nd.append(n)

    return (':'+str(level)+':').join(nd)

def populate_in_table(in_table):
    cur.execute('DROP TABLE IF EXISTS '+in_table)
    cur.execute('CREATE TABLE '+in_table+' (terms varchar(900), sentence varchar(900), max_level INTEGER, ID varchar(900), snippet varchar(900))')
    for row in csv.reader(open('table_small.csv', 'r')):
        delims = re.findall(r':[0-9]+:', row[0])
        if not delims:
            delims = [':0:']
        max_level = max(set(delims), key = lambda s: int(s[1:-1]))
        cur.execute('INSERT INTO '+in_table+' VALUES (?, ?, ?, ?, ?)', (row[0], row[2], int(max_level[1:-1]), row[3], row[1]))

    
def populate_terms(in_table, out_table, highest_level):
    cur.execute('CREATE TABLE "'+out_table+'" (piece varchar(900), alphabetical varchar(900), normalized_piece varchar(900), freq INTEGER, norm_freq INTEGER, break_level INTEGER, type INTEGER, original varchar(900),\
            full_normalized varchar(900), snippet varchar(900), sentence varchar(900), ID varchar(900)); exception when others then if SQLCODE = -955 then null; else raise; end if;')
    for i in range(highest_level, 0, -1):
        ias = str(i)
        cur.execute('SELECT * FROM '+in_table+' WHERE max_level = '+ias)
        for row in cur.fetchall():
            pieces = row[0].split(':'+ias+':')
            if len(pieces) > 1:
                for piece in pieces:
                    cur.execute('INSERT INTO '+out_table+' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (':'+piece+':', ':'+piece+':', ':'+piece+':', 0, 0, i, 0, ':'+row[0]+':', ':'+row[0]+':', row[4], row[1], row[3]))
        cur.execute('SELECT * FROM '+out_table+' WHERE break_level = '+str(i+1))
        for row in cur.fetchall():
            pieces = row[0].split(':'+ias+':')
            if len(pieces) > 1:
                for piece in pieces:
                    if(len(piece) > 0):
                        piece = piece.strip(':')
                        cur.execute('INSERT INTO '+out_table+' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (':'+piece+':', ':'+piece+':', ':'+piece+':', 0, 0, i, 0, row[7], row[8], row[9], row[10], row[11]))

def normalize(out_table, distinct_normal, highest_level):
    # Normalize Data
    for i in range(1, highest_level+1, 1):
        ias = str(i)
        # Calculate frequency
        cur.execute('SELECT DISTINCT piece FROM '+out_table+' WHERE break_level = '+ias)
        for row in cur.fetchall():
            freq = int(cur.execute('SELECT COUNT(*) FROM '+out_table+' WHERE piece = ? AND break_level = ?', (row[0], i)).fetchall()[0][0])
            alph = ':'+_make_alph(row[0].strip(':'), i-1)+':'
            cur.execute('UPDATE '+out_table+' SET freq = ?, alphabetical = ? WHERE piece = ? AND break_level = ?', (freq, alph, row[0], i))

        # Normalize based on highest frequency
        cur.execute('SELECT DISTINCT alphabetical from '+out_table+' WHERE break_level = '+ias)
        for row in cur.fetchall():
            cur.execute('SELECT DISTINCT piece, freq FROM '+out_table+' WHERE alphabetical = ? AND break_level = ? ORDER BY freq DESC', (row[0], i))
            nr = cur.fetchone()
            for old_row in cur.fetchall():
                cur.execute('UPDATE '+out_table+' SET piece = REPLACE(piece, ?, ?), alphabetical = REPLACE(alphabetical, ?, ?), normalized_piece = REPLACE(normalized_piece, ?, ?),\
                    full_normalized = REPLACE(full_normalized, ?, ?) WHERE break_level > ?', (old_row[0], nr[0], old_row[0], nr[0], old_row[0], nr[0], old_row[0], nr[0], i))
            cur.execute('UPDATE '+out_table+' SET normalized_piece = ?, norm_freq = ?, full_normalized = REPLACE(full_normalized, piece, ?) WHERE alphabetical = ? AND break_level = ?',
                        (nr[0], nr[1], nr[0], row[0], i))

    # If the flag distinct_normal is set, remove repeat terms in normalized piece
    if distinct_normal:
        cur.execute('SELECT DISTINCT normalized_piece, break_level FROM '+out_table+' WHERE break_level > 1')
        for row in cur.fetchall():
            piece = row[0].strip(':')
            norm_nd = _no_dups(piece, int(row[1])-1)
            cur.execute('UPDATE '+out_table+' SET normalized_piece = ? WHERE normalized_piece = ? AND break_level = ?', (':'+norm_nd+':', row[0], row[1]))

def clean_table(out_table, strip_colons, remove_multi_dots, remove_trailing_dots):
    if strip_colons:
        cur.execute("UPDATE "+out_table+" SET original = TRIM(BOTH ':' FROM original), full_normalized = TRIM(BOTH ':' FROM full_normalized)")
    if remove_multi_dots:
        cur.execute("UPDATE "+out_table+" SET original = REPLACE(original, '..', '.'), full_normalized = REPLACE(full_normalized, '..', '.')")
        cur.execute("UPDATE "+out_table+" SET original = REPLACE(original, '..', '.'), full_normalized = REPLACE(full_normalized, '..', '.')")
    if remove_trailing_dots:
        cur.execute("SELECT DISTINCT original, full_normalized FROM "+out_table+" WHERE original LIKE '%.%' OR  full_normalized LIKE '%.%'")
        for row in cur.fetchall():
            o = row[0].replace(',', '') if re.search(r'[A-Za-z]', row[0]) else row[0]
            n = row[1].replace(',', '') if re.search(r'[A-Za-z]', row[1]) else row[1]
            cur.execute("UPDATE "+out_table+" SET original = REPLACE(original, original, ?), full_normalized = REPLACE(full_normalized, full_normalized, ?) WHERE original = ? AND full_normalized = ?",
                        (o, n, row[0], row[1]))
            

# Only two required inputs, in_csv and out_csv
# It is NOT recommended to change the other flags
def normalization_program(in_table, out_table, highest_level=15, distinct_normal=True, strip_colons=False, remove_multi_dots=True, remove_trailing_dots=True):
    #print("Populating Originals Table...")
    #populate_in_table(in_table)
    print("Populating Term Table...")
    populate_terms(in_table, out_table, highest_level)
    print("Normalizing Terms...")
    normalize(out_table, distinct_normal, highest_level)
    print("Cleaning up...")
    clean_table(out_table, strip_colons, remove_multi_dots, remove_trailing_dots)
    conn.commit()
    conn.close()
    
if __name__ == '__main__':
    normalization_program('OriginalsSmall', 'TermsSmall')
