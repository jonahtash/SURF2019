import sqlite3
import csv
import re
import pandas as pd

# Intialize SQL Objects
conn = sqlite3.connect(':memory:')
cur = conn.cursor()

# Basically parameters but I'm too lazy to make functions
in_csv = 'table.csv'
out_csv = 'term_table.csv'
highest_level = 15

# Create 2 tables, Originals and Terms
cur.execute('CREATE TABLE Originals (terms text, max_level INTEGER)')
cur.execute('CREATE TABLE Terms (piece text, normalized_piece text, orignal text, break_level INTEGER)')

# Populate Originals
for row in csv.reader(open(in_csv, 'r')):
    delims = re.findall(r':[0-9]+:', row[0])
    if not delims:
        delims = [':0:']
    max_level = max(set(delims), key = lambda s: int(s[1:-1]))
    cur.execute('INSERT INTO Originals VALUES (?, ?)', (row[0], int(max_level[1:-1])))

#Populate Terms
for i in range(highest_level, 0, -1):
    ias = str(i)
    cur.execute('SELECT * FROM Originals WHERE max_level = '+ias)
    for row in cur.fetchall():
        pieces = row[0].split(':'+ias+':')
        if len(pieces) > 1:
            for piece in pieces:
                cur.execute('INSERT INTO Terms VALUES (?, ?, ?, ?)', (piece, piece, row[0], i))
    cur.execute('SELECT * FROM Terms WHERE break_level = '+str(i+1))
    for row in cur.fetchall():
        pieces = row[0].split(':'+ias+':')
        if len(pieces) > 1:
            for piece in pieces:
                cur.execute('INSERT INTO Terms VALUES (?, ?, ?, ?)', (piece, piece, row[1], i))
# Normalize Data
for i in range(1, highest_level+1, 1):
    ias = str(i)
    cur.execute('SELECT DISTINCT piece FROM Terms WHERE break_level = '+ias)
    for row in cur.fetchall():
        roots = row[0].split(':'+str(i-1)+':')
        if len(roots) > 1:
            b_a = roots[1]+':'+str(i-1)+':'+roots[0]
            count_a_b = int(cur.execute('SELECT COUNT(*) FROM Terms WHERE piece = ? AND break_level = ?', (row[0], i)).fetchall()[0][0])
            count_b_a = int(cur.execute('SELECT COUNT(*) FROM Terms WHERE piece = ? AND break_level = ?', (b_a, i)).fetchall()[0][0])
            n_phrase = row[0]
            if count_b_a > count_a_b:
                n_phrase = roots[1]+':'+str(i-1)+':'+roots[0]
                print(row[0], n_phrase)
                cur.execute('UPDATE Terms SET normalized_piece = ? WHERE piece = ? AND break_level = ?', (n_phrase, row[0], i))
                cur.execute('UPDATE Terms SET piece = REPLACE(piece, ?, ?),\
                        normalized_piece = REPLACE(normalized_piece, ?, ?) WHERE break_level > ?', (row[0], n_phrase, row[0], n_phrase, i))


# Output to csv
pd.read_sql(sql='SELECT * FROM terms ORDER BY break_level DESC', con=conn).to_csv(out_csv, index=False, sep=',', quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')
