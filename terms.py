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
out2_csv = 'terms_table_multi.csv'
highest_level = 15

# Create 3 tables, Originals, Terms and TermsMulti
# 3 Coluns: Originals
# 8 Columns: Terms
# 5 Columns: TermsMulti
cur.execute('CREATE TABLE Originals (terms text, max_level INTEGER, ID text)')
cur.execute('CREATE TABLE Terms (piece text, alphabetical text, normalized_piece text, freq INTEGER, norm_freq INTEGER, break_level INTEGER, original text, ID text)')
cur.execute('CREATE TABLE TermsMulti (piece text, break_level INTEGER, original text, source INTEGER, ID text)')

cur.execute('CREATE INDEX idx ON Terms (alphabetical, break_level)')

# Populate Originals
for row in csv.reader(open(in_csv, 'r')):
    delims = re.findall(r':[0-9]+:', row[0])
    if not delims:
        delims = [':0:']
    max_level = max(set(delims), key = lambda s: int(s[1:-1]))
    cur.execute('INSERT INTO Originals VALUES (?, ?, ?)', (row[0], int(max_level[1:-1]), row[3]))

#Populate Terms
for i in range(highest_level, 0, -1):
    ias = str(i)
    cur.execute('SELECT * FROM Originals WHERE max_level = '+ias)
    for row in cur.fetchall():
        pieces = row[0].split(':'+ias+':')
        if len(pieces) > 1:
            for piece in pieces:
                alph_list = piece.split(':'+str(i-1)+':')
                alph_list.sort()
                alph = (':'+str(i-1)+':').join(alph_list)
                cur.execute('INSERT INTO Terms VALUES (?, ?, ?, ?, ?, ?, ?, ?)', (piece, alph, piece, 0, 0, i, row[0], row[2]))
    cur.execute('SELECT * FROM Terms WHERE break_level = '+str(i+1))
    for row in cur.fetchall():
        pieces = row[0].split(':'+ias+':')
        if len(pieces) > 1:
            for piece in pieces:
                alph_list = piece.split(':'+str(i-1)+':')
                alph_list.sort()
                alph = (':'+str(i-1)+':').join(alph_list)
                cur.execute('INSERT INTO Terms VALUES (?, ?, ?, ?, ?, ?, ?, ?)', (piece, alph, piece, 0, 0, i, row[6], row[7]))
# Normalize Data
for i in range(1, highest_level+1, 1):
    ias = str(i)
    # Calculate frequency
    cur.execute('SELECT DISTINCT piece FROM Terms WHERE break_level = '+ias)
    for row in cur.fetchall():
        freq = int(cur.execute('SELECT COUNT(*) FROM Terms WHERE piece = ? AND break_level = ?', (row[0], i)).fetchall()[0][0])
        cur.execute('UPDATE Terms SET freq = ? WHERE piece = ? AND break_level = ?', (freq, row[0], i))

    # Normalize based on highest frequency
    cur.execute('SELECT DISTINCT alphabetical from Terms WHERE break_level = '+ias)
    for row in cur.fetchall():
        cur.execute('SELECT DISTINCT piece, freq FROM Terms WHERE alphabetical = ? AND break_level = ? ORDER BY freq DESC', (row[0], i))
        nr = cur.fetchone()
        cur.execute('UPDATE Terms SET normalized_piece = ?, norm_freq = ? WHERE alphabetical = ? AND break_level = ?', (nr[0], nr[1], row[0], i))
        for old_row in cur.fetchall():
            cur.execute('UPDATE Terms SET piece = REPLACE(piece, ?, ?),\
                normalized_piece = REPLACE(normalized_piece, ?, ?) WHERE break_level > ?', (old_row[0], nr[0], old_row[0], nr[0], i))

# Populate TermsMulti
cur.execute('SELECT DISTINCT piece, break_level, original, ID FROM Terms WHERE break_level <= 3')
for row in cur.fetchall():
    new_piece = ' ' + re.sub(r':[0-9]+:', ' ', row[0]) + ' '
    cur.execute('INSERT INTO TermsMulti VALUES (?, ?, ?, ?, ?)', (new_piece, row[1], row[2], 0, row[3]))

#cur.execute('SELECT DISTINCT * FROM TermsMulti')
#for row in cur.fetchall():
    

# Output to csv
pd.read_sql(sql='SELECT * FROM terms ORDER BY original DESC, break_level ASC', con=conn).to_csv(out_csv, index=False, sep=',', quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')
pd.read_sql(sql='SELECT * FROM TermsMulti ORDER BY original DESC, break_level ASC', con=conn).to_csv(out2_csv, index=False, sep=',', quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')
