import sqlite3
import csv
import re
import pandas as pd

# Intialize SQL Objects
conn = sqlite3.connect(':memory:')
cur = conn.cursor()

# Basically parameters but I'm too lazy to make functions
in_csv = 'test_set2.csv'
out_csv = 'terms_test_set.csv'
out2_csv = 'terms_table_multi.csv'
highest_level = 15
distinct_normal = True

# Create 2 tables, Originals, Terms and TermsMulti
# 4 Coluns: Originals
# 11 Columns: Terms
cur.execute('CREATE TABLE Originals (terms text, sentence text, max_level INTEGER, ID text)')
cur.execute('CREATE TABLE Terms (piece text, alphabetical text, normalized_piece text, freq INTEGER, norm_freq INTEGER, break_level INTEGER, type INTEGER, original text,\
            full_normalized text, sentence text, ID text)')
cur.execute('CREATE TABLE TermsMulti (piece text, type INTEGER, original text, full_normalized text, sentence text, ID text)')
cur.execute('CREATE INDEX idx ON Terms (alphabetical, break_level)')


def make_alph(s, l):
    li = s.split(':'+str(l)+':')
    li.sort()
    return (':'+str(l)+':').join(li)

def no_dups(s, level):
    if level < 1:
        return s
    li = s.split(':'+str(level)+':')
    nd = []
    for item in li:
        n = no_dups(item, level-1)
        if n not in nd:
            nd.append(n)
        
    return (':'+str(level)+':').join(nd)

def nset(l, n=4):
    out = []
    for i in range(len(l)-n):
        p = []
        for j in range(n):
            p.append(out[i+j])
        out.append(p)
    if len(out) < 1:
        out = [l]
        for i in range(n-len(l)):
            out[0].append('')
    return out
def wild(l):
    for i in range(len(l)):
        for j in range(len(l[i])):
            l[i][j] = '%'+l[i][j]+'%'

# Populate Originals
for row in csv.reader(open(in_csv, 'r')):
    delims = re.findall(r':[0-9]+:', row[0])
    if not delims:
        delims = [':0:']
    max_level = max(set(delims), key = lambda s: int(s[1:-1]))
    cur.execute('INSERT INTO Originals VALUES (?, ?, ?, ?)', (row[0], row[2], int(max_level[1:-1]), row[3]))

#Populate Terms
for i in range(highest_level, 0, -1):
    ias = str(i)
    cur.execute('SELECT * FROM Originals WHERE max_level = '+ias)
    for row in cur.fetchall():
        pieces = row[0].split(':'+ias+':')
        if len(pieces) > 1:
            for piece in pieces:
                alph = make_alph(piece, i-1)
                cur.execute('INSERT INTO Terms VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (piece, alph, piece, 0, 0, i, 0, row[0], row[0], row[1], row[3]))
    cur.execute('SELECT * FROM Terms WHERE break_level = '+str(i+1))
    for row in cur.fetchall():
        pieces = row[0].split(':'+ias+':')
        if len(pieces) > 1:
            for piece in pieces:
                alph = make_alph(piece, i-1)
                cur.execute('INSERT INTO Terms VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (piece, alph, piece, 0, 0, i, 0, row[6], row[6], row[9], row[10]))

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
        for old_row in cur.fetchall():
            cur.execute('UPDATE Terms SET piece = REPLACE(piece, ?, ?), alphabetical = REPLACE(alphabetical, ?, ?), normalized_piece = REPLACE(normalized_piece, ?, ?),\
                full_normalized = REPLACE(full_normalized, ?, ?) WHERE break_level > ?', (old_row[0], nr[0], old_row[0], nr[0], old_row[0], nr[0], old_row[0], nr[0], i))
        cur.execute('UPDATE Terms SET normalized_piece = ?, norm_freq = ?, full_normalized = REPLACE(full_normalized, piece, ?) WHERE alphabetical = ? AND break_level = ?', (nr[0], nr[1], nr[0], row[0], i))

if distinct_normal:
    cur.execute('SELECT DISTINCT normalized_piece, break_level FROM Terms WHERE break_level > 1')
    for row in cur.fetchall():
        norm_nd = no_dups(row[0], int(row[1])-1)
        cur.execute('UPDATE Terms SET normalized_piece = ? WHERE normalized_piece = ? AND break_level = ?', (norm_nd, row[0], row[1]))

cur.execute('SELECT DISTINCT full_normalized FROM Terms')
for row in cur.fetchall():
    pieces = re.split(r':[1-9][0-9]*:', row[0])
    pieces = list(set(pieces))
    fours = nset(pieces)
    wild(fours)
    for term in fours:
        cur.execute('SELECT DISTINCT original, full_normalized, sentence, ID FROM Terms WHERE break_level > 1 AND\
            original like ? AND original like ? AND original like ? AND original like ?', tuple(term))
        for item in cur.fetchall():
            cur.execute('INSERT INTO TermsMulti VALUES (?, ?, ?, ?, ?, ?)', (item[1], 1, item[0], item[1], item[2], item[3]))

# Output to csv
pd.read_sql(sql='SELECT * FROM Terms ORDER BY  original DESC, freq ASC, break_level ASC', con=conn).to_csv(out_csv, index=False, sep=',', quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')
pd.read_sql(sql='SELECT * FROM TermsMulti ORDER BY  original DESC', con=conn).to_csv(out2_csv, index=False, sep=',', quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')

