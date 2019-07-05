import sqlite3
import csv
import re
import pandas as pd

# Intialize Global SQL Objects
conn = sqlite3.connect(':memory:')
cur = conn.cursor()

# Create 2 tables, Originals and Terms
# 4 Coluns: Originals
# 11 Columns: Terms
cur.execute('CREATE TABLE Originals (terms text, sentence text, max_level INTEGER, ID text, snippet text)')
cur.execute('CREATE TABLE Terms (piece text, alphabetical text, normalized_piece text, freq INTEGER, norm_freq INTEGER, break_level INTEGER, type INTEGER, original text,\
            full_normalized text, snippet text, sentence text, ID text)')
cur.execute('CREATE INDEX idx ON Terms (alphabetical, break_level)')


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

def populate_originals(in_csv):
    for row in csv.reader(open(in_csv, 'r')):
        delims = re.findall(r':[0-9]+:', row[0])
        if not delims:
            delims = [':0:']
        max_level = max(set(delims), key = lambda s: int(s[1:-1]))
        cur.execute('INSERT INTO Originals VALUES (?, ?, ?, ?, ?)', (row[0], row[2], int(max_level[1:-1]), row[3], row[1])))

def populate_terms(highest_level):
    for i in range(highest_level, 0, -1):
        ias = str(i)
        cur.execute('SELECT * FROM Originals WHERE max_level = '+ias)
        for row in cur.fetchall():
            pieces = row[0].split(':'+ias+':')
            if len(pieces) > 1:
                for piece in pieces:
                    cur.execute(cur.execute('INSERT INTO Terms VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (':'+piece+':', ':'+piece+':', ':'+piece+':', 0, 0, i, 0, ':'+row[0]+':', ':'+row[0]+':', row[4], row[1], row[3])))
        cur.execute('SELECT * FROM Terms WHERE break_level = '+str(i+1))
        for row in cur.fetchall():
            pieces = row[0].split(':'+ias+':')
            if len(pieces) > 1:
                for piece in pieces:
                    if(len(piece) > 0):
                        piece = piece.strip(':')
                        cur.execute('INSERT INTO Terms VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (':'+piece+':', ':'+piece+':', ':'+piece+':', 0, 0, i, 0, row[7], row[8], row[9], row[10], row[11]))


def normalize(distinct_normal, highest_level):
    # Normalize Data
    for i in range(1, highest_level+1, 1):
        ias = str(i)
        # Calculate frequency
        cur.execute('SELECT DISTINCT piece FROM Terms WHERE break_level = '+ias)
        for row in cur.fetchall():
            freq = int(cur.execute('SELECT COUNT(*) FROM Terms WHERE piece = ? AND break_level = ?', (row[0], i)).fetchall()[0][0])
            alph = ':'+_make_alph(row[0].strip(':'), i-1)+':'
            cur.execute('UPDATE Terms SET freq = ?, alphabetical = ? WHERE piece = ? AND break_level = ?', (freq, alph, row[0], i))

        # Normalize based on highest frequency
        cur.execute('SELECT DISTINCT alphabetical from Terms WHERE break_level = '+ias)
        for row in cur.fetchall():
            cur.execute('SELECT DISTINCT piece, freq FROM Terms WHERE alphabetical = ? AND break_level = ? ORDER BY freq DESC', (row[0], i))
            nr = cur.fetchone()
            for old_row in cur.fetchall():
                cur.execute('UPDATE Terms SET piece = REPLACE(piece, ?, ?), alphabetical = REPLACE(alphabetical, ?, ?), normalized_piece = REPLACE(normalized_piece, ?, ?),\
                    full_normalized = REPLACE(full_normalized, ?, ?) WHERE break_level > ?', (old_row[0], nr[0], old_row[0], nr[0], old_row[0], nr[0], old_row[0], nr[0], i))
            cur.execute('UPDATE Terms SET normalized_piece = ?, norm_freq = ?, full_normalized = REPLACE(full_normalized, piece, ?) WHERE alphabetical = ? AND break_level = ?',
                        (nr[0], nr[1], nr[0], row[0], i))

    # If the flag distinct_normal is set, remove repeat terms in normalized piece
    if distinct_normal:
        cur.execute('SELECT DISTINCT normalized_piece, break_level FROM Terms WHERE break_level > 1')
        for row in cur.fetchall():
            piece = row[0].strip(':')
            norm_nd = _no_dups(piece, int(row[1])-1)
            cur.execute('UPDATE Terms SET normalized_piece = ? WHERE normalized_piece = ? AND break_level = ?', (':'+norm_nd+':', row[0], row[1]))

def clean_table(strip_colons, remove_multi_dots, remove_trailing_dots):
    if strip_colons:
        cur.execute("UPDATE Terms SET original = TRIM(BOTH ':' FROM original), full_normalized = TRIM(BOTH ':' FROM full_normalized)")
    if remove_multi_dots:
        cur.execute("UPDATE Terms SET original = REPLACE(original, '..', '.'), full_normalized = REPLACE(full_normalized, '..', '.')")
        cur.execute("UPDATE Terms SET original = REPLACE(original, '..', '.'), full_normalized = REPLACE(full_normalized, '..', '.')")
    if remove_trailing_dots:
        cur.execute("SELECT DISTINCT original, full_normalized FROM Terms WHERE original LIKE '%.%' OR  full_normalized LIKE '%.%'")
        for row in cur.fetchall():
            o = row[0].replace(',', '') if re.search(r'[A-Za-z]', row[0]) else row[0]
            n = row[1].replace(',', '') if re.search(r'[A-Za-z]', row[1]) else row[1]
            cur.execute("UPDATE Terms SET original = REPLACE(original, original, ?), full_normalized = REPLACE(full_normalized, full_normalized, ?) WHERE original = ? AND full_normalized = ?",
                        (o, n, row[0], row[1]))
            

# Only two required inputs, in_csv and out_csv
# It is NOT recommended to change the other flags
def normalization_program(in_csv, out_csv, highest_level=15, distinct_normal=True, strip_colons=False, remove_multi_dots=True, remove_trailing_dots=True):
    print("Populating Table from CSV...")
    populate_originals(in_csv)
    print("Populating Term Table...")
    populate_terms(highest_level)
    print("Normalizing Terms...")
    normalize(distinct_normal, highest_level)
    print("Cleaning up...")
    clean_table(strip_colons, remove_multi_dots, remove_trailing_dots)
    pd.read_sql(sql='SELECT * FROM Terms ORDER BY original DESC, freq ASC, break_level ASC', con=conn).to_csv(out_csv, index=False, sep=',', quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')

if __name__ == '__main__':
    normalization_program('table_small.csv', 'terms_small.csv')
