import sqlite3
import csv
import re
import pandas as pd

# Intialize Global SQL Objects
conn = sqlite3.connect(':memory:')
cur = conn.cursor()

# Create 4 tables Terms, FragmentsLT3(Fragments less than 3), Type1SearchTerms, Type2SearchTerms
cur.execute('CREATE TABLE Terms (full_normalized text, original text, ID text)')
cur.execute('CREATE TABLE FragmentsLT3 (piece text, original text, ID text)')
cur.execute('CREATE TABLE Type1SearchTerms (piece text, original text, ID text, terms text)')
cur.execute('CREATE TABLE Type2SearchTerms (piece text, ID text)')

cur.execute('CREATE INDEX idx_terms ON Terms (original, ID)')
cur.execute('CREATE INDEX idx_frag ON FragmentsLT3 (original, ID)')

# Helper to generate SQL LIKE statement from list of terms
# Statement will return from from Terms where a sentence contains every term in list l in no paticular order
# Ex. in: [a, b, c]
#    out: original LIKE '%:a:%' AND original LIKE '%:b:%' original LIKE '%:c:%'
def _make_like(l):
    return ("original LIKE '%:" + (":%' AND original LIKE '%:").join(l) + ":%'")

# Helper to generate SQL INTERSECT statement for type 2 terms
# Statement will return all IDs that contain every term in list l at least once
# Ex. in: [a, b, c]
#    out:  SELECT DISTINCT ID FROM Terms WHERE original LIKE '%:a:%' INTERSECT SELECT DISTINCT ID FROM Terms WHERE original LIKE '%:b:%' INTERSECT SELECT DISTINCT ...
def _make_int(l):
    return ("SELECT DISTINCT ID FROM Terms WHERE original LIKE '%:" + (":%' INTERSECT SELECT DISTINCT ID FROM Terms WHERE original LIKE '%:").join(l) + ":%'")

# Read in csv generated by terms.py program, INSERT into table Terms
def populate_terms(in_csv):
    for row in csv.reader(open(in_csv, 'r')):
        cur.execute('INSERT INTO Terms VALUES (?, ?, ?)', (row[8], row[7], row[10]))

# Generates table FragmentsLT3 by splitting each DISTINCT full_normalized at all levels >= 3, and
# inserting these pieces (fragments), along with metadata into the table.
# "symbol" terms, pieces that are in all caps at any level i.e. DNA, SQL are also inserted as individual fragments
def populate_fragments():
    cur.execute('SELECT DISTINCT full_normalized, original, ID FROM Terms')
    for row in cur.fetchall():
        strip = row[0].strip(':')
        pieces = re.split(r':[3-9][0-9]*:', strip)
        pieces = list(set(pieces))
        symbols = list(set(filter(lambda x: x.isalpha() and x == x.upper(), re.split(r':[0-9]+:', strip))))
        for piece in pieces:
            two_level = piece.split(':2:')
            if len(two_level) > 1:
                for two in two_level:
                    cur.execute('INSERT INTO FragmentsLT3 VALUES (?, ?, ?)', (two, row[1], row[2]))
            if re.search(r'(:0:)|(:1:)|(:2:)', piece):
                cur.execute('INSERT INTO FragmentsLT3 VALUES (?, ?, ?)', (piece, row[1], row[2]))
        for symbol in symbols:
            cur.execute('INSERT INTO FragmentsLT3 VALUES (?, ?, ?)', (symbol, row[1], row[2]))

# Populates table Type1SearchTerms with type 1 search terms
# Type 1 search terms represent any phrase in which the terms appear in any given order
# Ex. He ate green food. & He ate food, it was green. Should be recognised by the same search terms
def make_type_1():
    cur.execute('SELECT DISTINCT * FROM FragmentsLT3')
    for row in cur.fetchall():
        pieces = re.split(r':[1-9][0-9]*:', row[0])
        cur.execute('INSERT INTO Type1SearchTerms SELECT full_normalized, ?, ?, ? FROM Terms WHERE '+_make_like(pieces)+'AND original = ? AND ID = ?',
                    (row[1], row[2], ' '.join(pieces), row[1], row[2]))

# Populates table Type2SearchTerms with type 2 search terms
# Type 2 search terms recognize documents that contain a given set of search terms in any given order
# Ex. A book containing the sentences: Jim packed his bag.
#                                     (... 3 lines later ..)
#                                       He walked to school.
# Should be recongnized by the search terms "Jim walked school"
def make_type_2():
    cur.execute('SELECT DISTINCT full_normalized FROM Terms')
    for row in cur.fetchall():
        strip = row[0].strip(':')
        pieces = re.split(r':[1-9][0-9]*:', strip)
        pieces = list(set(pieces))
        statement = _make_int(pieces)
        cur.execute(statement)
        for ID in cur.fetchall():
            cur.execute('INSERT INTO Type2SearchTerms VALUES (?, ?)', (row[0], ID[0]))

def new_terms_program(in_csv, out_csv_type_1, out_csv_type_2):
    populate_terms(in_csv)
    populate_fragments()
    make_type_1()
    make_type_2()
    pd.read_sql(sql='SELECT DISTINCT * FROM Type1SearchTerms', con=conn).to_csv(out_csv_type_1, index=False, sep=',', quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')
    pd.read_sql(sql='SELECT DISTINCT * FROM Type2SearchTerms', con=conn).to_csv(out_csv_type_2, index=False, sep=',', quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')

if __name__ == '__main__':
    new_terms_program('terms_full_set.csv', 'type_one.csv', 'type_two.csv')
