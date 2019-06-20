import cx_Oracle
import csv
import re
import pandas as pd
import atexit

# Intialize Global SQL Objects
conn = cx_Oracle.connect('username/password@localhost')
cur = conn.cursor()

# commit changes on program exit
atexit.register(conn.commit)
atexit.register(cur.close)
atexit.register(conn.close)


# Helper to generate SQL LIKE statement from list of terms
# Statement will return from from Terms where a sentence contains every term in list l in no paticular order
# Ex. in: [a, b, c]
#    out: original LIKE '%:a:%' AND original LIKE '%:b:%' original LIKE '%:c:%'
def _make_like(l):
    return ("full_normalized LIKE '%:" + (":%' AND full_normalized LIKE '%:").join(l) + ":%'")

# Helper to generate SQL INTERSECT statement for type 2 terms
# Statement will return all IDs that contain every term in list l at least once
# Ex. in: [a, b, c]
#    out:  SELECT DISTINCT ID FROM Terms WHERE original LIKE '%:a:%' INTERSECT SELECT DISTINCT ID FROM Terms WHERE original LIKE '%:b:%' INTERSECT SELECT DISTINCT ...
def _make_int(in_term_table, l):
    return ("SELECT DISTINCT ID FROM "+in_term_table+" WHERE full_normalized LIKE '%:" + (":%' INTERSECT SELECT DISTINCT ID FROM "+in_term_table+" WHERE full_normalized LIKE '%:").join(l) + ":%'")

def _rem_chars(s, l):
    if l < 0:
        return s
    chars = ['-', '.', '=', '+', '/', ';', '*', '_', ':']
    pieces = s.split(':'+str(l)+':')
    ret = []
    for piece in pieces:
        piece = _rem_chars(piece, l-1)
        if len(piece) > 0 and piece not in chars:
            ret.append(piece)
    return (':'+str(l)+':').join(ret)


# Generates table FragmentsLT3 by splitting each DISTINCT full_normalized at all levels >= 3, and
# inserting these pieces (fragments), along with metadata into the table.
# "symbol" terms, pieces that are in all caps at any level i.e. DNA, SQL are also inserted as individual fragments
def populate_fragments(in_term_table, frag_table, new_term_table):
    #cur.execute('CREATE INDEX idx on Terms(full_normalized)')
    cur.execute('CREATE TABLE '+frag_table+' (randr_fragment varchar(max), original varchar(max), ID varchar(max)) exception when others then if SQLCODE = -955 then null; else raise; end if;')
    cur.execute('CREATE TABLE '+new_term_table+' (randr_fragment varchar(max), randr_fragment_broken varchar(max), term_randr_found varchar(max), type INTEGER, ID varchar(max)) exception when others then if SQLCODE = -955 then null; else raise; end if;')

    cur.execute('SELECT DISTINCT full_normalized, original, ID FROM '+in_term_table)
    for row in cur.fetchall():
        strip = row[0].strip(':')
        pieces = re.split(r':([3-9]|[1-9][0-9]+):', strip)
        pieces = list(set(pieces))
        symbols = list(set(filter(lambda x: x.isalpha() and x == x.upper(), re.split(r':[0-9]+:', strip))))
        for piece in pieces:
            piece = _rem_chars(piece, 2)
            if re.search(r'(:0:)|(:1:)|(:2:)', piece):
                cur.execute('INSERT INTO '+frag_table+' VALUES (?, ?, ?)', (piece, row[1], row[2]))
            two_level = piece.split(':2:')
            for two in two_level:
                if re.search(r'(:0:)|(:1:)|(:2:)', two):
                    cur.execute('INSERT INTO '+frag_table+' VALUES (?, ?, ?)', (two, row[1], row[2]))
                one_level = two.split(':1:')
                for one in one_level:
                    if re.search(r'(:0:)|(:1:)|(:2:)', one):
                        cur.execute('INSERT INTO '+frag_table+' VALUES (?, ?, ?)', (one, row[1], row[2]))
        for symbol in symbols:
            cur.execute('INSERT INTO '+frag_table+' VALUES (?, ?, ?)', (symbol, row[1], row[2]))

# Populates table Type1SearchTerms with type 1 search terms
# Type 1 search terms represent any phrase in which the terms appear in any given order
# Ex. He ate green food. & He ate food, it was green. Should be recognised by the same search terms
def make_type_1(in_term_table, frag_table, new_term_table):
    cur.execute('SELECT DISTINCT randr_fragment FROM '+frag_table)
    for row in cur.fetchall():
        pieces = re.split(r':[1-9][0-9]*:', row[0])
        cur.execute('INSERT INTO '+new_term_table+' SELECT ?, ?, full_normalized, 1, ID FROM '+in_term_table+' WHERE '+_make_like(pieces), (row[0], ' '.join(pieces)))

# Populates table Type2SearchTerms with type 2 search terms
# Type 2 search terms recognize documents that contain a given set of search terms in any given order
# Ex. A book containing the sentences: Jim packed his bag.
#                                     (... 3 lines later ..)
#                                       He walked to school.
# Should be recongnized by the search terms "Jim walked school"
def make_type_2(in_term_table, frag_table, new_term_table):
    cur.execute('SELECT DISTINCT randr_fragment FROM '+frag_table)
    for row in cur.fetchall():
        pieces = re.split(r':[1-9][0-9]*:', row[0])
        pieces = list(set(pieces))
        statement = _make_int(in_term_table, pieces)
        cur.execute(statement)
        for ID in cur.fetchall():
            cur.execute('INSERT INTO '+new_term_table+' VALUES (?, ?, ?, ?, ?)', (row[0], ' '.join(pieces), '', 2, ID[0]))

def new_terms_program(in_term_table, frag_table, new_term_table):
    print("Populating Fragments table...")
    populate_fragments(in_term_table, frag_table, new_term_table)
    conn.commit()
    print("Creating Type 1 Terms...")
    make_type_1(in_term_table, frag_table, new_term_table)
    conn.commit()
    print("Creating Type 2 Terms...")
    make_type_2(in_term_table, frag_table, new_term_table)
    conn.commit()
    cur.close()
    conn.close()
    print("FINISHED")

if __name__ == '__main__':
    new_terms_program('Terms', 'TypeZero', 'TypesOneAndTwo')
