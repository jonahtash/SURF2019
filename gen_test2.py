import random
import sqlite3
import pandas as pd
import csv
from copy import deepcopy as dcp
import re

conn = sqlite3.connect(':memory:')
cur = conn.cursor()

cur.execute('CREATE TABLE Data (term text, originals text, sentence text, document text)')

out_csv = 'test_set2.csv'
level = 4

words = ['rotation:0:octahedron', 'octahedron:0:rotation', 'atoms:0:structure:0:extensive', 'structure:0:atoms:0:extensive', 'extensive:0:structure:0:atoms']

for i in range(1, level):
    new_words = []
    ias = str(i)
    for j in [2, 3]:
        samples = []
        perms = []
        for k in range(10):
            sample = random.sample(words, j)
            while sample in samples:
                sample = random.sample(words, j)
            samples.append(sample)
        for sample in samples:
            for k in range(1 + 2*(j-1)):
                s = dcp(sample)
                random.shuffle(s)
                i = 0
                while s in samples and i < 100:
                    random.shuffle(s)
                    i += 1
                perms.append(s)
        new_words = new_words + samples + perms
    for word in new_words:
        sentence = re.sub(r':[0-9]+:', ' ', ' '.join(word))
        for j in range(random.randint(1, 15)):
            cur.execute('INSERT INTO Data VALUES (?, ?, ?, ?)', ((':'+ias+':').join(word), sentence, sentence, '10.1107/S160053681000228X'))
    words = list(map(lambda x: (':'+ias+':').join(x), new_words))

pd.read_sql(sql='SELECT * FROM Data', con=conn).to_csv(out_csv, index=False, sep=',', quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')
