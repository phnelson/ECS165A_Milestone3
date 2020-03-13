from template.db import Database
from template.query import Query
import os
from time import process_time
from random import choice, randrange
import sys
# Student Id and 4 grades
'''
READ ME!!
    Before using this demo, be sure that the Tail_Const is set to a value high enough
    to guaranteed that all updates are contained within the same block.
        config.py -> TAIL_CONST = 4
        
    This program is meant to run sequentially through all parts starting with an empty ECS165
    directory.
'''

db = Database()
db.open('ECS165')
print("Creating tables: 'Grades'")
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

# Measuring Insert Performance
print("Inserting two records, Keys= 92106429, 2106430")
query.insert(92106429, 93, 5, 7, 0)
query.insert(92106430, 8, 7, 6, 5)
rec = query.select(92106429, 0, [1,1,1,1,1])[0]
rec2 = query.select(92106430, 0, [1,1,1,1,1])[0]
print("Printing record data contents")
print(rec)
print(rec2)
rec = query.selectFull(92106429, 0)
rec2 = query.selectFull(92106430, 0)
print("Printing full record contents")
print(rec)
print(rec2)

db.close()