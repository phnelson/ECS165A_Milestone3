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
grades_table = db.get_table("Grades")
q = Query(grades_table)
keys = []

loop = 1000

rec = q.select(92106429, 0, [1,1,1,1,1])[0]
rec2 = q.select(92106430, 0, [1,1,1,1,1])[0]
print("Original Record Contents: ")
print(rec)
print(rec2)

print("Update Test ", loop, " times:")
record = q.select(92106429, 0, [1, 1, 1, 1, 1])[0]
updater = [None,None,100,None,None]
updater2 =[None,None,None,100,None]

for x in range(loop):
    rec3 = q.select(92106429, 0, [1,1,1,1,1])[0]
    q.update(92106429, *updater)
    #print(rec3)
    q.update(92106430, *updater2)
    rec4 = q.select(92106430, 0, [1,1,1,1,1])[0]
    #print(rec4)


rec3 = q.select(92106429, 0, [1,1,1,1,1])[0]
rec4 = q.select(92106430, 0, [1,1,1,1,1])[0]
print("Rec = ", rec3)
print("Rec2 = ", rec4)
rec3f = q.selectFull(92106429, 0)
rec4f = q.selectFull(92106430, 0)
print("Rec_full = ", rec3f)
print("Rec2_ full = ", rec4f)
db.close()