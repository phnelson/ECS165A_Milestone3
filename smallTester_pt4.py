from template.db import Database
from template.query import Query
import os

'''
READ ME!!
    Before using this demo, be sure that the Tail_Const is set to a value high enough
    to guaranteed that all updates are contained within the same block.
        config.py -> TAIL_CONST = 4

    This program is meant to run sequentially through all parts starting with an empty ECS165
    directory.
'''
db2 = Database()
db2.open("ECS165")
#db2.open()
print(db2)
g_table = db2.get_table('Grades')
q = Query(g_table)
rec3 = q.select(92106429, 0, [1,1,1,1,1])[0]
rec4 = q.select(92106430, 0, [1,1,1,1,1])[0]
print("Rec = ", rec3)
print("Rec2 = ", rec4)
rec3f = q.selectFull(92106429, 0)
rec4f = q.selectFull(92106430, 0)
print("Rec_full = ", rec3f)
print("Rec2_full = ", rec4f)
db2.close()