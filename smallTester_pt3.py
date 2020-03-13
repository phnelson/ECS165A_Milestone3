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
db = Database()
db.open("ECS165")
print(db)
g_table = db.get_table('Grades')
q = Query(g_table)

print("Merge Start")
q.table.merge(0)
print("Merge End")

db.close()