import psycopg2
import os

conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='postgres',
    host='localhost',
    port=5432
)

cur = conn.cursor()

cur.execute('SELECT 1;')
for record in cur:
    print(record)

cur.close()
conn.close()