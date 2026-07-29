import psycopg2
import os

conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='postgres',
    host='localhost',
    port=5432
)

conn.autocommit = True

with conn.cursor() as cur:
    cur.execute(open("tmp/fixture.sql", 'r').read())

conn.close()