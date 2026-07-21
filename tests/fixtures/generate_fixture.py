import psycopg2
import os

conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='postgres',
    host='localhost',
    port=5432
)

with conn.cursor() as cur:
    cur.execute(""" 
    CREATE TABLE test (
        x INT,
        y VARCHAR
    );
    """)

    conn.commit()

    cur.execute(
        """
        INSERT INTO test VALUES (1, 'Hello');
        INSERT INTO test VALUES (2, 'Goodbye');
    """)

    conn.commit()

    
conn.close()