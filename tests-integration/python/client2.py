import psycopg2

conn = psycopg2.connect("host=127.0.0.1 port=5432 user=tom password=pencil dbname=localdb")
conn.autocommit = True

with conn.cursor() as cur:
    cur.execute("INSERT INTO testtable VALUES (1)")
    print(cur.statusmessage)

with conn.cursor() as cur:
    cur.execute("SELECT * FROM testtable")
    print(cur.fetchall())
