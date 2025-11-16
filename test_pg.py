# test_pg.py
import os, psycopg2
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(host=os.getenv("PG_HOST"), port=os.getenv("PG_PORT"),
                        dbname=os.getenv("PG_DB"), user=os.getenv("PG_USER"), password=os.getenv("PG_PASS"))
cur = conn.cursor()
cur.execute("SELECT flight_no FROM flights_dwh WHERE origin=%s AND destination=%s",
            ("Delhi","London"))
print(cur.fetchall())
cur.close(); conn.close()
