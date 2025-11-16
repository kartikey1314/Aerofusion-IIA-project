#!/usr/bin/env python3
"""
seed_indigo_schedule_safe.py

Safe seeder that inserts IndiGo scheduled flights and respects the composite UNIQUE
constraint (flight_no, journey_date, departure_time) using ON CONFLICT DO NOTHING.

Place in backend/scripts/ and run:
python backend/scripts/seed_indigo_schedule_safe.py
"""

import os
import random
from datetime import date, datetime, time, timedelta
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

PG_CONN_INFO = dict(
    dbname=os.getenv("PG_DB", "aerofusion_db"),
    user=os.getenv("PG_USER", "master_user"),
    password=os.getenv("PG_PASS", "Kartikey@123"),
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", 5432))
)

# ROUTES: (to_city, frequency_hours, start_hour, typical_duration_minutes, fare_min, fare_max)
ROUTES = [
    ("Bengaluru", 2, 4, 120, 4000, 7000),   # every 2 hours starting 04:00
    ("Kolkata",  4, 5, 120, 5000, 8000),    # every 4 hours starting 05:00
    ("London",   6, 2, 480, 35000, 70000)   # every 6 hours starting 02:00
]

ORIGIN = "Delhi"
AIRLINE = "IndiGo"
PREFIX = "IG"   # flight_no prefix

START_DATE = date(2025, 11, 17)
END_DATE = date(2026, 1, 17)  # inclusive

random.seed(42)

def generate_seats(min_count=20, max_count=180):
    rows = range(10, 41)
    seats = [f"{r}{c}" for r in rows for c in ['A','B','C','D','E','F']]
    n = random.randint(min_count, min(max_count, len(seats)))
    return random.sample(seats, n)

def make_departure_hours(freq_hours, start_hour):
    hours = []
    h = start_hour % 24
    while h < 24:
        hours.append(h)
        h += freq_hours
    return hours

def add_minutes_to_time(t: time, minutes: int) -> time:
    dt = datetime.combine(date.today(), t) + timedelta(minutes=minutes)
    return dt.time()

def gen_flight_no(prefix, to_city, journey_date, seq):
    code = ''.join([c for c in to_city.upper() if c.isalpha()])[:3].ljust(3, 'X')
    return f"{prefix}{code}{journey_date.strftime('%Y%m%d')}{seq:03d}"

def connect():
    return psycopg2.connect(**PG_CONN_INFO)

def build_records():
    records = []
    seq_map = {}  # (date, to_city) -> seq
    for single_date in (START_DATE + timedelta(days=n) for n in range((END_DATE - START_DATE).days + 1)):
        for to_city, freq, start_hour, typical_dur, fare_min, fare_max in ROUTES:
            dep_hours = make_departure_hours(freq, start_hour)
            for hour in dep_hours:
                dep_time = time(hour, 0)
                duration = max(30, typical_dur + random.randint(-20, 60))
                arr_time = add_minutes_to_time(dep_time, duration)
                fare = random.randint(fare_min, fare_max)
                discount = random.choice([0,0,5,10])
                offer = f"{discount}%OFF" if discount else None
                # seats: ensure minimum seats for London
                if to_city == "London":
                    seats = generate_seats(min_count=50, max_count=300)
                else:
                    seats = generate_seats(min_count=20, max_count=220)
                booking_link = f"https://book.indigo.com/{ORIGIN}-{to_city}/{single_date.isoformat()}"

                key = (single_date, to_city)
                seq_map.setdefault(key, 1)
                seq = seq_map[key]
                seq_map[key] += 1

                flight_no = gen_flight_no(PREFIX, to_city, single_date, seq)

                # produce tuple matching your indigo_src columns
                records.append((
                    flight_no, AIRLINE, ORIGIN, to_city, single_date,
                    dep_time, arr_time, duration,
                    fare, discount, offer,
                    booking_link, seats
                ))
    return records

def insert_records(records, batch_size=500):
    insert_sql = """
    INSERT INTO indigo_src (
        flight_no, airline, from_city, to_city, journey_date,
        departure_time, arrival_time, duration_mins,
        fare, discount_percent, offer_name, booking_link, seats
    )
    VALUES %s
    ON CONFLICT (flight_no, journey_date, departure_time) DO NOTHING
    """
    conn = connect()
    cur = conn.cursor()
    total = 0
    try:
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            execute_values(cur, insert_sql, batch, page_size=batch_size)
            conn.commit()
            total += len(batch)
        print(f"Attempted to insert {total} rows (ON CONFLICT ignored existing).")
    except Exception as e:
        conn.rollback()
        print("Insert failed:", e)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    recs = build_records()
    print("Total records to attempt:", len(recs))
    insert_records(recs)
