import os
import traceback
from datetime import datetime, time
from typing import List, Dict, Any

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient
import random
from collections import defaultdict



load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "aerofusion_db")
PG_USER = os.getenv("PG_USER", "master_user")
PG_PASS = os.getenv("PG_PASS", "")

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://kartikey24046:Kartikey123@cluster0.zrknx9w.mongodb.net/aerofusion_db?retryWrites=true&w=majority"
)



def normalize_time(value):
    """
    Safely convert invalid times like '24:15' -> '00:15' (wrap to next day).
    Works for both strings and datetime.time objects.
    """
    if not value:
        return None
    try:
        if isinstance(value, time):
            return value
        s = str(value).strip()
        parts = s.split(":")
        if len(parts) < 2:
            return None
        h = int(parts[0]) % 24  # wrap 24, 25, etc. to 0–23
        m = int(parts[1])
        return time(h, m)
    except Exception:
        return None


def pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )


def mongo_db():
    """Connect safely to MongoDB (Atlas or local)."""
    client = MongoClient(MONGO_URI)
    try:
        db_obj = client.get_default_database()
        if db_obj is not None:
            return db_obj
        else:
            return client["aerofusion_db"]
    except Exception as e:
        print(f"⚠️ MongoDB connection warning: {e}")
        return client["aerofusion_db"]




def extract_indigo(pg) -> List[Dict[str, Any]]:
    """Extract from PostgreSQL (IndiGo)."""
    query = """
    SELECT flight_no, airline, from_city, to_city, journey_date,
           departure_time, arrival_time, duration_mins,
           fare, discount_percent, offer_name, booking_link, aircraft, seats
    FROM indigo_src
    """
    cur = pg.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()

    cols = [
        "flight_no", "airline", "from_city", "to_city", "journey_date",
        "departure_time", "arrival_time", "duration_mins", "fare",
        "discount_percent", "offer_name", "booking_link", "aircraft", "seats"
    ]

    out = []
    for r in rows:
        rec = dict(zip(cols, r))
        base_price = float(rec["fare"]) if rec["fare"] is not None else None
        disc = float(rec["discount_percent"]) if rec["discount_percent"] is not None else 0.0
        eff_price = base_price - (base_price * (disc / 100.0)) if base_price else None

        out.append({
            "airline": rec.get("airline") or "IndiGo",
            "flight_no": rec["flight_no"],
            "origin": rec["from_city"],
            "destination": rec["to_city"],
            "flight_date": rec["journey_date"],
            "departure_time": normalize_time(rec["departure_time"]),
            "arrival_time": normalize_time(rec["arrival_time"]),
            "duration_mins": rec["duration_mins"],
            "price": round(eff_price, 2) if eff_price is not None else None,
            "discount_percent": rec["discount_percent"],
            "offer_name": rec["offer_name"],
            "booking_link": rec["booking_link"],
            "aircraft": rec["aircraft"],
            "seats": rec["seats"],
        })
    return out


def extract_airindia(mdb) -> List[Dict[str, Any]]:
    """Extract from MongoDB (Air India)."""
    coll = mdb["airindia_flights"]
    docs = list(coll.find({}))

    out = []
    for d in docs:
        route = d.get("route", {})
        sched = d.get("schedule", {})
        pricing = d.get("pricing", {})
        offer = pricing.get("offer") or {}
        avail = d.get("availability", {})
        booking = d.get("booking", {})

        base_price = pricing.get("base_price")
        base_price = float(base_price) if base_price is not None else None
        disc = offer.get("discount") or 0
        try:
            disc = float(disc)
        except:
            disc = 0.0
        eff_price = base_price - (base_price * (disc / 100.0)) if base_price else None

        out.append({
            "airline": d.get("airline_name", "Air India"),
            "flight_no": d.get("flight_number"),
            "origin": route.get("origin"),
            "destination": route.get("destination"),
            "flight_date": sched.get("date"),
            "departure_time": normalize_time(sched.get("departure")),
            "arrival_time": normalize_time(sched.get("arrival")),
            "duration_mins": sched.get("duration_min"),
            "price": round(eff_price, 2) if eff_price is not None else None,
            "discount_percent": disc if disc is not None else None,
            "offer_name": offer.get("name"),
            "booking_link": booking.get("link"),
            "aircraft": d.get("aircraft_type"),
            "seats": avail.get("seats", []),
        })
    return out

def ensure_flight_no(records):
    """
    Fill missing flight_no in list of record dicts.
    Expects each record to have at least: 'airline' and 'flight_date' (date or ISO str).
    Modifies records in-place and returns them.
    """
    # collect existing flight_nos
    existing = set()
    for r in records:
        fn = r.get("flight_no")
        if fn:
            existing.add(str(fn))

    # counters per (prefix, date)
    counters = defaultdict(lambda: 1000 + random.randint(0, 500))

    def date_key(dt):
        if dt is None:
            return datetime.utcnow().date().isoformat()
        if isinstance(dt, str):
            try:
                return datetime.fromisoformat(dt).date().isoformat()
            except Exception:
                return dt.split("T")[0] if "T" in dt else dt
        if isinstance(dt, datetime):
            return dt.date().isoformat()
        # assume date object
        try:
            return dt.isoformat()
        except Exception:
            return datetime.utcnow().date().isoformat()

    created = 0
    for r in records:
        if r.get("flight_no"):
            continue
        airline = (r.get("airline") or "").lower()
        prefix = "AI" if "air india" in airline else "IG"
        # choose date part from flight_date or journey_date or schedule
        dt = r.get("flight_date") or r.get("journey_date") or r.get("date") or r.get("schedule", {}).get("date") if isinstance(r.get("schedule"), dict) else None
        dk = date_key(dt)
        key = (prefix, dk)
        # generate candidate until unique
        attempt = 0
        while True:
            candidate = f"{prefix}{dk.replace('-','')}{counters[key]:04d}"
            counters[key] += 1
            attempt += 1
            if candidate not in existing:
                break
            if attempt > 10000:
                raise RuntimeError("Too many collisions generating flight_no for " + str(key))
        r["flight_no"] = candidate
        existing.add(candidate)
        created += 1
    if created:
        print(f"[ETL] Generated {created} flight_no values for records missing them.")
    return records


def load_snapshot(pg, records: List[Dict[str, Any]]) -> int:
    """Load all unified records into DWH tables."""
    if not records:
        return 0

    for r in records:
        r["departure_time"] = normalize_time(r["departure_time"])
        r["arrival_time"] = normalize_time(r["arrival_time"])

    with pg:
        with pg.cursor() as cur:
            cur.execute("DELETE FROM flights_dwh")

            dwh_cols = (
                "airline", "flight_no", "origin", "destination", "flight_date",
                "departure_time", "arrival_time", "duration_mins", "price",
                "discount_percent", "offer_name", "booking_link", "aircraft", "seats"
            )
            dwh_values = [
                (
                    r["airline"], r["flight_no"], r["origin"], r["destination"], r["flight_date"],
                    r["departure_time"], r["arrival_time"], r["duration_mins"], r["price"],
                    r["discount_percent"], r["offer_name"], r["booking_link"], r["aircraft"], r["seats"]
                )
                for r in records
            ]
            execute_values(
                cur,
                f"INSERT INTO flights_dwh ({','.join(dwh_cols)}) VALUES %s",
                dwh_values
            )

            hist_cols = (
                "airline", "flight_no", "origin", "destination",
                "flight_date", "price", "discount_percent", "offer_name"
            )
            hist_values = [
                (
                    r["airline"], r["flight_no"], r["origin"], r["destination"], r["flight_date"],
                    r["price"], r["discount_percent"], r["offer_name"]
                )
                for r in records
            ]
            execute_values(
                cur,
                f"INSERT INTO offers_history ({','.join(hist_cols)}) VALUES %s",
                hist_values
            )
    return len(records)




def main():
    started = datetime.now()
    rows_ai = rows_ig = rows_loaded = 0
    status = "ok"
    notes = None

    try:
        print("Connecting Postgres...")
        pg = pg_conn()

        print("Connecting Mongo...")
        mdb = mongo_db()

        print("Extracting from Postgres (IndiGo)...")
        indigo = extract_indigo(pg)
        rows_ig = len(indigo)

        print("Extracting from Mongo (Air India)...")
        airindia = extract_airindia(mdb)
        rows_ai = len(airindia)

        all_records = indigo + airindia
        print(f"Total unified records: {len(all_records)}")
        # ensure every record has flight_no before loading
        all_records = ensure_flight_no(all_records)

        rows_loaded = load_snapshot(pg, all_records)
        print("Loaded {rows_loaded} records into DWH successfully.")

    except Exception as e:
        status = "failed"
        notes = f"{type(e).__name__}: {e}"
        print("ETL failed:", notes)
        traceback.print_exc()
    finally:
        try:
            with pg_conn() as pg2, pg2.cursor() as cur2:
                cur2.execute(
                    """
                    INSERT INTO etl_run_log
                    (started_at, finished_at, status, rows_airindia, rows_indigo, rows_loaded_dwh, notes)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (started, datetime.now(), status, rows_ai, rows_ig, rows_loaded, notes)
                )
            print("ETL run logged successfully.")
        except Exception as le:
            print("Failed to log ETL run:", le)


if __name__ == "__main__":
    main()
