#!/usr/bin/env python3
"""
assign_flight_numbers_mongo.py
- Finds Mongo docs in collection `airindia_flights` with missing or empty flight_no.
- Assigns unique flight_no like AIYYYYMMDDNNNN.
- Creates unique index on flight_no (if not exists).
"""
import os, sys
import certifi
import random
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    print("MONGO_URI not set in .env")
    sys.exit(1)

client = MongoClient(MONGO_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=15000)
db = client.get_default_database()
coll = db.get_collection("airindia_flights")

# ensure unique index (will not fail if already exists)
try:
    coll.create_index([("flight_no", ASCENDING)], unique=True, sparse=True)
    print("Ensured unique index on flight_no.")
except Exception as e:
    print("Could not create index (ok if already exists):", e)

# collect existing flight_no set for quick membership
existing = set()
for doc in coll.find({}, {"flight_no": 1}):
    fn = doc.get("flight_no")
    if fn:
        existing.add(str(fn))

def gen_flight_no_for_date(prefix, dt, counter):
    return f"{prefix}{dt.strftime('%Y%m%d')}{counter:04d}"

# find docs missing flight_no
cursor = coll.find({"$or": [{"flight_no": {"$exists": False}}, {"flight_no": None}, {"flight_no": ""}]})
to_update = list(cursor)
print("Found", len(to_update), "documents missing flight_no.")

# We'll keep a per-date counter starting at 1000 (or higher if collisions)
counters = {}

updates = 0
for doc in to_update:
    # derive a date string for determinism
    sched = doc.get("schedule") or {}
    date_str = sched.get("date") or sched.get("flight_date") or None
    if date_str:
        try:
            dt = datetime.fromisoformat(str(date_str)).date()
        except Exception:
            # try parse yyyy-mm-dd fallback
            try:
                dt = datetime.strptime(str(date_str), "%Y-%m-%d").date()
            except Exception:
                dt = datetime.utcnow().date()
    else:
        dt = datetime.utcnow().date()

    key = dt.isoformat()
    if key not in counters:
        # start at a random-ish base to reduce collision chance with seeded data
        counters[key] = 1000 + random.randint(0, 500)

    # generate until unique
    prefix = "AI"
    attempt = 0
    while True:
        candidate = gen_flight_no_for_date(prefix, dt, counters[key])
        counters[key] += 1
        attempt += 1
        if candidate not in existing:
            break
        if attempt > 10000:
            raise RuntimeError("Too many collisions generating flight_no for date " + key)

    # update the doc with flight_no
    try:
        res = coll.update_one({"_id": doc["_id"]}, {"$set": {"flight_no": candidate}})
        if res.modified_count:
            updates += 1
            existing.add(candidate)
    except DuplicateKeyError:
        # race or concurrent insert: skip and continue
        print("Duplicate detected for", candidate)
        continue
    except Exception as e:
        print("Update failed for _id", doc["_id"], e)

print(f"Completed. Updated {updates} documents with generated flight_no.")
client.close()
