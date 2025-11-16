#!/usr/bin/env python3
"""
federator.py — enhanced but backward-compatible federator.

- Normalizes prices (float), extracts departure_time where present.
- If parsed["airline"] is present, filters each source result to that airline before integration.
- Attempts to parse LLM JSON into 'llm_parsed' for easier consumption.
- Keeps previous outputs and printing behavior.
"""

import os
import json
import argparse
from datetime import datetime, timezone
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
from rapidfuzz import process, fuzz
import certifi
import sys

import analyzer
from llm_rewriter import rewrite_summary_prompt
from llm_client import make_prompt, safe_call_llm

load_dotenv()

# env variables (fallbacks)
PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")

DWH_PG_HOST = os.getenv("DWH_PG_HOST") or PG_HOST
DWH_PG_PORT = int(os.getenv("DWH_PG_PORT", PG_PORT))
DWH_PG_DB = os.getenv("DWH_PG_DB") or PG_DB
DWH_PG_USER = os.getenv("DWH_PG_USER") or PG_USER
DWH_PG_PASSWORD = os.getenv("DWH_PG_PASSWORD") or PG_PASS

INDIGO_PG_HOST = os.getenv("INDIGO_PG_HOST") or PG_HOST
INDIGO_PG_PORT = int(os.getenv("INDIGO_PG_PORT", PG_PORT))
INDIGO_PG_DB = os.getenv("INDIGO_PG_DB") or PG_DB
INDIGO_PG_USER = os.getenv("INDIGO_PG_USER") or PG_USER
INDIGO_PG_PASSWORD = os.getenv("INDIGO_PG_PASSWORD") or PG_PASS

MONGO_URI = os.getenv("MONGO_URI", None)
MONGO_ALLOW_INVALID_CERTS = os.getenv("MONGO_ALLOW_INVALID_CERTS", "false").lower() in ("1", "true", "yes")

CANONICAL_CITIES = ["Delhi","Chennai","Mumbai","Bangalore","Kolkata","Hyderabad","Pune","Ahmedabad"]
ALIAS_MAP = {"delgi":"Delhi","delh":"Delhi","chenai":"Chennai","banglore":"Bangalore","bengaluru":"Bangalore"}
FUZZY_THRESHOLD = int(os.getenv("FUZZY_MATCH_THRESHOLD","70"))

def fuzzy_city_normalize(city_raw: str):
    if not city_raw:
        return None
    key = city_raw.strip().lower()
    if key in ALIAS_MAP:
        return ALIAS_MAP[key]
    for c in CANONICAL_CITIES:
        if key == c.lower():
            return c
    best = process.extractOne(city_raw, CANONICAL_CITIES, scorer=fuzz.ratio)
    if best:
        candidate, score, _ = best
        if score >= FUZZY_THRESHOLD:
            return "Bangalore" if candidate.lower()=="bengaluru" else candidate
    return city_raw.title()

# Postgres helpers
def connect_pg(host, port, dbname, user, password):
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)

def execute_pg(host, port, dbname, user, password, sql_tpl, params):
    conn = connect_pg(host, port, dbname, user, password)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql_tpl, params)
        rows = cur.fetchall()
        cur.close()
        return [dict(r) for r in rows]
    finally:
        conn.close()

# SQL builders (same as analyzer's)
def build_param_sql_dwh(parsed):
    intent = str(parsed.get("intent","LIST")).upper()
    if intent == "AVG":
        return ("SELECT AVG(price) AS avg_price FROM flights_dwh WHERE origin = %s AND destination = %s;", [parsed.get("origin"), parsed.get("destination")])
    base = ("SELECT flight_no, airline, origin, destination, flight_date AS date, price, "
            "array_length(seats,1) AS seat_count, 'DWH' as source FROM flights_dwh WHERE 1=1")
    params=[]
    if parsed.get("origin"): base += " AND origin = %s"; params.append(parsed["origin"])
    if parsed.get("destination"): base += " AND destination = %s"; params.append(parsed["destination"])
    if parsed.get("date"): base += " AND flight_date = %s"; params.append(parsed["date"])
    if parsed.get("price_limit"): base += " AND price < %s"; params.append(parsed["price_limit"])
    if parsed.get("seat_count"): base += " AND array_length(seats,1) > %s"; params.append(parsed["seat_count"])
    if intent=="MIN": base += " ORDER BY price ASC LIMIT 1"
    return (base+";", params)

def build_param_sql_indigo(parsed):
    intent = str(parsed.get("intent","LIST")).upper()
    if intent == "AVG":
        return ("SELECT AVG(fare) AS avg_price FROM indigo_src WHERE from_city = %s AND to_city = %s;", [parsed.get("origin"), parsed.get("destination")])
    base = ("SELECT flight_no, airline, from_city AS origin, to_city AS destination, journey_date AS date, "
            "departure_time, fare as price, NULL as seat_count, 'IndiGo' as source FROM indigo_src WHERE 1=1")
    params=[]
    if parsed.get("origin"): base += " AND from_city = %s"; params.append(parsed["origin"])
    if parsed.get("destination"): base += " AND to_city = %s"; params.append(parsed["destination"])
    if parsed.get("date"): base += " AND journey_date = %s"; params.append(parsed["date"])
    if parsed.get("price_limit"): base += " AND fare < %s"; params.append(parsed["price_limit"])
    if parsed.get("seat_count"): base += " AND array_length(seats,1) > %s"; params.append(parsed["seat_count"])
    if intent=="MIN": base += " ORDER BY fare ASC LIMIT 1"
    return (base+";", params)

def build_mongo_filter(parsed):
    filt={}
    if parsed.get("origin"): filt["route.origin"]=parsed["origin"]
    if parsed.get("destination"): filt["route.destination"]=parsed["destination"]
    if parsed.get("date"): filt["schedule.date"]=parsed["date"]
    if parsed.get("price_limit"): filt["pricing.base_price"]={"$lt": parsed["price_limit"]}
    if parsed.get("seat_count"): filt["availability.seats_count"]={"$gt": parsed["seat_count"]}
    if parsed.get("airline"): filt["airline_name"]=parsed["airline"]
    return filt

# Mongo helper
def execute_mongo(filter_dict, limit=100):
    if not MONGO_URI:
        print("MONGO_URI not set; skipping Mongo.")
        return []
    try:
        client = MongoClient(MONGO_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=15000)
        client.admin.command("ping")
        print("Secure Mongo connection OK")
    except Exception as secure_err:
        print("Secure Mongo connect failed:", repr(secure_err))
        if MONGO_ALLOW_INVALID_CERTS:
            print("Retrying with tlsAllowInvalidCertificates=True (INSECURE) because MONGO_ALLOW_INVALID_CERTS=true")
            try:
                client = MongoClient(MONGO_URI, tls=True, tlsAllowInvalidCertificates=True, serverSelectionTimeoutMS=15000)
                client.admin.command("ping")
                print("Connected insecurely to Mongo (tlsAllowInvalidCertificates=True)")
            except Exception as insecure_err:
                print("Insecure retry failed:", repr(insecure_err))
                return []
        else:
            print("Set MONGO_ALLOW_INVALID_CERTS=true in .env if you want to retry insecurely (debug only).")
            return []
    try:
        db = client.get_default_database()
        coll = db.get_collection("airindia_flights")
        docs = list(coll.find(filter_dict).limit(limit))
    except Exception as e:
        print("Error reading Mongo:", repr(e))
        client.close()
        return []
    client.close()
    out=[]
    for d in docs:
        out.append({
            "flight_no": d.get("flight_no") or str(d.get("_id")),
            "airline": d.get("airline_name","Air India"),
            "origin": d.get("route",{}).get("origin"),
            "destination": d.get("route",{}).get("destination"),
            "date": d.get("schedule",{}).get("date"),
            "price": compute_effective_price_from_mongo(d),
            "seat_count": d.get("availability",{}).get("seats_count"),
            "departure_time": extract_departure_from_mongo(d),
            "source": "AirIndia"
        })
    return out

def compute_effective_price_from_mongo(doc):
    base = doc.get("pricing",{}).get("base_price")
    disc = None
    if isinstance(doc.get("pricing",{}).get("offer"), dict):
        disc = doc.get("pricing",{}).get("offer",{}).get("discount")
    if disc is None:
        disc = doc.get("pricing",{}).get("discount_percent")
    if base is None: return None
    try:
        if disc is not None:
            return float(base) * (1 - float(disc)/100.0)
        return float(base)
    except Exception:
        try: return float(base)
        except Exception: return None

def extract_departure_from_mongo(d):
    sched = d.get("schedule",{})
    if sched.get("departure"):
        return sched.get("departure")
    if sched.get("departure_time"):
        return sched.get("departure_time")
    return None

# normalize a single row from any source
def normalize_row(r: dict) -> dict:
    out = dict(r)
    price = out.get("price")
    if price is not None:
        try:
            out["price"] = float(price)
        except Exception:
            try:
                out["price"] = float(str(price).replace(",",""))
            except Exception:
                out["price"] = None
    else:
        out["price"] = None

    dt = out.get("departure_time") or out.get("departure") or out.get("dep_time") or out.get("departureTime")
    if not dt and isinstance(out.get("schedule"), dict):
        dt = out["schedule"].get("departure") or out["schedule"].get("departure_time")
    out["departure_time"] = dt if dt is not None else None

    if out.get("origin"):
        out["origin"] = str(out["origin"]).title()
    if out.get("destination"):
        out["destination"] = str(out["destination"]).title()

    if not out.get("flight_no"):
        out["flight_no"] = f"{out.get('source','UNKNOWN')}_{out.get('origin')}_{out.get('destination')}_{out.get('date')}"
    return out

# airline normalization for filtering
AIRLINE_ALIASES = {
    "airindia": "Air India",
    "air india": "Air India",
    "indigo": "IndiGo",
    "spicejet": "SpiceJet",
    "spice jet": "SpiceJet",
    "vistara": "Vistara",
    "jet airways": "Jet Airways",
    "jet": "Jet Airways",
    "gofirst": "GoFirst",
    "airasia": "AirAsia"
}

def normalize_airline_token(tok: str):
    if not tok:
        return None
    t = tok.strip().lower()
    if t in AIRLINE_ALIASES:
        return AIRLINE_ALIASES[t]
    for k,v in AIRLINE_ALIASES.items():
        if k in t:
            return v
    return tok.strip().title()

def filter_rows_by_airline(rows, airline_token):
    if not airline_token:
        return rows
    norm = normalize_airline_token(airline_token)
    if not norm:
        return rows
    def match(r):
        a = (r.get("airline") or "")
        s = (r.get("source") or "")
        if a and norm.lower() in str(a).lower():
            return True
        if s and norm.lower() in str(s).lower():
            return True
        return False
    return [r for r in (rows or []) if match(r)]

def integrate_results(dwh_rows, indigo_rows, mongo_rows):
    dwh_n = [normalize_row(r) for r in (dwh_rows or [])]
    indigo_n = [normalize_row(r) for r in (indigo_rows or [])]
    mongo_n = [normalize_row(r) for r in (mongo_rows or [])]

    merged={}
    for r in dwh_n:
        key = r.get("flight_no") or f"{r.get('origin')}-{r.get('destination')}-{r.get('date')}"
        merged.setdefault(key, {}).update(r)
    for r in indigo_n:
        key = r.get("flight_no") or f"{r.get('origin')}-{r.get('destination')}-{r.get('date')}"
        if key in merged:
            for k,v in r.items():
                if (merged[key].get(k) is None or merged[key].get(k)=="") and v is not None:
                    merged[key][k]=v
        else:
            merged.setdefault(key, {}).update(r)
    for r in mongo_n:
        key = r.get("flight_no") or f"{r.get('origin')}-{r.get('destination')}-{r.get('date')}"
        if key in merged:
            for k,v in r.items():
                if (merged[key].get(k) is None or merged[key].get(k)=="") and v is not None:
                    merged[key][k]=v
        else:
            merged.setdefault(key, {}).update(r)

    integrated = list(merged.values())
    try:
        integrated.sort(key=lambda x: (x.get("price") is None, x.get("price") or 0))
    except Exception:
        pass
    return integrated

# main flow
def run_query_interactive(query_text):
    try:
        if getattr(analyzer,"USE_LLM",False):
            parsed = analyzer.llm_parse(query_text)
        else:
            parsed = analyzer.regex_parse(query_text)
    except Exception as e:
        print("Analyzer LLM parse failed or missing; fallback to regex. Error:", e)
        parsed = analyzer.regex_parse(query_text)

    parsed["origin"] = fuzzy_city_normalize(parsed.get("origin"))
    parsed["destination"] = fuzzy_city_normalize(parsed.get("destination"))

    dwh_sql, dwh_params = build_param_sql_dwh(parsed)
    indigo_sql, indigo_params = build_param_sql_indigo(parsed)
    mongo_filter = build_mongo_filter(parsed)

    dwh_rows=[]; indigo_rows=[]; mongo_rows=[]
    if dwh_sql:
        try:
            print("Executing DWH on host:", DWH_PG_HOST, "params:", dwh_params)
            dwh_rows = execute_pg(DWH_PG_HOST, DWH_PG_PORT, DWH_PG_DB, DWH_PG_USER, DWH_PG_PASSWORD, dwh_sql, dwh_params)
        except Exception as e:
            print("DWH query failed:", e)
    if indigo_sql:
        try:
            print("Executing IndiGo on host:", INDIGO_PG_HOST, "params:", indigo_params)
            indigo_rows = execute_pg(INDIGO_PG_HOST, INDIGO_PG_PORT, INDIGO_PG_DB, INDIGO_PG_USER, INDIGO_PG_PASSWORD, indigo_sql, indigo_params)
        except Exception as e:
            print("IndiGo query failed:", e)
    if mongo_filter:
        print("Executing Mongo filter:", mongo_filter)
        mongo_rows = execute_mongo(mongo_filter)

    # If user explicitly requested an airline, filter each source result set
    requested_airline = parsed.get("airline")
    if requested_airline:
        dwh_rows = filter_rows_by_airline(dwh_rows, requested_airline)
        indigo_rows = filter_rows_by_airline(indigo_rows, requested_airline)
        mongo_rows = filter_rows_by_airline(mongo_rows, requested_airline)

    integrated = integrate_results(dwh_rows, indigo_rows, mongo_rows)

    rewritten_prompt = make_prompt(parsed, integrated)
    try:
        legacy_rewrite = rewrite_summary_prompt(parsed, integrated)
    except Exception:
        legacy_rewrite = None

    llm_summary = None
    llm_raw = None
    try:
        if getattr(analyzer,"USE_LLM",False) and safe_call_llm is not None:
            resp = safe_call_llm(rewritten_prompt)
            if isinstance(resp, dict) and resp.get("text"):
                text = resp["text"].strip()
                # try to extract json if wrapped in fences
                if text.startswith("```") and text.endswith("```"):
                    s = text.find("{")
                    e = text.rfind("}")
                    if s != -1 and e != -1 and e > s:
                        text = text[s:e+1]
                llm_summary = text
                llm_raw = resp.get("raw", None)
            else:
                llm_raw = resp
                try:
                    sorted_rows = [r for r in integrated if r.get("price") is not None]
                    sorted_rows.sort(key=lambda x: x.get("price"))
                    top3 = sorted_rows[:3]
                    llm_summary = "DETERMINISTIC FALLBACK (LLM call failed): " + ", ".join([f"{r.get('flight_no')}({r.get('price')})" for r in top3])
                except Exception:
                    llm_summary = "DETERMINISTIC FALLBACK (LLM call failed): no price info"
        else:
            try:
                sorted_rows = [r for r in integrated if r.get("price") is not None]
                sorted_rows.sort(key=lambda x: x.get("price"))
                top3 = sorted_rows[:3]
                llm_summary = "DETERMINISTIC FALLBACK (no LLM): " + ", ".join([f"{r.get('flight_no')}({r.get('price')})" for r in top3])
            except Exception:
                llm_summary = "DETERMINISTIC FALLBACK (no LLM): no price info"
            llm_raw = {"note": "LLM not called; analyzer.USE_LLM is False or llm_client.safe_call_llm not available."}
    except Exception as e:
        print("LLM summarization error:", e)
        try:
            sorted_rows = [r for r in integrated if r.get("price") is not None]
            sorted_rows.sort(key=lambda x: x.get("price"))
            top3 = sorted_rows[:3]
            llm_summary = "DETERMINISTIC FALLBACK (exception): " + ", ".join([f"{r.get('flight_no')}({r.get('price')})" for r in top3])
        except Exception:
            llm_summary = "DETERMINISTIC FALLBACK (exception): no price info"
        llm_raw = {"error": str(e)}

    # parse llm summary JSON
    llm_parsed = None
    if llm_summary:
        try:
            llm_parsed = json.loads(llm_summary)
        except Exception:
            try:
                s = llm_summary.find("{")
                e = llm_summary.rfind("}")
                if s != -1 and e != -1 and e > s:
                    llm_parsed = json.loads(llm_summary[s:e+1])
            except Exception:
                llm_parsed = None

    out = {
        "query": query_text,
        "parsed": parsed,
        "dwh_sql": {"sql": dwh_sql, "params": dwh_params},
        "indigo_sql": {"sql": indigo_sql, "params": indigo_params},
        "mongo_filter": mongo_filter,
        "dwh_results": dwh_rows,
        "indigo_results": indigo_rows,
        "mongo_results": mongo_rows,
        "integrated_results": integrated,
        "rewritten_prompt": rewritten_prompt,
        "legacy_rewrite": legacy_rewrite,
        "llm_summary": llm_summary,
        "llm_parsed": llm_parsed,
        "llm_raw": llm_raw
    }

    fname = f"output_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    with open(fname,"w",encoding="utf-8") as f:
        json.dump(out,f,indent=2,default=str)
    print(f"Written output to {fname}")
    print("=== Summary ===")
    print("Parsed:", json.dumps(parsed))
    print("DWH rows:", len(dwh_rows), "IndiGo rows:", len(indigo_rows), "Mongo rows:", len(mongo_rows))
    print("Top 3 integrated (flight_no(price)):", ", ".join([f"{r.get('flight_no')}({r.get('price')})" for r in integrated[:3]]))
    print("LLM summary:", llm_summary)
    return out

def run_batch(path):
    with open(path,"r") as f:
        queries = json.load(f)
    outputs=[]
    for q in queries:
        outputs.append(run_query_interactive(q))
    fname = f"batch_output_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    with open(fname,"w",encoding="utf-8") as f:
        json.dump(outputs,f,indent=2,default=str)
    print("Wrote batch output to", fname)

if __name__=="__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", help="json list of queries", default=None)
    args = ap.parse_args()
    if args.batch:
        run_batch(args.batch)
    else:
        q = input("Enter flight query: ").strip()
        run_query_interactive(q)
