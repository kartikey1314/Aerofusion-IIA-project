#!/usr/bin/env python3
"""
federator.py — returns:
 - separate results: dwh_results, indigo_results, mongo_results
 - integrated_results: merged and de-duplicated
 - decomposition: parsed, dwh_sql, indigo_sql, mongo_filter
 - rewritten_prompt (what was sent to LLM)
 - llm_summary (live LLM output or deterministic fallback)
 - llm_raw (raw LLM response or error info)

Drop this file into backend/Query_engine/ replacing the previous federator.py.
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
import ssl
import sys

import analyzer
from llm_rewriter import rewrite_summary_prompt

# new LLM client wrapper (create backend/Query_engine/llm_client.py as instructed)
from llm_client import make_prompt, safe_call_llm

load_dotenv()

# envs (single PG_* fallback)
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

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", None)

# canonicalization
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

# SQL builders
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
            "fare as price, NULL as seat_count, 'IndiGo' as source FROM indigo_src WHERE 1=1")
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

# Mongo helper (secure then optional insecure)
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
    # read collection
    try:
        db = client.get_default_database()
        coll = db.get_collection("airindia_flights")
        docs = list(coll.find(filter_dict).limit(limit))
    except Exception as e:
        print("Error reading Mongo:", repr(e))
        client.close()
        return []
    client.close()
    # normalize docs shape
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
            "source": "AirIndia"
        })
    return out

def compute_effective_price_from_mongo(doc):
    base = doc.get("pricing",{}).get("base_price")
    # support both pricing.offer.discount and pricing.discount_percent
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

# integration: also keep separate source lists
def integrate_results(dwh_rows, indigo_rows, mongo_rows):
    # create merged map keyed by flight_no or origin-dest-date
    merged={}
    for r in (dwh_rows or []):
        key = r.get("flight_no") or f"{r.get('origin')}-{r.get('destination')}-{r.get('date')}"
        merged.setdefault(key, {}).update(r)
    for r in (indigo_rows or []):
        key = r.get("flight_no") or f"{r.get('origin')}-{r.get('destination')}-{r.get('date')}"
        if key in merged:
            # fill missing fields
            for k,v in r.items():
                if (merged[key].get(k) is None or merged[key].get(k)=="") and v:
                    merged[key][k]=v
        else:
            merged.setdefault(key, {}).update(r)
    for r in (mongo_rows or []):
        key = r.get("flight_no") or f"{r.get('origin')}-{r.get('destination')}-{r.get('date')}"
        if key in merged:
            for k,v in r.items():
                if (merged[key].get(k) is None or merged[key].get(k)=="") and v:
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
    # parse using analyzer (LLM or regex)
    try:
        if getattr(analyzer,"USE_LLM",False):
            parsed = analyzer.llm_parse(query_text)
        else:
            parsed = analyzer.regex_parse(query_text)
    except Exception as e:
        print("Analyzer LLM parse failed or missing; fallback to regex. Error:", e)
        parsed = analyzer.regex_parse(query_text)

    # canonicalize
    parsed["origin"] = fuzzy_city_normalize(parsed.get("origin"))
    parsed["destination"] = fuzzy_city_normalize(parsed.get("destination"))

    # build parameterized SQL and mongo filter
    dwh_sql, dwh_params = build_param_sql_dwh(parsed)
    indigo_sql, indigo_params = build_param_sql_indigo(parsed)
    mongo_filter = build_mongo_filter(parsed)

    # run queries
    dwh_rows=[]
    indigo_rows=[]
    mongo_rows=[]
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

    # integrate
    integrated = integrate_results(dwh_rows, indigo_rows, mongo_rows)

    # rewrite prompt (explicit) and call LLM via llm_client with fallback
    # build a human-readable rewritten prompt for inspection / TA
    rewritten_prompt = make_prompt(parsed, integrated)
    # also keep the old rewriter output (if you want to compare)
    try:
        legacy_rewrite = rewrite_summary_prompt(parsed, integrated)
    except Exception:
        legacy_rewrite = None

    llm_summary = None
    llm_raw = None
    # use analyzer.USE_LLM & OPENAI_API_KEY to decide whether to call remote LLM
    try:
        if getattr(analyzer,"USE_LLM",False) and OPENAI_API_KEY:
            # call llm safely via wrapper (handles retries)
            resp = safe_call_llm(rewritten_prompt)
            if isinstance(resp, dict) and resp.get("text"):
                llm_summary = resp["text"]
                llm_raw = resp.get("raw", None)
            else:
                # resp is an error structure from safe_call_llm
                llm_raw = resp
                # deterministic fallback
                try:
                    sorted_rows = sorted([r for r in integrated if r.get("price") is not None], key=lambda x: x.get("price"))
                    top3 = sorted_rows[:3]
                    llm_summary = "DETERMINISTIC FALLBACK (LLM call failed): " + ", ".join([f"{r.get('flight_no')}({r.get('price')})" for r in top3])
                except Exception:
                    llm_summary = "DETERMINISTIC FALLBACK (LLM call failed): no price info"
        else:
            # deterministic fallback (no API key or USE_LLM is false)
            try:
                sorted_rows = sorted([r for r in integrated if r.get("price") is not None], key=lambda x: x.get("price"))
                top3 = sorted_rows[:3]
                llm_summary = "DETERMINISTIC FALLBACK (no LLM): " + ", ".join([f"{r.get('flight_no')}({r.get('price')})" for r in top3])
            except Exception:
                llm_summary = "DETERMINISTIC FALLBACK (no LLM): no price info"
            llm_raw = {"note": "LLM not called; USE_LLM or OPENAI_API_KEY not configured."}
    except Exception as e:
        print("LLM summarization error:", e)
        # final fallback
        try:
            sorted_rows = sorted([r for r in integrated if r.get("price") is not None], key=lambda x: x.get("price"))
            top3 = sorted_rows[:3]
            llm_summary = "DETERMINISTIC FALLBACK (exception): " + ", ".join([f"{r.get('flight_no')}({r.get('price')})" for r in top3])
        except Exception:
            llm_summary = "DETERMINISTIC FALLBACK (exception): no price info"
        llm_raw = {"error": str(e)}

    # output structure with separate + integrated results
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
        "llm_raw": llm_raw
    }

    fname = f"output_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    with open(fname,"w",encoding="utf-8") as f:
        json.dump(out,f,indent=2,default=str)
    print(f"Written output to {fname}")
    # print short console summary for TA
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
