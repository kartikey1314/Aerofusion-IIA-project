#!/usr/bin/env python3
"""
analyzer.py — Intent parser for Task 2(a) with optional LLM-backed parsing using Groq.

- USE_LLM toggles whether to attempt LLM parsing first. If LLM parsing fails or returns invalid JSON,
  falls back to regex_parse().
- Exposes: USE_LLM, llm_parse(text), regex_parse(text), run_interactive_query(raw_query)
- Keeps printed output format compatible with your older analyzer output.
"""

import os
import re
import json
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

load_dotenv()

# Toggle this (True attempts LLM parse via llm_client.safe_call_llm)
USE_LLM = True

# Try to import the llm_client wrapper (expects safe_call_llm(prompt) -> {'text':..., 'raw':...} or {'error':...})
try:
    from llm_client import safe_call_llm
except Exception:
    safe_call_llm = None

# ---------------------------
# Regex parser (robust)
# ---------------------------
def regex_parse(text: str) -> dict:
    t = text.strip()
    tl = t.lower()
    out = {
        "airline": None,
        "origin": None,
        "destination": None,
        "date": None,
        "price_limit": None,
        "seat_count": None,
        "intent": "LIST"
    }

    # Origin / destination: prefer "from X to Y" then "X to Y"
    m = re.search(r"from\s+([A-Za-z\s]+?)\s+to\s+([A-Za-z\s]+?)(?:\s|$|\?|$)", t, flags=re.IGNORECASE)
    if m:
        out["origin"] = m.group(1).strip().title()
        out["destination"] = m.group(2).strip().title()
    else:
        m2 = re.search(r"([A-Za-z\s]{2,})\s+to\s+([A-Za-z\s]{2,})", t, flags=re.IGNORECASE)
        if m2:
            out["origin"] = m2.group(1).strip().title()
            out["destination"] = m2.group(2).strip().title()

    # date tokens: ISO first, then keywords
    date_m = re.search(r"(?P<d>\d{4}-\d{2}-\d{2})", t)
    if date_m:
        out["date"] = date_m.group("d")
    else:
        if "tomorrow" in tl:
            out["date"] = (date.today() + timedelta(days=1)).isoformat()
        elif "today" in tl:
            out["date"] = date.today().isoformat()

    # price limit
    mprice = re.search(r"(?:under|below|less than|<)\s*(\d{2,7})", tl)
    if mprice:
        try:
            out["price_limit"] = int(mprice.group(1))
        except:
            out["price_limit"] = None

    # seat count
    mseat = re.search(r"(?:at least|minimum|more than|>=|>)?\s*(\d{1,3})\s+seats?", tl)
    if mseat:
        try:
            out["seat_count"] = int(mseat.group(1))
        except:
            out["seat_count"] = None

    # intent mapping
    if "cheapest" in tl or "lowest price" in tl or "lowest" in tl:
        out["intent"] = "MIN"
    elif "average" in tl or "avg" in tl or "mean" in tl:
        out["intent"] = "AVG"
    else:
        out["intent"] = "LIST"

    # Airline extraction (robust):
    # common patterns: "by AIRLINE", "on AIRLINE", "via AIRLINE", "AIRLINE flights", "AIRLINE only"
    m_air = re.search(r"(?:by|on|via)\s+([A-Za-z\s]+?)(?:\s+from|\s+to|\s+for|\s+tomorrow|\s+today|$|\?)", t, flags=re.IGNORECASE)
    if m_air:
        out["airline"] = _normalize_airline_token(m_air.group(1))
    else:
        # word-level detection
        m_air2 = re.search(r"\b(air india|airindia|indigo|spicejet|spice jet|vistara|jet airways|jet|gofirst|go first|airasia|vistara)\b", tl, flags=re.IGNORECASE)
        if m_air2:
            out["airline"] = _normalize_airline_token(m_air2.group(1))

    return out

# helper to normalize airline tokens to canonical display names
def _normalize_airline_token(tok: str):
    if not tok:
        return None
    t = tok.strip().lower()
    mapping = {
        "airindia": "Air India", "air india": "Air India",
        "indigo": "IndiGo",
        "spicejet": "SpiceJet", "spice jet": "SpiceJet",
        "vistara": "Vistara",
        "jet airways": "Jet Airways", "jet": "Jet Airways",
        "gofirst": "GoFirst", "go first": "GoFirst",
        "airasia": "AirAsia"
    }
    # direct match
    if t in mapping:
        return mapping[t]
    # substring match
    for k, v in mapping.items():
        if k in t:
            return v
    # title-case fallback
    return tok.strip().title()

# ---------------------------
# LLM-backed parsing helpers
# ---------------------------
def _make_parse_prompt(text: str) -> str:
    """
    Prompt Groq (or other LLM) to return strictly valid JSON for intent extraction.
    We instruct the model to return EXACTLY the JSON object with the required keys.
    """
    prompt = f"""You are an assistant that extracts structured intent from a user's short flight search sentence.
Respond ONLY with a JSON object (no commentary) with these keys:
- airline (string or null)
- origin (string or null)
- destination (string or null)
- date (ISO date YYYY-MM-DD or null)
- price_limit (number or null)
- seat_count (integer or null)
- intent (one of LIST, MIN, AVG)

Rules:
- If a value is not present, return null.
- Normalize city names to Title Case (e.g., 'delgi' -> 'Delhi').
- Convert relative dates: 'today'/'tomorrow' -> YYYY-MM-DD.
- If user mentioned an airline (e.g., 'by Air India', 'on indigo', 'airindia flights'), populate 'airline' with canonical name (Air India, IndiGo, SpiceJet, Vistara, etc.).
- Return valid JSON only.

User input:
\"\"\"{text}\"\"\""""
    return prompt

def _parse_llm_json(text_json: str) -> dict:
    """
    Parse LLM JSON string and coerce to expected keys. Raises ValueError if invalid.
    """
    # Try to find a JSON substring if the model added fences or commentary
    candidate = text_json.strip()
    # strip code fences if present
    if candidate.startswith("```"):
        # attempt to remove wrapping triple backticks
        candidate = candidate.strip("` \n")
    # If there's extra text, extract first { ... }
    if not candidate.startswith("{"):
        s = candidate.find("{")
        e = candidate.rfind("}")
        if s != -1 and e != -1 and e > s:
            candidate = candidate[s:e+1]
    try:
        parsed = json.loads(candidate)
    except Exception as e:
        raise ValueError(f"Invalid JSON from LLM: {e}\nRaw was: {text_json[:500]}")
    if not isinstance(parsed, dict):
        raise ValueError("Parsed JSON is not an object")
    # ensure keys, normalize types
    keys = ["airline","origin","destination","date","price_limit","seat_count","intent"]
    out = {}
    for k in keys:
        v = parsed.get(k) if isinstance(parsed, dict) else None
        if isinstance(v, str) and v.strip() == "":
            v = None
        out[k] = v
    # normalize date
    if out.get("date"):
        try:
            dt = datetime.fromisoformat(out["date"]).date()
            out["date"] = dt.isoformat()
        except Exception:
            # leave as-is
            pass
    # normalize airline token if string
    if out.get("airline") and isinstance(out["airline"], str):
        out["airline"] = _normalize_airline_token(out["airline"])
    # normalize intent
    if out.get("intent") is None:
        out["intent"] = "LIST"
    else:
        out["intent"] = str(out["intent"]).upper()
        if out["intent"] not in ("LIST","MIN","AVG"):
            out["intent"] = "LIST"
    # normalize cities
    if out.get("origin") and isinstance(out["origin"], str):
        out["origin"] = out["origin"].strip().title()
    if out.get("destination") and isinstance(out["destination"], str):
        out["destination"] = out["destination"].strip().title()
    # coerce numeric fields
    if out.get("price_limit") is not None:
        try:
            out["price_limit"] = float(out["price_limit"])
        except:
            out["price_limit"] = None
    if out.get("seat_count") is not None:
        try:
            out["seat_count"] = int(out["seat_count"])
        except:
            out["seat_count"] = None
    return out

def llm_parse(text: str) -> dict:
    """
    Use the LLM via safe_call_llm to extract intent. Falls back to regex_parse on any error.
    """
    if safe_call_llm is None:
        return regex_parse(text)
    prompt = _make_parse_prompt(text)
    resp = safe_call_llm(prompt)
    # resp -> {'text':..., 'raw':...} or {'error':...}
    if isinstance(resp, dict) and resp.get("text"):
        try:
            parsed = _parse_llm_json(resp["text"])
            # ensure keys present
            for k in ["airline","origin","destination","date","price_limit","seat_count","intent"]:
                parsed.setdefault(k, None)
            return parsed
        except Exception:
            # fallback to regex
            return regex_parse(text)
    else:
        # fallback to regex
        return regex_parse(text)

# ---------------------------
# SQL / Mongo builders (compatible with your federator)
# ---------------------------
def build_indigo_sql(parsed: dict):
    base = ("SELECT flight_no, airline, from_city AS origin, to_city AS destination, journey_date AS date, "
            "departure_time, fare as price, NULL as seat_count, 'IndiGo' as source FROM indigo_src WHERE 1=1")
    params = []
    if parsed.get("origin"): base += " AND from_city = %s"; params.append(parsed["origin"])
    if parsed.get("destination"): base += " AND to_city = %s"; params.append(parsed["destination"])
    if parsed.get("date"): base += " AND journey_date = %s"; params.append(parsed["date"])
    return (base + ";", params)

def build_dwh_sql(parsed: dict):
    base = ("SELECT flight_no, airline, origin, destination, flight_date AS date, price, array_length(seats,1) AS seat_count, 'DWH' as source FROM flights_dwh WHERE 1=1")
    params = []
    if parsed.get("origin"): base += " AND origin = %s"; params.append(parsed["origin"])
    if parsed.get("destination"): base += " AND destination = %s"; params.append(parsed["destination"])
    if parsed.get("date"): base += " AND flight_date = %s"; params.append(parsed["date"])
    return (base + ";", params)

def build_mongo_filter(parsed: dict):
    filt = {}
    if parsed.get("origin"): filt["route.origin"] = parsed["origin"]
    if parsed.get("destination"): filt["route.destination"] = parsed["destination"]
    if parsed.get("date"): filt["schedule.date"] = parsed["date"]
    if parsed.get("airline"): filt["airline_name"] = parsed["airline"]
    return filt

# ---------------------------
# CLI / main behavior — preserve original prints
# ---------------------------
def run_interactive_query(raw_query: str = None):
    if raw_query is None:
        raw_query = input("Enter flight query: ").strip()

    parsed = None
    try:
        if USE_LLM and safe_call_llm is not None:
            try:
                parsed = llm_parse(raw_query)
            except Exception as e:
                print("LLM parse failed, switching to regex:", e)
                parsed = regex_parse(raw_query)
        else:
            parsed = regex_parse(raw_query)
    except Exception as e:
        print("Parse error, falling back to regex:", e)
        parsed = regex_parse(raw_query)

    # Prepare print-friendly copy (lowercase origin/destination like older output)
    parsed_for_print = parsed.copy()
    if parsed_for_print.get("origin"):
        parsed_for_print["origin"] = parsed_for_print["origin"].lower()
    if parsed_for_print.get("destination"):
        parsed_for_print["destination"] = parsed_for_print["destination"].lower()

    print("\nParsed Intent:")
    print(json.dumps(parsed_for_print, indent=2))

    # Build SQLs & mongo filter
    indigo_sql, indigo_params = build_indigo_sql(parsed)
    dwh_sql, dwh_params = build_dwh_sql(parsed)
    mongo_filter = build_mongo_filter(parsed)

    print("\nIndiGo (Postgres - Structured Source):")
    print(indigo_sql.replace("%s", "'{}'").format(*[p for p in indigo_params]) if indigo_params else indigo_sql)

    print("\nData Warehouse (Postgres - Unified):")
    print(dwh_sql.replace("%s","'{}'").format(*[p for p in dwh_params]) if dwh_params else dwh_sql)

    print("\nAir India (MongoDB - Semi-Structured Source):")
    print("db.airindia_flights.find({")
    mf_lines = []
    for k,v in mongo_filter.items():
        mf_lines.append(f'  "{k}": "{v}"')
    if mf_lines:
        print(",\n".join(mf_lines))
    print("});\n")

    print("LLM Instruction:")
    print("Summarize cheapest and available flight options with contextual insights.")

    return parsed

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        q = " ".join(sys.argv[1:])
        run_interactive_query(q)
    else:
        run_interactive_query()
