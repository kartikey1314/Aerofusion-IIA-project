import os
import json
import re
from datetime import datetime, timedelta
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Use OpenAI API if available
try:
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    USE_LLM = True
except Exception as e:
    print("OpenAI SDK not available or key missing – using regex fallback.")
    USE_LLM = False


# -----------------------------
# Helper: Simple date resolver
# -----------------------------
def resolve_date(text: str) -> str:
    today = datetime.today().date()
    if "tomorrow" in text:
        return str(today + timedelta(days=1))
    elif "today" in text:
        return str(today)
    else:
        match = re.search(r"(\d{1,2})\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)", text)
        if match:
            month_map = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
                         "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
            day, month = int(match.group(1)), month_map[match.group(2)]
            return str(datetime(datetime.now().year, month, day).date())
    return None


# -----------------------------
# Regex fallback parser
# -----------------------------
def regex_parse(query: str) -> Dict[str, Any]:
    query = query.lower()
    parsed = {
        "airline": None,
        "origin": None,
        "destination": None,
        "date": resolve_date(query),
        "price_limit": None,
        "seat_count": None,
        "intent": "LIST"
    }

    # Detect cities
    from_to = re.search(r"from\s+(\w+)\s+to\s+(\w+)", query)
    if from_to:
        parsed["origin"], parsed["destination"] = from_to.groups()

    # Detect airline
    if "indigo" in query:
        parsed["airline"] = "IndiGo"
    elif "air india" in query:
        parsed["airline"] = "Air India"

    # Detect price
    price = re.search(r"(under|less than|below)\s*(\d+)", query)
    if price:
        parsed["price_limit"] = int(price.group(2))

    # Detect seat count
    seat = re.search(r"(more than|at least|min)\s*(\d+)\s*seats?", query)
    if seat:
        parsed["seat_count"] = int(seat.group(2))

    # Detect intent
    if "cheapest" in query or "lowest" in query:
        parsed["intent"] = "MIN"
    elif "average" in query or "mean" in query:
        parsed["intent"] = "AVG"

    return parsed


# -----------------------------
# LLM-based query understanding
# -----------------------------
def llm_parse(query: str) -> Dict[str, Any]:
    prompt = f"""
    You are a flight query parser. Extract the following details from the user's question:
    airline, origin, destination, date (YYYY-MM-DD or 'tomorrow'), price_limit, seat_count, and intent (MIN, AVG, LIST).
    Return as JSON.
    User query: "{query}"
    """
    try:
        response = client.responses.create(model="gpt-4.1-mini", input=prompt)
        text = response.output[0].content[0].text.strip()
        return json.loads(text)
    except Exception as e:
        print("LLM parse failed, switching to regex:", e)
        return regex_parse(query)


# -----------------------------
# Query decomposition
# -----------------------------
def decompose(parsed: Dict[str, Any]) -> Dict[str, str]:
    origin = parsed.get("origin")
    destination = parsed.get("destination")
    date = parsed.get("date")
    airline = parsed.get("airline")
    intent = str(parsed.get("intent", "LIST")).upper()
    price_limit = parsed.get("price_limit")
    seat_count = parsed.get("seat_count")

    dwh_sql = (
        "SELECT flight_no, airline, origin, destination, flight_date, price, "
        "array_length(seats, 1) AS seat_count "
        "FROM flights_dwh WHERE 1=1"
    )
    if origin: dwh_sql += f" AND origin='{origin}'"
    if destination: dwh_sql += f" AND destination='{destination}'"
    if date: dwh_sql += f" AND flight_date='{date}'"
    if price_limit: dwh_sql += f" AND price < {price_limit}"
    if seat_count: dwh_sql += f" AND array_length(seats, 1) > {seat_count}"
    if intent == "MIN": dwh_sql += " ORDER BY price ASC LIMIT 1;"
    elif intent == "AVG": dwh_sql = f"SELECT AVG(price) FROM flights_dwh WHERE origin='{origin}' AND destination='{destination}';"
    else: dwh_sql += ";"

    # ---------- IndiGo SQL ----------
    indigo_sql = (
        "SELECT flight_no, airline, from_city AS origin, to_city AS destination, journey_date, "
        "fare, discount_percent, offer_name FROM indigo_src WHERE 1=1"
    )
    if origin: indigo_sql += f" AND from_city='{origin}'"
    if destination: indigo_sql += f" AND to_city='{destination}'"
    if date: indigo_sql += f" AND journey_date='{date}'"
    if price_limit: indigo_sql += f" AND fare < {price_limit}"
    if seat_count: indigo_sql += f" AND array_length(seats, 1) > {seat_count}"
    if intent == "MIN": indigo_sql += " ORDER BY fare ASC LIMIT 1;"
    elif intent == "AVG": indigo_sql = f"SELECT AVG(fare) FROM indigo_src WHERE from_city='{origin}' AND to_city='{destination}';"
    else: indigo_sql += ";"

    # ---------- Air India Mongo ----------
    mongo_cond = {}
    if origin: mongo_cond["route.origin"] = origin
    if destination: mongo_cond["route.destination"] = destination
    if date: mongo_cond["schedule.date"] = date
    if price_limit: mongo_cond["pricing.base_price"] = {"$lt": price_limit}
    if seat_count: mongo_cond["availability.seats_count"] = {"$gt": seat_count}
    if airline: mongo_cond["airline_name"] = airline
    mongo_cond = {k: v for k, v in mongo_cond.items() if v is not None}
    mongo_query = f"db.airindia_flights.find({json.dumps(mongo_cond, indent=2)});"

    llm_task = "Summarize cheapest and available flight options with contextual insights."

    return {
        "indigo_sql": indigo_sql.strip(),
        "dwh_sql": dwh_sql.strip(),
        "mongo_query": mongo_query,
        "llm_instruction": llm_task
    }


# -----------------------------
# Main Execution
# -----------------------------
if __name__ == "__main__":
    user_query = input("Enter flight query: ").strip()
    parsed = llm_parse(user_query) if USE_LLM else regex_parse(user_query)

    print("\nParsed Intent:")
    print(json.dumps(parsed, indent=2))

    queries = decompose(parsed)

    print("\nIndiGo (Postgres - Structured Source):")
    print(queries["indigo_sql"])

    print("\nData Warehouse (Postgres - Unified):")
    print(queries["dwh_sql"])

    print("\nAir India (MongoDB - Semi-Structured Source):")
    print(queries["mongo_query"])

    print("\nLLM Instruction:")
    print(queries["llm_instruction"])

# import os, re, json, datetime
# from datetime import date
# from typing import Dict, Any
# from dotenv import load_dotenv

# # Load environment variables
# load_dotenv()

# # ----------------------------
# #LLM Integration (OpenAI)
# # ----------------------------
# try:
#     from openai import OpenAI
#     OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
#     client = OpenAI(api_key=OPENAI_API_KEY)
# except Exception:
#     client = None
#     print("using mock LLM responses.")

# # ----------------------------
# # Load keyword rules
# # ----------------------------
# base_dir = os.path.dirname(__file__)
# rules_path = os.path.join(base_dir, "rules.json")

# with open(rules_path) as f:
#     RULES = json.load(f)

# # ----------------------------
# # Date Resolver
# # ----------------------------
# MONTHS = {
#     "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
#     "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
#     "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
#     "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
# }

# def resolve_date(text: str) -> str:
#     text = text.lower()
#     today = date.today()

#     if "tomorrow" in text:
#         return str(today + datetime.timedelta(days=1))
#     if "today" in text:
#         return str(today)

#     m = re.search(r"\d{4}-\d{2}-\d{2}", text)
#     if m:
#         return m.group(0)


#     m = re.search(r"(\d{1,2})\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s*(\d{4})?", text)
#     if m:
#         day = int(m.group(1))
#         mon = MONTHS[m.group(2)[:3]]
#         yr = int(m.group(3)) if m.group(3) else today.year
#         try:
#             return str(date(yr, mon, day))
#         except ValueError:
#             pass

#     return str(today)

# # ----------------------------
# # Rule-Based Analyzer
# # ----------------------------
# def analyze_query(user_query: str) -> Dict[str, Any]:
#     q = user_query.lower()
#     result = {
#         "airline": None,
#         "origin": None,
#         "destination": None,
#         "intent": "LIST",
#         "price_limit": None,
#         "date": resolve_date(q)
#     }

#     for airline in RULES["airlines"]:
#         if airline in q:
#             result["airline"] = airline.title()

#     for key, val in RULES["intents"].items():
#         if key in q:
#             result["intent"] = val

#     # Origin detection
#     m = re.search(r"from\s+(\w+)", q)
#     if m:
#         result["origin"] = m.group(1).title()
#     else:
#         m = re.search(r"(\w+)\s+to\s+\w+", q)
#         if m:
#             result["origin"] = m.group(1).title()

#     # Destination
#     m = re.search(r"to\s+(\w+)", q)
#     if m:
#         result["destination"] = m.group(1).title()

#     # Price limit
#     m = re.search(r"(under|less than|below)\s+(\d+)", q)
#     if m:
#         result["price_limit"] = int(m.group(2))

#     # Seat count
#     m = re.search(r"(more than|at least|min(?:imum)? of)?\s*(\d+)\s*(\+)?\s*seats", q)
#     result["seat_count"] = int(m.group(2)) if m else None

#     return result



# def llm_extract_intent(user_query: str) -> dict:
#     """
#     Uses LLM to extract structured query parameters.
#     Falls back to regex-based analyze_query if LLM unavailable.
#     """
#     prompt = f"""
#     You are AeroFusion's intelligent flight query parser.
#     Extract structured query fields from this text query.
#     Always return JSON with:
#     airline, origin, destination, date, price_limit, seat_count, intent.

#     Example:
#     Input: "Show cheapest Air India flight from Delhi to London tomorrow under 70000"
#     Output:
#     {{
#         "airline": "Air India",
#         "origin": "Delhi",
#         "destination": "London",
#         "date": "tomorrow",
#         "price_limit": 70000,
#         "seat_count": null,
#         "intent": "cheapest"
#     }}

#     Now parse this query:
#     {user_query}
#     """

#     if client and os.getenv("OPENAI_API_KEY"):
#         try:
#             response = client.chat.completions.create(
#                 model="gpt-4o-mini",
#                 messages=[
#                     {"role": "system", "content": "You are a JSON-only query parser."},
#                     {"role": "user", "content": prompt}
#                 ],
#                 max_tokens=150
#             )
#             content = response.choices[0].message.content.strip()
#             try:
#                 parsed = json.loads(content)
#                 return parsed
#             except json.JSONDecodeError:
#                 print("⚠️ Invalid JSON from LLM, falling back to regex parser.")
#         except Exception as e:
#             print(f"⚠️ LLM extract error: {e}")

#     # Fallback
#     return analyze_query(user_query)

# # Query Decomposition
# def decompose(parsed: Dict[str, Any]) -> Dict[str, str]:
#     origin = parsed.get("origin")
#     destination = parsed.get("destination")
#     date = parsed.get("date")
#     airline = parsed.get("airline")
#     intent = str(parsed.get("intent", "LIST")).upper()
#     price_limit = parsed.get("price_limit")
#     seat_count = parsed.get("seat_count")

#     sql = (
#         "SELECT flight_no, airline, origin, destination, flight_date, price, array_length(seats, 1) AS seat_count "
#         "FROM flights_dwh WHERE 1=1"
#     )
#     if origin: sql += f" AND origin='{origin}'"
#     if destination: sql += f" AND destination='{destination}'"
#     if date: sql += f" AND flight_date='{date}'"
#     if price_limit: sql += f" AND price < {price_limit}"
#     if seat_count: sql += f" AND array_length(seats, 1) > {seat_count}"

#     if "MIN" in intent or "CHEAP" in intent:
#         sql += " ORDER BY price ASC LIMIT 1;"
#     elif "AVG" in intent:
#         sql = (
#             f"SELECT AVG(price) AS avg_price FROM flights_dwh "
#             f"WHERE origin='{origin}' AND destination='{destination}';"
#         )
#     else:
#         sql += ";"

#     cond = {}
#     if origin: cond["route.origin"] = origin
#     if destination: cond["route.destination"] = destination
#     if date: cond["schedule.date"] = date
#     if price_limit: cond["pricing.base_price"] = {"$lt": price_limit}
#     mongo = json.dumps(cond, indent=2)

#     llm_task = "Summarize cheapest and available options with contextual insights."


#     clean_sql = " ".join(sql.split())

#     return {
#         "sql_query": clean_sql,
#         "mongo_query": mongo,
#         "llm_instruction": llm_task
#     }

# # LLM Reasoner

# def llm_reasoning(parsed: Dict[str, Any], decomposed: Dict[str, str]) -> str:
#     prompt = f"""
#     You are AeroFusion's flight advisor.
#     User intent: {parsed}
#     SQL query: {decomposed['sql_query']}
#     Mongo query: {decomposed['mongo_query']}
#     Task: {decomposed['llm_instruction']}
#     Generate a natural-language summary or recommendation (2-3 sentences).
#     """

#     if client and os.getenv("OPENAI_API_KEY"):
#         try:
#             response = client.chat.completions.create(
#                 model="gpt-4o-mini",
#                 messages=[
#                     {"role": "system", "content": "You are a flight analytics assistant."},
#                     {"role": "user", "content": prompt}
#                 ],
#                 max_tokens=120
#             )
#             return response.choices[0].message.content.strip()
#         except Exception as e:
#             print("LLM reasoning error: {e}")
#             return "Based on available trends, prices seem stable. You can book now."
#     else:
#         return "Free llm try again"

# # ----------------------------
# # Main Execution (CLI)
# # ----------------------------
# if __name__ == "__main__":
#     user_query = input("Enter text query: ")
#     print("Extracting query intent (via LLM + fallback)...")
#     parsed = llm_extract_intent(user_query)
#     print(json.dumps(parsed, indent=2))

#     decomposed = decompose(parsed)
#     print("Decomposed Queries:")
#     print(json.dumps(decomposed, indent=2))

#     # print("LLM Reasoning:")
#     # print(llm_reasoning(parsed, decomposed))
