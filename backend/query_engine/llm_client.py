# # backend/Query_engine/llm_client.py
# import os
# import time
# from dotenv import load_dotenv
# import openai
# from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# load_dotenv()
# OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # change if you want

# if OPENAI_API_KEY:
#     openai.api_key = OPENAI_API_KEY

# class LLMError(Exception):
#     pass

# def make_prompt(parsed_intent: dict, integrated_rows: list) -> str:
#     """
#     Construct a concise, informative prompt for the LLM.
#     Customize this template to your needs.
#     """
#     # short summary of parsed intent
#     summary_lines = [
#         "You are an assistant that summarizes flight search results for a user.",
#         "User intent (parsed):",
#     ]
#     for k,v in parsed_intent.items():
#         summary_lines.append(f"- {k}: {v}")
#     summary_lines.append("")
#     summary_lines.append("Return:")
#     summary_lines.append("1) a short summarized list of top 5 cheapest flights (flight_no, airline, origin, destination, date, departure_time, price).")
#     summary_lines.append("2) any integration notes or conflicting records found.")
#     summary_lines.append("")
#     summary_lines.append("Data rows (first 30 shown):")
#     # show at most 30 rows
#     for r in (integrated_rows or [])[:30]:
#         summary_lines.append(f"- {r.get('flight_no')} | {r.get('airline')} | {r.get('origin')} -> {r.get('destination')} | {r.get('date')} {r.get('departure_time')} | price={r.get('price')}")
#     summary_lines.append("")
#     summary_lines.append("Provide the summary in JSON with keys: top_flights (list), notes (string).")
#     return "\n".join(summary_lines)

# @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10),
#        retry=retry_if_exception_type(Exception))
# def call_llm(prompt: str, model: str = DEFAULT_MODEL, max_tokens: int = 400) -> dict:
#     """
#     Call OpenAI LLM. Retries on transient errors.
#     Returns dict: { 'text': <str>, 'usage': {...} } or raises LLMError.
#     """
#     if not OPENAI_API_KEY:
#         raise LLMError("OPENAI_API_KEY not configured")

#     try:
#         resp = openai.ChatCompletion.create(  # if using chat-like API
#             model=model,
#             messages=[{"role":"system","content":"You are a helpful assistant."},
#                       {"role":"user","content": prompt}],
#             max_tokens=max_tokens,
#             temperature=0.0,
#         )
#         text = resp.choices[0].message.content.strip()
#         return {"text": text, "raw": resp}
#     except Exception as e:
#         # let retry wrapper handle retries
#         raise

# def safe_call_llm(prompt: str, model: str = DEFAULT_MODEL, max_tokens: int = 400):
#     try:
#         return call_llm(prompt, model=model, max_tokens=max_tokens)
#     except Exception as e:
#         # return structured error so caller can fallback
#         return {"error": str(e)}



# backend/Query_engine/llm_client.py
import os
import json
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_API_BASE = os.getenv("GROQ_API_BASE", "https://api.groq.com")

# Try to import official groq SDK (if installed). If not, we'll use HTTP via requests.
_try_imported_sdk = False
_groq_client = None
try:
    from groq import Groq
    _try_imported_sdk = True
    if GROQ_API_KEY:
        _groq_client = Groq(api_key=GROQ_API_KEY)
except Exception:
    _try_imported_sdk = False
    _groq_client = None

# If no SDK available, we'll use requests for HTTP calls
if not _try_imported_sdk:
    import requests  # standard requests fallback

# -------------------------
# Prompt builder (keeps old format)
# -------------------------
def make_prompt(parsed_intent: dict, integrated_rows: list) -> str:
    """
    Build a concise, informative prompt for summarization.
    Returns a string prompt to send to the LLM.
    """
    summary_lines = [
        "You are an assistant that summarizes flight search results for a user.",
        "User intent (parsed):",
    ]
    for k, v in (parsed_intent or {}).items():
        summary_lines.append(f"- {k}: {v}")
    summary_lines.append("")
    summary_lines.append("Return:")
    summary_lines.append("1) a short summarized list of top 5 cheapest flights (flight_no, airline, origin, destination, date, departure_time, price).")
    summary_lines.append("2) any integration notes or conflicting records found.")
    summary_lines.append("")
    summary_lines.append("Data rows (first 30 shown):")
    for r in (integrated_rows or [])[:30]:
        summary_lines.append(f"- {r.get('flight_no')} | {r.get('airline')} | {r.get('origin')} -> {r.get('destination')} | {r.get('date')} {r.get('departure_time')} | price={r.get('price')}")
    summary_lines.append("")
    summary_lines.append("Important: return valid JSON only (no extra commentary). Provide keys: top_flights (list), notes (string).")
    return "\n".join(summary_lines)

# -------------------------
# Internal callers with retry
# -------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10),
       retry=retry_if_exception_type(Exception))
def _call_groq_sdk(prompt: str, model: str):
    """Call Groq via SDK (if available)."""
    if not _groq_client:
        raise RuntimeError("Groq SDK client not configured (GROQ_API_KEY missing or SDK not installed).")
    # SDK chat interface: adapt to SDK method names if they differ in your SDK version
    resp = _groq_client.chat.completions.create(
        model=model,
        messages=[{"role":"system","content":"You are a helpful assistant."},
                  {"role":"user","content":prompt}],
        max_tokens=512,
        temperature=0.0
    )
    # Extract text (SDKs vary in shape)
    text = None
    try:
        if hasattr(resp, "choices") and len(resp.choices) > 0:
            choice = resp.choices[0]
            # many SDKs put content under choice.message.content
            if hasattr(choice, "message") and getattr(choice.message, "content", None):
                text = choice.message.content
            elif hasattr(choice, "text"):
                text = choice.text
    except Exception:
        pass
    return {"text": text, "raw": resp}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10),
       retry=retry_if_exception_type(Exception))
def _call_groq_http(prompt: str, model: str):
    """HTTP fallback using requests to call Groq REST API."""
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY not configured in environment")
    url = f"{GROQ_API_BASE}/v1/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 512,
        "temperature": 0.0
    }
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code >= 400:
        # raise to trigger retry wrapper (and surface error)
        raise RuntimeError(f"Groq HTTP error {r.status_code}: {r.text}")
    resp = r.json()
    text = None
    if "choices" in resp and len(resp["choices"]) > 0:
        c = resp["choices"][0]
        if "message" in c and "content" in c["message"]:
            text = c["message"]["content"]
        elif "text" in c:
            text = c["text"]
    return {"text": text, "raw": resp}

def _call_groq(prompt: str, model: str):
    """Pick SDK or HTTP method depending on availability."""
    model = model or GROQ_MODEL
    if _try_imported_sdk and _groq_client:
        return _call_groq_sdk(prompt, model)
    else:
        return _call_groq_http(prompt, model)

# -------------------------
# Public safe wrapper
# -------------------------
def safe_call_llm(prompt: str, model: str = None, max_tokens: int = 512):
    """
    Public wrapper used by federator/analyzer.
    Returns: {'text': <str>, 'raw': <provider_raw>} on success OR {'error': <str>} on failure.
    """
    try:
        resp = _call_groq(prompt, model=model or GROQ_MODEL)
        if resp is None or resp.get("text") is None:
            return {"error": "Empty response from Groq", "raw": resp}
        return {"text": resp["text"].strip(), "raw": resp.get("raw")}
    except Exception as e:
        return {"error": str(e)}

# Allow quick local testing
if __name__ == "__main__":
    prompt = make_prompt({"origin":"Delhi","destination":"Kolkata","date":"2025-11-17"}, [
        {"flight_no":"IGKOL20251117001","airline":"IndiGo","origin":"Delhi","destination":"Kolkata","date":"2025-11-17","departure_time":"06:00","price":4917.6}
    ])
    print("PROMPT PREVIEW:\n", prompt[:1000])
    print("Calling Groq (safe_call_llm)...\n")
    print(safe_call_llm(prompt))
