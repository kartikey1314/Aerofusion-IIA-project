# backend/Query_engine/llm_client.py
import os
import time
from dotenv import load_dotenv
import openai
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # change if you want

if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY

class LLMError(Exception):
    pass

def make_prompt(parsed_intent: dict, integrated_rows: list) -> str:
    """
    Construct a concise, informative prompt for the LLM.
    Customize this template to your needs.
    """
    # short summary of parsed intent
    summary_lines = [
        "You are an assistant that summarizes flight search results for a user.",
        "User intent (parsed):",
    ]
    for k,v in parsed_intent.items():
        summary_lines.append(f"- {k}: {v}")
    summary_lines.append("")
    summary_lines.append("Return:")
    summary_lines.append("1) a short summarized list of top 5 cheapest flights (flight_no, airline, origin, destination, date, departure_time, price).")
    summary_lines.append("2) any integration notes or conflicting records found.")
    summary_lines.append("")
    summary_lines.append("Data rows (first 30 shown):")
    # show at most 30 rows
    for r in (integrated_rows or [])[:30]:
        summary_lines.append(f"- {r.get('flight_no')} | {r.get('airline')} | {r.get('origin')} -> {r.get('destination')} | {r.get('date')} {r.get('departure_time')} | price={r.get('price')}")
    summary_lines.append("")
    summary_lines.append("Provide the summary in JSON with keys: top_flights (list), notes (string).")
    return "\n".join(summary_lines)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10),
       retry=retry_if_exception_type(Exception))
def call_llm(prompt: str, model: str = DEFAULT_MODEL, max_tokens: int = 400) -> dict:
    """
    Call OpenAI LLM. Retries on transient errors.
    Returns dict: { 'text': <str>, 'usage': {...} } or raises LLMError.
    """
    if not OPENAI_API_KEY:
        raise LLMError("OPENAI_API_KEY not configured")

    try:
        resp = openai.ChatCompletion.create(  # if using chat-like API
            model=model,
            messages=[{"role":"system","content":"You are a helpful assistant."},
                      {"role":"user","content": prompt}],
            max_tokens=max_tokens,
            temperature=0.0,
        )
        text = resp.choices[0].message.content.strip()
        return {"text": text, "raw": resp}
    except Exception as e:
        # let retry wrapper handle retries
        raise

def safe_call_llm(prompt: str, model: str = DEFAULT_MODEL, max_tokens: int = 400):
    try:
        return call_llm(prompt, model=model, max_tokens=max_tokens)
    except Exception as e:
        # return structured error so caller can fallback
        return {"error": str(e)}
