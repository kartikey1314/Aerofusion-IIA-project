# backend/Query_engine/test_llm.py
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# import the helper functions
from llm_client import make_prompt, safe_call_llm

def main():
    parsed = {"origin":"Delhi","destination":"Kolkata","date":"2025-11-17"}
    prompt = make_prompt(parsed, [])
    print("=== Prompt (first 400 chars) ===")
    print(prompt[:400])
    print("=== Calling LLM (safe_call_llm) ===")
    res = safe_call_llm(prompt)
    print("=== RESULT (raw) ===")
    print(res)

if __name__ == "__main__":
    main()
