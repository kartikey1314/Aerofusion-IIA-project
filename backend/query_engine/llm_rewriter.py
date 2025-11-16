"""
llm_rewriter.py
Provides a deterministic, safe rewriting of the LLM prompt for summarization/insight generation.
"""

def rewrite_summary_prompt(parsed, integrated_rows):
    """
    parsed: dict from analyzer (intent, origin, destination, date, etc.)
    integrated_rows: list of unified result dicts

    Returns a string prompt suitable for calling an LLM.
    """
    lines = []
    lines.append("You are a flight summarization assistant.")
    lines.append("User intent (parsed):")
    lines.append(str(parsed))
    lines.append("")
    lines.append("Here are unified flight results from multiple sources (top 10 shown):")
    for r in integrated_rows[:10]:
        lines.append(f"- {r.get('flight_no')} | {r.get('airline')} | {r.get('origin')}→{r.get('destination')} | {r.get('date')} | price={r.get('price')} | seats={r.get('seat_count')} | source={r.get('source')}")
    lines.append("")
    # Task rewriting depending on parsed intent
    intent = str(parsed.get("intent", "LIST")).upper()
    if intent == "MIN":
        task = "Find the single cheapest flight option and explain why (source, price)."
    elif intent == "AVG":
        task = "Compute the average price (report numeric avg) and comment on dispersion if visible."
    else:
        task = "Summarize cheapest and available flight options, call out 2-3 best choices with price and source, and give 2 short actionable suggestions for booking (1-2 sentences)."
    lines.append("Task: " + task)
    lines.append("")
    lines.append("Return the answer in 3 short paragraphs: 1) summary sentence listing best options; 2) insights (why/notes); 3) booking suggestions.")
    return "\n".join(lines)
