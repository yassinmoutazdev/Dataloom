import json

try:
    import ollama
except ImportError:
    ollama = None

try:
    import openai
except ImportError:
    openai = None

SUMMARY_SYSTEM_PROMPT = """You are a data analyst assistant. You receive a user question and structured query results.

Write ONE clear, specific sentence that directly answers the question.

Rules:
- Use ONLY the numbers and values in the results. Never compute, estimate, or invent.
- Never calculate averages of averages or aggregates of aggregates.
- If results are paginated (showing X of Y total rows), acknowledge you're showing partial results.
- Be specific — mention actual top values by name when relevant.
- Do not mention SQL, databases, tables, or technical details.
- If results are empty, say so clearly."""


def summarize(question: str, results: list, headers: list,
              model_config: dict, total_rows: int = None) -> str:
    if not results:
        return "The query returned no results."

    # Build context about pagination
    shown = len(results)
    pagination_note = ""
    if total_rows and total_rows > shown:
        pagination_note = f"\nNote: These are the first {shown} of {total_rows} total rows."

    # Format sample rows as JSON
    rows_as_dicts = [dict(zip(headers, [str(v) for v in row])) for row in results[:15]]
    result_json = json.dumps(rows_as_dicts, indent=2)

    prompt = f"""User question: "{question}"
{pagination_note}
Results:
{result_json}

One sentence answer:"""

    provider = model_config.get("provider", "ollama")

    if provider == "openai" and openai:
        client = openai.OpenAI(api_key=model_config["api_key"])
        response = client.chat.completions.create(
            model=model_config["model"],
            messages=[
                {"role": "system", "content": SUMMARY_SYSTEM_PROMPT},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=150
        )
        return response.choices[0].message.content.strip()
    else:
        response = ollama.chat(
            model=model_config["model"],
            messages=[
                {"role": "system", "content": SUMMARY_SYSTEM_PROMPT},
                {"role": "user", "content": prompt}
            ],
            options={"temperature": 0.1}
        )
        return response["message"]["content"].strip()
