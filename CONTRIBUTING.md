# Contributing to Dataloom

Thank you for taking the time to contribute. This document explains how the codebase is structured, what the conventions are, and how to add new features correctly.

---

## Before you start

- Open an issue before starting significant work. This avoids duplicated effort and lets us discuss the approach before code is written.
- For small fixes (typos, documentation, obvious bugs) you can open a pull request directly.
- Run the full test suite before submitting anything. Contributions that break existing tests will not be merged.
- Security: Never commit API keys or secrets. Keep them in your local `.env` and add only placeholder values to `.env.example`. If you accidentally commit a secret, rotate it immediately.

---

## Setting up the development environment

```bash
git clone https://github.com/yassinmoutazdev/Dataloom.git
cd Dataloom
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -r requirements-test.txt
cp .env.example .env             # fill in your values
```

Run the tests to confirm everything is working:

```bash
pytest
```

---

## How the codebase is organised

Understanding where things live is essential before making any change.

| File | Responsibility |
|---|---|
| `intent_parser.py` | LLM interface — question → JSON intent. Contains `INTENT_SYSTEM_PROMPT` and all provider branches in `_call_model()`. |
| `validator.py` | Schema gatekeeper — validates the intent against the live database schema. The only barrier between the LLM and the SQL builder. |
| `sql_builder.py` | Deterministic SQL renderer — intent + db_type → (sql, params). Zero LLM calls. All dialect differences live here as lookup dicts. |
| `core.py` | Pipeline orchestrator — coordinates all stages from question to result. |
| `app.py` | Flask server — all routes, session management, provider configuration. |
| `schema.py` | Schema discovery — reads tables, columns, types, and FK paths from the live database at startup. |
| `db_connector.py` | Query execution — parameterised queries, read-only sessions, reconnect support. |
| `memory.py` | Intent history — follow-up detection and context merging across turns. |

**The core principle:** the LLM produces a JSON intent object. Deterministic Python converts that intent to SQL. This separation is the foundation of the engine — do not add LLM calls anywhere outside `intent_parser.py`.

---

## Adding a new SQL feature

Every SQL feature touches exactly three files. Follow this order.

### Step 1 — Define the intent field in `intent_parser.py`

Add a new section to `INTENT_SYSTEM_PROMPT` describing:
- The new field name and its valid values
- An example JSON snippet
- When to use it and when not to
- Add an entry to the Decision Tree at the bottom of the prompt

### Step 2 — Add validation in `validator.py`

Add a validation block inside `validate_intent()` or `normalize_metrics()` that:
- Checks the new field against valid constant sets
- Verifies any column references against `schema_map` using `_col_exists()`
- Returns a descriptive error message on failure

Add new constants to the relevant set (e.g. `VALID_AGGREGATIONS`, `VALID_OPERATORS`) if needed.

### Step 3 — Add rendering in `sql_builder.py`

Add the SQL generation logic inside `_compile_metrics()` (for new aggregation types) or `build_sql()` (for new clause types).

All dialect-specific SQL belongs in a lookup dict at the top of the file — never hardcoded inside a function body. Follow the pattern of the existing matrices (`TIME_RANGE_SQL`, `DATE_BUCKET_SQL`, `EXTRACT_SQL`, `DATE_ARITH_SQL`):

```python
MY_NEW_FEATURE_SQL = {
    "postgresql": "...",
    "mysql":      "...",
    "sqlite":     "...",
}
```

### Step 4 — Write tests before considering the feature done

Add tests covering:
- Happy path for each dialect (PostgreSQL, MySQL, SQLite)
- Invalid input rejection
- Edge cases (empty values, nulls, boundary conditions)
- A regression check confirming previous tests still pass

**No feature ships without tests.**

---

## Adding a new AI provider

New providers require changes in three files.

### `intent_parser.py` — `_call_model()`

Add a new branch for the provider. If it uses an OpenAI-compatible endpoint, reuse the `openai` package with a custom `base_url`:

```python
if provider == "myprovider":
    client = _oai.OpenAI(
        api_key=model_config["api_key"],
        base_url="https://api.myprovider.com/v1",
        timeout=60.0,
    )
    response = client.chat.completions.create(
        model=model_config["model"],
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        temperature=0,
        response_format={"type": "json_object"},
    )
    raw = response.choices[0].message.content.strip()
```

If it uses a different SDK (like Anthropic), add a graceful import and follow the existing Anthropic pattern.

### `app.py` — three places

1. **`_default_model_config()`** — add an env var fallback branch
2. **`_test_model_connection()`** — add a branch that fires a minimal live call to verify credentials
3. **`setup_provider_models()`** — add the provider to `FALLBACK` with a curated model list, and implement live model fetching if the provider's API supports it

### `templates/setup.html`

Add a provider card to the grid and a `<div class="provider-fields">` block with the relevant input fields (API key, model selector, any provider-specific fields like endpoint or API version).

---

## Documentation standards

Follow `DOCUMENTATION_STANDARDS.md` for all docstrings and inline comments.

The short version:
- Every public function needs a docstring with Args, Returns, and Raises sections
- Every module needs a module-level docstring
- Comments explain *why*, not *what* — if the code is clear, a comment restating it adds noise
- Sprint tags (`# 4A-1`, `# 4B-2` etc.) must be preserved exactly — they are part of the project's history

---

## Pull request checklist

Before submitting:

- [ ] `pytest` passes with no failures
- [ ] New SQL features touch all three files: `intent_parser.py`, `validator.py`, `sql_builder.py`
- [ ] New providers touch all three locations in `app.py` plus `intent_parser.py`
- [ ] Dialect matrices cover PostgreSQL, MySQL, and SQLite
- [ ] Docstrings follow `DOCUMENTATION_STANDARDS.md`
- [ ] No LLM calls added outside `intent_parser.py`
- [ ] No SQL strings hardcoded inside function bodies — dialect differences belong in lookup dicts

---

## Reporting bugs

Open an issue with:
- What you expected to happen
- What actually happened
- The question you asked and the database type you were using
- Any error messages from the browser console or server logs
- Your provider and model (API keys are never needed — do not include them)

---

## Code of Conduct & License

- See the project license: https://github.com/yassinmoutazdev/Dataloom/blob/main/LICENSE
- If present, please follow the Code of Conduct: https://github.com/yassinmoutazdev/Dataloom/blob/main/CODE_OF_CONDUCT.md