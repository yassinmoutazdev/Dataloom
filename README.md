# DB Assistant

A natural language database query tool that translates plain English questions into SQL and returns results — without ever letting the LLM write SQL directly.

Built to solve the core hallucination problem in most NL-to-SQL tools: when a language model generates SQL, it invents column names, table names, and joins that don't exist. DB Assistant solves this with a deterministic architecture where SQL is always constructed by validated Python code, never by the model.

---

## How It Works

The pipeline has two LLM calls. Everything in between is pure Python:

```
User question
     ↓
[LLM #1]  Intent Parser   →  Structured JSON only — no SQL
     ↓
[Code]    Validator        →  Checks every field against the live schema
     ↓
[Code]    SQL Builder      →  Deterministically constructs parameterized SQL
     ↓
          Database         →  PostgreSQL · MySQL · SQLite
     ↓
[LLM #2]  Summarizer       →  Receives raw results, writes plain-English answer
```

The model's only job is to understand language and map it to fields. SQL correctness is guaranteed by code. If a query fails, the error is sent back to the model for one self-correction attempt — the corrected intent goes through validation again before execution.

---

## Features

- **Hallucination-proof SQL** — deterministic builder, validated against live schema
- **Multi-database** — PostgreSQL, MySQL/MariaDB, SQLite
- **Multi-model** — any Ollama model (local) or OpenAI API
- **Web UI** — chat interface with dark/light mode, inline results, SQL toggle
- **CLI** — colored terminal interface with spinner feedback
- **Follow-up questions** — conversation memory merges context across turns
- **Auto-join discovery** — reads FK constraints from `information_schema` at startup
- **Parameterized queries** — filter values never concatenated into SQL strings
- **Connection wizard** — Power BI-style setup form on first run, saves credentials

---

## Quickstart

**Prerequisites:** Python 3.11+, [Ollama](https://ollama.ai) running locally with at least one model pulled (e.g. `ollama pull mistral`).

```bash
# 1. Clone and set up environment
git clone https://github.com/your-username/db-assistant.git
cd db-assistant
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run — connection wizard launches automatically on first run
python main.py        # CLI interface
python app.py         # Web interface → open http://localhost:5000
```

On first run, the connection wizard walks you through database setup interactively. Credentials are saved to `.env` — subsequent runs connect automatically.

---

## Configuration

All configuration lives in `.env`. Copy `.env.example` to get started:

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `DB_TYPE` | `postgresql` | Database type: `postgresql`, `mysql`, or `sqlite` |
| `DB_HOST` | `localhost` | Database host (PostgreSQL / MySQL) |
| `DB_PORT` | `5432` | Database port |
| `DB_NAME` | — | Database name |
| `DB_USER` | — | Database username |
| `DB_PASSWORD` | — | Database password |
| `DB_PATH` | — | File path (SQLite only, e.g. `./mydb.sqlite`) |
| `MODEL_PROVIDER` | `ollama` | Model provider: `ollama` or `openai` |
| `OLLAMA_MODEL` | `mistral` | Ollama model name (e.g. `llama3.1`, `phi3`, `gemma3`) |
| `OPENAI_API_KEY` | — | Required if `MODEL_PROVIDER=openai` |
| `OPENAI_MODEL` | `gpt-4o` | OpenAI model name |
| `FLASK_SECRET` | random | Session secret for web UI (set a fixed value in production) |

---

## Project Structure

```
db-assistant/
├── core.py                   # Shared pipeline — CLI and web both call run_pipeline()
├── main.py                   # CLI interface
├── app.py                    # Flask web server
├── intent_parser.py          # LLM call #1 — returns structured JSON intent
├── validator.py              # Validates intent against live schema
├── sql_builder.py            # Builds parameterized SQL from validated intent
├── summarizer.py             # LLM call #2 — summarizes results in plain English
├── schema.py                 # Reads live schema + auto-discovers FK join paths
├── db_connector.py           # Multi-database connection handling
├── memory.py                 # Stores recent intents for follow-up support
├── schema_descriptions.json  # Optional human annotations on tables and columns
├── templates/
│   └── index.html            # Web UI — single-file HTML/CSS/JS
├── requirements.txt
└── .env.example
```

---

## Annotating Your Schema

For databases with cryptic column names or non-obvious relationships, you can add plain-English descriptions that get included in the model's context:

```bash
# In the CLI
describe
> Table name: fact_order_items
> Column name (blank for table description):
> Description: Main orders fact table. Use this as the starting point for all queries.
```

Or edit `schema_descriptions.json` directly:

```json
{
  "fact_order_items": {
    "_description": "Main orders fact table. Join to dim_customers via customer_id.",
    "price": "Item sale price in BRL, excluding freight"
  }
}
```

---

## MySQL Setup Note

MySQL support requires an additional driver not included by default:

```bash
pip install mysql-connector-python
```

Then uncomment the line in `requirements.txt`.

---

## Limitations

- **Complex multi-join queries** are unreliable on small local models (7B parameters). Works well on 70B+ models or OpenAI. The validator catches most failures and prompts self-correction.
- **No authentication** on the web interface — designed for local and internal use. Do not expose to the public internet without adding an auth layer.
- **Single-process design** — the Flask app uses a single shared database connection. For multi-worker deployments, replace with a connection pool (e.g. SQLAlchemy).
- **Schema required** — the tool reads your live schema at startup. Views and stored procedures are not currently indexed.

---

## Architecture Notes

**Why JSON intent instead of direct SQL generation?**
Asking the model to produce a JSON object with specific fields (metric, aggregation, fact_table, group_by, filters) separates language understanding from query construction. Errors have a known location — if the model misunderstands the question, the validator catches it before any query runs. If validation passes, the SQL builder produces correct output deterministically.

**Why two LLM calls?**
The summarizer receives only raw query results and the original question — it cannot hallucinate schema because no schema is in its context. This is a deliberate constraint: the model that touches data never touches schema, and the model that touches schema never touches data.

**Why parameterized queries?**
Filter values are passed to the database driver as parameters (`%s` / `?`), never concatenated into SQL strings. This eliminates SQL injection through the filter path regardless of what the model returns.

---

## Tech Stack

- **Python 3.11+**
- **Flask** — web server
- **psycopg2-binary** — PostgreSQL driver
- **Ollama** — local LLM inference
- **OpenAI** — optional cloud model support
- **tabulate** — CLI result formatting
