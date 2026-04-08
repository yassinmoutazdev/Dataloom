# Dataloom

**Ask questions about your database in plain English. Get instant SQL results.**

Dataloom is a natural language to SQL engine. You type a question — *"what are the top 10 product categories by revenue?"* — and Dataloom parses the intent, builds a validated parameterised SQL query, executes it against your database, and returns the results in a clean table. No SQL knowledge required.

The defining design decision: **the LLM never writes SQL.** It produces a structured JSON intent object. Deterministic Python converts that intent to SQL. This makes the engine testable, safe, and portable across database dialects.

---

## How it works

```
User question
     │
     ▼
┌─────────────────┐
│  Intent Parser  │  LLM call #1 → structured JSON intent
│ intent_parser.py│  (metrics, joins, filters, group_by, ...)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Validator    │  Schema-aware gatekeeper. Checks every column,
│  validator.py   │  auto-repairs joins via BFS, blocks injection.
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   SQL Builder   │  Deterministic. Zero LLM calls.
│ sql_builder.py  │  Same intent always produces same SQL.
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Execution    │  Parameterised queries. Read-only sessions.
│ db_connector.py │  Auto-retry on connection loss.
└────────┬────────┘
         │
         ▼
     Result table
```

The LLM is called exactly once per query in the happy path. If execution fails, a self-correction loop feeds the error back to the LLM for a targeted fix — then re-validates and re-executes.

---

## Features

- **Natural language queries** — ask in plain English, get a result table
- **Multi-database** — PostgreSQL, MySQL, SQLite
- **Multi-provider** — OpenAI, Google Gemini, Anthropic Claude, xAI Grok, OpenRouter, Azure OpenAI, Ollama (local)
- **Intent-first architecture** — LLM produces JSON, Python builds SQL; fully testable and deterministic
- **Security hardening** — injection blocklist, schema validation, read-only DB sessions, parameterised queries
- **Web UI** — dark indigo chat interface with SQL disclosure, confidence badges, paginated result tables
- **CLI** — `main.py` for terminal use
- **Export** — download results as CSV or Excel
- **Session history** — file-backed, survives server restarts
- **Schema descriptions** — annotate columns with plain English descriptions to improve accuracy
- **Model management** — pin models, switch providers, add custom model names in-app
- **Auto-reconnect** — transparent connection recovery without user intervention
- **Responsive** — works on mobile, tablet, and desktop

---

## Supported SQL patterns

Dataloom handles a wide range of analytical query types:

| Category | Examples |
|---|---|
| Aggregations | SUM, COUNT, AVG, MAX, MIN |
| Multi-metric | Total revenue AND order count AND avg value in one query |
| Grouping | By state, category, month, day-of-week |
| Time filters | Last 7 days, last month, specific year (2017), date buckets |
| Joins | Multi-hop BFS pathfinding through Snowflake schemas |
| Anti-joins | Products never ordered, customers with no activity |
| NULL checks | IS NULL / IS NOT NULL filters |
| CASE WHEN | Classify customers as High/Medium/Low, weekend vs weekday |
| Window functions | RANK, ROW_NUMBER, DENSE_RANK, LAG, LEAD, running totals |
| CTEs | Multi-step aggregations, 360-degree customer views |
| Percentiles | PERCENTILE_CONT (median), NTILE (deciles) |
| Date arithmetic | Average shipping delay, time between events |
| Set operations | INTERSECT, EXCEPT, UNION |
| Subqueries | Correlated filters, scalar subqueries (% of total) |

---

## Quick start

### Prerequisites

- Python 3.11+
- One of: PostgreSQL, MySQL, or SQLite database
- One of: an API key for a cloud provider, or [Ollama](https://ollama.com) running locally

### 1. Clone and install

```bash
git clone https://github.com/your-username/dataloom.git
cd dataloom
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

For Anthropic Claude support, add the optional package:

```bash
pip install anthropic
```

### 2. Configure environment

Copy the example file and fill in your values:

```bash
cp .env.example .env
```

Minimum required for Ollama (free, local):

```env
MODEL_PROVIDER=ollama
OLLAMA_MODEL=mistral
FLASK_SECRET=your-random-secret-here
```

Minimum required for cloud providers (example with Gemini free tier):

```env
MODEL_PROVIDER=gemini
GEMINI_API_KEY=your-key-here
GEMINI_MODEL=gemini-2.5-flash-lite
FLASK_SECRET=your-random-secret-here
```

Generate a strong Flask secret:
```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

### 3. Run

```bash
python app.py
```

Open `http://localhost:5000` — the setup wizard will guide you through connecting your database and configuring your AI model.

---

## Environment variables

### Flask

| Variable | Default | Description |
|---|---|---|
| `FLASK_SECRET` | random (insecure) | Session signing key. **Set this in production.** |
| `RATE_LIMIT` | `20` | Max queries per session per hour |

### Model provider

| Variable | Description |
|---|---|
| `MODEL_PROVIDER` | One of: `openai`, `gemini`, `anthropic`, `xai`, `openrouter`, `azure`, `ollama` |
| `OPENAI_API_KEY` | OpenAI API key |
| `OPENAI_MODEL` | Default: `gpt-4o` |
| `GEMINI_API_KEY` | Google AI Studio API key |
| `GEMINI_MODEL` | Default: `gemini-2.5-flash-lite` |
| `ANTHROPIC_API_KEY` | Anthropic API key |
| `ANTHROPIC_MODEL` | Default: `claude-sonnet-4-5` |
| `XAI_API_KEY` | xAI API key |
| `XAI_MODEL` | Default: `grok-3-mini` |
| `OPENROUTER_API_KEY` | OpenRouter API key |
| `OPENROUTER_MODEL` | Default: `openai/gpt-4o` |
| `AZURE_OPENAI_KEY` | Azure OpenAI API key |
| `AZURE_OPENAI_ENDPOINT` | Azure endpoint URL |
| `AZURE_OPENAI_MODEL` | Azure deployment name |
| `AZURE_OPENAI_API_VERSION` | Default: `2024-02-01` |
| `OLLAMA_MODEL` | Default: `mistral` |
| `OLLAMA_HOST` | Default: `http://localhost:11434` |

### Database (optional — for demo/pre-configured deployments)

| Variable | Description |
|---|---|
| `DB_TYPE` | `postgresql`, `mysql`, or `sqlite` |
| `DB_HOST` | Database host |
| `DB_PORT` | Database port |
| `DB_NAME` | Database name |
| `DB_USER` | Database user |
| `DB_PASSWORD` | Database password |
| `DB_DISPLAY_NAME` | Override the display name shown in the UI (e.g. `Olist Demo`) |
| `DATABASE_URL` | Parsed automatically if set — overrides individual DB vars (Railway/Heroku format) |

### Demo mode

| Variable | Description |
|---|---|
| `DEMO_MODE` | `true` to auto-connect to the configured database and lock the setup wizard |

---

## Supported providers

All providers are configured through the setup wizard UI or via environment variables. API keys are stored only in the server-side session and never written to disk or returned to the client.

| Provider | Notes |
|---|---|
| **OpenAI** | GPT-4o, GPT-4o-mini, GPT-3.5-turbo and more. Live model list fetched from API. |
| **Google Gemini** | Free tier available via AI Studio. Flash-Lite recommended for demos. |
| **Anthropic** | Claude Sonnet and Haiku series. Requires `pip install anthropic`. |
| **xAI** | Grok-3 series via OpenAI-compatible endpoint. |
| **OpenRouter** | Access 300+ models from multiple providers under one API key. |
| **Azure OpenAI** | Enterprise deployments. Requires endpoint URL and deployment name. |
| **Ollama** | Fully local inference. Auto-discovers available models. No API key needed. |

---

## Project structure

```
dataloom/
├── app.py                    # Flask server — all routes, session management, provider config
├── core.py                   # Pipeline orchestrator — 10-stage question → result flow
├── intent_parser.py          # LLM interface — question → JSON intent, all provider branches
├── validator.py              # Schema gatekeeper — validates intent, BFS join repair, injection guard
├── sql_builder.py            # SQL renderer — deterministic, zero LLM calls, 5 dialect matrices
├── schema.py                 # Schema discovery — reads tables/columns/types/FKs with caching
├── db_connector.py           # Query execution — parameterised, read-only, reconnect support
├── memory.py                 # Intent history — follow-up detection and context merging
├── history_store.py          # File-backed session persistence — survives server restarts
├── utils.py                  # Export helpers — CSV (UTF-8 BOM) and styled Excel
├── main.py                   # CLI interface
├── schema_descriptions.json  # Human-authored column descriptions for improved accuracy
├── templates/
│   ├── index.html            # Chat interface — messages, result tables, model selector
│   └── setup.html            # Setup wizard — DB connection + provider configuration
├── requirements.txt          # Core dependencies
├── requirements-test.txt     # Test dependencies
├── Procfile                  # Gunicorn entry point for cloud deployments
├── runtime.txt               # Python version pin
└── .env.example              # Environment variable template
```

---

## The pipeline in detail

`core.py` coordinates the following stages for every query:

1. **Vague pre-screen** — regex patterns catch underspecified questions before any LLM call
2. **Intent parse** — LLM produces structured JSON describing the query
3. **Clarification check** — surface model-requested follow-ups to the user
4. **Follow-up detection** — merge context from previous turns for conversational continuity
5. **Question injection** — `_question` field injected for ranking keyword detection downstream
6. **Validate** — every column, join, and aggregation checked against the live schema
7. **Build SQL** — deterministic renderer; same intent always produces same SQL
8. **Execute** — parameterised query, read-only session
9. **Auto-retry** — on SQL error: model self-correction loop; on connection error: reconnect + replay
10. **Persist** — successful intents saved to session history for follow-up queries

---

## Running tests

```bash
pip install -r requirements-test.txt
pytest
```

The test suite covers all SQL patterns, validator logic, BFS join paths, dialect matrices, and security injection cases.

---

## Deployment

### Local (development)

```bash
python app.py
```

### Production (Gunicorn)

```bash
gunicorn app:app --bind 0.0.0.0:8080 --workers 2 --timeout 120
```

### Railway / Render / Heroku

1. Connect your GitHub repository
2. Set all required environment variables in the platform dashboard
3. The `Procfile` and `runtime.txt` are already configured
4. Add a PostgreSQL service — the platform will inject `DATABASE_URL` automatically
5. Set `DEMO_MODE=true` to auto-connect and lock the setup wizard for public demos

---

## Schema descriptions

Dataloom can use human-authored column descriptions to improve intent accuracy for ambiguous or domain-specific column names. Edit `schema_descriptions.json`:

```json
{
  "fact_order_items": {
    "price": "The item sale price in BRL, excluding freight",
    "freight_value": "Shipping cost charged to the customer in BRL"
  },
  "dim_customers": {
    "customer_state": "Two-letter Brazilian state code (e.g. SP, RJ)"
  }
}
```

Descriptions are injected into the schema text the LLM receives. They are particularly useful when column names are abbreviated, domain-specific, or could be confused for other concepts (e.g. `value` vs `price` vs `total`).

---

## Security

- **SQL injection** — blocked at two independent layers: intent validator (injection blocklist + literal regex on CASE WHEN values) and parameterised query execution
- **Read-only sessions** — database connections are set to read-only at the driver level on connect; `UPDATE`, `DELETE`, `DROP`, and DDL are blocked regardless of what the LLM produces
- **API key safety** — keys entered through the setup wizard are stored only in the server-side session dict; they are never returned to the client, never logged, and never written to disk
- **Request size limit** — 16 KB max request body
- **Rate limiting** — configurable per-session query limit (default: 20/hour)
- **Schema validation** — every column reference in the intent is checked against the live schema before any SQL is constructed

---

## Architecture notes

**Why intent-first?**

Asking the LLM to write SQL directly produces hallucinated column names, untestable output, and no validation surface. The intent-first model separates concerns cleanly:

- The LLM's job is interpretation
- The validator's job is correctness
- The builder's job is rendering

Given identical intent JSON, `build_sql()` produces identical SQL every time. This makes the system debuggable, testable, and safe.

**Why BFS join resolution?**

Snowflake schemas often require multi-hop join paths (e.g. `fact_orders → dim_products → dim_categories`). The LLM only needs to identify *what* columns to group by — the BFS engine figures out *how* to get there through the FK graph. The LLM is not good at graph traversal; BFS is perfect at it.

**Why no connection pooling?**

The current single-connection-per-session model is intentional at this deployment scale. Each session owns exactly one database connection, opened at setup and closed when the session expires. Pooling is deferred until connection contention becomes a measured problem.

---

## Contributing

Issues and pull requests are welcome. Before submitting:

1. Run the full test suite — `pytest`
2. Follow the documentation standards in `DOCUMENTATION_STANDARDS.md`
3. For new SQL features, add entries to all three files: `intent_parser.py` (prompt), `validator.py` (schema check), `sql_builder.py` (rendering + dialect matrix)
4. New providers require branches in both `intent_parser.py` (`_call_model`) and `app.py` (`_test_model_connection`, `_default_model_config`, `setup_provider_models`)

---

## License

MIT
