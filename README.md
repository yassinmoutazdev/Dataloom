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
git clone https://github.com/yassinmoutazdev/dataloom
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
You can customize schema descriptions in "Schema Tuner" in the Settings panel as well. 

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

## Limitations

These are known boundaries of the current architecture. They are not bugs — they are the honest edges of the intent-first design when confronted with real-world database questions.

### The intent schema is a fixed contract

The JSON intent structure (`metrics`, `group_by`, `joins`, `filters`, etc.) was designed to cover a broad set of analytical patterns, but it is a closed vocabulary. Any question that requires SQL constructs outside that vocabulary — for example, recursive CTEs, `PIVOT`/`UNPIVOT`, `MERGE`, full-text search predicates, or database-specific extensions like PostgreSQL's `LATERAL` joins or `TABLESAMPLE` — cannot be expressed in the intent and will either be silently approximated or rejected by the validator. Adding support for a new SQL pattern requires coordinated changes across `intent_parser.py` (prompt), `validator.py` (schema check), and `sql_builder.py` (rendering + dialect matrix).

### Ambiguous or semantically overloaded column names degrade accuracy

The LLM maps natural language terms to schema column names using the schema text and any `schema_descriptions.json` hints. When multiple columns could plausibly represent the same concept — e.g. `price`, `unit_price`, `total_price`, `sale_amount` — the model must guess. That guess is not validated against business intent, only against schema existence. A query for "total revenue" may silently aggregate the wrong column, and the result will look numerically plausible while being semantically wrong. Schema descriptions mitigate this but require manual upkeep.

### One LLM call, one fact table

Each query is anchored to a single `fact_table`. Questions that naturally span multiple fact tables — "compare active customers who placed orders with those who only browsed" — cannot be expressed cleanly in a single intent object. The model will attempt to force the query into one table context, often producing an incorrect or incomplete result. Multi-fact-table questions require either schema redesign or manual SQL.

### BFS join repair is topology-only, not semantic

The auto-repair mechanism finds the shortest FK path between two tables in the graph. It does not know whether that path is semantically correct for the question. In schemas with multiple valid join paths (e.g. a `users` table reachable through both `orders` and `reviews`), BFS will always pick the shortest path, which may not be the one the user intended. There is no mechanism to surface join path ambiguity to the user.

### The vague-question pre-screen is regex-based and shallow

`is_vague_question()` catches a hardcoded list of open-ended phrasing patterns (`"how are we doing"`, `"tell me about"`, etc.). It will miss vague questions phrased differently, and it will occasionally false-positive on legitimate but similarly worded queries. There is no semantic understanding at this stage — it is a literal string match before any LLM call is made.

### The self-correction loop is bounded and not guaranteed to converge

When SQL execution fails, the error message is fed back to the LLM for a targeted fix. This loop runs a fixed number of times. If the root cause of the failure is a structural mismatch between the intent schema and what the question actually needs — rather than a simple column name error — the loop will exhaust its retries without producing a valid result. The user sees a failure message but gets no actionable explanation of why their question cannot be answered.

### No support for write operations by design

Dataloom enforces read-only database sessions at the driver level. This is a deliberate security decision, but it means the engine cannot be used for questions that imply data modification — even innocuous ones like "add a tag to this customer" or "mark these orders as processed." Any such question will fail silently or produce a confusing error.

### Context window limits cap schema size

The full schema text — table names, column names, types, and any `schema_descriptions.json` annotations — is injected into every LLM prompt. For databases with hundreds of tables and thousands of columns, this schema text can approach or exceed the model's effective context window, causing the model to truncate or misread the schema. There is no schema pruning or relevance-based selection; the full schema is always sent.

### Local model quality is highly variable

When running against Ollama with smaller local models (e.g. Mistral 7B), JSON intent quality degrades significantly on complex queries involving CTEs, window functions, correlated subqueries, or multi-metric groupings. The self-correction loop helps, but smaller models produce structurally malformed JSON more often, which the validator rejects entirely rather than repairing. Cloud-hosted frontier models are strongly recommended for production use.

### Session memory is shallow

Follow-up detection merges context from the previous turn only. Dataloom does not maintain a full conversational history that the LLM reasons over. A three-turn conversation — "show me top customers", "filter to last year", "now break it down by region" — may lose the first turn's context by the third question, producing a query that groups by region without the original customer ranking.

---

## Model recommendations

The intent-first architecture makes a specific demand on the model: produce well-formed, schema-grounded JSON from a natural language question — not SQL, not prose. This is a structured extraction task that rewards instruction-following, schema comprehension, and consistency over raw reasoning depth. The following recommendations reflect current benchmark evidence (as of April 2026) and the specific failure modes observed in this pipeline.

### Cloud providers

**Best for reasoning-heavy / ambiguous queries: Claude Sonnet 4.6 (Anthropic)**

Claude Sonnet 4.6 (`claude-sonnet-4-6`) is now the strongest recommendation for production Dataloom deployments. It scores 79.6% on SWE-bench Verified and leads among models tested on complex multi-step structured-output tasks. Its behaviour on ambiguous schemas is particularly well-suited to this pipeline: it consistently surfaces `clarification_needed` when confidence is low rather than producing a plausible-but-wrong intent, which is the safer failure mode in a user-facing deployment. Claude's extended reasoning capability handles queries where the correct metric depends on interpreting domain-specific column names — the hardest class of failures in the intent-first model. The Anthropic provider requires a separate `pip install anthropic` and relies on prompt-level JSON constraints rather than a native `json_object` response format.

**Best instruction-following / native JSON mode: GPT-4.1 (OpenAI)**

GPT-4.1 remains a strong and predictable choice, particularly valued for its native `json_object` response format (`response_format: {"type": "json_object"}`), which eliminates the markdown-fence stripping heuristic that other providers require. Its instruction-following is highly consistent across repeated queries and its 1-million-token context window is a genuine advantage for large schemas. Where GPT-4.1 excels in this pipeline is literal schema adherence on well-specified questions — it reliably maps column names exactly as provided. Its relative weakness compared to Claude 4.x is on ambiguous or multi-hop reasoning queries, where it is more likely to commit confidently to an incorrect intent rather than requesting clarification. It is the most forgiving model when the intent schema pushes against its edges, tending to produce a best-effort valid intent rather than failing outright.

**Best cost-to-performance for high volume: Gemini 2.5 Flash / Flash-Lite (Google)**

For deployments with high query volume and budget constraints, Gemini Flash-Lite offers competitive intent quality at a fraction of the cost of Claude Sonnet 4.6 or GPT-4.1. It uses the same OpenAI-compatible endpoint in Dataloom, supports `json_object` mode, and handles the majority of straightforward aggregation and grouping queries without issue. It is less reliable on the more complex intent types (CTEs, correlated subqueries, NTILE), so consider pairing it with a fallback to a stronger model on low-confidence intents. For teams that want frontier-level accuracy at a significantly lower price point, Gemini 2.5 Pro is also worth evaluating — it has closed much of the quality gap with the top-tier models at a competitive cost.

**Enterprise deployments: Azure OpenAI**

For organizations that cannot send data to external APIs, Azure OpenAI provides GPT-4.1 and other models behind a private endpoint with SOC 2 compliance. Configuration requires an endpoint URL and deployment name in addition to the API key. Performance is identical to the OpenAI provider for the same underlying model.

### Local models (Ollama)

Local inference is suitable for development, demo environments, and privacy-sensitive deployments where no data can leave the network. Quality degrades significantly for complex queries. The local model landscape has shifted considerably — the Qwen 2.5 family now clearly leads for structured JSON output tasks and is the recommended default.

**Recommended: `qwen2.5:14b`**

Qwen 2.5 14B is the current recommended default for Ollama deployments, replacing the earlier `mistral` and `mistral-nemo` recommendations. It produces significantly more consistently valid JSON than Mistral 7B on multi-metric and multi-filter intents, handles the basic aggregation/grouping/filter patterns reliably, and fits comfortably on a 16 GB RAM system. Enable Ollama's `format: "json"` parameter in API calls alongside Dataloom's existing retry loop — this combination resolves the large majority of malformed-output failures at this model size. If hardware is limited to 8 GB RAM, `qwen2.5:7b` is the recommended fallback, still outperforming Mistral 7B on structured extraction tasks.

**For better complex query support: `qwen2.5:32b` or `deepseek-r1:32b`**

Larger parameter counts meaningfully improve JSON schema compliance and multi-join intent quality. `qwen2.5:32b` is the strongest all-around local choice for complex Dataloom queries — it handles CTEs, window functions, NTILE, and correlated subqueries substantially better than the 14B variant and runs on a single high-end GPU (RTX 4090 or equivalent). For workloads that skew toward queries requiring multi-step reasoning to resolve ambiguity — where the correct metric is not obvious from column names alone — `deepseek-r1:32b` is a strong alternative. Its chain-of-thought reasoning approach is particularly effective at the disambiguation step before producing the final intent JSON.

**For 8 GB RAM systems: `llama3.3:8b`**

`llama3.3:8b` replaces `mistral:7b` as the recommended lightweight option. It produces fewer malformed JSON intents on multi-filter queries while fitting comfortably within 8 GB RAM. Mistral 7B remains acceptable for demos and simple single-metric queries, but `llama3.3:8b` is the better default for any workload that regularly exercises joins or grouped aggregations.

**Avoid for production: models under 13B parameters**

Sub-13B models (including `mistral:7b` and `qwen2.5:7b`) frequently produce structurally malformed intent JSON on anything beyond simple single-metric queries. The validator will reject these intents, and the self-correction loop rarely recovers from a fundamental JSON structure error. They are acceptable for demos and local development against simple schemas, but not for real workloads.

### Provider selection summary

| Use case | Recommended model |
|---|---|
| Production, best accuracy | Claude Sonnet 4.6 (`anthropic`) |
| Instruction-following, native JSON mode | GPT-4.1 (`openai`) |
| Frontier performance, cost-sensitive | Gemini 2.5 Pro (`gemini`) |
| High volume, budget | Gemini 2.5 Flash-Lite (`gemini`) |
| Enterprise / private network | Azure OpenAI with GPT-4.1 deployment |
| Local / offline, standard | `qwen2.5:14b` via Ollama |
| Local / offline, complex queries | `qwen2.5:32b` or `deepseek-r1:32b` via Ollama |
| Local / 8 GB RAM | `llama3.3:8b` via Ollama |
| Development / demo | Any Ollama model; `qwen2.5:14b` is the recommended default |

---

## The Dataloom Story

Dataloom started with laziness.

I was working through a self-made SQL training file — a collection of exercises I had put together to sharpen my query-writing skills without relying on AI. The plan was simple: one question a day for two months, solve it myself, get feedback from ChatGPT on my approach.

I made it to day four.

On the fourth day, I stopped mid-exercise and started thinking. In my actual work, I was already using LLMs to help write queries. I had enough SQL knowledge to evaluate whether the output was correct — I could spot hallucinations, catch wrong joins, question the logic. So the question hit me: why am I spending time getting *better* at writing SQL when I already have the ability to *direct and evaluate* a system that writes it?

The fear driving most people toward "learn more SQL" is the same fear driving most people away from AI: *it's going to replace us.* But that fear, if it directs you toward competing with AI instead of leveraging it, points you in the wrong direction. The people who get displaced aren't the ones who lack a specific technical skill — they're the ones who resist change altogether.

So instead of continuing the SQL exercises, I asked a different question: what if I just built the system?

---

At that point, I had essentially zero programming knowledge.

Not "junior developer" zero. Actual zero. I didn't know what `pip install` did. I didn't know what a terminal was. The first time someone told me to run `python app.py`, Claude had to explain what that meant, and I typed it into the chat window three times before I understood it went into a different place entirely.

I also didn't know what Git or GitHub were. The first twenty to thirty versions of Dataloom — everything up through v3.0 — were managed by copying entire folders. When Claude produced a new version of a file, I downloaded it and replaced the old one manually. That was my version control. The concept of a commit, a repository, a branch — none of it existed in my mental model yet. The first time I actually used git add . && git commit && git push as a real workflow and not just commands I was copying from a chat window was in the final versions of the project. Before that, GitHub was something I had heard of but couldn't have explained the purpose of.

The first working version ran in a CLI. I built somewhere around twenty iterations of it before I had anything with a visual interface. Each one taught me something — not always about code, but about thinking. About what it means to have a system that does something reliably. About the difference between "it works when I test it" and "it works."

When I finally opened VS Code for the first time, I was genuinely intimidated. It looked like a cockpit. I closed it and went back to the Claude chat window. Then I opened it again. Then I slowly started spending more time there than in the chat.

---

The project hit a real wall at deployment.

Getting it onto Railway — turning it from something that ran on my machine into something accessible to anyone — was the first time I seriously thought about stopping. The number of things I didn't know was enormous: what a database connection string was, what environment variables actually did, what Gunicorn was and why it existed, why the app that worked locally crashed in a container.

I took a break. During that time I completed a Generative AI for Software Development course from deeplearning.ai, which gave me a more grounded understanding of both AI systems and software development practices. I came back to the project with a different perspective.

I realized I had been building without a test suite. No regression tests, no automated validation — just manual testing against my own intuition. Claude had generated a few test files early on and I had treated them as a nice-to-have. The course made clear they were the foundation. I went back and rewrote them properly.

I realized I had been building without documentation standards. I created a `DOCUMENTATION_STANDARDS.md` file to guide the AI when writing docstrings, then had to apply it retroactively across every module.

The project got better as my awareness grew — not my coding skill, but my *awareness*. My understanding of what questions to ask, what decisions mattered, what to defer and what to lock in. The code was mostly built by AI. The architecture, the decisions, the direction — that was mine.

---

The clearest thing Dataloom gave me wasn't the product.

It was direction.

I'm an undergraduate student, and for a long time I wasn't sure which area I am going to pursue. Software Developement never came to mind. By the end of this project, the answer was obvious in a way it had never been before. I found myself wondering why it took this long to see it.

The project was built from ideas more than code. There are parts of the codebase I couldn't write from scratch today. But the reasoning behind every major decision — the intent-first architecture, the validator as a security boundary, the deterministic SQL builder — I built that. The AI wrote the syntax. I wrote the thinking.

That distinction matters. It's the same distinction Dataloom itself is built on: the LLM handles the translation, the human provides the direction. The system only works because someone decided what it should do and why.

Dataloom was built out of laziness, curiosity, ignorance, stubbornness, a Railway deployment disaster, and about two months of learning how to think about software. It is what happens when you stop being afraid of the tools and start asking what you can build with them.

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
