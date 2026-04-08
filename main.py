#!/usr/bin/env python3
"""
CLI entry point for Dataloom.

Owns the interactive terminal loop: connects to the database, loads the schema,
accepts natural-language questions, drives the full intent→validate→SQL→execute
pipeline, and handles pagination and per-session model selection.

Depends on: schema, intent_parser, validator, sql_builder, memory.
"""

import os
import sys
import json
import threading
import time
import psycopg2
from dotenv import load_dotenv
from tabulate import tabulate

from schema import get_schema, save_description
from intent_parser import (
    parse_intent, parse_retry, parse_validation_retry,
    is_vague_question, _strip_meta,
)
from validator import validate_intent, normalize_joins
from sql_builder import build_sql
from memory import IntentMemory


# ── ANSI colour helpers ──────────────────────────────────────────────────────

class C:
    """ANSI escape codes used throughout the CLI for coloured output."""
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    GREEN   = "\033[92m"
    CYAN    = "\033[96m"
    YELLOW  = "\033[93m"
    RED     = "\033[91m"
    WHITE   = "\033[97m"


def print_info(msg):    print(f"  {C.CYAN}ℹ{C.RESET}  {msg}")
def print_success(msg): print(f"  {C.GREEN}✓{C.RESET}  {msg}")
def print_warn(msg):    print(f"  {C.YELLOW}⚠{C.RESET}  {msg}")
def print_error(msg):   print(f"  {C.RED}✗{C.RESET}  {msg}")
def divider():          print(f"  {C.DIM}{'─' * 54}{C.RESET}")


# ── Display helpers ──────────────────────────────────────────────────────────

def banner():
    """Print the Dataloom ASCII-art welcome banner."""
    print(f"""
{C.CYAN}{C.BOLD}╔══════════════════════════════════════════════════════╗
║                                                      ║
║   ██████╗ ██████╗      █████╗ ██╗   v2.2            ║
║   ██╔══██╗██╔══██╗    ██╔══██╗██║                   ║
║   ██║  ██║██████╔╝    ███████║SB║                   ║
║   ██║  ██║██╔══██╗    ██╔══██║SB║                   ║
║   ██████╔╝██████╔╝    ██║  ██║SB║                   ║
║   ╚═════╝ ╚═════╝     ╚═╝  ╚═╝╚═╝                   ║
║                                                      ║
║    Deterministic SQL · Self-Correcting · v2.2        ║
╚══════════════════════════════════════════════════════╝{C.RESET}
""")


def print_intent(intent: dict) -> None:
    """Pretty-print a parsed intent dict for debug visibility."""
    print(f"\n  {C.DIM}Parsed intent:{C.RESET}")
    print(f"    {C.CYAN}{json.dumps(intent, indent=4)}{C.RESET}\n")


def print_sql(sql: str, label: str = "Generated SQL") -> None:
    """Print a SQL string with syntax highlighting and a labelled header."""
    print(f"  {C.DIM}{label}:{C.RESET}")
    for line in sql.split("\n"):
        print(f"    {C.YELLOW}{line}{C.RESET}")
    print()


def help_text() -> None:
    """Print the list of available CLI commands."""
    print(f"""
  {C.BOLD}Commands:{C.RESET}
  {C.CYAN}  schema{C.RESET}          Show database schema
  {C.CYAN}  describe{C.RESET}        Add descriptions to tables/columns
  {C.CYAN}  history{C.RESET}         Show recent query intents
  {C.CYAN}  next{C.RESET}            Show next page of last results
  {C.CYAN}  back{C.RESET}            Show previous page of last results
  {C.CYAN}  model{C.RESET}           Show current model
  {C.CYAN}  clear{C.RESET}           Clear screen
  {C.CYAN}  help{C.RESET}            Show this help
  {C.CYAN}  exit{C.RESET}            Quit
""")


# ── Spinner ──────────────────────────────────────────────────────────────────

def spinner_thread(stop_event: threading.Event, message: str) -> None:
    """Animate a braille spinner in-place until stop_event is set.

    Args:
        stop_event: Threading event that signals the spinner to exit.
        message: Label shown beside the spinner (e.g. "Parsing intent").
    """
    import itertools
    frames = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
    for f in itertools.cycle(frames):
        if stop_event.is_set():
            break
        sys.stdout.write(f"\r  {C.CYAN}{f}{C.RESET}  {message}...")
        sys.stdout.flush()
        time.sleep(0.08)
    sys.stdout.write("\r" + " " * 50 + "\r")
    sys.stdout.flush()


def run_with_spinner(message: str, fn, *args, **kwargs):
    """Run fn(*args, **kwargs) on the main thread while displaying a spinner.

    The spinner runs in a daemon thread and is guaranteed to stop — and the
    terminal line cleared — before this function returns, even if fn raises.

    Args:
        message: Human-readable label shown beside the spinner.
        fn: Callable to execute.
        *args: Positional arguments forwarded to fn.
        **kwargs: Keyword arguments forwarded to fn.

    Returns:
        Whatever fn returns.

    Raises:
        Any exception raised by fn (re-raised after the spinner stops).
    """
    stop = threading.Event()
    t = threading.Thread(target=spinner_thread, args=(stop, message), daemon=True)
    t.start()
    try:
        result = fn(*args, **kwargs)
    finally:
        stop.set()
        t.join()
    return result


# ── Database + config ────────────────────────────────────────────────────────

def connect_db():
    """Open a read-only psycopg2 connection using credentials from .env.

    Returns:
        An open psycopg2 connection with autocommit off and readonly=True.

    Raises:
        SystemExit: If DB_NAME is missing from the environment.
        psycopg2.Error: If the connection attempt fails.
    """
    load_dotenv()
    config = {
        "host":     os.getenv("DB_HOST", "localhost"),
        "port":     os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB_NAME"),
        "user":     os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }
    if not config["database"]:
        print_error("DB_NAME not set in .env")
        sys.exit(1)
    print_info(f"Connecting to {config['host']}:{config['port']}/{config['database']}...")
    conn = psycopg2.connect(**config)
    conn.set_session(readonly=True)
    print_success("Connected.")
    return conn


def get_model_config() -> dict:
    """Build the model config dict from environment variables.

    Reads MODEL_PROVIDER and the corresponding model/key env vars.
    Defaults to Ollama with model 'mistral' if MODEL_PROVIDER is unset.

    Returns:
        Dict with at minimum 'provider' and 'model' keys.
    """
    load_dotenv()
    provider = os.getenv("MODEL_PROVIDER", "ollama").lower()
    if provider == "openai":
        return {
            "provider": "openai",
            "model": os.getenv("OPENAI_MODEL", "gpt-4o"),
            "api_key": os.getenv("OPENAI_API_KEY", "")
        }
    return {
        "provider": "ollama",
        "model": os.getenv("OLLAMA_MODEL", "mistral")
    }


def run_query(conn, sql: str, params=None) -> tuple[list, list]:
    """Execute a parameterised SQL statement and return all rows.

    Args:
        conn: An open psycopg2 connection.
        sql: The SQL string to execute. Must use %s placeholders.
        params: Optional list of parameter values; defaults to an empty list.

    Returns:
        A (headers, rows) tuple where headers is a list of column name
        strings and rows is a list of row tuples.

    Raises:
        psycopg2.Error: On any database-level execution failure.
    """
    cursor = conn.cursor()
    cursor.execute(sql, params or [])
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    return headers, rows


# ── Result display ───────────────────────────────────────────────────────────

def display_results(
    headers: list,
    rows: list,
    page: int = 1,
    page_size: int = 20,
    total: int | None = None,
) -> None:
    """Render a page of query results as a table and print a row-count footer.

    Args:
        headers: Column name strings for the table header row.
        rows: The subset of rows for this page (already sliced by the caller).
        page: Current page number (1-indexed), used for the footer message.
        page_size: Rows per page, used to calculate page count in the footer.
        total: Total number of rows across all pages; defaults to len(rows).
    """
    if not rows:
        print_warn("No results returned.")
        return
    total = total or len(rows)
    start = (page - 1) * page_size + 1
    end   = start + len(rows) - 1
    print()
    print(tabulate(rows, headers=headers, tablefmt="rounded_outline",
                   numalign="right", stralign="left"))
    if total > page_size:
        pages = (total + page_size - 1) // page_size
        print_info(f"Page {page}/{pages} — rows {start}–{end} of {total} total — type {C.CYAN}next{C.RESET} for more.")
    else:
        print_info(f"{total} row{'s' if total != 1 else ''} returned.")


# ── Model selection ──────────────────────────────────────────────────────────

def select_model(model_config: dict) -> dict:
    """Prompt the user to choose from installed models at startup.

    For OpenAI, confirms the configured model name and allows overriding it.
    For Ollama, lists all installed models and lets the user pick by number
    or partial name. Falls back to the configured default if Ollama is
    unreachable or the user presses Enter without choosing.

    Args:
        model_config: The model config dict produced by get_model_config().

    Returns:
        The (possibly updated) model config dict with 'model' set to the
        user's selection.
    """
    provider = model_config.get("provider", "ollama")

    if provider == "openai":
        print_info(f"OpenAI mode — using model: {C.CYAN}{model_config['model']}{C.RESET}")
        change = input(f"  Press Enter to continue or type a different model name: ").strip()
        if change:
            model_config["model"] = change
        return model_config

    # Ollama — fetch installed models
    try:
        import ollama as _ollama
        response = _ollama.list()
        # Handle both old (dict) and new (object with .models) SDK response formats
        if hasattr(response, "models"):
            models = response.models
            model_names = [m.model for m in models] if models else []
        elif isinstance(response, dict):
            models = response.get("models", [])
            # .get() can return None for either key, so filter before calling .lower()
            model_names = [
                name for name in
                (m.get("name") or m.get("model") for m in models)
                if name is not None
            ]
        else:
            model_names = []

        if not model_names:
            print_warn("No models found in Ollama. Using default from .env.")
            return model_config

        print(f"\n  {C.BOLD}Available models:{C.RESET}")
        for i, name in enumerate(model_names, 1):
            marker = f"{C.GREEN}●{C.RESET}" if name == model_config["model"] else f"{C.DIM}○{C.RESET}"
            print(f"    {marker}  {i}. {name}")
        print(f"  {C.DIM}(current: {model_config['model']}){C.RESET}\n")

        raw_input = input(f"  Pick a model (number or name) or press Enter to keep current: ").strip()

        if not raw_input:
            return model_config

        raw: str = raw_input
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(model_names):
                model_config["model"] = model_names[idx]
            else:
                print_warn("Invalid number, keeping current model.")
        else:
            # Accept partial name match; model_names contains no None values here
            # raw is guaranteed to be non-empty string here due to earlier check
            if raw is None or raw == "":
                return model_config  # Should never happen, but for type safety
            matches = [m for m in model_names if m is not None and raw.lower() in m.lower()]
            if len(matches) == 1:
                model_config["model"] = matches[0]
            elif len(matches) > 1:
                print_warn(f"Multiple matches: {matches}. Be more specific.")
            else:
                print_warn(f"Model '{raw}' not found in Ollama. Keeping current.")

        print_success(f"Using model: {C.CYAN}{model_config['model']}{C.RESET}")
        return model_config

    except Exception as e:
        print_warn(f"Could not fetch Ollama models: {e}. Using default.")
        return model_config


# ── Main loop ────────────────────────────────────────────────────────────────

def main() -> None:
    """Start the Dataloom CLI session.

    Connects to the database, loads the schema, selects a model, then enters
    the interactive question loop. Each iteration runs the full
    intent→validate→SQL→execute pipeline and paginates results.
    """
    banner()

    try:
        conn = connect_db()
    except Exception as e:
        print_error(f"Database connection failed: {e}")
        sys.exit(1)

    print_info("Loading schema...")
    try:
        # get_schema returns four values: text, map, types, and join-path dict
        schema_text, schema_map, schema_types, join_paths = get_schema(conn)
        print_success(f"Schema loaded. {len(schema_map)} tables found.")
    except Exception as e:
        print_error(f"Schema load failed: {e}")
        sys.exit(1)

    model_config = get_model_config()
    model_config = select_model(model_config)
    print_info(f"Model: {C.CYAN}{model_config['model']}{C.RESET} via {C.CYAN}{model_config['provider']}{C.RESET}")

    memory   = IntentMemory(max_size=5)
    PAGE_SIZE = 20
    last_all_rows: list = []
    last_headers: list  = []
    current_page        = 0

    divider()
    print(f"\n  {C.WHITE}Ready. Ask anything or type {C.CYAN}help{C.WHITE}.{C.RESET}\n")

    while True:
        try:
            raw = input(f"  {C.BOLD}{C.CYAN}You{C.RESET}  › ").strip()
        except (KeyboardInterrupt, EOFError):
            print(f"\n\n  {C.DIM}Goodbye.{C.RESET}\n")
            break

        if not raw:
            continue
        cmd = raw.lower()

        if cmd in ("exit", "quit", "q"):
            print(f"\n  {C.DIM}Goodbye.{C.RESET}\n")
            break
        elif cmd == "help":
            help_text()
        elif cmd == "clear":
            os.system("cls" if os.name == "nt" else "clear")
            banner()
        elif cmd == "schema":
            print(f"\n{C.DIM}{schema_text}{C.RESET}\n")
        elif cmd == "model":
            print_info(f"Provider: {model_config['provider']} | Model: {model_config['model']}")
        elif cmd == "next":
            if not last_all_rows:
                print_warn("No previous results to paginate.")
                continue
            current_page += 1
            start = current_page * PAGE_SIZE
            page_rows = last_all_rows[start:start + PAGE_SIZE]
            if not page_rows:
                print_warn("No more rows — you've reached the end.")
                current_page -= 1
            else:
                display_results(last_headers, page_rows,
                                page=current_page + 1,
                                page_size=PAGE_SIZE,
                                total=len(last_all_rows))

        elif cmd in ("back", "prev", "previous"):
            if not last_all_rows:
                print_warn("No previous results to paginate.")
                continue
            if current_page == 0:
                print_warn("You're already on the first page.")
                continue
            current_page -= 1
            start = current_page * PAGE_SIZE
            page_rows = last_all_rows[start:start + PAGE_SIZE]
            display_results(last_headers, page_rows,
                            page=current_page + 1,
                            page_size=PAGE_SIZE,
                            total=len(last_all_rows))
        elif cmd == "history":
            recent = memory.get_recent(5)
            if not recent:
                print_info("No history yet.")
            else:
                print()
                for i, e in enumerate(recent, 1):
                    print(f"  {C.DIM}{i}.{C.RESET} {e['question']}")
                    print(f"     {C.DIM}→ {e['intent'].get('metric')} from {e['intent'].get('fact_table')}{C.RESET}")
                print()
        elif cmd == "describe":
            table  = input("  Table name: ").strip()
            # Empty string is the correct sentinel for "no column specified";
            # save_description expects str, not Optional[str], so pass "" directly
            column = input("  Column name (blank for table description): ").strip()
            desc   = input("  Description: ").strip()
            if table and desc:
                save_description(table, column, desc)
                # Reload all four schema values after a metadata update
                schema_text, schema_map, schema_types, join_paths = get_schema(conn)
                print_success("Saved and schema reloaded.")
            else:
                print_warn("Table name and description are required.")

        else:
            # ── Main query flow ───────────────────────────────────

            # 1. Pre-screen for vague questions before calling the model
            if is_vague_question(raw):
                print(f"\n  {C.YELLOW}?{C.RESET}  Could you be more specific? For example: 'What is the total revenue this month?' or 'How many orders were placed last week?'\n")
                continue

            # 2. Parse intent
            try:
                recent_intents = [e["intent"] for e in memory.get_recent(3)]
                intent = run_with_spinner(
                    "Parsing intent",
                    parse_intent,
                    raw, schema_text, recent_intents, model_config
                )
            except Exception as e:
                print_error(f"Intent parsing failed: {e}")
                continue

            # 3. Clarification check — model-level + confidence gate
            if intent.get("clarification_needed"):
                print(f"\n  {C.YELLOW}?{C.RESET}  {intent['clarification_needed']}\n")
                continue

            # Confidence gate: low confidence always triggers clarification
            confidence = (intent.get("confidence") or "high").lower()
            if confidence == "low" and not intent.get("clarification_needed"):
                print(f"\n  {C.YELLOW}?{C.RESET}  I'm not confident I understood that correctly. "
                      f"Could you rephrase or be more specific about what you'd like to measure?\n")
                continue
            if confidence == "medium":
                print(f"  {C.YELLOW}⚠{C.RESET}  Medium confidence — I made an assumption. "
                      f"Verify the result matches your intent.\n")

            # Post-validation: generic metric on a short question is likely underspecified
            metric = (intent.get("metric") or "").lower()
            generic_metrics = {"count", "total", "number", "sum", "avg", "value", "amount"}
            if metric in generic_metrics and len(raw.split()) <= 5:
                print(f"\n  {C.YELLOW}?{C.RESET}  Could you be more specific? What would you like to measure? (e.g. revenue, orders, customers, freight)\n")
                continue

            # 4. Follow-up detection — intent level only, no keyword heuristics
            if memory.is_followup(intent):
                intent = memory.merge_with_previous(intent)
                print_info("Follow-up detected — merged with previous intent.")

            # Inject original question into intent for sql_builder ranking detection
            intent["_question"] = raw

            print_intent(intent)

            # 5. Validate — with one validation-level retry before giving up
            clean_intent = _strip_meta(intent)
            is_valid, errors = validate_intent(clean_intent, schema_map, schema_types)
            if not is_valid:
                print_warn(f"Validation failed ({len(errors)} error(s)). Attempting correction...")
                for err in errors:
                    print(f"    {C.RED}• {err}{C.RESET}")
                try:
                    fixed_intent = run_with_spinner(
                        "Correcting intent",
                        parse_validation_retry,
                        raw, intent, errors, model_config,
                    )
                    fixed_intent["_question"] = raw
                    clean_fixed = _strip_meta(fixed_intent)
                    is_valid2, errors2 = validate_intent(clean_fixed, schema_map, schema_types)
                    if not is_valid2:
                        print_error("Correction still invalid — cannot proceed:")
                        for err in errors2:
                            print(f"    {C.RED}• {err}{C.RESET}")
                        print_info("Try rephrasing or use 'describe' to add context.")
                        continue
                    print_success("Validation correction succeeded.")
                    intent       = fixed_intent
                    clean_intent = clean_fixed
                except Exception as ve:
                    print_error(f"Validation correction failed: {ve}")
                    print_info("Try rephrasing or use 'describe' to add context.")
                    continue

            # 6. Build SQL
            try:
                sql, params = build_sql(clean_intent)
            except Exception as e:
                print_error(f"SQL build failed: {e}")
                continue

            print_sql(sql)

            # 7. Execute — up to 2 auto-retries on DB failure
            headers, all_rows = None, None
            for attempt in range(2):
                try:
                    headers, all_rows = run_with_spinner("Running query", run_query, conn, sql, params)
                    break
                except Exception as exec_error:
                    conn.rollback()
                    if attempt == 0:
                        print_warn(f"Query failed: {exec_error}")
                        print_info(f"Attempting self-correction (attempt {attempt + 1}/2)...")
                        try:
                            fixed_intent = run_with_spinner(
                                "Correcting intent",
                                parse_retry,
                                sql, str(exec_error), intent, model_config,
                            )
                            fixed_intent["_question"] = raw
                            clean_fixed = _strip_meta(fixed_intent)
                            is_valid_r, errors_r = validate_intent(clean_fixed, schema_map, schema_types)
                            if not is_valid_r:
                                print_error("Corrected intent invalid:")
                                for err in errors_r:
                                    print(f"    {C.RED}• {err}{C.RESET}")
                                break
                            sql, params = build_sql(clean_fixed)
                            print_sql(sql, label="Corrected SQL")
                            intent       = fixed_intent
                            clean_intent = clean_fixed
                        except Exception as retry_err:
                            print_error(f"Self-correction failed: {retry_err}")
                            break
                    else:
                        print_error(f"Query failed after 2 attempts: {exec_error}")
                        print_info("Try rephrasing your question.")

            if all_rows is None:
                continue

            # 8. Store and paginate
            if headers is not None and all_rows is not None:
                last_all_rows = all_rows
                last_headers = headers
                current_page = 0
                first_page = all_rows[:PAGE_SIZE]
                display_results(headers, first_page, page=1, page_size=PAGE_SIZE, total=len(all_rows))

            # 9. Save to memory
            memory.add(intent, raw)

    conn.close()


if __name__ == "__main__":
    main()
