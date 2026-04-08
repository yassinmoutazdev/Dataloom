"""
Flask web server and HTTP API layer for Dataloom.

Owns session lifecycle, all REST routes, and the multi-provider model
configuration layer. Every browser session maintains isolated database
contexts; switching databases parks the current context and loads the target
so query history never bleeds across databases.

In-memory session shape (keyed by Flask session cookie ``sid``):

    _session_store[sid] = {
        "last_seen":    float,
        "model_config": dict,        # provider, model, api_key, pinned_models
        "active_db":    str | None,
        "contexts": {
            "<db_name>": {
                "conn":         connection,
                "db_type":      str,
                "schema_text":  str,
                "schema_map":   dict,
                "schema_types": dict,
                "join_paths":   dict,
                "memory":       IntentMemory,
                "history":      list,
                "credentials":  dict,
            }
        }
    }

Depends on: db_connector, schema, memory, core, history_store, utils (optional).
"""

import os
import re
import sys
import uuid
import time
import threading
from flask import Flask, request, jsonify, send_from_directory, send_file, session, redirect, url_for
from dotenv import load_dotenv

load_dotenv()

from db_connector import (
    connect_with_credentials,
    discover_databases,
    save_credentials,
    load_saved_credentials,
)
from schema import get_schema
from memory import IntentMemory
from core import run_pipeline, init_join_paths
import history_store
try:
    from utils import export_csv, export_excel, make_export_filename
    HAS_UTILS = True
except ImportError:
    def export_csv(*_a, **_kw) -> bytes:  # type: ignore[misc]
        raise RuntimeError("utils module not available — cannot export CSV")
    def export_excel(*_a, **_kw) -> bytes:  # type: ignore[misc]
        raise RuntimeError("utils module not available — cannot export Excel")
    def make_export_filename(*_a, **_kw) -> str:  # type: ignore[misc]
        raise RuntimeError("utils module not available — cannot generate export filename")
    HAS_UTILS = False

app = Flask(__name__, static_folder="static", template_folder="templates")
app.secret_key = os.getenv("FLASK_SECRET", os.urandom(24).hex())

# Hard cap on request body size — prevents large payloads from reaching route logic.
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024   # 16 KB max request body

# Sliding-window rate limit applied per session inside the /api/query route.
RATE_LIMIT          = int(os.getenv("RATE_LIMIT", "20"))
RATE_LIMIT_WINDOW   = 3600   # seconds

# Sessions idle longer than this are eligible for purge by _schedule_purge().
SESSION_TTL_SECONDS = 3600
_session_store: dict[str, dict] = {}


# ── Model config ──────────────────────────────────────────────────

def _default_model_config() -> dict:
    """Build a model config dict seeded from environment variables.

    Called exactly once per new session by ``_get_session()``. Any subsequent
    ``POST /api/setup/model`` call overwrites ``sess["model_config"]`` in place,
    so env-var values only act as a fallback for headless / CI deployments that
    skip the setup wizard.

    Returns:
        A model config dict containing at minimum ``provider``, ``model``, and
        ``pinned_models``. Cloud providers also include ``api_key``; Ollama
        includes ``host``; Azure additionally includes ``endpoint`` and
        ``api_version``.
    """
    provider = os.getenv("MODEL_PROVIDER", "ollama").lower()
    if provider == "openai":
        _m = os.getenv("OPENAI_MODEL", "gpt-4o")
        return {"provider": "openai",  "model": _m, "api_key": os.getenv("OPENAI_API_KEY", ""),
                "pinned_models": [_m]}
    if provider == "gemini":
        _m = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite-preview-06-17")
        return {"provider": "gemini",  "model": _m, "api_key": os.getenv("GEMINI_API_KEY", ""),
                "pinned_models": [_m]}
    if provider == "anthropic":
        _m = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-5")
        return {"provider": "anthropic", "model": _m, "api_key": os.getenv("ANTHROPIC_API_KEY", ""),
                "pinned_models": [_m]}
    if provider == "xai":
        _m = os.getenv("XAI_MODEL", "grok-3-mini")
        return {"provider": "xai",     "model": _m, "api_key": os.getenv("XAI_API_KEY", ""),
                "pinned_models": [_m]}
    if provider == "openrouter":
        _m = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o")
        return {"provider": "openrouter", "model": _m, "api_key": os.getenv("OPENROUTER_API_KEY", ""),
                "pinned_models": [_m]}
    if provider == "azure":
        _m = os.getenv("AZURE_OPENAI_DEPLOYMENT", "")
        return {"provider": "azure",   "model": _m,
                "api_key": os.getenv("AZURE_OPENAI_API_KEY", ""),
                "endpoint": os.getenv("AZURE_OPENAI_ENDPOINT", ""),
                "api_version": os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
                "pinned_models": [_m] if _m else []}
    _m = os.getenv("OLLAMA_MODEL", "mistral")
    return {"provider": "ollama", "model": _m,
            "host": os.getenv("OLLAMA_HOST", "http://localhost:11434"),
            "pinned_models": [_m]}


def _test_model_connection(provider: str, config: dict) -> None:
    """Fire a minimal live call to verify provider credentials before storing them.

    Credentials are read exclusively from ``config`` — never from env vars — so
    this is safe to call with wizard-supplied values before they are persisted
    to the session.

    Args:
        provider: One of ``"openai"``, ``"gemini"``, ``"anthropic"``, ``"xai"``,
            ``"openrouter"``, ``"azure"``, or ``"ollama"``.
        config: Provider config dict. Must contain ``"model"`` and, for cloud
            providers, ``"api_key"``. Azure also requires ``"endpoint"``.

    Raises:
        RuntimeError: If the provider package is missing, credentials are
            invalid, the host is unreachable, or the requested model does not
            exist on the Ollama instance.
    """
    # ── OpenAI-compatible providers (share the same code path) ────
    _OAI_COMPAT = {
        "openai":      None,   # default base_url
        "gemini":      "https://generativelanguage.googleapis.com/v1beta/openai/",
        "xai":         "https://api.x.ai/v1",
        "openrouter":  "https://openrouter.ai/api/v1",
    }
    if provider in _OAI_COMPAT:
        try:
            import openai as _oai
        except ImportError:
            raise RuntimeError("'openai' package not installed.  Run: pip install openai")
        kwargs: dict = {"api_key": config["api_key"], "timeout": 15.0}
        if _OAI_COMPAT[provider]:
            kwargs["base_url"] = _OAI_COMPAT[provider]
        if provider == "openrouter":
            kwargs["default_headers"] = {
                "HTTP-Referer": "https://dataloom.app",
                "X-Title":      "Dataloom",
            }
        client = _oai.OpenAI(**kwargs)
        client.chat.completions.create(
            model=config["model"],
            messages=[{"role": "user", "content": "Reply with: ok"}],
            max_tokens=5,
        )
        return

    if provider == "anthropic":
        try:
            import anthropic as _ant # type: ignore
        except ImportError:
            raise RuntimeError("'anthropic' package not installed.  Run: pip install anthropic")
        client = _ant.Anthropic(api_key=config["api_key"])
        client.messages.create(
            model=config["model"],
            max_tokens=5,
            messages=[{"role": "user", "content": "Reply with: ok"}],
        )
        return

    if provider == "azure":
        try:
            import openai as _oai
        except ImportError:
            raise RuntimeError("'openai' package not installed.  Run: pip install openai")
        if not config.get("endpoint"):
            raise RuntimeError("Azure endpoint URL is required")
        client = _oai.AzureOpenAI(
            api_key=config["api_key"],
            azure_endpoint=config["endpoint"],
            api_version=config.get("api_version", "2024-02-01"),
            timeout=15.0,
        )
        client.chat.completions.create(
            model=config["model"],
            messages=[{"role": "user", "content": "Reply with: ok"}],
            max_tokens=5,
        )
        return

    if provider == "ollama":
        import urllib.request as _req
        import json as _json
        host  = config.get("host", "http://localhost:11434").rstrip("/")
        model = config.get("model", "")
        if not model:
            raise RuntimeError("Model name is required")
        # Verify the host is reachable by hitting /api/tags
        try:
            with _req.urlopen(f"{host}/api/tags", timeout=8) as r:
                body  = _json.loads(r.read())
                names = [m.get("name", "") for m in body.get("models", [])]
        except Exception as exc:
            raise RuntimeError(f"Could not reach Ollama at {host}: {exc}")
        # Warn if the model isn't present, but don't block (user may have typed a valid alias)
        clean_names = [n.split(":")[0] for n in names]
        if names and model.split(":")[0] not in clean_names:
            raise RuntimeError(
                f"Model '{model}' not found in Ollama at {host}. "
                f"Available: {', '.join(names[:8])}"
            )
        return

    raise RuntimeError(f"Unknown provider: {provider}")


# ── Session helpers ───────────────────────────────────────────────

# How often the background timer fires to evict expired sessions.
# Stale sessions may linger up to this long past SESSION_TTL_SECONDS before
# their DB connections are closed — acceptable because connections are idle
# by that point. Moving purge off the hot path avoids dict-iteration overhead
# on every incoming request.
_PURGE_INTERVAL_SECONDS = 600  # 10 minutes


def _purge_stale_sessions() -> None:
    """Close DB connections and remove sessions that have exceeded SESSION_TTL_SECONDS."""
    cutoff = time.time() - SESSION_TTL_SECONDS
    stale  = [sid for sid, s in _session_store.items() if s["last_seen"] < cutoff]
    for sid in stale:
        for ctx in _session_store[sid].get("contexts", {}).values():
            try:
                ctx["conn"].close()
            except Exception:
                pass
        del _session_store[sid]


def _schedule_purge():
    """Run _purge_stale_sessions() on a recurring background timer."""
    try:
        _purge_stale_sessions()
    except Exception:
        pass  # Never let a background error surface to a request
    t = threading.Timer(_PURGE_INTERVAL_SECONDS, _schedule_purge)
    t.daemon = True   # Dies with the process — no shutdown hook needed
    t.start()


_schedule_purge()   # Kick off the first timer at module load


def _get_session() -> dict:
    """Return the server-side session dict for the current request, creating it if absent.

    Assigns a UUID to the Flask cookie ``sid`` on first call. Touching
    ``last_seen`` on every call keeps active sessions from being evicted.

    Returns:
        The mutable session dict from ``_session_store``.
    """
    sid = session.get("sid")
    if not sid:
        sid = str(uuid.uuid4())
        session["sid"] = sid
    if sid not in _session_store:
        _session_store[sid] = {
            "last_seen":    time.time(),
            "model_config": _default_model_config(),
            "active_db":    None,
            "contexts":     {},
            "query_count":  0,
            "window_start": time.time(),
        }
    else:
        _session_store[sid]["last_seen"] = time.time()
    return _session_store[sid]


def _get_active_context() -> dict | None:
    """Return the context dict for the active database, or None if none is selected."""
    sess   = _get_session()
    active = sess.get("active_db")
    if not active:
        return None
    return sess["contexts"].get(active)


def _clean_db_name(raw: str) -> str:
    """Convert a raw database identifier into a human-readable display name.

    Replaces underscores and hyphens with spaces and title-cases the result,
    e.g. ``"olist_logistics"`` → ``"Olist Logistics"``. The ``DB_DISPLAY_NAME``
    env var overrides this transformation entirely when set.

    Args:
        raw: The internal database name as stored in the session.

    Returns:
        A space-separated, title-cased display string, or the env-var override.
    """
    override = os.getenv("DB_DISPLAY_NAME", "").strip()
    if override:
        return override
    return re.sub(r"[_-]+", " ", raw).title()


def _build_context(conn, db_type: str, credentials: dict) -> dict:
    """Construct a fully initialised database context dict for a live connection.

    Calls ``get_schema`` to populate the schema maps and registers join paths
    with the sql_builder via ``init_join_paths``.

    Args:
        conn: An open database connection (psycopg2, MySQLdb, or sqlite3).
        db_type: One of ``"postgresql"``, ``"mysql"``, or ``"sqlite"``.
        credentials: The connection parameters used to establish ``conn``,
            stored so the session can reconnect on failure.

    Returns:
        A context dict ready to be stored under ``sess["contexts"][db_name]``.
    """
    if conn is None:
        raise RuntimeError("_build_context() called with a None connection object")
    schema_text, schema_map, schema_types, join_paths = get_schema(conn, db_type)
    if schema_text is None:
        raise RuntimeError("get_schema() returned None for schema_text")
    if schema_map is None:
        raise RuntimeError("get_schema() returned None for schema_map")
    if schema_types is None:
        raise RuntimeError("get_schema() returned None for schema_types")
    if join_paths is None:
        raise RuntimeError("get_schema() returned None for join_paths")
    init_join_paths(join_paths)
    return {
        "conn":        conn,
        "db_type":     db_type,
        "schema_text": schema_text,
        "schema_map":  schema_map,
        "schema_types":schema_types,
        "join_paths":  join_paths,
        "memory":      IntentMemory(max_size=20),
        "history":     [],
        "credentials": credentials,
    }


def _session_is_ready() -> bool:
    """Return True only when the session has an active database context loaded."""
    sess   = _get_session()
    active = sess.get("active_db")
    return bool(active and active in sess["contexts"])


# ── Page routes ───────────────────────────────────────────────────

@app.route("/")
def index():
    """Serve the main application shell, redirecting to /setup if no DB is connected."""
    if not _session_is_ready():
        return redirect(url_for("setup"))
    return send_from_directory("templates", "index.html")


@app.route("/setup")
def setup():
    """Serve the multi-step setup wizard."""
    return send_from_directory("templates", "setup.html")


# ── Setup API ─────────────────────────────────────────────────────

@app.route("/api/setup/status")
def setup_status():
    """Return saved server credentials so the wizard can pre-fill on revisit."""
    creds = load_saved_credentials() or {}
    return jsonify({"has_saved_credentials": bool(creds), "credentials": creds})


@app.route("/api/setup/discover", methods=["POST"])
def setup_discover():
    """Return the list of databases accessible with the supplied server credentials.

    Args (JSON body):
        db_type: One of ``"postgresql"``, ``"mysql"``, or ``"sqlite"``.
        host: Database server hostname.
        port: Server port as a string.
        user: Database username.
        password: Database password.

    Returns:
        JSON ``{ databases: [str] }`` on success, or ``{ error: str }`` with
        HTTP 400 on connection failure or invalid ``db_type``.
    """
    data     = request.json or {}
    db_type  = data.get("db_type", "").lower()
    host     = data.get("host", "localhost")
    port     = str(data.get("port", "5432"))
    user     = data.get("user", "")
    password = data.get("password", "")

    if db_type not in ("postgresql", "mysql", "sqlite"):
        return jsonify({"error": "Invalid database type"}), 400
    try:
        databases = discover_databases(db_type, host, port, user, password)
        return jsonify({"databases": databases})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/setup/connect", methods=["POST"])
def setup_connect():
    """Connect to a specific database, load its schema, and set it as the active context.

    On success, server credentials (not the database name) are persisted to
    ``.env`` so the wizard can pre-fill them on the next visit. The database
    name itself is intentionally not saved — users select it interactively.

    Args (JSON body):
        db_type: One of ``"postgresql"``, ``"mysql"``, or ``"sqlite"``.
        host, port, user, password: Server credentials (PostgreSQL / MySQL).
        database: Target database name (PostgreSQL / MySQL).
        path: File path (SQLite only).

    Returns:
        JSON ``{ ok, db_name, display_name, db_type, tables }`` on success, or
        ``{ error: str }`` with HTTP 400 / 500 on failure.
    """
    data     = request.json or {}
    db_type  = data.get("db_type", "").lower()
    host     = data.get("host", "localhost")
    port     = str(data.get("port", "5432"))
    user     = data.get("user", "")
    password = data.get("password", "")
    dbname   = data.get("database", "").strip()
    path     = data.get("path", "").strip()

    if db_type not in ("postgresql", "mysql", "sqlite"):
        return jsonify({"error": "Invalid database type"}), 400

    actual_dbname = path if db_type == "sqlite" else dbname
    if not actual_dbname:
        return jsonify({"error": "Database name / path is required"}), 400

    credentials = {
        "db_type": db_type, "host": host, "port": port,
        "user": user, "password": password, "dbname": actual_dbname,
    }

    try:
        conn = connect_with_credentials(db_type, host, port, actual_dbname, user, password)
    except Exception as e:
        return jsonify({"error": f"Connection failed: {e}"}), 400

    if conn is None:
        return jsonify({"error": "Connection failed: driver returned no connection object"}), 500

    try:
        ctx = _build_context(conn, db_type, credentials)
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        return jsonify({"error": f"Schema load failed: {e}"}), 500

    # Persist server credentials (not the database name) to .env
    try:
        if db_type == "sqlite":
            save_credentials(db_type, path=path)
        else:
            save_credentials(db_type, host=host, port=port, user=user, password=password)
    except Exception:
        pass

    sess = _get_session()
    sess["contexts"][actual_dbname] = ctx
    sess["active_db"] = actual_dbname
    # Restore persisted history from disk into this context
    sid = session.get("sid")
    if sid:
        history_store.restore_into_session(sid, sess["contexts"])

    return jsonify({
        "ok":           True,
        "db_name":      actual_dbname,
        "display_name": _clean_db_name(actual_dbname),
        "db_type":      db_type,
        "tables":       len(ctx["schema_map"]),
    })


@app.route("/api/setup/model", methods=["POST"])
def setup_model():
    """Validate and store AI provider credentials at the end of the setup wizard.

    Fires a minimal live LLM call via ``_test_model_connection`` before
    accepting credentials. The ``api_key`` is stored only in the server-side
    session dict — it is never echoed back to the client and never written to
    disk. Any existing ``pinned_models`` list in the session is preserved so
    a reconfigure mid-session does not discard the user's pin selections.

    Args (JSON body):
        provider: One of the VALID_PROVIDERS identifiers.
        api_key: Provider API key (not required for Ollama).
        model: Model name or deployment name (Azure).
        host: Ollama base URL (Ollama only, default ``http://localhost:11434``).
        endpoint: Azure OpenAI resource URL (Azure only).
        api_version: Azure REST API version (Azure only, default ``2024-02-01``).

    Returns:
        JSON ``{ success: true, provider, model }`` on success, or
        ``{ error: str }`` with HTTP 400 on validation or connection failure.
    """
    data        = request.json or {}
    provider    = data.get("provider",    "").lower().strip()
    api_key     = data.get("api_key",     "").strip()
    model       = data.get("model",       "").strip()
    host        = data.get("host",        "http://localhost:11434").strip()
    endpoint    = data.get("endpoint",    "").strip()   # Azure only
    api_version = data.get("api_version", "2024-02-01").strip()  # Azure only

    VALID_PROVIDERS = ("openai", "gemini", "anthropic", "xai", "openrouter", "azure", "ollama")
    if provider not in VALID_PROVIDERS:
        return jsonify({"error": f"Unknown provider '{provider}'"}), 400
    if provider not in ("ollama",) and not api_key:
        return jsonify({"error": "API key is required for this provider"}), 400
    if provider == "azure" and not endpoint:
        return jsonify({"error": "Azure endpoint URL is required"}), 400
    if not model:
        return jsonify({"error": "Model name is required"}), 400

    # Build a transient config just for the connection test
    config: dict = {"provider": provider, "model": model}
    if api_key:
        config["api_key"] = api_key
    if provider == "ollama":
        config["host"] = host or "http://localhost:11434"
    if provider == "azure":
        config["endpoint"]    = endpoint
        config["api_version"] = api_version

    try:
        _test_model_connection(provider, config)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400

    # Credentials verified — persist to session (server-side only)
    sess = _get_session()
    # Preserve any existing pinned_models; seed with the new active model otherwise.
    existing_pinned = sess.get("model_config", {}).get("pinned_models", [])
    config["pinned_models"] = existing_pinned if existing_pinned else [model]
    if model not in config["pinned_models"]:
        config["pinned_models"].insert(0, model)
    sess["model_config"] = config

    # Return success without echoing the api_key back
    return jsonify({"success": True, "provider": provider, "model": model})


@app.route("/api/setup/ollama-models")
def setup_ollama_models():
    """Return the list of models available on a given Ollama host.

    Hits the Ollama REST API directly rather than the Python SDK to avoid the
    versioned ``@1.0.0`` suffix that ``model`` field carries in newer SDK
    releases. The ``name`` field always returns the short form expected by
    ``ollama chat``.

    Args (query string):
        host: Ollama base URL. Defaults to ``http://localhost:11434``.

    Returns:
        JSON ``{ models: [str] }`` on success, or ``{ error: str }`` with
        HTTP 400 if the host is unreachable.
    """
    import urllib.request as _req
    import json as _json

    host = (request.args.get("host") or "http://localhost:11434").strip().rstrip("/")
    try:
        with _req.urlopen(f"{host}/api/tags", timeout=8) as r:
            body  = _json.loads(r.read())
        # Always use the short "name" field (e.g. "gemma3:4b"), not "model" which
        # may carry a versioned "@1.0.0" suffix that Ollama's chat API rejects.
        names = [m.get("name", "") for m in body.get("models", []) if m.get("name")]
        return jsonify({"models": names})
    except Exception as exc:
        return jsonify({"error": f"Could not reach Ollama at {host}: {exc}"}), 400


@app.route("/api/setup/provider-models", methods=["POST"])
def setup_provider_models():
    """Return a live or fallback model list for a provider during the setup wizard.

    Called by the wizard's model-selector dropdown before the user clicks
    "Test connection", so the list is populated before credentials are stored.
    The ``api_key`` is used only for this request and is never persisted.

    Falls back to a curated static list when the live fetch fails or when no
    key has been entered yet. Returns ``source: "live"`` or ``"fallback"`` so
    the frontend can display an appropriate hint.

    Args (JSON body):
        provider: Provider identifier string.
        api_key: Provider API key (optional — omitting returns the fallback list).
        endpoint: Azure resource URL (Azure only).
        api_version: Azure API version (Azure only).

    Returns:
        JSON ``{ models: [str], source: str }`` and optionally ``{ error: str }``
        when a live fetch was attempted but failed.
    """
    import json as _json

    FALLBACK: dict[str, list[str]] = {
        "openai":     ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-3.5-turbo"],
        "gemini":     ["gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite",
                       "gemini-2.0-flash", "gemini-1.5-pro", "gemini-1.5-flash"],
        "anthropic":  ["claude-opus-4-5", "claude-sonnet-4-5", "claude-haiku-4-5",
                       "claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022"],
        "xai":        ["grok-3", "grok-3-mini", "grok-3-fast", "grok-2-1212"],
        "openrouter": ["openai/gpt-4o", "openai/gpt-4o-mini",
                       "anthropic/claude-sonnet-4-5", "anthropic/claude-haiku-4-5",
                       "google/gemini-2.5-flash", "meta-llama/llama-3.3-70b-instruct",
                       "mistralai/mistral-7b-instruct"],
        "azure":      [],   # deployment names are instance-specific
    }

    data        = request.json or {}
    provider    = data.get("provider", "").lower().strip()
    api_key     = data.get("api_key",  "").strip()
    endpoint    = data.get("endpoint", "").strip()
    api_version = data.get("api_version", "2024-02-01").strip()

    if provider not in FALLBACK and provider != "ollama":
        return jsonify({"error": f"Unknown provider '{provider}'"}), 400

    fallback = FALLBACK.get(provider, [])

    # ── Anthropic: no public models endpoint yet ──────────────────
    if provider == "anthropic":
        return jsonify({"models": fallback, "source": "fallback"})

    # ── Azure: deployment names are user-defined, no global list ──
    if provider == "azure":
        return jsonify({"models": [], "source": "none",
                        "hint": "Enter your deployment name in the field below"})

    # ── OpenAI-compatible providers ───────────────────────────────
    _OAI_BASE = {
        "openai":     None,
        "xai":        "https://api.x.ai/v1",
        "openrouter": "https://openrouter.ai/api/v1",
    }

    if provider in _OAI_BASE:
        if not api_key:
            return jsonify({"models": fallback, "source": "fallback"})
        try:
            import openai as _oai
            kwargs: dict = {"api_key": api_key, "timeout": 10.0}
            if _OAI_BASE[provider]:
                kwargs["base_url"] = _OAI_BASE[provider]
            if provider == "openrouter":
                kwargs["default_headers"] = {
                    "HTTP-Referer": "https://dataloom.app",
                    "X-Title":      "Dataloom",
                }
            client = _oai.OpenAI(**kwargs)
            raw_models = list(client.models.list())

            if provider == "openai":
                # Keep only chat-capable models; exclude embeddings/audio/image
                CHAT_PREFIXES = ("gpt-", "o1", "o3", "o4", "chatgpt-")
                names = sorted(
                    [m.id for m in raw_models
                     if any(m.id.startswith(p) for p in CHAT_PREFIXES)],
                    reverse=True,
                )
            elif provider == "openrouter":
                # OpenRouter model IDs are "provider/model-name"
                names = sorted([m.id for m in raw_models])
            else:
                # xAI
                names = sorted([m.id for m in raw_models], reverse=True)

            return jsonify({"models": names or fallback,
                            "source": "live" if names else "fallback"})
        except Exception:
            return jsonify({"models": fallback, "source": "fallback"})

    # ── Gemini: use the REST models list endpoint ─────────────────
    if provider == "gemini":
        if not api_key:
            return jsonify({"models": fallback, "source": "no_key"})
        try:
            import urllib.request as _req
            url  = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
            with _req.urlopen(url, timeout=10) as r:
                body = _json.loads(r.read())
            # Accept both generateContent and streamGenerateContent — newer models
            # may only list streamGenerateContent in supportedGenerationMethods.
            names = [
                m["name"].replace("models/", "")
                for m in body.get("models", [])
                if any(method in m.get("supportedGenerationMethods", [])
                       for method in ("generateContent", "streamGenerateContent"))
            ]
            names = sorted(names, reverse=True)
            return jsonify({"models": names or fallback,
                            "source": "live" if names else "fallback"})
        except Exception as exc:
            return jsonify({"models": fallback, "source": "fallback",
                            "error": str(exc)})

    return jsonify({"models": fallback, "source": "fallback"})


# ── Database management API ───────────────────────────────────────

@app.route("/api/databases")
def list_databases():
    """Return all database contexts connected in the current session.

    Returns:
        JSON array of objects with ``db_name``, ``display_name``, ``db_type``,
        ``tables``, ``active``, and ``history_count`` fields.
    """
    sess   = _get_session()
    active = sess.get("active_db")
    return jsonify([
        {
            "db_name":       name,
            "display_name":  _clean_db_name(name),
            "db_type":       ctx["db_type"],
            "tables":        len(ctx["schema_map"]),
            "active":        name == active,
            "history_count": len(ctx["history"]),
        }
        for name, ctx in sess["contexts"].items()
    ])


@app.route("/api/databases/switch", methods=["POST"])
def switch_database():
    """Change the active database context for the current session.

    The previous context is parked but not destroyed; its schema, memory, and
    history remain in the session and are restored if the user switches back.

    Args (JSON body):
        db_name: The internal name of a database already connected this session.

    Returns:
        JSON ``{ ok, db_name, display_name, db_type, tables }`` on success, or
        ``{ error: str }`` with HTTP 400 / 404 on failure.
    """
    data    = request.json or {}
    db_name = data.get("db_name", "").strip()

    if not db_name:
        return jsonify({"error": "db_name is required"}), 400

    sess = _get_session()
    if db_name not in sess["contexts"]:
        return jsonify({"error": f"Database '{db_name}' not connected in this session"}), 404

    sess["active_db"] = db_name
    ctx = sess["contexts"][db_name]

    return jsonify({
        "ok":           True,
        "db_name":      db_name,
        "display_name": _clean_db_name(db_name),
        "db_type":      ctx["db_type"],
        "tables":       len(ctx["schema_map"]),
    })


@app.route("/api/databases/add", methods=["POST"])
def add_database():
    """Connect an additional database mid-session without leaving the chat."""
    return setup_connect()


# ── Query API ─────────────────────────────────────────────────────


@app.route("/api/export", methods=["POST"])
def api_export():
    """Stream query results to the client as a downloadable CSV or Excel file.

    Falls back to CSV silently when ``openpyxl`` is not installed and the
    requested format is ``xlsx``.

    Args (JSON body):
        headers: Ordered list of column name strings.
        records: List of row dicts matching the header keys.
        format: ``"csv"`` (default) or ``"xlsx"``.

    Returns:
        A file attachment response, or ``{ error: str }`` with HTTP 400 / 500
        when data is missing or the export module is unavailable.
    """
    data    = request.get_json(force=True) or {}
    headers = data.get("headers", [])
    records = data.get("records", [])
    fmt     = data.get("format", "csv").lower()

    if not headers or not records:
        return jsonify({"error": "No data to export."}), 400

    if not HAS_UTILS:
        return jsonify({"error": "Export module not available."}), 500

    try:
        filename = make_export_filename(fmt)
        if fmt == "xlsx":
            try:
                file_bytes = export_excel(headers, records)
                mimetype   = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            except RuntimeError:
                # openpyxl not installed — fall back to CSV
                file_bytes = export_csv(headers, records)
                mimetype   = "text/csv"
                filename   = filename.replace(".xlsx", ".csv")
        else:
            file_bytes = export_csv(headers, records)
            mimetype   = "text/csv"

        buf = __import__('io').BytesIO(file_bytes)
        return send_file(
            buf,
            mimetype=mimetype,
            as_attachment=True,
            download_name=filename,
        )
    except Exception as e:
        return jsonify({"error": f"Export failed: {e}"}), 500


@app.route("/api/query", methods=["POST"])
def query():
    """Run a natural-language question through the full pipeline and return results.

    Enforces a per-session sliding-window rate limit (``RATE_LIMIT`` queries per
    ``RATE_LIMIT_WINDOW`` seconds). Connection errors from the database are
    surfaced with a user-friendly reconnect prompt; all other pipeline errors
    return a generic rephrasing suggestion so internal details are not leaked.

    Args (JSON body):
        question: The natural-language question string.

    Returns:
        JSON pipeline result containing ``success``, ``sql``, ``headers``,
        ``records``, ``total_rows``, ``confidence``, and optionally ``error``.
        HTTP 429 when the rate limit is exceeded.
    """
    sess = _get_session()
    ctx  = _get_active_context()

    if not ctx:
        return jsonify({"error": "No database connected. Please complete setup."}), 400

    data     = request.json or {}
    question = (data.get("question") or "").strip()
    if not question:
        return jsonify({"error": "No question provided"}), 400

    # ── Rate limiting ─────────────────────────────────────────────
    now = time.time()
    if now - sess.get("window_start", now) > RATE_LIMIT_WINDOW:
        sess["query_count"]  = 0
        sess["window_start"] = now
    if sess.get("query_count", 0) >= RATE_LIMIT:
        remaining = int(RATE_LIMIT_WINDOW - (now - sess["window_start"]))
        mins = max(1, remaining // 60)
        return jsonify({
            "success": False,
            "error": f"You’ve reached the {RATE_LIMIT} query limit for this hour. "
                     f"Please wait {mins} minute{'s' if mins != 1 else ''} and try again.",
            "confidence": "low",
        }), 429
    sess["query_count"] = sess.get("query_count", 0) + 1
    try:
        result = run_pipeline(
            question=question,
            schema_text=ctx["schema_text"],
            schema_map=ctx["schema_map"],
            schema_types=ctx["schema_types"],
            memory=ctx["memory"],
            model_config=sess["model_config"],
            conn=ctx["conn"],
            db_type=ctx["db_type"],
            credentials=ctx.get("credentials"),
        )
    except Exception as pipeline_error:
        err_msg = str(pipeline_error).lower()
        if any(k in err_msg for k in ("connection", "closed", "broken", "gone away", "timeout")):
            return jsonify({
                "success": False,
                "error": "Lost connection to the database. Please refresh the page and reconnect.",
                "confidence": "low",
            })
        return jsonify({
            "success": False,
            "error": "Something went wrong. Try rephrasing your question.",
            "confidence": "low",
        })

    if result is None:
        return jsonify({
            "success": False,
            "error": "Pipeline returned no result. Try rephrasing your question.",
            "confidence": "low",
        })

    # Append to in-memory + disk history on success
    if result["success"]:
        entry = {
            "question":   question,
            "sql":        result.get("sql"),
            "metric":     (result.get("intent") or {}).get("metric"),
            "table":      (result.get("intent") or {}).get("fact_table"),
            "confidence": result.get("confidence", "high"),
            "ts":         time.time(),
        }
        ctx["history"].append(entry)
        sid = session.get("sid")
        if sid and sess.get("active_db"):
            history_store.append(sid, sess["active_db"], entry)

    # If core.py reconnected, update the stored connection in context
    if result.get("_fresh_conn"):
        fresh = result.pop("_fresh_conn")
        ctx["conn"] = fresh
    result.pop("_reconnected", None)

    rows    = result.get("rows") or []
    hdrs    = result.get("headers") or []
    if rows and hdrs:
        result["records"]    = [dict(zip(hdrs, row)) for row in rows]
        result["total_rows"] = len(rows)
    else:
        result["records"]    = []
        result["total_rows"] = 0

    result.pop("rows", None)
    return jsonify(result)


# ── Schema, history, model ────────────────────────────────────────

@app.route("/api/schema")
def schema_route():
    """Return the active database schema as text and a structured table map.

    Returns:
        JSON ``{ text: str, tables: { table: [col, ...] } }``, or
        ``{ error: str }`` with HTTP 400 if no database is connected.
    """
    ctx = _get_active_context()
    if not ctx:
        return jsonify({"error": "No database connected"}), 400
    return jsonify({
        "text":   ctx["schema_text"],
        "tables": {t: cols for t, cols in ctx["schema_map"].items()},
    })


@app.route("/api/history")
@app.route("/api/history/<db_name>")
def history(db_name: str | None = None):
    """Return the query history for a database, merging in-memory and disk records.

    The disk-backed store is preferred when a ``sid`` cookie is present, as it
    survives server restarts. Falls back to the in-memory list otherwise.

    Args:
        db_name: Database to retrieve history for. Defaults to the active DB.

    Returns:
        JSON array of history entry dicts, newest first, capped at 50 items.
    """
    sess   = _get_session()
    target = db_name or sess.get("active_db")
    if not target:
        return jsonify([])
    sid = session.get("sid")
    if sid:
        entries = history_store.get(sid, target, limit=50) or []
    elif target in sess["contexts"]:
        entries = list(reversed(sess["contexts"][target]["history"][-50:]))
    else:
        entries = []
    return jsonify(entries)


@app.route("/api/clear", methods=["POST"])
def clear_memory():
    """Clear the LLM conversation memory for the active database.

    Resets follow-up query context without touching the visible history log.
    """
    ctx = _get_active_context()
    if ctx:
        ctx["memory"].clear()
    return jsonify({"ok": True})


@app.route("/api/history/clear", methods=["POST"])
def clear_history():
    """Wipe the query history for the active database from both memory and disk."""
    sess   = _get_session()
    target = sess.get("active_db")
    sid    = session.get("sid")
    if target and target in sess["contexts"]:
        sess["contexts"][target]["history"] = []
    if sid and target:
        history_store.clear(sid, target)
    return jsonify({"ok": True})


@app.route("/api/status")
def status():
    """Return a summary of the current session state.

    Used by the frontend on load and after database switches to sync UI state.

    Returns:
        JSON with ``configured``, ``db_name``, ``display_name``, ``db_type``,
        ``tables``, ``model``, ``provider``, and ``databases`` fields.
    """
    sess   = _get_session()
    ctx    = _get_active_context()
    active = sess.get("active_db")
    return jsonify({
        "configured":   bool(ctx),
        "db_name":      active,
        "display_name": _clean_db_name(active) if active else None,
        "db_type":      ctx["db_type"] if ctx else None,
        "tables":       len(ctx["schema_map"]) if ctx else 0,
        "model":        sess["model_config"].get("model"),
        "provider":     sess["model_config"].get("provider"),
        "databases":    list(sess["contexts"].keys()),
    })


@app.route("/api/models")
def models():
    """Return the session's pinned model list without hitting any provider API.

    This is the hot-path called every time the model dropdown opens, so it
    must be fast. The full provider catalog is available via
    ``GET /api/models/catalog``, which is called only when the user opens the
    Manage Models panel.

    Back-compat: sessions created before ``pinned_models`` was introduced get
    a synthetic list seeded from the active model.

    Returns:
        JSON ``{ models: [str], current: str }`` where ``models`` contains
        only the user's pinned selections.
    """
    sess    = _get_session()
    cfg     = sess["model_config"]
    current = cfg.get("model")
    pinned  = cfg.get("pinned_models")

    # Back-compat: sessions created before pinned_models existed
    if not pinned:
        pinned = [current] if current else []
        cfg["pinned_models"] = pinned

    # Guarantee the active model is always visible
    if current and current not in pinned:
        pinned = [current] + pinned
        cfg["pinned_models"] = pinned

    return jsonify({"models": pinned, "current": current})


@app.route("/api/models/catalog")
def models_catalog():
    """Return the full live model catalog for the session's active provider.

    Fetches from the provider API when credentials are available, falling back
    to a curated static list on failure. Always merges any pinned models that
    are not in the catalog (e.g. custom Ollama tags) so the frontend can render
    eye-toggle state correctly for every pinned entry.

    Called only when the user opens the Manage Models panel — never on a
    routine dropdown open.

    Returns:
        JSON ``{ models: [str], current: str, pinned: [str], provider: str }``
        and optionally ``{ error: str }`` when Ollama is unreachable.
    """
    import json as _json

    sess     = _get_session()
    cfg      = sess["model_config"]
    provider = cfg.get("provider", "ollama")
    current  = cfg.get("model")
    pinned   = cfg.get("pinned_models", [current] if current else [])

    FALLBACK: dict[str, list[str]] = {
        "openai":     ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-3.5-turbo"],
        "gemini":     ["gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite",
                       "gemini-2.0-flash", "gemini-1.5-pro", "gemini-1.5-flash"],
        "anthropic":  ["claude-opus-4-5", "claude-sonnet-4-5", "claude-haiku-4-5",
                       "claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022"],
        "xai":        ["grok-3", "grok-3-mini", "grok-3-fast", "grok-2-1212"],
        "openrouter": ["openai/gpt-4o", "openai/gpt-4o-mini",
                       "anthropic/claude-sonnet-4-5", "anthropic/claude-haiku-4-5",
                       "google/gemini-2.5-flash", "meta-llama/llama-3.3-70b-instruct",
                       "mistralai/mistral-7b-instruct"],
        "azure":      [],
    }
    fallback = FALLBACK.get(provider, [])

    def _resp(all_models: list) -> dict:
        # Ensure active model present; merge pinned into catalog if custom
        merged = list(all_models)
        for m in pinned:
            if m and m not in merged:
                merged.insert(0, m)
        return {"models": merged, "current": current, "pinned": pinned,
                "provider": provider}

    if provider == "anthropic":
        return jsonify(_resp(fallback))

    if provider == "azure":
        return jsonify(_resp([current] if current else []))

    _OAI_BASE = {
        "openai":     None,
        "xai":        "https://api.x.ai/v1",
        "openrouter": "https://openrouter.ai/api/v1",
    }
    if provider in _OAI_BASE and cfg.get("api_key"):
        try:
            import openai as _oai
            kwargs: dict = {"api_key": cfg["api_key"], "timeout": 12.0}
            if _OAI_BASE[provider]:
                kwargs["base_url"] = _OAI_BASE[provider]
            if provider == "openrouter":
                kwargs["default_headers"] = {
                    "HTTP-Referer": "https://dataloom.app",
                    "X-Title":      "Dataloom",
                }
            raw = list(_oai.OpenAI(**kwargs).models.list())
            if provider == "openai":
                CHAT_PREFIXES = ("gpt-", "o1", "o3", "o4", "chatgpt-")
                names = sorted(
                    [m.id for m in raw if any(m.id.startswith(p) for p in CHAT_PREFIXES)],
                    reverse=True)
            else:
                names = sorted([m.id for m in raw], reverse=True)
            return jsonify(_resp(names or fallback))
        except Exception:
            pass

    if provider == "gemini" and cfg.get("api_key"):
        try:
            import urllib.request as _req
            url = (f"https://generativelanguage.googleapis.com/v1beta/models"
                   f"?key={cfg['api_key']}")
            with _req.urlopen(url, timeout=12) as r:
                body = _json.loads(r.read())
            names = sorted([
                m["name"].replace("models/", "")
                for m in body.get("models", [])
                if any(method in m.get("supportedGenerationMethods", [])
                       for method in ("generateContent", "streamGenerateContent"))
            ], reverse=True)
            return jsonify(_resp(names or fallback))
        except Exception:
            pass

    if provider == "ollama":
        try:
            import urllib.request as _req2
            host2 = cfg.get("host", "http://localhost:11434").rstrip("/")
            with _req2.urlopen(f"{host2}/api/tags", timeout=10) as r:
                body2 = _json.loads(r.read())
            names = [m.get("name", "") for m in body2.get("models", []) if m.get("name")]
            return jsonify(_resp(names))
        except Exception as exc:
            return jsonify(_resp([]) | {"error": f"Could not reach Ollama: {exc}"})

    return jsonify(_resp(fallback))


@app.route("/api/models/pin", methods=["POST"])
def pin_model():
    """Add a model to the session's pinned list.

    Args (JSON body):
        model: The model identifier string to pin.

    Returns:
        JSON ``{ pinned: [str] }`` reflecting the updated list.
    """
    data  = request.json or {}
    model = data.get("model", "").strip()
    if not model:
        return jsonify({"error": "model required"}), 400
    sess   = _get_session()
    cfg    = sess["model_config"]
    pinned = cfg.setdefault("pinned_models", [cfg.get("model", "")])
    if model not in pinned:
        pinned.append(model)
    return jsonify({"pinned": pinned})


@app.route("/api/models/unpin", methods=["POST"])
def unpin_model():
    """Remove a model from the session's pinned list.

    The active model cannot be unpinned; attempting to do so returns HTTP 400.

    Args (JSON body):
        model: The model identifier string to unpin.

    Returns:
        JSON ``{ pinned: [str] }`` reflecting the updated list, or
        ``{ error: str }`` with HTTP 400 if the model is currently active.
    """
    data  = request.json or {}
    model = data.get("model", "").strip()
    if not model:
        return jsonify({"error": "model required"}), 400
    sess    = _get_session()
    cfg     = sess["model_config"]
    current = cfg.get("model")
    if model == current:
        return jsonify({"error": "Cannot unpin the active model"}), 400
    cfg["pinned_models"] = [m for m in cfg.get("pinned_models", []) if m != model]
    return jsonify({"pinned": cfg["pinned_models"]})


@app.route("/api/schema/descriptions", methods=["GET", "POST"])
def schema_descriptions():
    """Read or write the human-authored column description file.

    ``GET`` returns the current ``schema_descriptions.json`` contents.
    ``POST`` saves new descriptions and immediately hot-reloads the schema
    for the active session context so the LLM sees the updated hints without
    requiring a reconnect.

    Returns:
        GET: JSON object (empty ``{}`` when the file does not exist).
        POST: ``{ ok: true }`` on success, or ``{ error: str }`` with HTTP 500
        on write or schema-reload failure.
    """
    path = os.path.join(os.path.dirname(__file__), "schema_descriptions.json")

    if request.method == "GET":
        try:
            with open(path) as f:
                import json as _json
                return jsonify(_json.load(f))
        except FileNotFoundError:
            return jsonify({})

    # POST — save and reload
    data = request.json or {}
    try:
        with open(path, "w") as f:
            import json as _json
            _json.dump(data, f, indent=2)
    except Exception as e:
        return jsonify({"error": f"Could not save: {e}"}), 500

    # Hot-reload schema for the active session context
    ctx = _get_active_context()
    if ctx:
        try:
            schema_text, schema_map, schema_types, join_paths = get_schema(ctx["conn"], ctx["db_type"])
            init_join_paths(join_paths)
            ctx["schema_text"]  = schema_text
            ctx["schema_map"]   = schema_map
            ctx["schema_types"] = schema_types
            ctx["join_paths"]   = join_paths
        except Exception as e:
            return jsonify({"error": f"Schema reload failed: {e}"}), 500

    return jsonify({"ok": True})


@app.route("/api/model", methods=["POST"])
def set_model():
    """Switch the active model for this session. Auto-pins the model if not already pinned."""
    data  = request.json or {}
    model = data.get("model", "").strip()
    if not model:
        return jsonify({"error": "No model specified"}), 400
    sess   = _get_session()
    cfg    = sess["model_config"]
    cfg["model"] = model
    pinned = cfg.setdefault("pinned_models", [])
    if model not in pinned:
        pinned.append(model)
    return jsonify({"model": model})


# ── Entry point ───────────────────────────────────────────────────

if __name__ == "__main__":
    print("✓ Dataloom — open http://localhost:5000")
    app.run(debug=False, port=5000, host="127.0.0.1")
