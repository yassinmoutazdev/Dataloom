"""
app.py — Flask web server for Dataloom v2.10

Session architecture:
  Each browser session maintains its own database contexts.
  Switching databases parks the current context and loads the target,
  keeping query history isolated per database.

  _session_store[sid] = {
      "last_seen":    float,
      "model_config": dict,
      "active_db":    str | None,
      "contexts": {
          "<db_name>": {
              "conn":        connection,
              "db_type":     str,
              "schema_text": str,
              "schema_map":  dict,
              "schema_types":dict,
              "join_paths":  dict,
              "memory":      IntentMemory,
              "history":     list,
              "credentials": dict,
          }
      }
  }
"""

import os
import re
import sys
import uuid
import time
from flask import Flask, request, jsonify, send_from_directory, send_file, session, redirect, url_for
from dotenv import load_dotenv

load_dotenv()

# ── Railway DATABASE_URL support ──────────────────────────────────
# Railway injects DATABASE_URL automatically. Parse it into individual
# env vars so the rest of the app works without changes.
_db_url = os.getenv("DATABASE_URL", "")
if _db_url and _db_url.startswith("postgres"):
    import re as _re
    _m = _re.match(
        r"postgres(?:ql)?://([^:]+):([^@]+)@([^:]+):([\d]+)/(.+)", _db_url
    )
    if _m:
        os.environ.setdefault("DB_TYPE",     "postgresql")
        os.environ.setdefault("DB_USER",     _m.group(1))
        os.environ.setdefault("DB_PASSWORD", _m.group(2))
        os.environ.setdefault("DB_HOST",     _m.group(3))
        os.environ.setdefault("DB_PORT",     _m.group(4))
        os.environ.setdefault("DB_NAME",     _m.group(5))

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
    HAS_UTILS = False

app = Flask(__name__, static_folder="static", template_folder="templates")
app.secret_key = os.getenv("FLASK_SECRET", os.urandom(24).hex())

# ── Security config ───────────────────────────────────────────────
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024   # 16 KB max request body

# DEMO_MODE: set DEMO_MODE=true in .env to lock down the setup route
# for public deployments — users can query but cannot reconfigure the DB.
DEMO_MODE = os.getenv("DEMO_MODE", "false").lower() in ("true", "1", "yes")

# Rate limiting: max queries per session per hour
RATE_LIMIT          = int(os.getenv("RATE_LIMIT", "20"))
RATE_LIMIT_WINDOW   = 3600   # seconds

SESSION_TTL_SECONDS = 3600
_session_store: dict[str, dict] = {}


# ── Model config ──────────────────────────────────────────────────

def _default_model_config() -> dict:
    provider = os.getenv("MODEL_PROVIDER", "ollama").lower()
    if provider == "openai":
        return {
            "provider": "openai",
            "model":    os.getenv("OPENAI_MODEL", "gpt-4o"),
            "api_key":  os.getenv("OPENAI_API_KEY", ""),
        }
    if provider == "gemini":
        return {
            "provider": "gemini",
            "model":    os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite-preview-06-17"),
            "api_key":  os.getenv("GEMINI_API_KEY", ""),
        }
    return {"provider": "ollama", "model": os.getenv("OLLAMA_MODEL", "mistral")}


# ── Session helpers ───────────────────────────────────────────────

def _purge_stale_sessions():
    cutoff = time.time() - SESSION_TTL_SECONDS
    stale  = [sid for sid, s in _session_store.items() if s["last_seen"] < cutoff]
    for sid in stale:
        for ctx in _session_store[sid].get("contexts", {}).values():
            try:
                ctx["conn"].close()
            except Exception:
                pass
        del _session_store[sid]


def _get_session() -> dict:
    _purge_stale_sessions()
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
    sess   = _get_session()
    active = sess.get("active_db")
    if not active:
        return None
    return sess["contexts"].get(active)


def _clean_db_name(raw: str) -> str:
    """'olist_logistics' → 'Olist Logistics'. Overridable via DB_DISPLAY_NAME env var."""
    override = os.getenv("DB_DISPLAY_NAME", "").strip()
    if override:
        return override
    return re.sub(r"[_-]+", " ", raw).title()


def _build_context(conn, db_type: str, credentials: dict) -> dict:
    schema_text, schema_map, schema_types, join_paths = get_schema(conn, db_type)
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
    sess   = _get_session()
    active = sess.get("active_db")
    return bool(active and active in sess["contexts"])


# ── Page routes ───────────────────────────────────────────────────

def _try_auto_connect():
    """
    In DEMO_MODE (or when DATABASE_URL is set), auto-connect the session
    to the pre-configured database without going through the setup wizard.
    Called on every request to / before the session is ready.
    """
    db_name = os.getenv("DB_NAME")
    db_type = os.getenv("DB_TYPE", "postgresql")
    host     = os.getenv("DB_HOST", "localhost")
    port     = str(os.getenv("DB_PORT", "5432"))
    user     = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "")

    if not db_name:
        return False   # No DB configured — fall through to setup wizard

    try:
        sess = _get_session()
        if db_name in sess["contexts"]:
            sess["active_db"] = db_name
            return True   # Already connected in this session

        credentials = {
            "db_type": db_type, "host": host, "port": port,
            "user": user, "password": password, "dbname": db_name,
        }
        conn = connect_with_credentials(db_type, host, port, db_name, user, password)
        ctx  = _build_context(conn, db_type, credentials)
        sess["contexts"][db_name] = ctx
        sess["active_db"] = db_name
        # Restore any persisted history
        sid = session.get("sid")
        if sid:
            import history_store
            history_store.restore_into_session(sid, sess["contexts"])
        return True
    except Exception:
        return False


@app.route("/")
def index():
    if not _session_is_ready():
        # Try auto-connect from environment (Railway / DEMO_MODE)
        if not _try_auto_connect():
            return redirect(url_for("setup"))
    return send_from_directory("templates", "index.html")


@app.route("/setup")
def setup():
    return send_from_directory("templates", "setup.html")


# ── Setup API ─────────────────────────────────────────────────────

@app.route("/api/setup/status")
def setup_status():
    """Return saved server credentials so the wizard can pre-fill on revisit."""
    creds = load_saved_credentials()
    return jsonify({"has_saved_credentials": bool(creds), "credentials": creds})


@app.route("/api/setup/discover", methods=["POST"])
def setup_discover():
    """Given server credentials, return list of accessible databases."""
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
    if DEMO_MODE:
        return jsonify({
            "error": "Setup is disabled in demo mode. The database is pre-configured."
        }), 403
    """Connect to a specific database, load schema, set as active context."""
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


# ── Database management API ───────────────────────────────────────

@app.route("/api/databases")
def list_databases():
    """List all databases the user has connected to in this session."""
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
    """Switch the active database. Chat display clears; history is preserved per DB."""
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
    """Export query results as CSV or Excel."""
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
    ctx = _get_active_context()
    if not ctx:
        # Silent auto-reconnect — handles Railway session expiry without page refresh
        _try_auto_connect()
        ctx = _get_active_context()
    if not ctx:
        return jsonify({"error": "No database connected. Please complete setup."}), 400

    data     = request.json or {}
    question = (data.get("question") or "").strip()
    if not question:
        return jsonify({"error": "No question provided"}), 400

    sess = _get_session()

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

    if result.get("rows") and result.get("headers"):
        result["records"]    = [dict(zip(result["headers"], row)) for row in result["rows"]]
        result["total_rows"] = len(result["rows"])
    else:
        result["records"]    = []
        result["total_rows"] = 0

    result.pop("rows", None)
    return jsonify(result)


# ── Schema, history, model ────────────────────────────────────────

@app.route("/api/schema")
def schema_route():
    ctx = _get_active_context()
    if not ctx:
        return jsonify({"error": "No database connected"}), 400
    return jsonify({
        "text":   ctx["schema_text"],
        "tables": {t: cols for t, cols in ctx["schema_map"].items()},
    })


@app.route("/api/history")
@app.route("/api/history/<db_name>")
def history(db_name: str = None):
    """Return query history — merges in-memory and disk-backed store."""
    sess   = _get_session()
    target = db_name or sess.get("active_db")
    if not target:
        return jsonify([])
    sid = session.get("sid")
    if sid:
        entries = history_store.get(sid, target, limit=50)
    elif target in sess["contexts"]:
        entries = list(reversed(sess["contexts"][target]["history"][-50:]))
    else:
        entries = []
    return jsonify(entries)


@app.route("/api/clear", methods=["POST"])
def clear_memory():
    """Clear conversation memory for the active database (history log preserved)."""
    ctx = _get_active_context()
    if ctx:
        ctx["memory"].clear()
    return jsonify({"ok": True})


@app.route("/api/history/clear", methods=["POST"])
def clear_history():
    """Wipe query history — both in-memory and on disk — for the active DB."""
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
    sess     = _get_session()
    provider = sess["model_config"].get("provider", "ollama")
    if provider != "ollama":
        current = sess["model_config"].get("model")
        return jsonify({"models": [current], "current": current})
    try:
        import ollama as _ollama
        response = _ollama.list()
        if hasattr(response, "models"):
            names = [m.model for m in response.models]
        elif isinstance(response, dict):
            names = [m.get("name") or m.get("model", "") for m in response.get("models", [])]
        else:
            names = []
        return jsonify({"models": names, "current": sess["model_config"].get("model")})
    except Exception:
        return jsonify({"models": [], "error": "Could not reach Ollama"}), 500


@app.route("/api/schema/descriptions", methods=["GET", "POST"])
def schema_descriptions():
    """
    GET  — return the current schema_descriptions.json.
    POST — save new descriptions and hot-reload schema for the active context.
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
    """Switch the active model for this session."""
    data  = request.json or {}
    model = data.get("model", "").strip()
    if not model:
        return jsonify({"error": "No model specified"}), 400
    sess = _get_session()
    sess["model_config"]["model"] = model
    return jsonify({"model": model})


# ── Entry point ───────────────────────────────────────────────────

if __name__ == "__main__":
    print("✓ Dataloom — open http://localhost:5000")
    app.run(debug=False, port=5000, host="127.0.0.1")
