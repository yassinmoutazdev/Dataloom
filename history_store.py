"""
history_store.py — Dataloom v3.0  (Milestone 1.4)

Persists query history to disk, keyed by session_id.
Each session gets its own JSON file under query_history/.

File layout:
    query_history/
        <session_id>.json        ← { "<db_name>": [ entry, ... ], ... }

Entry schema:
    {
        "question":   str,
        "sql":        str | None,
        "table":      str | None,
        "confidence": str,          # "high" | "medium" | "low"
        "ts":         float,        # Unix timestamp
    }

AUTH MIGRATION NOTE (Milestone 1.6):
    When authentication is added, replace session_id with user_id everywhere
    in this file. The rest of the codebase requires zero changes.
    Search for: "# AUTH: swap session_id → user_id here"
"""

import json
import os
import time
from pathlib import Path

HISTORY_DIR  = Path("query_history")
MAX_ENTRIES  = 200   # per db per session


def _path(session_id: str) -> Path:
    HISTORY_DIR.mkdir(exist_ok=True)
    # AUTH: swap session_id → user_id here
    safe = session_id.replace("/", "_").replace("\\", "_")
    return HISTORY_DIR / f"{safe}.json"


def load(session_id: str) -> dict:
    """
    Load persisted history for a session.
    Returns { db_name: [entry, ...], ... }
    Returns empty dict if no file exists yet.
    """
    p = _path(session_id)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def append(session_id: str, db_name: str, entry: dict) -> None:
    """
    Append one query entry to the store for (session_id, db_name).
    Silently drops malformed entries. Trims to MAX_ENTRIES.
    """
    if not session_id or not db_name:
        return

    data = load(session_id)
    db_history = data.get(db_name, [])

    db_history.append({
        "question":   str(entry.get("question") or ""),
        "sql":        entry.get("sql") or None,
        "table":      entry.get("table") or None,
        "confidence": str(entry.get("confidence") or "high"),
        "ts":         entry.get("ts") or time.time(),
    })

    # Trim to cap
    if len(db_history) > MAX_ENTRIES:
        db_history = db_history[-MAX_ENTRIES:]

    data[db_name] = db_history

    p = _path(session_id)
    try:
        p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        pass   # Non-fatal — in-memory history still works


def get(session_id: str, db_name: str, limit: int = 50) -> list:
    """
    Return the last `limit` entries for (session_id, db_name), newest first.
    """
    data = load(session_id)
    entries = data.get(db_name, [])
    return list(reversed(entries[-limit:]))


def clear(session_id: str, db_name: str | None = None) -> None:
    """
    Clear history.
    - If db_name given: wipe only that database's history.
    - If db_name is None: wipe all history for this session (delete file).
    """
    p = _path(session_id)
    if db_name is None:
        if p.exists():
            p.unlink()
        return

    data = load(session_id)
    if db_name in data:
        data[db_name] = []
        try:
            p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError:
            pass


def restore_into_session(session_id: str, contexts: dict) -> None:
    """
    Called on server startup / session reconnect.
    Loads persisted history back into the in-memory contexts dict
    for all databases that are already connected.

    contexts = sess["contexts"]  — mutated in place.
    """
    data = load(session_id)
    for db_name, entries in data.items():
        if db_name in contexts:
            # Merge: keep any in-memory entries already there, then append disk entries
            existing_ts = {e["ts"] for e in contexts[db_name].get("history", [])}
            new_entries = [e for e in entries if e["ts"] not in existing_ts]
            contexts[db_name]["history"] = (
                contexts[db_name].get("history", []) + new_entries
            )
