"""
Persistent query history storage for Dataloom v3.0 (Milestone 1.4).

Owns disk-backed history for all sessions. Each session's history is
stored as a single JSON file under ``query_history/``, keyed by
``session_id``. The file schema is intentional: one flat file per session
makes atomic reads and writes straightforward without a database.

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

Public API: load, append, get, clear, restore_into_session
"""

import json
import os
import time
from pathlib import Path

# Root directory for all per-session history files.
HISTORY_DIR  = Path("query_history")

# Per-database, per-session cap; oldest entries are trimmed when exceeded.
MAX_ENTRIES  = 200


def _path(session_id: str) -> Path:
    """Resolve and return the history file path for a session.

    Creates ``HISTORY_DIR`` on first call if it does not exist. Sanitizes
    ``session_id`` so it is safe to use as a filename on all platforms.

    Args:
        session_id: Opaque session identifier from the caller.

    Returns:
        ``Path`` object pointing to the session's JSON file.
    """
    HISTORY_DIR.mkdir(exist_ok=True)
    # AUTH: swap session_id → user_id here
    safe = session_id.replace("/", "_").replace("\\", "_")
    return HISTORY_DIR / f"{safe}.json"


def load(session_id: str) -> dict:
    """Load persisted history for a session from disk.

    Args:
        session_id: Opaque session identifier.

    Returns:
        Mapping of ``{db_name: [entry, ...], ...}``. Returns an empty dict
        if no file exists yet or the file cannot be parsed.
    """
    p = _path(session_id)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def append(session_id: str, db_name: str, entry: dict) -> None:
    """Append one query entry to the store for (session_id, db_name).

    Silently drops the entry if ``session_id`` or ``db_name`` is falsy.
    Normalises all fields to their expected types before writing so that
    partially populated entries from callers do not corrupt the file.
    Trims the per-database list to ``MAX_ENTRIES`` (oldest first) after
    each append.

    Write failures are swallowed — in-memory history continues to work
    even when the disk is unavailable.

    Args:
        session_id: Opaque session identifier.
        db_name: Database connection name the query was run against.
        entry: Dict with any subset of keys: ``question``, ``sql``,
            ``table``, ``confidence``, ``ts``.
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

    if len(db_history) > MAX_ENTRIES:
        db_history = db_history[-MAX_ENTRIES:]

    data[db_name] = db_history

    p = _path(session_id)
    try:
        p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        # Non-fatal — in-memory history still works
        pass


def get(session_id: str, db_name: str, limit: int = 50) -> list:
    """Return the most recent entries for (session_id, db_name), newest first.

    Args:
        session_id: Opaque session identifier.
        db_name: Database connection name to retrieve history for.
        limit: Maximum number of entries to return. Defaults to 50.

    Returns:
        List of entry dicts ordered newest-first. Returns an empty list if
        no history exists for the given pair.
    """
    data = load(session_id)
    entries = data.get(db_name, [])
    return list(reversed(entries[-limit:]))


def clear(session_id: str, db_name: str | None = None) -> None:
    """Clear stored history for a session, in whole or per database.

    Args:
        session_id: Opaque session identifier.
        db_name: If provided, only that database's history is cleared and
            the file is rewritten. If ``None``, the entire session file is
            deleted.
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
    """Reload persisted history into the live in-memory session contexts.

    Called on server startup or session reconnect to bridge the gap
    between the disk store and the in-memory ``contexts`` dict. Only
    databases already present in ``contexts`` are hydrated — databases
    that have been disconnected since the last write are skipped.

    Deduplication is done by timestamp: entries already present in memory
    (matched by ``ts``) are not duplicated.

    Args:
        session_id: Opaque session identifier.
        contexts: The ``sess["contexts"]`` mapping for this session.
            Mutated in place: each matching database's ``"history"`` list
            gains the entries that exist on disk but not in memory.
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
