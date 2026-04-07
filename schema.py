"""
Database schema extraction and formatting utilities.

This module provides functions to connect to various database types (PostgreSQL, MySQL, SQLite)
and retrieve their schema information, including tables, columns, data types, and foreign key relationships.
It also supports adding custom descriptions for tables and columns, storing them in `schema_descriptions.json`.
"""
import json
import os

DESCRIPTIONS_FILE = "schema_descriptions.json"

# ── OPT 1: In-memory schema cache ────────────────────────────────────────────
# Schema reads are cheap but not free — each one fires several queries against
# information_schema / PRAGMA. On Railway (or any container host), sessions
# are frequently torn down and recreated. Without caching, every new session
# re-reads a schema that hasn't changed.
#
# Cache key: (db_type, host_or_path, port, dbname)
# Cache value: (schema_text, schema_map, schema_types, join_paths)
#
# Invalidation: call invalidate_schema_cache(key) when a connection error
# is detected in core.py. The next get_schema() call will rebuild from DB.
# Risk: stale cache if schema changes (ALTER TABLE, etc.) mid-session.
# Mitigation: the /api/schema/descriptions POST route already calls get_schema()
# directly; operators can also restart the process to force a full rebuild.
_SCHEMA_CACHE: dict[tuple, tuple] = {}


def _make_cache_key(conn, db_type: str) -> tuple | None:
    """Derive a stable cache key from the live connection object.

    Returns None if the key cannot be determined (safe fallback: skip cache).
    """
    try:
        if db_type == "postgresql":
            info = conn.info
            return (db_type, info.host or "", str(info.port or ""), info.dbname or "")
        elif db_type == "mysql":
            return (db_type, conn.server_host or "", str(conn.server_port or ""), conn.database or "")
        elif db_type == "sqlite":
            row = conn.execute("PRAGMA database_list").fetchone()
            # row = (seq, name, file) — file is the path, or "" for :memory:
            path = row[2] if row else ""
            return (db_type, path, "", "")
    except Exception:
        return None


def invalidate_schema_cache(conn, db_type: str) -> None:
    """Remove a cached schema entry when a connection error is detected.

    Called by core.py after a connection failure so the next session
    gets a fresh schema read rather than potentially stale cached data.
    """
    key = _make_cache_key(conn, db_type)
    if key:
        _SCHEMA_CACHE.pop(key, None)


def get_schema(conn, db_type: str = "postgresql") -> tuple[str, dict, dict, dict]:
    """Retrieve database schema information for a given connection and database type.

    Results are cached in-memory by (db_type, host, port, dbname) so that
    container restarts and session expiry do not trigger repeated schema reads
    against an unchanged database.

    Args:
        conn: The database connection object (e.g., psycopg2, mysql.connector, sqlite3 connection).
        db_type: The type of the database. Supported types are "postgresql", "mysql", and "sqlite".
                 Defaults to "postgresql".

    Returns:
        tuple: A tuple containing:
            - schema_text (str): A human-readable schema string, formatted for LLM context.
            - schema_map (dict): A dictionary where keys are table names and values are lists of column names.
            - schema_types (dict): A dictionary mapping table names to another dictionary, which maps
              column names to their data types.
            - join_paths (dict): A dictionary representing foreign key relationships, structured as
              {source_table: {target_table: "source_table.col = target_table.col"}}.
    """
    cache_key = _make_cache_key(conn, db_type)
    if cache_key and cache_key in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[cache_key]

    descriptions = {}
    if os.path.exists(DESCRIPTIONS_FILE):
        with open(DESCRIPTIONS_FILE, "r") as f:
            descriptions = json.load(f)

    if db_type == "sqlite":
        result = _get_schema_sqlite(conn, descriptions)
    elif db_type == "mysql":
        result = _get_schema_mysql(conn, descriptions)
    else:
        result = _get_schema_postgresql(conn, descriptions)

    if cache_key:
        _SCHEMA_CACHE[cache_key] = result

    return result


def _get_schema_postgresql(conn, descriptions: dict) -> tuple[str, dict, dict, dict]:
    """Extract schema information specifically for a PostgreSQL database.

    Args:
        conn: The PostgreSQL database connection object.
        descriptions: A dictionary of custom table and column descriptions.

    Returns:
        tuple: A tuple containing schema_text, schema_map, schema_types, and join_paths
               specific to the PostgreSQL database.
    """
    cursor = conn.cursor()

    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    tables = [row[0] for row in cursor.fetchall()]

    if not tables:
        return "No tables found.", {}, {}, {}

    schema_map, schema_types, schema_parts, join_paths = {}, {}, [], {}

    for table in tables:
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'public'
            ORDER BY ordinal_position
        """, (table,))
        columns = cursor.fetchall()

        cursor.execute("""
            SELECT kcu.column_name, ccu.table_name, ccu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = %s
        """, (table,))
        foreign_keys = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

        cursor.execute("SELECT reltuples::bigint FROM pg_class WHERE relname = %s", (table,))
        row_count = (cursor.fetchone() or [0])[0]

        schema_map[table]   = [row[0] for row in columns]
        schema_types[table] = {row[0]: row[1] for row in columns}
        schema_parts.append(_format_table(table, columns, foreign_keys, row_count, descriptions))
        # Build adjacency list — store EVERY FK edge in both directions
        # Format: join_paths[table_a][table_b] = "table_a.col = table_b.col"
        for col, (ref_table, ref_col) in foreign_keys.items():
            condition = f"{ref_table}.{ref_col} = {table}.{col}"
            join_paths.setdefault(table, {})[ref_table]     = condition
            join_paths.setdefault(ref_table, {})[table]     = condition

    return "\n\n".join(schema_parts), schema_map, schema_types, join_paths


def _get_schema_mysql(conn, descriptions: dict) -> tuple[str, dict, dict, dict]:
    """Extract schema information specifically for a MySQL database.

    Args:
        conn: The MySQL database connection object.
        descriptions: A dictionary of custom table and column descriptions.

    Returns:
        tuple: A tuple containing schema_text, schema_map, schema_types, and join_paths
               specific to the MySQL database.
    """
    cursor = conn.cursor()
    db_name = conn.database

    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """, (db_name,))
    tables = [row[0] for row in cursor.fetchall()]

    if not tables:
        return "No tables found.", {}, {}, {}

    schema_map, schema_types, schema_parts, join_paths = {}, {}, [], {}

    for table in tables:
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position
        """, (table, db_name))
        columns = cursor.fetchall()

        cursor.execute("""
            SELECT column_name, referenced_table_name, referenced_column_name
            FROM information_schema.key_column_usage
            WHERE table_name = %s AND table_schema = %s
            AND referenced_table_name IS NOT NULL
        """, (table, db_name))
        foreign_keys = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

        cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
        row_count = cursor.fetchone()[0]

        schema_map[table]   = [row[0] for row in columns]
        schema_types[table] = {row[0]: row[1] for row in columns}
        schema_parts.append(_format_table(table, columns, foreign_keys, row_count, descriptions))
        for col, (ref_table, ref_col) in foreign_keys.items():
            condition = f"{ref_table}.{ref_col} = {table}.{col}"
            join_paths.setdefault(table, {})[ref_table] = condition
            join_paths.setdefault(ref_table, {})[table] = condition

    return "\n\n".join(schema_parts), schema_map, schema_types, join_paths


def _get_schema_sqlite(conn, descriptions: dict) -> tuple[str, dict, dict, dict]:
    """Extract schema information specifically for an SQLite database.

    Args:
        conn: The SQLite database connection object.
        descriptions: A dictionary of custom table and column descriptions.

    Returns:
        tuple: A tuple containing schema_text, schema_map, schema_types, and join_paths
               specific to the SQLite database.
    """
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]

    if not tables:
        return "No tables found.", {}, {}, {}

    schema_map, schema_types, schema_parts, join_paths = {}, {}, [], {}

    for table in tables:
        cursor.execute(f"PRAGMA table_info('{table}')")
        pragma_rows = cursor.fetchall()
        # PRAGMA returns: cid, name, type, notnull, dflt_value, pk
        columns = [(row[1], row[2], "NO" if row[3] else "YES") for row in pragma_rows]

        cursor.execute(f"PRAGMA foreign_key_list('{table}')")
        fk_rows = cursor.fetchall()
        foreign_keys = {row[3]: (row[2], row[4]) for row in fk_rows}

        cursor.execute(f"SELECT COUNT(*) FROM '{table}'")
        row_count = cursor.fetchone()[0]

        schema_map[table]   = [row[0] for row in columns]
        schema_types[table] = {row[0]: row[1] for row in columns}
        schema_parts.append(_format_table(table, columns, foreign_keys, row_count, descriptions))
        for col, (ref_table, ref_col) in foreign_keys.items():
            condition = f"{ref_table}.{ref_col} = {table}.{col}"
            join_paths.setdefault(table, {})[ref_table] = condition
            join_paths.setdefault(ref_table, {})[table] = condition

    return "\n\n".join(schema_parts), schema_map, schema_types, join_paths


def _format_table(table, columns, foreign_keys, row_count, descriptions) -> str:
    """Format table and column details into a human-readable string.

    Args:
        table: The name of the table.
        columns: A list of column information (name, type, nullable).
        foreign_keys: A dictionary of foreign key relationships for the table.
        row_count: The number of rows in the table.
        descriptions: A dictionary of custom table and column descriptions.

    Returns:
        A formatted string representation of the table schema, including descriptions and FKs.
    """
    table_desc = descriptions.get(table, {}).get("_description", "")
    header = f"Table: {table} (~{row_count:,} rows)"
    if table_desc:
        header += f"\nDescription: {table_desc}"

    col_lines = []
    for col_name, data_type, nullable in columns:
        col_desc  = descriptions.get(table, {}).get(col_name, "")
        fk_info   = ""
        if col_name in foreign_keys:
            fk_table, fk_col = foreign_keys[col_name]
            fk_info = f" → {fk_table}.{fk_col}"
        null_info = "" if nullable == "YES" else " NOT NULL"
        line = f"  - {col_name} ({data_type}{null_info}){fk_info}"
        if col_desc:
            line += f"  # {col_desc}"
        col_lines.append(line)

    return f"{header}\nColumns:\n" + "\n".join(col_lines)


def save_description(table: str, column: str, description: str):
    """Save a custom description for a table or a specific column to the descriptions file.

    This function updates the `schema_descriptions.json` file. If the column parameter is
    an empty string, the description is applied to the table itself.

    Args:
        table: The name of the table to describe.
        column: The name of the column to describe. Use an empty string for a table description.
        description: The descriptive text to save.

    Side Effects:
        Writes the updated descriptions to the `schema_descriptions.json` file.
    """
    descriptions = {}
    if os.path.exists(DESCRIPTIONS_FILE):
        with open(DESCRIPTIONS_FILE, "r") as f:
            descriptions = json.load(f)

    if table not in descriptions:
        descriptions[table] = {}

    key = column if column else "_description"
    descriptions[table][key] = description

    with open(DESCRIPTIONS_FILE, "w") as f:
        json.dump(descriptions, f, indent=2)
