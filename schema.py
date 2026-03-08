import json
import os

DESCRIPTIONS_FILE = "schema_descriptions.json"


def get_schema(conn, db_type: str = "postgresql") -> tuple[str, dict, dict, dict]:
    """
    Returns:
      schema_text:  human-readable schema for LLM context
      schema_map:   { table: [col1, col2, ...] }
      schema_types: { table: { col: data_type } }
      join_paths:   { dim_table: "fact.col = dim.col" } auto-discovered from FK constraints
    """
    descriptions = {}
    if os.path.exists(DESCRIPTIONS_FILE):
        with open(DESCRIPTIONS_FILE, "r") as f:
            descriptions = json.load(f)

    if db_type == "sqlite":
        return _get_schema_sqlite(conn, descriptions)
    elif db_type == "mysql":
        return _get_schema_mysql(conn, descriptions)
    else:
        return _get_schema_postgresql(conn, descriptions)


def _get_schema_postgresql(conn, descriptions: dict) -> tuple[str, dict, dict]:
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


def _get_schema_mysql(conn, descriptions: dict) -> tuple[str, dict, dict]:
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


def _get_schema_sqlite(conn, descriptions: dict) -> tuple[str, dict, dict]:
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
