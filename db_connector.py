
DB_DISPLAY_NAMES = {
    "postgresql": "PostgreSQL",
    "mysql":      "MySQL / MariaDB",
    "sqlite":     "SQLite",
}

DB_DEFAULT_PORTS = {
    "postgresql": "5432",
    "mysql":      "3306",
}


def _input_field(label: str, default: str = "", secret: bool = False) -> str:
    """Prompt for a single field with an optional default."""
    import getpass
    if default:
        prompt = f"  {label} [{default}]: "
    else:
        prompt = f"  {label}: "
    if secret:
        val = getpass.getpass(prompt)
    else:
        val = input(prompt).strip()
    return val if val else default


def _save_to_env(values: dict):
    """Write connection details to .env file silently."""
    env_path = ".env"
    # Read existing .env if present
    existing = {}
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    existing[k.strip()] = v.strip()
    # Merge new values
    existing.update(values)
    # Write back
    with open(env_path, "w") as f:
        f.write("# DB Assistant configuration\n")
        for k, v in existing.items():
            f.write(f"{k}={v}\n")


def is_configured() -> bool:
    """Check if .env has enough config to attempt a connection."""
    load_dotenv()
    db_type = os.getenv("DB_TYPE", "").lower()
    if db_type == "sqlite":
        return bool(os.getenv("DB_PATH"))
    elif db_type in ("postgresql", "mysql"):
        return all([
            os.getenv("DB_NAME"),
            os.getenv("DB_USER"),
            os.getenv("DB_HOST"),
        ])
    return False


def connection_wizard() -> tuple:
    """
    Interactive connection form shown on first run or failed connection.
    Returns (conn, db_type).
    """
    print()
    print("  ┌─────────────────────────────────────────┐")
    print("  │         Database Connection Setup        │")
    print("  └─────────────────────────────────────────┘")
    print()
    print("  Select your database:")
    db_options = list(DB_DISPLAY_NAMES.items())
    for i, (key, name) in enumerate(db_options, 1):
        print(f"    {i}. {name}")
    print()

    while True:
        choice = input("  Enter number: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(db_options):
            db_type = db_options[int(choice) - 1][0]
            break
        print("  Invalid choice, try again.")

    print()
    print(f"  ── {DB_DISPLAY_NAMES[db_type]} connection details ──")
    print()

    env_values = {"DB_TYPE": db_type}

    if db_type == "sqlite":
        path = _input_field("Database file path", default="./database.sqlite")
        env_values["DB_PATH"] = path

    else:
        default_port = DB_DEFAULT_PORTS[db_type]
        host     = _input_field("Host", default="localhost")
        port     = _input_field("Port", default=default_port)
        dbname   = _input_field("Database name")
        username = _input_field("Username")
        password = _input_field("Password", secret=True)

        env_values.update({
            "DB_HOST":     host,
            "DB_PORT":     port,
            "DB_NAME":     dbname,
            "DB_USER":     username,
            "DB_PASSWORD": password,
        })

    # Test connection
    print()
    print("  Testing connection...", end="", flush=True)
    try:
        # Temporarily set env vars for this attempt
        for k, v in env_values.items():
            os.environ[k] = v

        conn, db_type_out = connect(db_type)
        print(" ✓ Connected!")
        print()

        # Ask to save
        save = input("  Save connection for next time? (Y/n): ").strip().lower()
        if save != "n":
            _save_to_env(env_values)
            print("  Connection saved.")

        print()
        return conn, db_type_out

    except Exception as e:
        print(f" ✗ Failed: {e}")
        print()
        retry = input("  Try again? (Y/n): ").strip().lower()
        if retry != "n":
            return connection_wizard()
        else:
            raise ConnectionError("Could not connect to database.") from e

import os
from dotenv import load_dotenv

load_dotenv()

SUPPORTED_TYPES = ("postgresql", "mysql", "sqlite")


def get_db_type() -> str:
    db_type = os.getenv("DB_TYPE", "postgresql").lower().strip()
    if db_type not in SUPPORTED_TYPES:
        raise ValueError(f"Unsupported DB_TYPE '{db_type}'. Choose from: {SUPPORTED_TYPES}")
    return db_type


def connect(db_type: str = None):
    """
    Returns (connection, db_type) for the configured database.
    Connection is always read-only where supported.
    """
    db_type = db_type or get_db_type()

    if db_type == "postgresql":
        return _connect_postgresql(), "postgresql"

    elif db_type == "mysql":
        return _connect_mysql(), "mysql"

    elif db_type == "sqlite":
        return _connect_sqlite(), "sqlite"


def _connect_postgresql():
    try:
        import psycopg2
    except ImportError:
        raise ImportError("psycopg2-binary is required for PostgreSQL. Run: pip install psycopg2-binary")

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    conn.set_session(readonly=True)
    return conn


def _connect_mysql():
    try:
        import mysql.connector
    except ImportError:
        raise ImportError("mysql-connector-python is required for MySQL. Run: pip install mysql-connector-python")

    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    return conn


def _connect_sqlite():
    import sqlite3
    db_path = os.getenv("DB_PATH")
    if not db_path:
        raise ValueError("DB_PATH must be set in .env for SQLite (e.g. DB_PATH=./mydb.sqlite)")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def reconnect(credentials: dict):
    """
    Re-establish a dropped connection using stored credentials.
    credentials = { db_type, host, port, user, password, dbname }
    Returns a fresh connection object, or raises on failure.
    Called by core.py when OperationalError is caught mid-query.
    """
    db_type = credentials.get("db_type", "postgresql").lower()

    if db_type == "sqlite":
        import sqlite3
        path = credentials.get("dbname", "")
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        return conn

    host     = credentials.get("host", "localhost")
    port     = str(credentials.get("port", "5432"))
    dbname   = credentials.get("dbname", "")
    user     = credentials.get("user", "")
    password = credentials.get("password", "")

    return connect_with_credentials(db_type, host, port, dbname, user, password)


def run_query(conn, sql: str, db_type: str, params: list = None) -> tuple[list, list]:
    """
    Execute a SELECT query and return (headers, rows).
    Uses parameterized queries to prevent SQL injection.
    Returns ([], []) if the query produces no column metadata (e.g. non-SELECT).
    """
    cursor = conn.cursor()
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)

    if not cursor.description:
        return [], []

    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]

    if db_type == "sqlite":
        rows = [tuple(row) for row in rows]

    return headers, rows


def rollback(conn, db_type: str):
    """Safe rollback — SQLite handles transactions differently."""
    try:
        if db_type != "sqlite":
            conn.rollback()
    except Exception:
        pass


# ── Web UI connection helpers (no .env required) ──────────────────

SYSTEM_DATABASES = {
    "postgresql": {"postgres", "template0", "template1", "rdsadmin"},
    "mysql":      {"information_schema", "mysql", "performance_schema", "sys"},
    "sqlite":     set(),
}


def connect_with_credentials(db_type: str, host: str, port: str,
                              dbname: str, user: str, password: str):
    """
    Connect using explicit credentials (not from .env).
    Used by the web setup wizard. Returns a raw connection.
    """
    db_type = db_type.lower()
    if db_type == "postgresql":
        try:
            import psycopg2
        except ImportError:
            raise ImportError("psycopg2-binary is required. Run: pip install psycopg2-binary")
        conn = psycopg2.connect(
            host=host, port=port, database=dbname,
            user=user, password=password, connect_timeout=10,
        )
        conn.set_session(readonly=True)
        return conn

    elif db_type == "mysql":
        try:
            import mysql.connector
        except ImportError:
            raise ImportError("mysql-connector-python is required. Run: pip install mysql-connector-python")
        return mysql.connector.connect(
            host=host, port=int(port), database=dbname,
            user=user, password=password, connection_timeout=10,
        )

    elif db_type == "sqlite":
        import sqlite3
        conn = sqlite3.connect(dbname)
        conn.row_factory = sqlite3.Row
        return conn

    else:
        raise ValueError(f"Unsupported db_type: {db_type}")


def discover_databases(db_type: str, host: str, port: str,
                       user: str, password: str) -> list[str]:
    """
    Connect to the database server and return a list of user-accessible
    databases, filtering out known system databases.
    """
    db_type = db_type.lower()
    system_dbs = SYSTEM_DATABASES.get(db_type, set())

    if db_type == "postgresql":
        conn = connect_with_credentials("postgresql", host, port, "postgres", user, password)
        cur = conn.cursor()
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
        names = [row[0] for row in cur.fetchall()]
        conn.close()
        return [n for n in names if n not in system_dbs]

    elif db_type == "mysql":
        conn = connect_with_credentials("mysql", host, port, "information_schema", user, password)
        cur = conn.cursor()
        cur.execute("SHOW DATABASES")
        names = [row[0] for row in cur.fetchall()]
        conn.close()
        return [n for n in names if n not in system_dbs]

    elif db_type == "sqlite":
        # SQLite has no concept of multiple databases — return the file path itself
        return ["(current file)"]

    else:
        raise ValueError(f"Unsupported db_type: {db_type}")


def save_credentials(db_type: str, host: str = "", port: str = "",
                     user: str = "", password: str = "", path: str = ""):
    """
    Persist server credentials (not the database name) to .env so
    the setup wizard can pre-fill fields on next launch.
    Database selection always happens in the UI.
    """
    values = {"DB_TYPE": db_type}
    if db_type == "sqlite":
        values["DB_PATH"] = path
    else:
        values.update({
            "DB_HOST": host,
            "DB_PORT": port,
            "DB_USER": user,
            "DB_PASSWORD": password,
        })
    _save_to_env(values)


def load_saved_credentials() -> dict:
    """
    Return server credentials from .env for pre-filling the setup wizard.
    Returns empty dict if nothing is saved.
    """
    load_dotenv(override=True)
    db_type = os.getenv("DB_TYPE", "").lower()
    if not db_type:
        return {}
    creds = {"db_type": db_type}
    if db_type == "sqlite":
        creds["path"] = os.getenv("DB_PATH", "")
    else:
        creds["host"]     = os.getenv("DB_HOST", "localhost")
        creds["port"]     = os.getenv("DB_PORT", "5432" if db_type == "postgresql" else "3306")
        creds["user"]     = os.getenv("DB_USER", "")
        creds["password"] = os.getenv("DB_PASSWORD", "")
    return creds
