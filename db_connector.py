"""
Database connection management for DB Assistant.

Provides connection handling for PostgreSQL, MySQL/MariaDB, and SQLite databases.
Includes interactive setup wizard, credential management, and query execution.
All connections are read-only where supported for safety.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Database display names for user-friendly output
DB_DISPLAY_NAMES = {
    "postgresql": "PostgreSQL",
    "mysql":      "MySQL / MariaDB",
    "sqlite":     "SQLite",
}

# Default ports for database types
DB_DEFAULT_PORTS = {
    "postgresql": "5432",
    "mysql":      "3306",
}

# Supported database types
SUPPORTED_TYPES = ("postgresql", "mysql", "sqlite")

# System databases to filter out during discovery
SYSTEM_DATABASES = {
    "postgresql": {"postgres", "template0", "template1", "rdsadmin"},
    "mysql":      {"information_schema", "mysql", "performance_schema", "sys"},
    "sqlite":     set(),
}


def _input_field(label: str, default: str = "", secret: bool = False) -> str:
    """Prompt for a single field with an optional default.
    
    Args:
        label: The field label to display to the user.
        default: Default value if user enters nothing.
        secret: Whether to hide input (for passwords).
    
    Returns:
        User input or default value.
    """
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
    """Write connection details to .env file, preserving existing values.
    
    Args:
        values: Dictionary of key-value pairs to write to .env.
    """
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
    """Check if .env has sufficient configuration to attempt a connection.
    
    Returns:
        True if required connection parameters are present for the configured DB type.
    """
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
    """Interactive connection setup wizard for first run or failed connections.
    
    Guides user through database selection and credential input. Tests connection
    and optionally saves credentials to .env file.
    
    Returns:
        Tuple of (connection_object, database_type_string).
    
    Raises:
        ConnectionError: If connection fails and user chooses not to retry.
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


def get_db_type() -> str:
    """Get the database type from environment configuration.
    
    Returns:
        Database type string (postgresql, mysql, or sqlite).
    
    Raises:
        ValueError: If DB_TYPE is not one of the supported types.
    """
    db_type = os.getenv("DB_TYPE", "postgresql").lower().strip()
    if db_type not in SUPPORTED_TYPES:
        raise ValueError(f"Unsupported DB_TYPE '{db_type}'. Choose from: {SUPPORTED_TYPES}")
    return db_type


def connect(db_type: str | None = None):
    """Establish a read-only connection to the configured database.
    
    Args:
        db_type: Optional database type override. Uses DB_TYPE from env if not provided.
    
    Returns:
        Tuple of (connection_object, database_type_string).

    Raises:
        ValueError: If db_type resolves to an unsupported value.
    """
    db_type = db_type or get_db_type()

    if db_type == "postgresql":
        return _connect_postgresql(), "postgresql"

    elif db_type == "mysql":
        return _connect_mysql(), "mysql"

    elif db_type == "sqlite":
        return _connect_sqlite(), "sqlite"

    else:
        raise ValueError(f"Unsupported db_type: '{db_type}'. Choose from: {SUPPORTED_TYPES}")


def _connect_postgresql():
    """Establish PostgreSQL connection with read-only session.
    
    Returns:
        psycopg2 connection object.
    
    Raises:
        ImportError: If psycopg2-binary is not installed.
    """
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
    """Establish MySQL connection.
    
    Returns:
        mysql.connector connection object.
    
    Raises:
        ImportError: If mysql-connector-python is not installed.
    """
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
    """Establish SQLite connection with row factory for named access.
    
    Returns:
        sqlite3 connection object.
    
    Raises:
        ValueError: If DB_PATH is not set in environment.
    """
    import sqlite3
    db_path = os.getenv("DB_PATH")
    if not db_path:
        raise ValueError("DB_PATH must be set in .env for SQLite (e.g. DB_PATH=./mydb.sqlite)")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def reconnect(credentials: dict):
    """Re-establish a dropped connection using stored credentials.
    
    Used by core.py when OperationalError is caught mid-query.
    
    Args:
        credentials: Dictionary containing connection parameters.
            Expected keys: db_type, host, port, user, password, dbname.
    
    Returns:
        Fresh connection object.

    Raises:
        RuntimeError: If credentials dict is None or empty.
        ValueError: If db_type is not a supported value.
    """
    if not credentials:
        raise RuntimeError("reconnect() called with empty or None credentials")
    db_type = credentials.get("db_type", "postgresql").lower()
    if db_type not in SUPPORTED_TYPES:
        raise ValueError(f"Unsupported db_type in credentials: '{db_type}'")

    if db_type == "sqlite":
        import sqlite3
        path = credentials.get("dbname", "")
        if not path:
            raise ValueError("SQLite reconnect requires 'dbname' (file path) in credentials")
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        return conn

    host     = credentials.get("host", "localhost")
    port     = str(credentials.get("port", "5432"))
    dbname   = credentials.get("dbname", "")
    user     = credentials.get("user", "")
    password = credentials.get("password", "")

    if not dbname:
        raise ValueError("reconnect() requires 'dbname' in credentials")

    return connect_with_credentials(db_type, host, port, dbname, user, password)


def run_query(conn, sql: str, db_type: str, params: list | None = None) -> tuple[list, list]:
    """Execute a SELECT query safely using parameterized queries.
    
    Args:
        conn: Database connection object.
        sql: SQL query string.
        db_type: Database type for type-specific handling.
        params: Optional parameters for parameterized query.
    
    Returns:
        Tuple of (headers_list, rows_list). Returns empty lists for non-SELECT queries.
    """
    if conn is None:
        raise RuntimeError("run_query called with a None connection object")
    if not sql or not sql.strip():
        raise ValueError("run_query called with an empty SQL string")
    cursor = conn.cursor()
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)

    if not cursor.description:
        return [], []

    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]

    # Convert SQLite Row objects to tuples for consistency
    if db_type == "sqlite":
        rows = [tuple(row) for row in rows]

    return headers, rows


def rollback(conn, db_type: str):
    """Safely rollback transaction, handling SQLite's different transaction model.
    
    Args:
        conn: Database connection object.
        db_type: Database type to determine rollback behavior.
    """
    try:
        # SQLite handles transactions differently - don't attempt rollback
        if db_type != "sqlite":
            conn.rollback()
    except Exception:
        # Silent fail on rollback errors
        pass


def connect_with_credentials(db_type: str, host: str, port: str,
                              dbname: str, user: str, password: str):
    """Connect using explicit credentials (not from .env).
    
    Used by the web setup wizard. Includes connection timeouts for better UX.
    
    Args:
        db_type: Database type (postgresql, mysql, sqlite).
        host: Database host address.
        port: Database port.
        dbname: Database name.
        user: Database username.
        password: Database password.
    
    Returns:
        Raw database connection object.
    
    Raises:
        ImportError: If required database driver is not installed.
        ValueError: If unsupported database type is specified.
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
    """Discover user-accessible databases on the server, filtering system databases.
    
    Args:
        db_type: Database type (postgresql, mysql, sqlite).
        host: Database host address.
        port: Database port.
        user: Database username.
        password: Database password.
    
    Returns:
        List of database names available to the user.
    
    Raises:
        ValueError: If unsupported database type is specified.
    """
    db_type = db_type.lower()
    system_dbs = SYSTEM_DATABASES.get(db_type, set())

    if db_type == "postgresql":
        conn = connect_with_credentials("postgresql", host, port, "postgres", user, password)
        cur = conn.cursor()
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
        names = [str(tuple(row)[0]) for row in cur.fetchall()]
        conn.close()
        return [n for n in names if n not in system_dbs]

    elif db_type == "mysql":
        conn = connect_with_credentials("mysql", host, port, "information_schema", user, password)
        cur = conn.cursor()
        cur.execute("SHOW DATABASES")
        names = [str(tuple(row)[0]) for row in cur.fetchall()]
        conn.close()
        return [n for n in names if n not in system_dbs]

    elif db_type == "sqlite":
        # SQLite has no concept of multiple databases - return the file path itself
        return ["(current file)"]

    else:
        raise ValueError(f"Unsupported db_type: {db_type}")


def save_credentials(db_type: str, host: str = "", port: str = "",
                     user: str = "", password: str = "", path: str = ""):
    """Persist server credentials to .env for pre-filling setup wizard fields.
    
    Database selection always happens in the UI, so only server credentials are saved.
    
    Args:
        db_type: Database type (postgresql, mysql, sqlite).
        host: Database host address.
        port: Database port.
        user: Database username.
        password: Database password.
        path: SQLite database file path.
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
    """Load server credentials from .env for pre-filling setup wizard.
    
    Returns:
        Dictionary of saved credentials, or empty dict if none exist.
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
