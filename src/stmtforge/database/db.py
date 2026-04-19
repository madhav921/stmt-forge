"""SQLite database layer for CCAnalyser."""

import sqlite3
import hashlib
from datetime import datetime
from pathlib import Path

import pandas as pd

from stmtforge.utils.config import load_config, resolve_path
from stmtforge.utils.logging_config import get_logger

logger = get_logger("database")


class Database:
    """SQLite database for storing transactions and metadata."""

    def __init__(self, db_path: str = None):
        if db_path is None:
            config = load_config()
            db_path = config["database"]["path"]
        self.db_path = resolve_path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Initialize database tables."""
        with self._get_conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    description TEXT NOT NULL,
                    amount REAL NOT NULL,
                    type TEXT NOT NULL CHECK(type IN ('debit', 'credit')),
                    category TEXT DEFAULT 'Others',
                    bank TEXT NOT NULL,
                    card_name TEXT,
                    card_last4 TEXT,
                    source_file TEXT,
                    file_hash TEXT,
                    balance REAL,
                    reward_points REAL,
                    statement_received_date TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    txn_hash TEXT UNIQUE
                );

                CREATE TABLE IF NOT EXISTS statements_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_hash TEXT UNIQUE NOT NULL,
                    original_path TEXT,
                    unlocked_path TEXT,
                    bank TEXT,
                    card_name TEXT,
                    email_date TEXT,
                    email_subject TEXT,
                    filename TEXT,
                    sender TEXT,
                    message_id TEXT,
                    card_last4 TEXT,
                    statement_period_start TEXT,
                    statement_period_end TEXT,
                    transaction_count INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    processed_at TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS gmail_messages (
                    message_id TEXT PRIMARY KEY,
                    sender TEXT,
                    email_date TEXT,
                    email_subject TEXT,
                    status TEXT,
                    processed_at TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS pipeline_state (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS extraction_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_hash TEXT NOT NULL,
                    filename TEXT,
                    extraction_method TEXT,
                    raw_text TEXT,
                    llm_raw_output TEXT,
                    cleaned_json TEXT,
                    transaction_count INTEGER DEFAULT 0,
                    confidence_score REAL DEFAULT 0.0,
                    llm_model TEXT,
                    error_message TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                );

                CREATE INDEX IF NOT EXISTS idx_txn_date ON transactions(date);
                CREATE INDEX IF NOT EXISTS idx_txn_bank ON transactions(bank);
                CREATE INDEX IF NOT EXISTS idx_txn_category ON transactions(category);
                CREATE INDEX IF NOT EXISTS idx_txn_type ON transactions(type);
                CREATE INDEX IF NOT EXISTS idx_txn_file_hash ON transactions(file_hash);
                CREATE INDEX IF NOT EXISTS idx_stmt_file_hash ON statements_metadata(file_hash);
                CREATE INDEX IF NOT EXISTS idx_extlog_file_hash ON extraction_log(file_hash);
            """)

            # Migration: add new columns if DB already exists
            self._migrate_add_column(conn, "transactions", "card_name", "TEXT")
            self._migrate_add_column(conn, "transactions", "reward_points", "REAL")
            self._migrate_add_column(conn, "transactions", "statement_received_date", "TEXT")
            self._migrate_add_column(conn, "statements_metadata", "card_name", "TEXT")
            self._migrate_add_column(conn, "statements_metadata", "email_subject", "TEXT")
            self._migrate_add_column(conn, "statements_metadata", "filename", "TEXT")
            self._migrate_add_column(conn, "gmail_messages", "email_subject", "TEXT")

            # Create indexes on new columns (after migration)
            try:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_txn_card_name ON transactions(card_name)")
            except sqlite3.OperationalError:
                pass

        logger.debug("Database initialized")

    def _migrate_add_column(self, conn, table: str, column: str, col_type: str):
        """Add a column to a table if it doesn't already exist."""
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            logger.info(f"Migrated: added {column} to {table}")
        except sqlite3.OperationalError:
            pass  # Column already exists

    # ── Transaction methods ──────────────────────────────────────

    def insert_transactions(self, df: pd.DataFrame, bank: str, source_file: str,
                            file_hash: str, card_name: str = None,
                            reward_points: float = None,
                            statement_received_date: str = None) -> int:
        """
        Insert transactions from a DataFrame. Uses txn_hash for deduplication.
        Returns number of new rows inserted.
        """
        if df.empty:
            return 0

        inserted = 0
        with self._get_conn() as conn:
            for _, row in df.iterrows():
                # Generate unique hash for this transaction
                txn_hash = self._txn_hash(
                    row["date"], row["description"], row["amount"],
                    row["type"], bank, row.get("card_last4", ""),
                )

                # Per-row card_name overrides file-level card_name
                row_card_name = row.get("card_name") or card_name
                row_reward_pts = row.get("reward_points") or reward_points

                try:
                    conn.execute("""
                        INSERT INTO transactions
                        (date, description, amount, type, category, bank,
                         card_name, card_last4, source_file, file_hash,
                         balance, reward_points, statement_received_date, txn_hash)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(txn_hash) DO UPDATE SET
                            card_name = excluded.card_name,
                            category = excluded.category
                    """, (
                        row["date"],
                        row["description"],
                        row["amount"],
                        row["type"],
                        row.get("category", "Others"),
                        bank,
                        row_card_name,
                        row.get("card_last4"),
                        source_file,
                        file_hash,
                        row.get("balance"),
                        row_reward_pts,
                        statement_received_date,
                        txn_hash,
                    ))
                    if conn.total_changes:
                        inserted += 1
                except sqlite3.IntegrityError:
                    pass  # Duplicate, skip

        logger.info(f"Inserted {inserted}/{len(df)} transactions from {source_file}")
        return inserted

    def _txn_hash(self, date: str, description: str, amount: float,
                  txn_type: str, bank: str, card_last4: str) -> str:
        """Generate a unique hash for a transaction."""
        key = f"{date}|{description}|{amount:.2f}|{txn_type}|{bank}|{card_last4 or ''}"
        return hashlib.sha256(key.encode()).hexdigest()

    def get_transactions(self, filters: dict = None) -> pd.DataFrame:
        """Query transactions with optional filters. Returns DataFrame."""
        query = "SELECT * FROM transactions WHERE 1=1"
        params = []

        if filters:
            if filters.get("date_from"):
                query += " AND date >= ?"
                params.append(filters["date_from"])
            if filters.get("date_to"):
                query += " AND date <= ?"
                params.append(filters["date_to"])
            if filters.get("bank"):
                if isinstance(filters["bank"], list):
                    placeholders = ",".join("?" * len(filters["bank"]))
                    query += f" AND bank IN ({placeholders})"
                    params.extend(filters["bank"])
                else:
                    query += " AND bank = ?"
                    params.append(filters["bank"])
            if filters.get("category"):
                if isinstance(filters["category"], list):
                    placeholders = ",".join("?" * len(filters["category"]))
                    query += f" AND category IN ({placeholders})"
                    params.extend(filters["category"])
                else:
                    query += " AND category = ?"
                    params.append(filters["category"])
            if filters.get("type"):
                query += " AND type = ?"
                params.append(filters["type"])
            if filters.get("amount_min"):
                query += " AND amount >= ?"
                params.append(filters["amount_min"])
            if filters.get("amount_max"):
                query += " AND amount <= ?"
                params.append(filters["amount_max"])
            if filters.get("search"):
                query += " AND description LIKE ?"
                params.append(f"%{filters['search']}%")
            if filters.get("card_last4"):
                query += " AND card_last4 = ?"
                params.append(filters["card_last4"])
            if filters.get("card_name"):
                if isinstance(filters["card_name"], list):
                    placeholders = ",".join("?" * len(filters["card_name"]))
                    query += f" AND card_name IN ({placeholders})"
                    params.extend(filters["card_name"])
                else:
                    query += " AND card_name = ?"
                    params.append(filters["card_name"])

        query += " ORDER BY date DESC"

        with self._get_conn() as conn:
            df = pd.read_sql_query(query, conn, params=params)

        return df

    def get_summary(self) -> dict:
        """Get summary statistics."""
        with self._get_conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
            total_spend = conn.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE type='debit'"
            ).fetchone()[0]
            banks = [r[0] for r in conn.execute(
                "SELECT DISTINCT bank FROM transactions"
            ).fetchall()]
            categories = [r[0] for r in conn.execute(
                "SELECT DISTINCT category FROM transactions"
            ).fetchall()]
            date_range = conn.execute(
                "SELECT MIN(date), MAX(date) FROM transactions"
            ).fetchone()

        return {
            "total_transactions": total,
            "total_spend": total_spend,
            "banks": banks,
            "categories": categories,
            "date_range": {
                "start": date_range[0] if date_range else None,
                "end": date_range[1] if date_range else None,
            },
        }

    def get_date_anchor_options(self) -> dict:
        """Get date anchors for dashboard date-range defaults/options."""
        with self._get_conn() as conn:
            period_end_rows = conn.execute(
                """
                SELECT statement_period_end
                FROM statements_metadata
                WHERE statement_period_end IS NOT NULL
                  AND TRIM(statement_period_end) != ''
                """
            ).fetchall()

            latest_statement_end = None
            if period_end_rows:
                parsed_dates = pd.to_datetime(
                    [r[0] for r in period_end_rows],
                    errors="coerce",
                    dayfirst=True,
                )
                parsed_dates = parsed_dates.dropna()
                if not parsed_dates.empty:
                    latest_statement_end = parsed_dates.max().strftime("%Y-%m-%d")

            latest_statement_received = conn.execute(
                """
                SELECT MAX(COALESCE(date(email_date), date(created_at)))
                FROM statements_metadata
                WHERE status = 'completed'
                """
            ).fetchone()[0]

            txn_min = conn.execute("SELECT MIN(date) FROM transactions").fetchone()[0]

        return {
            "latest_statement_end_date": latest_statement_end,
            "latest_statement_received_date": latest_statement_received,
            "current_date": datetime.now().strftime("%Y-%m-%d"),
            "transaction_min_date": txn_min,
        }

    def get_monthly_spend(self) -> pd.DataFrame:
        """Get monthly spend aggregation."""
        query = """
            SELECT
                strftime('%Y-%m', date) as month,
                SUM(CASE WHEN type='debit' THEN amount ELSE 0 END) as total_debit,
                SUM(CASE WHEN type='credit' THEN amount ELSE 0 END) as total_credit,
                COUNT(*) as transaction_count
            FROM transactions
            GROUP BY month
            ORDER BY month
        """
        with self._get_conn() as conn:
            return pd.read_sql_query(query, conn)

    def get_category_spend(self, date_from: str = None, date_to: str = None) -> pd.DataFrame:
        """Get spending by category."""
        query = "SELECT category, SUM(amount) as total, COUNT(*) as count FROM transactions WHERE type='debit'"
        params = []
        if date_from:
            query += " AND date >= ?"
            params.append(date_from)
        if date_to:
            query += " AND date <= ?"
            params.append(date_to)
        query += " GROUP BY category ORDER BY total DESC"

        with self._get_conn() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def get_merchant_spend(self, date_from: str = None, date_to: str = None,
                           limit: int = 20) -> pd.DataFrame:
        """Get top merchants by spend."""
        query = "SELECT description as merchant, SUM(amount) as total, COUNT(*) as count FROM transactions WHERE type='debit'"
        params = []
        if date_from:
            query += " AND date >= ?"
            params.append(date_from)
        if date_to:
            query += " AND date <= ?"
            params.append(date_to)
        query += " GROUP BY description ORDER BY total DESC LIMIT ?"
        params.append(limit)

        with self._get_conn() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def get_daily_spend(self, date_from: str = None, date_to: str = None) -> pd.DataFrame:
        """Get daily spend for heatmap."""
        query = "SELECT date, SUM(amount) as total FROM transactions WHERE type='debit'"
        params = []
        if date_from:
            query += " AND date >= ?"
            params.append(date_from)
        if date_to:
            query += " AND date <= ?"
            params.append(date_to)
        query += " GROUP BY date ORDER BY date"

        with self._get_conn() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def get_banks(self) -> list:
        """Get list of distinct banks."""
        with self._get_conn() as conn:
            rows = conn.execute("SELECT DISTINCT bank FROM transactions ORDER BY bank").fetchall()
        return [r[0] for r in rows]

    def get_categories(self) -> list:
        """Get list of distinct categories."""
        with self._get_conn() as conn:
            rows = conn.execute("SELECT DISTINCT category FROM transactions ORDER BY category").fetchall()
        return [r[0] for r in rows]

    def get_cards(self) -> list:
        """Get list of distinct card_last4 values."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT card_last4 FROM transactions WHERE card_last4 IS NOT NULL ORDER BY card_last4"
            ).fetchall()
        return [r[0] for r in rows]

    def get_card_names(self) -> list:
        """Get list of distinct card names."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT card_name FROM transactions WHERE card_name IS NOT NULL ORDER BY card_name"
            ).fetchall()
        return [r[0] for r in rows]

    def export_attachment_metadata_csv(self, output_path: str) -> str:
        """Export all statement metadata to CSV."""
        with self._get_conn() as conn:
            df = pd.read_sql_query(
                "SELECT file_hash, original_path, bank, card_name, email_date, "
                "email_subject, filename, sender, message_id, card_last4, "
                "statement_period_start, statement_period_end, transaction_count, "
                "status, processed_at, created_at "
                "FROM statements_metadata ORDER BY email_date DESC",
                conn,
            )
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output, index=False)
        logger.info(f"Exported {len(df)} statement records to {output}")
        return str(output)

    # ── Statement metadata methods ───────────────────────────────

    def record_statement(self, file_hash: str, original_path: str, bank: str,
                         email_date: str = None, sender: str = None,
                         message_id: str = None, card_name: str = None,
                         email_subject: str = None, filename: str = None,
                         **kwargs) -> bool:
        """Record a statement file in metadata. Returns True if new."""
        with self._get_conn() as conn:
            try:
                conn.execute("""
                    INSERT INTO statements_metadata
                    (file_hash, original_path, bank, card_name, email_date,
                     email_subject, filename, sender, message_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(file_hash) DO UPDATE SET
                        card_name = excluded.card_name
                """, (file_hash, original_path, bank, card_name, email_date,
                       email_subject, filename, sender, message_id))
                return conn.total_changes > 0
            except sqlite3.IntegrityError:
                return False

    def update_statement_status(self, file_hash: str, status: str,
                                transaction_count: int = 0,
                                error_message: str = None, **kwargs):
        """Update processing status of a statement."""
        with self._get_conn() as conn:
            conn.execute("""
                UPDATE statements_metadata
                SET status = ?, transaction_count = ?, error_message = ?,
                    processed_at = datetime('now')
                WHERE file_hash = ?
            """, (status, transaction_count, error_message, file_hash))

    def is_file_processed(self, file_hash: str) -> bool:
        """Check if a file has already been successfully processed."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT status FROM statements_metadata WHERE file_hash = ?",
                (file_hash,)
            ).fetchone()
        return row is not None and row[0] == "completed"

    # ── Gmail message tracking ───────────────────────────────────

    def record_message(self, message_id: str, sender: str, email_date: str,
                       status: str, email_subject: str = None):
        """Record a Gmail message as processed."""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO gmail_messages
                (message_id, sender, email_date, email_subject, status, processed_at)
                VALUES (?, ?, ?, ?, ?, datetime('now'))
            """, (message_id, sender, email_date, email_subject, status))

    def get_processed_message_ids(self) -> set:
        """Get set of already-processed Gmail message IDs."""
        with self._get_conn() as conn:
            rows = conn.execute("SELECT message_id FROM gmail_messages").fetchall()
        return {r[0] for r in rows}

    # ── Pipeline state ───────────────────────────────────────────

    def get_last_fetch_date(self) -> str | None:
        """Get the date of the last Gmail fetch."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT value FROM pipeline_state WHERE key = 'last_fetch_date'"
            ).fetchone()
        return row[0] if row else None

    def update_last_fetch_date(self, date: str):
        """Update the last fetch date."""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO pipeline_state (key, value, updated_at)
                VALUES ('last_fetch_date', ?, datetime('now'))
            """, (date,))

    def get_pipeline_state(self, key: str) -> str | None:
        """Get a pipeline state value."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT value FROM pipeline_state WHERE key = ?", (key,)
            ).fetchone()
        return row[0] if row else None

    def set_pipeline_state(self, key: str, value: str):
        """Set a pipeline state value."""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO pipeline_state (key, value, updated_at)
                VALUES (?, ?, datetime('now'))
            """, (key, value))

    # ── Extraction log methods ───────────────────────────────────

    def store_extraction_log(self, file_hash: str, filename: str,
                             extraction_method: str, raw_text: str,
                             llm_raw_output: str = None,
                             cleaned_json: str = None,
                             transaction_count: int = 0,
                             confidence_score: float = 0.0,
                             llm_model: str = None,
                             error_message: str = None):
        """Store extraction log entry for a processed PDF."""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO extraction_log
                (file_hash, filename, extraction_method, raw_text,
                 llm_raw_output, cleaned_json, transaction_count,
                 confidence_score, llm_model, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (file_hash, filename, extraction_method, raw_text,
                  llm_raw_output, cleaned_json, transaction_count,
                  confidence_score, llm_model, error_message))

    def get_extraction_log(self, file_hash: str = None) -> pd.DataFrame:
        """Get extraction log entries, optionally filtered by file_hash."""
        query = "SELECT * FROM extraction_log"
        params = []
        if file_hash:
            query += " WHERE file_hash = ?"
            params.append(file_hash)
        query += " ORDER BY created_at DESC"
        with self._get_conn() as conn:
            return pd.read_sql_query(query, conn, params=params)
