import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "data", "linkedin_analytics.db")
DDL_PATH = os.path.join(BASE_DIR, "sql", "ddl_star_schema.sql")


class DatabaseManager:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.getenv("DATABASE_URL", DEFAULT_DB_PATH)
        if self.db_path.startswith("sqlite:///"):
            self.db_path = self.db_path.replace("sqlite:///", "")
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def init_schema(self, ddl_file: Optional[str] = None) -> None:
        target_ddl = ddl_file or DDL_PATH
        if not os.path.exists(target_ddl):
            raise FileNotFoundError(f"DDL script not found at {target_ddl}")

        with open(target_ddl, "r", encoding="utf-8") as f:
            ddl_script = f.read()

        with self.get_connection() as conn:
            conn.executescript(ddl_script)
            conn.commit()

    def populate_dim_date(self, start_date: str = "2026-01-01", days: int = 365) -> None:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        records = []

        for i in range(days):
            current = start + timedelta(days=i)
            date_key = int(current.strftime("%Y%m%d"))
            full_date = current.strftime("%Y-%m-%d")
            day_of_week = current.strftime("%A")
            is_weekend = 1 if current.weekday() >= 5 else 0
            month_name = current.strftime("%B")
            quarter = f"Q{(current.month - 1) // 3 + 1}"
            year = current.year

            records.append(
                (date_key, full_date, day_of_week, is_weekend, month_name, quarter, year)
            )

        query = """
        INSERT OR IGNORE INTO dim_date 
        (date_key, full_date, day_of_week, is_weekend, month_name, quarter, year)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """
        with self.get_connection() as conn:
            conn.executemany(query, records)
            conn.commit()


if __name__ == "__main__":
    db = DatabaseManager()
    db.init_schema()
    db.populate_dim_date()
    print("Database schema and dim_date initialized successfully.")