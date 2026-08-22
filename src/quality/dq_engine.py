import os
import sqlite3
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.getenv("DATABASE_URL", os.path.join(BASE_DIR, "data", "linkedin_analytics.db")).replace("sqlite:///", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [DQ Engine] %(message)s")
logger = logging.getLogger(__name__)


class DataQualityEngine:
    def __init__(self, run_id: str = None):
        self.run_id = run_id or str(uuid.uuid4())[:8]
        self.db_path = DB_PATH
        self.weights = {
            "Completeness": 0.25,
            "Uniqueness": 0.25,
            "Validity": 0.20,
            "Referential_Integrity": 0.15,
            "Timeliness": 0.15
        }
        self.pass_threshold = 85.0

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def check_completeness(self, conn: sqlite3.Connection) -> float:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN date_key IS NULL OR agent_key IS NULL OR campaign_key IS NULL THEN 1 ELSE 0 END) as nulls
            FROM fact_daily_outreach;
        """)
        row = cursor.fetchone()
        if not row or row["total"] == 0:
            return 100.0
        return round(((row["total"] - row["nulls"]) / row["total"]) * 100.0, 2)

    def check_uniqueness(self, conn: sqlite3.Connection) -> float:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as total FROM fact_daily_outreach;")
        total = cursor.fetchone()["total"]
        if total == 0:
            return 100.0

        cursor.execute("""
            SELECT COUNT(*) as distinct_grains FROM (
                SELECT DISTINCT date_key, agent_key, campaign_key, icp_key FROM fact_daily_outreach
            );
        """)
        distinct_count = cursor.fetchone()["distinct_grains"]
        return round((distinct_count / total) * 100.0, 2)

    def check_validity(self, conn: sqlite3.Connection) -> float:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN invites_sent < 0 OR invites_accepted < 0 OR messages_sent < 0 OR replies_received < 0 THEN 1 ELSE 0 END) as invalid_rows
            FROM fact_daily_outreach;
        """)
        row = cursor.fetchone()
        if not row or row["total"] == 0:
            return 100.0
        return round(((row["total"] - row["invalid_rows"]) / row["total"]) * 100.0, 2)

    def check_referential_integrity(self, conn: sqlite3.Connection) -> float:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) as orphans FROM fact_daily_outreach f
            LEFT JOIN dim_agent a ON f.agent_key = a.agent_key
            LEFT JOIN dim_campaign c ON f.campaign_key = c.campaign_key
            WHERE a.agent_key IS NULL OR c.campaign_key IS NULL;
        """)
        orphans = cursor.fetchone()["orphans"]
        cursor.execute("SELECT COUNT(*) as total FROM fact_daily_outreach;")
        total = cursor.fetchone()["total"]
        if total == 0:
            return 100.0
        return 100.0 if orphans == 0 else round(((total - orphans) / total) * 100.0, 2)

    def check_timeliness(self, conn: sqlite3.Connection) -> float:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(date_key) as latest_date FROM fact_daily_outreach;")
        latest = cursor.fetchone()["latest_date"]
        return 100.0 if latest is not None else 0.0

    def run_all_checks(self) -> Dict[str, Any]:
        conn = self._get_connection()
        cursor = conn.cursor()

        dimension_scores = {
            "Completeness": self.check_completeness(conn),
            "Uniqueness": self.check_uniqueness(conn),
            "Validity": self.check_validity(conn),
            "Referential_Integrity": self.check_referential_integrity(conn),
            "Timeliness": self.check_timeliness(conn)
        }

        # Weighted Composite Score
        composite_score = sum(dimension_scores[dim] * self.weights[dim] for dim in self.weights)
        composite_score = round(composite_score, 2)
        overall_status = "PASS" if composite_score >= self.pass_threshold else "FAIL"

        timestamp = datetime.now(timezone.utc).isoformat()

        for dim, score in dimension_scores.items():
            status = "PASS" if score >= self.pass_threshold else "WARN"
            cursor.execute("""
                INSERT INTO dq_results_history (run_id, check_timestamp, dimension, metric_name, score, status, details)
                VALUES (?, ?, ?, ?, ?, ?, ?);
            """, (self.run_id, timestamp, dim, f"{dim}_Metric", score, status, f"Score: {score}% against threshold {self.pass_threshold}%"))

        conn.commit()
        conn.close()

        logger.info(f"Composite Data Quality Score: {composite_score}% | Status: {overall_status}")
        return {
            "run_id": self.run_id,
            "composite_score": composite_score,
            "status": overall_status,
            "dimension_scores": dimension_scores
        }


if __name__ == "__main__":
    engine = DataQualityEngine()
    summary = engine.run_all_checks()
    print("DQ Summary:", summary)