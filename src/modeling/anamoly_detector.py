import os
import sqlite3
import numpy as np
import pandas as pd
import logging
from typing import Dict, Any

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.getenv("DATABASE_URL", os.path.join(BASE_DIR, "data", "linkedin_analytics.db")).replace("sqlite:///", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [Risk Model] %(message)s")
logger = logging.getLogger(__name__)


class OutreachRiskModel:
    def __init__(self):
        self.db_path = DB_PATH

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        return conn

    def calculate_risk_and_limits(self) -> pd.DataFrame:
        conn = self._get_connection()
        
        query = """
        SELECT 
            f.fact_key,
            f.date_key,
            f.agent_key,
            a.account_age_tier,
            a.daily_invite_limit,
            a.daily_message_limit,
            f.invites_sent,
            f.invites_accepted,
            f.messages_sent,
            f.replies_received
        FROM fact_daily_outreach f
        JOIN dim_agent a ON f.agent_key = a.agent_key;
        """
        df = pd.read_sql_query(query, conn)

        if df.empty:
            logger.warning("No records found to calculate anomaly scores.")
            conn.close()
            return df

        # Step 1: Feature Engineering (Rates)
        df["acceptance_rate"] = np.where(df["invites_sent"] > 0, df["invites_accepted"] / df["invites_sent"], 0.0)
        df["reply_rate"] = np.where(df["messages_sent"] > 0, df["replies_received"] / df["messages_sent"], 0.0)

        # Step 2: Z-Score & Decay Calculation
        mean_acc = df["acceptance_rate"].mean()
        std_acc = df["acceptance_rate"].std() if len(df) > 1 and df["acceptance_rate"].std() > 0 else 0.1

        df["z_score_acc"] = (df["acceptance_rate"] - mean_acc) / std_acc

        # Composite Risk Score Calculation [0 - 100]
        # Low acceptance or steep drop increases risk score
        df["anomaly_score"] = np.clip(
            (1.0 - df["acceptance_rate"]) * 50 + (1.0 - df["reply_rate"]) * 30 + np.where(df["z_score_acc"] < -1.5, 20, 0),
            0.0, 100.0
        ).round(2)

        df["is_anomalous"] = np.where(df["anomaly_score"] >= 70.0, 1, 0)

        # Dynamic Capacity Ceiling Recommendation
        df["recommended_daily_invites"] = np.where(
            df["is_anomalous"] == 1,
            (df["daily_invite_limit"] * 0.5).astype(int), # Throttling down under risk
            df["daily_invite_limit"]
        )

        # Step 3: Update Fact Table with Scores
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute("""
                UPDATE fact_daily_outreach
                SET anomaly_score = ?, is_anomalous = ?
                WHERE fact_key = ?;
            """, (float(row["anomaly_score"]), int(row["is_anomalous"]), int(row["fact_key"])))

        conn.commit()
        conn.close()
        logger.info(f"Statistical Anomaly model executed. Scored {len(df)} outreach fact records.")
        return df


if __name__ == "__main__":
    model = OutreachRiskModel()
    results = model.calculate_risk_and_limits()
    print("Risk & Capacity Summary:")
    print(results[["fact_key", "account_age_tier", "acceptance_rate", "anomaly_score", "is_anomalous", "recommended_daily_invites"]])