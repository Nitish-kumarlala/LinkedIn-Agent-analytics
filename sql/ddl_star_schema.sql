-- Star Schema DDL for LinkedIn Agent Analytics Platform

CREATE TABLE IF NOT EXISTS dim_agent (
    agent_key INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id VARCHAR(64) UNIQUE NOT NULL,
    agent_name VARCHAR(128) NOT NULL,
    account_age_tier VARCHAR(32) NOT NULL,
    risk_classification VARCHAR(32) NOT NULL,
    daily_invite_limit INTEGER NOT NULL,
    daily_message_limit INTEGER NOT NULL,
    status VARCHAR(32) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_key INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id VARCHAR(64) UNIQUE NOT NULL,
    campaign_name VARCHAR(128) NOT NULL,
    objective VARCHAR(64),
    status VARCHAR(32) DEFAULT 'ACTIVE',
    start_date DATE,
    end_date DATE
);

CREATE TABLE IF NOT EXISTS dim_target_icp (
    icp_key INTEGER PRIMARY KEY AUTOINCREMENT,
    icp_id VARCHAR(64) UNIQUE NOT NULL,
    job_title VARCHAR(128) NOT NULL,
    industry VARCHAR(128) NOT NULL,
    region VARCHAR(64) DEFAULT 'Global'
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    day_of_week VARCHAR(16) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    month_name VARCHAR(16) NOT NULL,
    quarter VARCHAR(4) NOT NULL,
    year INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_daily_outreach (
    fact_key INTEGER PRIMARY KEY AUTOINCREMENT,
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    agent_key INTEGER NOT NULL REFERENCES dim_agent(agent_key),
    campaign_key INTEGER NOT NULL REFERENCES dim_campaign(campaign_key),
    icp_key INTEGER NOT NULL REFERENCES dim_target_icp(icp_key),
    invites_sent INTEGER DEFAULT 0,
    invites_accepted INTEGER DEFAULT 0,
    messages_sent INTEGER DEFAULT 0,
    replies_received INTEGER DEFAULT 0,
    qualified_leads INTEGER DEFAULT 0,
    anomaly_score REAL DEFAULT 0.0,
    is_anomalous BOOLEAN DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_agent_campaign_date UNIQUE (date_key, agent_key, campaign_key, icp_key)
);

CREATE TABLE IF NOT EXISTS pipeline_run_metadata (
    run_id VARCHAR(64) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(32) NOT NULL,
    rows_extracted INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    rows_updated INTEGER DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS dq_results_history (
    check_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id VARCHAR(64) NOT NULL,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dimension VARCHAR(32) NOT NULL,
    metric_name VARCHAR(64) NOT NULL,
    score REAL NOT NULL,
    status VARCHAR(16) NOT NULL,
    details TEXT
);