# Production DAX Measure Layer

## Core Volume Metrics
Total Invites Sent = SUM(fact_daily_outreach[invites_sent])
Total Invites Accepted = SUM(fact_daily_outreach[invites_accepted])
Total Messages Sent = SUM(fact_daily_outreach[messages_sent])
Total Replies Received = SUM(fact_daily_outreach[replies_received])
Total Qualified Leads = SUM(fact_daily_outreach[qualified_leads])

## Efficiency & Conversion Rates
Acceptance Rate = 
DIVIDE(
    [Total Invites Accepted],
    [Total Invites Sent],
    0
)

Reply Rate = 
DIVIDE(
    [Total Replies Received],
    [Total Messages Sent],
    0
)

Lead Conversion Rate = 
DIVIDE(
    [Total Qualified Leads],
    [Total Invites Accepted],
    0
)

## Capacity & Health Metrics
Daily Invite Limit = MAX(dim_agent[daily_invite_limit])
Daily Message Limit = MAX(dim_agent[daily_message_limit])

Invite Capacity Utilization % = 
DIVIDE(
    [Total Invites Sent],
    [Daily Invite Limit],
    0
)

## Risk & Anomaly Intelligence
Average Anomaly Score = AVERAGE(fact_daily_outreach[anomaly_score])

Anomalous Record Count = 
CALCULATE(
    COUNTROWS(fact_daily_outreach),
    fact_daily_outreach[is_anomalous] = 1
)

Risk Classification Status = 
IF(
    [Average Anomaly Score] >= 70,
    "HIGH RISK (Shadow-ban Danger)",
    IF(
        [Average Anomaly Score] >= 40,
        "MODERATE RISK (Warning)",
        "HEALTHY (Safe)"
    )
)