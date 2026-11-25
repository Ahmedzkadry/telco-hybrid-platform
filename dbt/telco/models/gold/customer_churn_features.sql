{{ config(materialized='table') }}

WITH bounds AS (
    -- one stable snapshot date: the last day of the batch data
    SELECT
        MAX(activity_date) AS snapshot_date,
        LEAST(30, COUNT(DISTINCT activity_date)) AS window_days
    FROM {{ source('telco_raw', 'daily_activity') }}
),

activity AS (
    SELECT
        a.customer_id,
        SUM(a.usage_minutes)  AS total_minutes,
        SUM(a.data_gb)        AS total_data_gb,
        SUM(a.support_calls)  AS support_calls_30d,
        SUM(a.late_payments)  AS late_payments_30d,
        MAX(a.activity_date)  AS last_activity_date
    FROM {{ source('telco_raw', 'daily_activity') }} a
    CROSS JOIN bounds b
    WHERE a.activity_date > b.snapshot_date - 30
    GROUP BY a.customer_id
)

SELECT
    c.customer_id,
    b.snapshot_date,
    c.region,
    c.plan_type,
    c.contract_type,
    c.age,
    c.tenure_months,
    c.monthly_fee::double precision                                  AS monthly_fee,
    c.autopay,
    COALESCE(a.total_minutes, 0)  / b.window_days                    AS avg_daily_minutes,
    COALESCE(a.total_data_gb, 0)  / b.window_days                    AS avg_daily_data_gb,
    COALESCE(a.support_calls_30d, 0)                                 AS support_calls_30d,
    COALESCE(a.late_payments_30d, 0)                                 AS late_payments_30d,
    COALESCE(b.snapshot_date - a.last_activity_date, b.window_days)  AS days_since_last_activity,
    c.churned
FROM {{ source('telco_raw', 'customers') }} c
CROSS JOIN bounds b
LEFT JOIN activity a ON a.customer_id = c.customer_id
