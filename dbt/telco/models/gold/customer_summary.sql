WITH customers AS (
    SELECT * FROM {{ source('telco_raw', 'customers') }}
),
activity_agg AS (
    SELECT
        customer_id,
        COUNT(DISTINCT activity_date) AS active_days,
        SUM(events_count)             AS total_events,
        SUM(usage_minutes)            AS total_usage_minutes,
        SUM(data_gb)                  AS total_data_gb,
        SUM(support_calls)            AS total_support_calls,
        SUM(late_payments)            AS total_late_payments
    FROM {{ source('telco_raw', 'daily_activity') }}
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.age,
    c.region,
    c.plan_type,
    c.contract_type,
    c.tenure_months,
    c.monthly_fee,
    c.autopay,
    c.churned,
    COALESCE(a.active_days, 0)         AS active_days,
    COALESCE(a.total_events, 0)        AS total_events,
    COALESCE(a.total_usage_minutes, 0) AS total_usage_minutes,
    COALESCE(a.total_data_gb, 0)       AS total_data_gb,
    COALESCE(a.total_support_calls, 0) AS total_support_calls,
    COALESCE(a.total_late_payments, 0) AS total_late_payments
FROM customers c
LEFT JOIN activity_agg a
    ON c.customer_id = a.customer_id
