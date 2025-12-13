{{ config(materialized='table') }}

-- Domain Money Meta creative-level model with schedule_total as separate column
SELECT
    date,
    account,
    campaign,
    adset,
    ad,
    ad_id,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(CASE WHEN conversionname = 'schedule_total' THEN allconv ELSE 0 END) AS MQL,
FROM {{ ref('facebook_ads_performance') }}
WHERE account = 'Domain Money Ad Account'
GROUP BY 1, 2, 3, 4, 5, 6

ORDER BY 1 DESC
