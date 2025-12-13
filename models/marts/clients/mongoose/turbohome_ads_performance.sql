{{ config(materialized='table') }}

-- TurboHome specific model with Lead Score conversions as separate columns
SELECT
    date,
    'Google' AS platform,
    account,
    campaign,
    adgroup,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(CASE WHEN conversion_action_name = 'Lead Score | 2' THEN conversions ELSE 0 END) AS lead_score_2,
    SUM(CASE WHEN conversion_action_name = 'Lead Score | 3' THEN conversions ELSE 0 END) AS lead_score_3
FROM {{ ref('google_ads_adgroup_performance') }}
WHERE account = 'TurboHome'
GROUP BY 1, 2, 3, 4, 5

ORDER BY 1 DESC
