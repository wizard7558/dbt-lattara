{{ config(materialized='table') }}

-- TurboHome specific model with Lead Score conversions as separate columns
SELECT
    date,
    'Google' AS platform,
    account,
    campaign,
    ad_group,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(CASE WHEN conversion_name = 'Lead Score | 2' THEN conversions ELSE 0 END) AS lead_score_2,
    SUM(CASE WHEN conversion_name = 'Lead Score | 3' THEN conversions ELSE 0 END) AS lead_score_3
FROM {{ ref('all_ads_performance') }}
WHERE account = 'TurboHome'
GROUP BY 1, 2, 3, 4, 5

ORDER BY 1 DESC
