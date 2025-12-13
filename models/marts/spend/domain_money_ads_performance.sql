{{ config(materialized='table') }}

-- Domain Money combined model at ad group level with optimization conversions pivoted
-- Meta: schedule_total | Google: HubSpot - Marketing Qualified Lead

-- Facebook at Ad Set level
SELECT
    date,
    'Meta' AS platform,
    account,
    campaign,
    adset AS ad_group,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(CASE WHEN conversionname = 'schedule_total' THEN allconv ELSE 0 END) AS schedule_total,
    0 AS hubspot_mql,
    SUM(CASE WHEN conversionname = 'schedule_total' THEN allconv ELSE 0 END) AS total_optimization_conversions,
    SUM(allconv) AS total_conversions,
    NULL AS conversion_value
FROM {{ ref('facebook_ads_performance') }}
WHERE account = 'Domain Money Ad Account'
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- Google at Ad Group level
SELECT
    date,
    'Google' AS platform,
    account,
    campaign,
    adgroup AS ad_group,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    0 AS schedule_total,
    SUM(CASE WHEN conversion_action_name = 'HubSpot - Marketing Qualified Lead' THEN conversions ELSE 0 END) AS hubspot_mql,
    SUM(CASE WHEN conversion_action_name = 'HubSpot - Marketing Qualified Lead' THEN conversions ELSE 0 END) AS total_optimization_conversions,
    SUM(conversions) AS total_conversions,
    SUM(conversion_value) AS conversion_value
FROM {{ ref('google_ads_adgroup_performance') }}
WHERE account = 'Domain Money'
GROUP BY 1, 2, 3, 4, 5

ORDER BY 1 DESC
