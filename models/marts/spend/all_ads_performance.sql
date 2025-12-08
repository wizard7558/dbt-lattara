{{ config(materialized='table') }}

-- Facebook Ads aggregated to Ad Set level
SELECT
    date,
    'Facebook' AS platform,
    account,
    campaign,
    adset AS ad_group,
    conversionname AS conversion_name,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(allconv) AS conversions,
    NULL AS conversion_value
FROM {{ ref('facebook_ads_performance') }}
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

-- Google Ads at Campaign level (use campaign_performance for accurate totals)
SELECT
    date,
    'Google' AS platform,
    account,
    campaign,
    NULL AS ad_group,
    NULL AS conversion_name,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(conversion_value) AS conversion_value
FROM {{ ref('google_ads_campaign_performance') }}
GROUP BY 1, 2, 3, 4

ORDER BY 1 DESC
