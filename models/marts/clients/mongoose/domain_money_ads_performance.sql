{{ config(materialized='table') }}

-- Domain Money combined model at ad group level with optimization conversions pivoted
-- Meta: schedule_total | Google: HubSpot - Marketing Qualified Lead

SELECT
    date,
    Platform,
    Account,
    Campaign,
    ad_group AS AdGroup,
    SUM(spend) AS Spend,
    SUM(impressions) AS Impressions,
    SUM(clicks) AS Clicks,
    SUM(CASE 
            WHEN conversion_name = 'schedule_total' THEN conversions 
            WHEN conversion_name = 'HubSpot - Marketing Qualified Lead' THEN conversions
            ELSE 0 
        END) AS MQL
FROM {{ ref('all_ads_performance') }}
WHERE account like '%Domain%'
GROUP BY date,
    Platform,
    Account,
    Campaign,
    ad_group
