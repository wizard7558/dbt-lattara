{{ config(materialized = "table") }}

-- Google Ads Performance at Campaign Level
-- Uses campaign_stats to capture ALL spend (including Performance Max, Display)
-- No join to keyword_conversions prevents spend duplication

SELECT
    date,
    account,
    customer_id,
    campaign,
    campaign_id,
    campaign_type,
    spend,
    impressions,
    clicks,
    conversions,
    conversion_value,
    div0(spend, clicks) AS cpc,
    div0(clicks, impressions) * 100 AS ctr,
    div0(spend, conversions) AS cpa,
    div0(conversion_value, spend) AS roas
FROM {{ ref('int_google_campaign_spend') }}
ORDER BY 1 DESC, 2, 3
