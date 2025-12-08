-- int_google_campaign_spend.sql
-- Uses campaign_stats as the source to capture ALL spend
-- Including Performance Max, Display, and other non-keyword campaign types

SELECT 
    cs.date,
    acc.account_name AS account,
    cs.customer_id,
    cam.campaign_name AS campaign,
    cs.id AS campaign_id,
    SUM(cs.cost_micros) / 1000000 AS spend,
    SUM(cs.impressions) AS impressions,
    SUM(cs.clicks) AS clicks,
    SUM(cs.conversions) AS conversions,
    SUM(cs.conversions_value) AS conversion_value
FROM FIVETRAN_DATABASE.GOOGLE_ADS_FM.CAMPAIGN_STATS cs
LEFT JOIN {{ ref('v_stg_google_accounts') }} acc
    ON acc.customer_id = cs.customer_id
LEFT JOIN {{ ref('v_stg_google_campaigns') }} cam
    ON cam.campaign_id = cs.id
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1 DESC
