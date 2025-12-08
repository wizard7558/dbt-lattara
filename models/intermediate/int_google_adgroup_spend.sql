-- int_google_adgroup_spend.sql
-- Ad group level spend from ad_group_stats
-- Captures Search, Demand Gen, Video (excludes Performance Max which has no ad groups)

SELECT 
    ags.date,
    acc.account_name AS account,
    ags.customer_id,
    cam.campaign_name AS campaign,
    ags.campaign_id,
    cam.advertising_channel_type AS campaign_type,
    adg.ad_group_name AS adgroup,
    ags.id AS ad_group_id,
    SUM(ags.cost_micros) / 1000000 AS spend,
    SUM(ags.impressions) AS impressions,
    SUM(ags.clicks) AS clicks,
    SUM(ags.conversions) AS conversions,
    SUM(ags.conversions_value) AS conversion_value
FROM FIVETRAN_DATABASE.GOOGLE_ADS_FM.AD_GROUP_STATS ags
LEFT JOIN {{ ref('v_stg_google_accounts') }} acc
    ON acc.customer_id = ags.customer_id
LEFT JOIN {{ ref('v_stg_google_campaigns') }} cam
    ON cam.campaign_id = ags.campaign_id
LEFT JOIN {{ ref('v_stg_google_adgroup') }} adg
    ON adg.ad_group_id = ags.id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY 1 DESC
