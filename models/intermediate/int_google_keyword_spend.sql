-- int_google_keyword_spend.sql
-- Keyword level spend from keyword_stats
-- Only captures Search campaigns (Demand Gen, Video, PMax have no keywords)

SELECT 
    kws.date,
    acc.account_name AS account,
    kws.customer_id,
    cam.campaign_name AS campaign,
    kws.campaign_id,
    adg.ad_group_name AS adgroup,
    kws.ad_group_id,
    kws.ad_group_criterion_criterion_id AS keyword_id,
    kw.keyword_text AS keyword,
    kw.keyword_match_type AS match_type,
    SUM(kws.cost_micros) / 1000000 AS spend,
    SUM(kws.impressions) AS impressions,
    SUM(kws.clicks) AS clicks
FROM FIVETRAN_DATABASE.GOOGLE_ADS_FM.KEYWORD_STATS kws
LEFT JOIN {{ ref('v_stg_google_accounts') }} acc
    ON acc.customer_id = kws.customer_id
LEFT JOIN {{ ref('v_stg_google_campaigns') }} cam
    ON cam.campaign_id = kws.campaign_id
LEFT JOIN {{ ref('v_stg_google_adgroup') }} adg
    ON adg.ad_group_id = kws.ad_group_id
LEFT JOIN {{ ref('v_stg_google_keyword') }} kw
    ON kw.criterion_id = kws.ad_group_criterion_criterion_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
ORDER BY 1 DESC
