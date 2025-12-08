-- int_google_keyword_conversions.sql
-- Keyword-level conversions by conversion action
-- This is the grain: date + keyword + conversion_action_name

SELECT 
    date,
    acc.account_name AS account,
    kc.customer_id,
    cam.campaign_name AS campaign,
    kc.campaign_id,
    adg.ad_group_name AS adgroup,
    kc.ad_group_id,
    kc.ad_group_criterion_criterion_id AS keyword_id,
    kw.keyword_text AS keyword,
    kw.keyword_match_type AS match_type,
    kc.conversion_action_name,
    SUM(kc.all_conversions) AS conversions,
    SUM(kc.all_conversions_value) AS conversion_value
FROM FIVETRAN_DATABASE.GOOGLE_ADS_FM.KEYWORD_CONVERSIONS kc
LEFT JOIN {{ ref('v_stg_google_accounts') }} acc
    ON acc.customer_id = kc.customer_id
LEFT JOIN {{ ref('v_stg_google_campaigns') }} cam
    ON cam.campaign_id = kc.campaign_id
LEFT JOIN {{ ref('v_stg_google_adgroup') }} adg
    ON adg.ad_group_id = kc.ad_group_id
LEFT JOIN {{ ref('v_stg_google_keyword') }} kw
    ON kw.criterion_id = kc.ad_group_criterion_criterion_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
ORDER BY 1 DESC
