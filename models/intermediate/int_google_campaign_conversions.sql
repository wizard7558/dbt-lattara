-- int_google_campaign_conversions.sql
-- Campaign-level conversions by conversion action
-- Grain: date + campaign + conversion_action_name

SELECT 
    cc.date,
    acc.account_name AS account,
    cc.customer_id,
    cam.campaign_name AS campaign,
    cc.id AS campaign_id,
    cam.advertising_channel_type AS campaign_type,
    cc.conversion_action_name,
    SUM(cc.all_conversions) AS conversions,
    SUM(cc.all_conversions_value) AS conversion_value
FROM FIVETRAN_DATABASE.GOOGLE_ADS_FM.CAMPAIGN_CONVERSIONS cc
LEFT JOIN {{ ref('v_stg_google_accounts') }} acc
    ON acc.customer_id = cc.customer_id
LEFT JOIN {{ ref('v_stg_google_campaigns') }} cam
    ON cam.campaign_id = cc.id
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1 DESC
