-- int_google_adgroup_conversions.sql
-- Ad group-level conversions by conversion action
-- Grain: date + ad_group + conversion_action_name

SELECT 
    ac.date,
    acc.account_name AS account,
    ac.customer_id,
    cam.campaign_name AS campaign,
    ac.campaign_id,
    cam.advertising_channel_type AS campaign_type,
    adg.ad_group_name AS adgroup,
    ac.id AS ad_group_id,
    ac.conversion_action_name,
    SUM(ac.all_conversions) AS conversions,
    SUM(ac.all_conversions_value) AS conversion_value
FROM FIVETRAN_DATABASE.GOOGLE_ADS_FM.ADGROUP_CONVERSIONS ac
LEFT JOIN {{ ref('v_stg_google_accounts') }} acc
    ON acc.customer_id = ac.customer_id
LEFT JOIN {{ ref('v_stg_google_campaigns') }} cam
    ON cam.campaign_id = ac.campaign_id
LEFT JOIN {{ ref('v_stg_google_adgroup') }} adg
    ON adg.ad_group_id = ac.id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY 1 DESC
