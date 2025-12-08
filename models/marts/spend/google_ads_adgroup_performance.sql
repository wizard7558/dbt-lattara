{{ config(materialized = "table") }}

-- Google Ads Ad Group Performance
-- Combines ad group spend with conversions WITHOUT duplication
-- Uses ROW_NUMBER to assign spend only to first row per adgroup/date
-- Captures Search, Demand Gen, and Video (Performance Max excluded - no ad groups)

WITH adgroup_spend AS (
    SELECT * FROM {{ ref('int_google_adgroup_spend') }}
),

adgroup_conversions AS (
    SELECT * FROM {{ ref('int_google_adgroup_conversions') }}
),

joined AS (
    SELECT 
        COALESCE(ags.date, ac.date) AS date,
        COALESCE(ags.account, ac.account) AS account,
        COALESCE(ags.customer_id, ac.customer_id) AS customer_id,
        COALESCE(ags.campaign, ac.campaign) AS campaign,
        COALESCE(ags.campaign_id, ac.campaign_id) AS campaign_id,
        COALESCE(ags.campaign_type, ac.campaign_type) AS campaign_type,
        COALESCE(ags.adgroup, ac.adgroup) AS adgroup,
        COALESCE(ags.ad_group_id, ac.ad_group_id) AS ad_group_id,
        ac.conversion_action_name,
        ags.spend,
        ags.impressions,
        ags.clicks,
        ac.conversions,
        ac.conversion_value,
        ROW_NUMBER() OVER (
            PARTITION BY 
                COALESCE(ags.date, ac.date),
                COALESCE(ags.customer_id, ac.customer_id),
                COALESCE(ags.ad_group_id, ac.ad_group_id)
            ORDER BY ac.conversion_action_name NULLS LAST
        ) AS rn
    FROM adgroup_spend ags
    FULL OUTER JOIN adgroup_conversions ac
        ON ags.date = ac.date
        AND ags.customer_id = ac.customer_id
        AND ags.ad_group_id = ac.ad_group_id
)

SELECT 
    date,
    account,
    customer_id,
    campaign,
    campaign_id,
    campaign_type,
    adgroup,
    ad_group_id,
    conversion_action_name,
    CASE WHEN rn = 1 THEN spend ELSE 0 END AS spend,
    CASE WHEN rn = 1 THEN impressions ELSE 0 END AS impressions,
    CASE WHEN rn = 1 THEN clicks ELSE 0 END AS clicks,
    COALESCE(conversions, 0) AS conversions,
    COALESCE(conversion_value, 0) AS conversion_value,
    div0(CASE WHEN rn = 1 THEN spend ELSE 0 END, CASE WHEN rn = 1 THEN clicks ELSE 0 END) AS cpc,
    div0(CASE WHEN rn = 1 THEN clicks ELSE 0 END, CASE WHEN rn = 1 THEN impressions ELSE 0 END) * 100 AS ctr,
    div0(CASE WHEN rn = 1 THEN spend ELSE 0 END, COALESCE(conversions, 0)) AS cpa,
    div0(COALESCE(conversion_value, 0), CASE WHEN rn = 1 THEN spend ELSE 0 END) AS roas
FROM joined
ORDER BY 1 DESC, 2, 3, 4, 5
