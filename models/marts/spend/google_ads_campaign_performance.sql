{{ config(materialized = "table") }}

-- Google Ads Campaign Performance
-- Combines campaign spend with conversions WITHOUT duplication
-- Uses ROW_NUMBER to assign spend only to first row per campaign/date
-- Captures ALL spend (including Performance Max, Demand Gen, Video)

WITH campaign_spend AS (
    SELECT * FROM {{ ref('int_google_campaign_spend') }}
),

campaign_conversions AS (
    SELECT * FROM {{ ref('int_google_campaign_conversions') }}
),

joined AS (
    SELECT 
        COALESCE(cs.date, cc.date) AS date,
        COALESCE(cs.account, cc.account) AS account,
        COALESCE(cs.customer_id, cc.customer_id) AS customer_id,
        COALESCE(cs.campaign, cc.campaign) AS campaign,
        COALESCE(cs.campaign_id, cc.campaign_id) AS campaign_id,
        COALESCE(cs.campaign_type, cc.campaign_type) AS campaign_type,
        cc.conversion_action_name,
        cs.spend,
        cs.impressions,
        cs.clicks,
        cc.conversions,
        cc.conversion_value,
        ROW_NUMBER() OVER (
            PARTITION BY 
                COALESCE(cs.date, cc.date),
                COALESCE(cs.customer_id, cc.customer_id),
                COALESCE(cs.campaign_id, cc.campaign_id)
            ORDER BY cc.conversion_action_name NULLS LAST
        ) AS rn
    FROM campaign_spend cs
    FULL OUTER JOIN campaign_conversions cc
        ON cs.date = cc.date
        AND cs.customer_id = cc.customer_id
        AND cs.campaign_id = cc.campaign_id
)

SELECT 
    date,
    account,
    customer_id,
    campaign,
    campaign_id,
    campaign_type,
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
ORDER BY 1 DESC, 2, 3, 4
