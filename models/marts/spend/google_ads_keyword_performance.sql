{{ config(materialized = "table") }}

-- Google Ads Keyword Performance
-- Combines keyword spend with conversions WITHOUT duplication
-- Uses ROW_NUMBER to assign spend only to first row per keyword/date
-- Only captures Search campaigns (other campaign types have no keywords)

WITH keyword_spend AS (
    SELECT * FROM {{ ref('int_google_keyword_spend') }}
),

keyword_conversions AS (
    SELECT * FROM {{ ref('int_google_keyword_conversions') }}
),

optimization_conv AS (
    SELECT * FROM {{ ref('optimization_conversions') }}
    WHERE platform = 'google'
),

-- Join spend and conversions, assign row number to prevent duplication
joined AS (
    SELECT 
        COALESCE(ks.date, kc.date) AS date,
        COALESCE(ks.account, kc.account) AS account,
        COALESCE(ks.customer_id, kc.customer_id) AS customer_id,
        COALESCE(ks.campaign, kc.campaign) AS campaign,
        COALESCE(ks.campaign_id, kc.campaign_id) AS campaign_id,
        COALESCE(ks.adgroup, kc.adgroup) AS adgroup,
        COALESCE(ks.ad_group_id, kc.ad_group_id) AS ad_group_id,
        COALESCE(ks.keyword_id, kc.keyword_id) AS keyword_id,
        COALESCE(ks.keyword, kc.keyword) AS keyword,
        COALESCE(ks.match_type, kc.match_type) AS match_type,
        kc.conversion_action_name,
        ks.spend,
        ks.impressions,
        ks.clicks,
        kc.conversions,
        kc.conversion_value,
        -- Assign spend only to first row per keyword/date to prevent duplication
        ROW_NUMBER() OVER (
            PARTITION BY 
                COALESCE(ks.date, kc.date),
                COALESCE(ks.customer_id, kc.customer_id),
                COALESCE(ks.keyword_id, kc.keyword_id)
            ORDER BY kc.conversion_action_name NULLS LAST
        ) AS rn
    FROM keyword_spend ks
    FULL OUTER JOIN keyword_conversions kc
        ON ks.date = kc.date
        AND ks.customer_id = kc.customer_id
        AND ks.keyword_id = kc.keyword_id
)

SELECT 
    j.date,
    j.account,
    j.customer_id,
    j.campaign,
    j.campaign_id,
    j.adgroup,
    j.ad_group_id,
    j.keyword_id,
    j.keyword,
    j.match_type,
    j.conversion_action_name,
    -- Spend metrics only on first row to prevent duplication
    CASE WHEN j.rn = 1 THEN j.spend ELSE 0 END AS spend,
    CASE WHEN j.rn = 1 THEN j.impressions ELSE 0 END AS impressions,
    CASE WHEN j.rn = 1 THEN j.clicks ELSE 0 END AS clicks,
    -- Conversion metrics on all rows
    COALESCE(j.conversions, 0) AS conversions,
    COALESCE(j.conversion_value, 0) AS conversion_value,
    -- Calculated metrics (use raw values, aggregation handles dedup)
    div0(CASE WHEN j.rn = 1 THEN j.spend ELSE 0 END, CASE WHEN j.rn = 1 THEN j.clicks ELSE 0 END) AS cpc,
    div0(CASE WHEN j.rn = 1 THEN j.clicks ELSE 0 END, CASE WHEN j.rn = 1 THEN j.impressions ELSE 0 END) * 100 AS ctr,
    div0(CASE WHEN j.rn = 1 THEN j.spend ELSE 0 END, COALESCE(j.conversions, 0)) AS cpa,
    div0(COALESCE(j.conversion_value, 0), CASE WHEN j.rn = 1 THEN j.spend ELSE 0 END) AS roas,
    CASE WHEN oc.optimization_conversion IS NOT NULL THEN TRUE ELSE FALSE END AS is_optimization_conversion
FROM joined j
LEFT JOIN optimization_conv oc
    ON j.account = oc.account
    AND j.conversion_action_name = oc.optimization_conversion
ORDER BY 1 DESC, 2, 3, 4, 5, 6, 7
