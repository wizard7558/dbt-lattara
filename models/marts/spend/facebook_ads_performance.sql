{{config(materialized = "table")}} 

SELECT CAST(f.DATE AS DATE) AS DATE
     , f.ACCOUNT
     , f.CAMPAIGN
     , f.ADSET
     , f.AD
     , f.AD_ID
     , f.CONVERSIONNAME
     , f.SPEND_RAW/f.DIVIDEND AS SPEND
     , f.IMPRESSIONS_RAW/f.DIVIDEND AS IMPRESSIONS
     , f.CLICKS_RAW/f.DIVIDEND AS CLICKS
     , f.ALLCONV
     , CASE WHEN oc.optimization_conversion IS NOT NULL THEN TRUE ELSE FALSE END AS IS_OPTIMIZATION_CONVERSION
FROM {{ ref('int_facebook_conversions') }} f
LEFT JOIN {{ ref('optimization_conversions') }} oc
    ON f.ACCOUNT = oc.account
    AND f.CONVERSIONNAME = oc.optimization_conversion
    AND oc.platform = 'facebook'
ORDER BY DATE DESC
     , CAMPAIGN ASC
     , ADSET ASC
     , AD ASC
     , CONVERSIONNAME ASC
