{{config(materialized = "table")}} 

select date
     , 'Meta' as platform
     , account
     , campaign
     , sum(spend) as spend
     , sum(impressions) as impressions
     , sum(clicks) as clicks
     , sum(revenue) as revenue
from {{ ref('facebook_ads_performance') }}
group by 1,2,3,4

union all

select date
     , 'Google' as platform
     , account
     , campaign
     , sum(spend) as spend
     , sum(impressions) as impressions
     , sum(clicks) as clicks
     , sum(revenue) as revenue
from {{ ref('google_ads_performance') }}
group by 1,2,3,4

union all

select date
     , 'TikTok' as platform
     , account
     , campaign
     , sum(spend) as spend
     , sum(impressions) as impressions
     , sum(clicks) as clicks
     , sum(revenue) as revenue
from {{ ref('tiktok_ads_performance') }}
group by 1,2,3,4

union all

select date
     , 'LinkedIn' as platform
     , account
     , campaign
     , sum(spend) as spend
     , sum(impressions) as impressions
     , sum(clicks) as clicks
     , sum(revenue) as revenue
from {{ ref('linkedin_ads_performance') }}
where spend > 0 
group by 1,2,3,4

order by 1 desc, 2 asc, 5 desc