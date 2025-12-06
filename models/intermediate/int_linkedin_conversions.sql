
with campaigns as (
select cast(day as date) as date
     , creative_id
     , conversion_id
     , sum(external_website_conversions) as external_website_conversions
     , sum(conversion_value_in_local_currency) as revenue
from FIVETRAN_DATABASE.LINKEDIN_ADS.AD_ANALYTICS_BY_CREATIVE_WITH_CONVERSION_BREAKDOWN
where external_website_conversions > 0
group by 1
       , 2
       , 3
),
latest_entities as (
select cr.id as creative_id
    , cr.name as creative_name
     , cr.campaign_id
     , camp.name as campaign_name
     , camp.id as campaign_id_final
     , camp.campaign_group_id
     , cg.name as campaign_group_name
     , cg.account_id
     , a.name as account_name
from {{ ref('v_stg_linkedin_creatives') }} cr
left join {{ ref('v_stg_linkedin_campaigns') }} camp
       on camp.id = cr.campaign_id
left join {{ ref('v_stg_linkedin_campaign_groups') }} cg
       on cg.id = camp.campaign_group_id
left join {{ ref('v_stg_linkedin_accounts') }}a
       on a.id = cg.account_id
)

select c.date
     , le.account_id as account_id
     , le.account_name as account
     , le.campaign_group_name as campaign_group
     , le.campaign_name as campaign
     , le.campaign_id_final as campaign_id
     , le.creative_name as creative
     , c.creative_id
     , conv.name as conversion
     , c.external_website_conversions
     , c.revenue
from campaigns c
left join latest_entities le
       on le.creative_id = c.creative_id
left join {{ ref('v_stg_linkedin_conversion_history') }} conv
       on c.conversion_id = conv.id
where le.campaign_id_final is not null