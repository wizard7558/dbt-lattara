with campaigns as (
select cast(day as date) as date
     , creative_id as creativeid
     , cost_in_usd as cost
     , sends + impressions as impressions
     , clicks as clicks
     , one_click_leads as leads
     , opens as opens
     , one_click_lead_form_opens as leadformopens
     , approximate_member_reach as reach
from FIVETRAN_DATABASE.LINKEDIN_ADS.AD_ANALYTICS_BY_CREATIVE
)

select c.date
     , a.name as account
     , cg.name as campaigngroup
     , camp.name as campaign
     , camp.id as campaignid
     , cr.name as creative
     , c.creativeid
     , coalesce(c.cost,0) as cost
     , coalesce(c.impressions,0) as impressions
     , coalesce(c.clicks,0) as clicks
     , coalesce(c.leads,0) as leads
     , c.opens
     , coalesce(c.leadformopens,0) as leadformopens
     , c.reach
from campaigns c
left join {{ ref('v_stg_linkedin_creatives') }} cr
       on cr.id = c.creativeid
left join {{ ref('v_stg_linkedin_campaigns') }} camp
       on camp.id = cr.campaign_id
left join {{ ref('v_stg_linkedin_campaign_groups') }} cg
       on cg.id = camp.campaign_group_id
left join {{ ref('v_stg_linkedin_accounts') }} a
       on a.id = cg.account_id
order by c.date desc