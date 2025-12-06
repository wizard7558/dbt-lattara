select id
         , name
         , campaign_group_id
    from FIVETRAN_DATABASE.LINKEDIN_ADS.CAMPAIGN_HISTORY
    qualify row_number() over(partition by id order by last_modified_time desc) = 1