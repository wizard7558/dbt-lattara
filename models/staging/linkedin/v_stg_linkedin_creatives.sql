select id
         , campaign_id
    from FIVETRAN_DATABASE.LINKEDIN_ADS.CREATIVE_HISTORY
    qualify row_number() over(partition by id order by last_modified_at desc) = 1