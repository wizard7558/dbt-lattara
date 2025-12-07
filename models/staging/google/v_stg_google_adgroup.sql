    SELECT id as ad_group_id
         , campaign_id
         , name as ad_group_name
    FROM (
        SELECT id
             , campaign_id
             , name
             , ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM FIVETRAN_DATABASE.GOOGLE_ADS_FM.AD_GROUP_HISTORY
    )
    WHERE rn = 1