select id
         , name
    from FIVETRAN_DATABASE.LINKEDIN_ADS.ACCOUNT_HISTORY
    qualify row_number() over(partition by id order by last_modified_time desc) = 1