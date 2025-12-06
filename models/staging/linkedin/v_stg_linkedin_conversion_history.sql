select id
         , name
    from FIVETRAN_DATABASE.LINKEDIN_ADS.CONVERSION_HISTORY
    qualify row_number() over(partition by id order by last_modified_at desc) = 1