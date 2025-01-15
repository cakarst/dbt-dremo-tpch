-- tests fail if there are any records returned! 


select * FROM  {{ref("stg_snowflake_tpch__lineitem")}} where L_discount >.50  --no item should have >50% discount