
with 

customer as 
(select * from {{ref("stg_snowflake_tpch__customer")}}),

orders as
(select * from {{ref("stg_snowflake_tpch__orders")}}),

lineitem
 as 
(select * from {{ref("stg_snowflake_tpch__lineitem")}}),

supplier as
(select * from {{ref("stg_snowflake_tpch__supplier")}}),

nation as
(select * from {{ref("stg_snowflake_tpch__nation")}}),

region as
(select * from {{ref("stg_snowflake_tpch__region")}}),

-- could become intermediate view.
shipping_filtered as
(
		select * from {{ref("shipping_volume_year")}}
		where
			 (
				(supp_nation= 'FRANCE' and cust_nation= 'GERMANY')
				or (supp_nation = 'GERMANY' and cust_nation = 'FRANCE')
			)
			and l_year =1995
	)


select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	shipping_filtered
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year