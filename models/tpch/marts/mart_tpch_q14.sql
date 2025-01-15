

with 

lineitem
 as 
(select * from {{ref("stg_snowflake_tpch__lineitem")}}),

part as
(select * from {{ref("stg_snowflake_tpch__part")}})


select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1995-09-01'
	AND l_shipdate < DATEADD(month, 1, '1995-09-01')