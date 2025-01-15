
with lineitem as 

(select * from {{ref("stg_snowflake_tpch__lineitem")}})


select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1994-01-01'
	AND l_shipdate < DATEADD(year, 1, '1994-01-01')
	and l_discount between .06 - 0.01 and .06 + 0.01
	and l_quantity < 24
