
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
(select * from {{ref("stg_snowflake_tpch__region")}})



select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and o_orderdate >= date '1994-01-01'
	AND o_orderdate < DATEADD(year, 1, '1994-01-01')
group by
	n_name
order by
	revenue desc