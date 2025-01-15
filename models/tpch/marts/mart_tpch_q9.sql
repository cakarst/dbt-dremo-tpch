
with 

part as 
(select * from {{ref("stg_snowflake_tpch__part")}}),

supplier as
(select * from {{ref("stg_snowflake_tpch__supplier")}}),

lineitem
 as 
(select * from {{ref("stg_snowflake_tpch__lineitem")}}),

partsupp as
(select * from {{ref("stg_snowflake_tpch__partsupp")}}),

orders as
(select * from {{ref("stg_snowflake_tpch__orders")}}),

nation as
(select * from {{ref("stg_snowflake_tpch__nation")}}),

-- could be generalized to rmeove the like and then added to intermediate
profit as 
(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%green%'
	) 


select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	 profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc
