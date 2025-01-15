
with 

supplier
 as 
(select * from {{ref("stg_snowflake_tpch__supplier")}}), 

nation
 as 
(select * from {{ref("stg_snowflake_tpch__nation")}}),

lineitem 
as 
(select * from {{ref("stg_snowflake_tpch__lineitem")}}),

part 
as 
(select * from {{ref("stg_snowflake_tpch__part")}}),

partsupp 
as 
(select * from {{ref("stg_snowflake_tpch__partsupp")}}),

supplier_set 
as 
(
		select
			ps_suppkey
		from
			partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like 'forest%'
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= date '1994-01-01'
                	AND l_shipdate < DATEADD(year, 1, '1994-01-01')
			)
	)


select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in (select * from supplier_set)
	and s_nationkey = n_nationkey
	and n_name = 'CANADA'
order by
	s_name