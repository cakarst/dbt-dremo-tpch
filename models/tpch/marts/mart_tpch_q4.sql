
with 

orders as
(select * from {{ref("stg_snowflake_tpch__orders")}}),

lineitem
 as 
(select * from {{ref("stg_snowflake_tpch__lineitem")}})


select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= date '1993-07-01'
	AND o_orderdate < DATEADD(month, 3, '1993-07-01')
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority