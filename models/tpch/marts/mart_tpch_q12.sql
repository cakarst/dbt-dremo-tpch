
with 

lineitem
 as 
(select * from {{ref("stg_snowflake_tpch__lineitem")}}),

orders as
(select * from {{ref("stg_snowflake_tpch__orders")}})


select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('MAIL', 'SHIP')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '1994-01-01'
	AND o_orderdate < DATEADD(year, 1, '1994-01-01')
group by
	l_shipmode
order by
	l_shipmode
