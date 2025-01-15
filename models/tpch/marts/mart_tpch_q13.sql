with 

customer as 
(select * from {{ref("stg_snowflake_tpch__customer")}}),

orders as
(select * from {{ref("stg_snowflake_tpch__orders")}}),

c_orders as
(select
			c_custkey,
			count(o_orderkey) as c_count
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%special%requests%'
		group by
			c_custkey)


select
	c_count,
	count(*) as custdist
from
	 c_orders 
group by
	c_count
order by
	custdist desc,
	c_count desc