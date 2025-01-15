-- from Q7
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


shipping as
(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
	)

Select * from shipping