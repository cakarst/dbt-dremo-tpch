with 

part as 
(select * from {{ref("stg_snowflake_tpch__part")}}),


supplier as
(select * from {{ref("stg_snowflake_tpch__supplier")}}),

partsupp as 
(select * from {{ref("stg_snowflake_tpch__partsupp")}}),

nation as
(select * from {{ref("stg_snowflake_tpch__nation")}}),

region as
(select * from {{ref("stg_snowflake_tpch__region")}})


select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	part,
	supplier,
	partsupp,
	nation,
	region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 15
	and p_type like '%BRASS'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'EUROPE'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			partsupp,
			supplier,
			nation,
			region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'EUROPE'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey LIMIT 100