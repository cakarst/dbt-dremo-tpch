
with 

partsupp
 as 
(select * from {{ref("stg_snowflake_tpch__partsupp")}}), 

part
 as 
(select * from {{ref("stg_snowflake_tpch__part")}}), 

supplier
 as 
(select * from {{ref("stg_snowflake_tpch__supplier")}}), 

customer_complaints
as
(
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)

select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#45'
	and p_type not like 'MEDIUM POLISHED%'
	and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
	and ps_suppkey not in (select * from customer_complaints)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size