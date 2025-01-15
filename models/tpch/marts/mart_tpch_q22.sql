

with 

customer
 as 
(select * from {{ref("stg_snowflake_tpch__customer")}}), 

orders 
as 
(select * from {{ref("stg_snowflake_tpch__orders")}}),

custsale 
as 
(
		select
      SUBSTRING(c_phone, 1, 2) AS cntrycode,
			c_acctbal
		from
			customer
		where
          SUBSTRING(c_phone, 1, 2) IN
				('13', '31', '23', '29', '30', '18', '17')
			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					customer
				where
					c_acctbal > 0.00
                    AND SUBSTRING(c_phone, 1, 2) IN
						('13', '31', '23', '29', '30', '18', '17')
			)
			and not exists (
				select
					*
				from
					orders
				where
					o_custkey = c_custkey
			)
	) 




select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	custsale
group by
	cntrycode
order by
	cntrycode