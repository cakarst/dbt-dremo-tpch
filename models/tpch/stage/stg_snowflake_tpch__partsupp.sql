with source as ( 
    Select * from {{source("tpch", "partsupp")}}
)
Select * from source