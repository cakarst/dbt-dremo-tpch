with source as ( 
    Select * from {{source("tpch", "orders")}}
)
Select * from source