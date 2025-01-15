with source as ( 
    Select * from {{source("tpch", "customer")}}
)
Select * from source