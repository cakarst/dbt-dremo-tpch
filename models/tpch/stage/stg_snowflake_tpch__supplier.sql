with source as ( 
    Select * from {{source("tpch", "supplier")}}
)
Select * from source