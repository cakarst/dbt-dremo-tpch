with source as ( 
    Select * from {{source("tpch", "lineitem")}}
)
Select * from source