with source as ( 
    Select * from {{source("tpch", "region")}}
)
Select * from source