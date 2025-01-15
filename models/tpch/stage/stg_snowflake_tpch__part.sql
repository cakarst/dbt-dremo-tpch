with source as ( 
    Select * from {{source("tpch", "part")}}
)
Select * from source