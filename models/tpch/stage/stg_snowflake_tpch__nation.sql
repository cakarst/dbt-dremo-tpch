with source as ( 
    Select * from {{source("tpch", "nation")}}
)
Select * from source