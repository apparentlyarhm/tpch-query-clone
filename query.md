```sql
select 
n_name, 
sum(l_extendedprice * (1 - l_discount)) as revenue 
from 
customer c, 
orders o, 
lineitem l, 
supplier s, 
nation n, 
region r 
where 
c_custkey = o_custkey 
and l_orderkey = o_orderkey 
and l_suppkey = s_suppkey 
and c_nationkey = s_nationkey 
and s_nationkey = n_nationkey 
and n_regionkey = r_regionkey 
and r_name = 'ASIA' 
and o_orderdate >= '1994-01-01' 
and o_orderdate < '1995-01-01' 
group by 
n_name 
order by 
revenue desc 
```