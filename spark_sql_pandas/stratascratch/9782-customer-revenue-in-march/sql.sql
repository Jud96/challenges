select cust_id,sum(total_order_cost)
from orders
where order_date between '2019-03-01' and '2019-03-31'
group by cust_id
order by 2 desc;