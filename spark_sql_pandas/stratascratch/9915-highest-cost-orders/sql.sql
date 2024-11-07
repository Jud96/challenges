select c.first_name,
sum(total_order_cost) as total_order_cost,
order_date
from
customers c  join orders o on c.id = o.cust_id
where order_date between '2019-02-01' and '2019-05-01'
group by c.id,c.first_name,order_date
order by 2 desc
limit 1