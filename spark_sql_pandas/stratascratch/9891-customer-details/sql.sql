select 
first_name,
last_name,
city,
order_details
from customers c left join orders o on c.id = o.cust_id
order by first_name asc