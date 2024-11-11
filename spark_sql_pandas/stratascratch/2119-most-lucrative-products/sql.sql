select product_id,sum(units_sold*cost_in_dollars) from online_orders
where date between '2022-01-01' and '2022-06-30'
group by product_id
order by 2 desc
limit 5
