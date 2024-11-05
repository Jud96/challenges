select
id_guest,
sum_n_messages,
dense_rank() over(order by sum_n_messages desc) ranking
from 
(
select 
id_guest,
sum(n_messages)  as sum_n_messages
from airbnb_contacts
group by id_guest)t