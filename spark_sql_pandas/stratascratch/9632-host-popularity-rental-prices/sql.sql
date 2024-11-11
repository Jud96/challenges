WITH hosts AS (
  SELECT DISTINCT 
    CONCAT(price, room_type, host_since, zipcode, number_of_reviews) AS host_id,
    number_of_reviews,
    price
  FROM airbnb_host_searches
)
select 
    popularity as host_pop_rating,
    min(price) as min_price,
    AVG(price) as avg_price,
    max(price) as max_price
from 
(select
number_of_reviews,
price, 
case
when number_of_reviews = 0 then 'New'
when number_of_reviews>= 1  and number_of_reviews <= 5  then 'Rising'
when number_of_reviews>= 6  and  number_of_reviews <= 15  then 'Trending Up'
when number_of_reviews>= 16  and number_of_reviews <= 40  then 'Popular'
else 'Hot' end popularity
from hosts) t
group by t.popularity