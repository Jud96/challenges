select city
from airbnb_search_details
group by city 
order by sum(array_length(string_to_array(amenities,','),1)) desc 
limit 1