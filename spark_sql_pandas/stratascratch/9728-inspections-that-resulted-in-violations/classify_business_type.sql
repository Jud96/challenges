select 
distinct on(business_name)
business_name,
case 
when lower(business_name) like '%restaurant%' then 'restaurant'
when lower(business_name)  like '%cafe%' then 'cafe'
when lower(business_name)  like '%café%' then 'cafe'
when lower(business_name)  like '%coffee%' then 'cafe'
when lower(business_name)  like '%school%' then 'school'
else 'other' end 
from sf_restaurant_health_violations;