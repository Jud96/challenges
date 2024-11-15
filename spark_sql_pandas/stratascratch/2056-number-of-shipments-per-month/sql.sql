select to_char(shipment_date,'YYYY-MM'), count(shipment_id) from amazon_shipment
group by 1