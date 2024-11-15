# Import your libraries
import pandas as pd

# Start writing code
amazon_shipment = pd.read_csv('amazon_shipment.csv')
amazon_shipment['shipment_date'] = pd.to_datetime(amazon_shipment['shipment_date'])
amazon_shipment['year_month'] = amazon_shipment['shipment_date'].dt.strftime('%Y-%m')
amazon_shipment.groupby('year_month').size().reset_index(name='count')