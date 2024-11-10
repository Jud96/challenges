select company ,continent from forbes_global_2010_2014 
where sector = 'Financials'
and profits = (select max(profits) from  forbes_global_2010_2014) 