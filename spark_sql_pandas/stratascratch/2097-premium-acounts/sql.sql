SELECT a.entry_date,
count(a.account_id) premium_paid_accounts, 
count(b.account_id) premium_paid_accounts_after_7d 
FROM premium_accounts_by_day a 
LEFT JOIN premium_accounts_by_day b ON a.account_id = b.account_id 
AND (b.entry_date - a.entry_date) = 7 AND b.final_price > 0 
WHERE a.final_price > 0
GROUP BY a.entry_date
ORDER BY a.entry_date
LIMIT 7