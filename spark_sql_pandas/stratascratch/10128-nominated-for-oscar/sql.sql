SELECT 
    count(distinct movie) AS n_movies_by_abi
FROM oscar_nominees
WHERE 
    nominee = 'Abigail Breslin'