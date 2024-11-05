
SELECT
	NATIONALITY,
	COUNT(DISTINCT UNIT_ID)
FROM
	AIRBNB_UNITS UNIT
	JOIN AIRBNB_HOSTS H ON UNIT.HOST_ID = H.HOST_ID
WHERE
	AGE < 30
	AND UNIT_TYPE = 'Apartment'
GROUP BY
	NATIONALITY
