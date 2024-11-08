SELECT DISTINCT
	ON (HOST_ID, GUEST_ID) HOST_ID,
	GUEST_ID
FROM
	AIRBNB_HOSTS
	JOIN AIRBNB_GUESTS USING (NATIONALITY, GENDER);