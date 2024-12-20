WITH
	APPROVED_DISTINCT_VIDEO AS (
		SELECT
			COUNT(DISTINCT VIDEO_ID) AS NUM,
			USER_FIRSTNAME,
			USER_LASTNAME
		FROM
			USER_FLAGS
		WHERE
			FLAG_ID IS NOT NULL
		GROUP BY
			USER_FIRSTNAME,
			USER_LASTNAME
	)
SELECT
	CONCAT(USER_FIRSTNAME, ' ', USER_LASTNAME) AS FULL_NAME
FROM
	APPROVED_DISTINCT_VIDEO
WHERE
	NUM = (
		SELECT
			MAX(NUM)
		FROM
			APPROVED_DISTINCT_VIDEO
	)