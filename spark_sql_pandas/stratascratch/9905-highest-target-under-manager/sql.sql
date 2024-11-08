SELECT
	FIRST_NAME,
	TARGET
FROM
	SALESFORCE_EMPLOYEES
WHERE
	MANAGER_ID = 13
	AND TARGET = (
		SELECT
			MAX(TARGET)
		FROM
			SALESFORCE_EMPLOYEES
		WHERE
			MANAGER_ID = 13
	)